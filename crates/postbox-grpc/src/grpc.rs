//! gRPC front end over `postbox-core`, built on `tonic`.
//!
//! The schema lives in `proto/postbox.proto`. We compile it at build time
//! using `tonic-build`. Proto types are re-exported under
//! `postbox_grpc::proto` so the service implementation can refer to them.
//!
//! Like the HTTP layer, every call here is a thin shim over
//! `postbox-core`; no business logic is duplicated.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use postbox_core::{
    validate_agent_id, FailureKind, FanoutRequest, MailboxStore, PoisonReason, SendRequest,
};
use tonic::{transport::Server, Request, Response, Status};
use ulid::Ulid;

pub mod proto {
    tonic::include_proto!("postbox.v1");
}

use proto::{
    postbox_service_server::{PostboxService, PostboxServiceServer},
    ClaimRequest as GrpcClaimRequest, ClaimResponse as GrpcClaimResponse, ClaimResponseMessage,
    CommitRequest as GrpcCommitRequest, CommitResponse as GrpcCommitResponse,
    DeadLetter as GrpcDeadLetter, EnsureMailboxRequest as GrpcEnsureMailboxRequest,
    EnsureMailboxResponse as GrpcEnsureMailboxResponse,
    FanoutSendRequest as GrpcFanoutSendRequest, FanoutSendResponse as GrpcFanoutSendResponse,
    GetMailboxRequest as GrpcGetMailboxRequest, GetMailboxResponse as GrpcGetMailboxResponse,
    GetMailboxStatsRequest as GrpcGetMailboxStatsRequest,
    GetMailboxStatsResponse as GrpcGetMailboxStatsResponse,
    Headers as GrpcHeaders, IsCommittedRequest as GrpcIsCommittedRequest,
    IsCommittedResponse as GrpcIsCommittedResponse,
    ListDeadLettersRequest as GrpcListDeadLettersRequest,
    ListDeadLettersResponse as GrpcListDeadLettersResponse,
    ListMailboxesRequest as GrpcListMailboxesRequest,
    ListMailboxesResponse as GrpcListMailboxesResponse, Mailbox as GrpcMailbox,
    MailboxStats as GrpcMailboxStats, Message as GrpcMessage,
    PeekRequest as GrpcPeekRequest, PeekResponse as GrpcPeekResponse,
    PurgeDeadLettersRequest as GrpcPurgeDeadLettersRequest,
    PurgeDeadLettersResponse as GrpcPurgeDeadLettersResponse,
    RejectValidationRequest as GrpcRejectValidationRequest,
    RejectValidationResponse as GrpcRejectValidationResponse,
    ReleaseRequest as GrpcReleaseRequest, ReleaseResponse as GrpcReleaseResponse,
    ReplayDeadLetterRequest as GrpcReplayDeadLetterRequest,
    ReplayDeadLetterResponse as GrpcReplayDeadLetterResponse,
    SendMessageRequest as GrpcSendMessageRequest, SendMessageResponse as GrpcSendMessageResponse,
    StreamClaimRequest as GrpcStreamClaimRequest,
};

/// gRPC adapter over a [`MailboxStore`].
pub struct PostboxGrpc {
    store: Arc<dyn MailboxStore>,
}

impl PostboxGrpc {
    pub fn new(store: Arc<dyn MailboxStore>) -> Self {
        Self { store }
    }

    pub fn into_server(self) -> PostboxServiceServer<Self> {
        PostboxServiceServer::new(self)
    }
}

fn core_err_to_grpc(e: postbox_core::PostboxError) -> Status {
    use postbox_core::PostboxError::*;
    let code = match &e {
        MailboxNotFound { .. } | MessageNotFound(_) => tonic::Code::NotFound,
        EmptyCheckpointToken(_) | InvalidAgentId(_) | InvalidHeaders(_) | PayloadTooLarge { .. } => {
            tonic::Code::InvalidArgument
        }
        MailboxFull { .. } => tonic::Code::ResourceExhausted,
        AlreadyCommitted(_) => tonic::Code::AlreadyExists,
        MessageNotClaimable { .. } | MessageNotClaimed(_) | NotClaimedByYou { .. } => {
            tonic::Code::FailedPrecondition
        }
        Storage(_) => tonic::Code::Internal,
    };
    Status::new(code, e.to_string())
}

fn st_to_ms(t: std::time::SystemTime) -> i64 {
    postbox_core::types::system_time_to_millis(t)
}

fn message_to_grpc(m: postbox_core::Message) -> GrpcMessage {
    GrpcMessage {
        message_id: m.message_id.to_string(),
        mailbox_id: m.mailbox_id,
        sender_id: m.sender_id,
        payload: m.payload.to_vec(),
        headers: Some(GrpcHeaders {
            entries: m.headers.into_iter().collect(),
        }),
        priority: m.priority,
        created_at_ms: st_to_ms(m.created_at),
        visible_at_ms: st_to_ms(m.visible_at),
        status: match m.status {
            postbox_core::MessageStatus::Pending => 0,
            postbox_core::MessageStatus::Claimed => 1,
            postbox_core::MessageStatus::Committed => 2,
            postbox_core::MessageStatus::DeadLettered => 3,
        } as i32,
        attempt_count: m.attempt_count,
        lease_expires_at_ms: m.lease_expires_at.map(st_to_ms).unwrap_or(0),
        claimed_by: m.claimed_by.unwrap_or_default(),
        committed_at_ms: m.committed_at.map(st_to_ms).unwrap_or(0),
        checkpoint_token: m.checkpoint_token.unwrap_or_default(),
        expires_at_ms: m.expires_at.map(st_to_ms).unwrap_or(0),
    }
}

fn mailbox_to_grpc(m: postbox_core::Mailbox) -> GrpcMailbox {
    GrpcMailbox {
        agent_id: m.agent_id,
        capacity: m.capacity as u64,
        ordering_mode: match m.ordering_mode {
            postbox_core::OrderingMode::Fifo => "fifo".into(),
            postbox_core::OrderingMode::Unordered => "unordered".into(),
            postbox_core::OrderingMode::Priority => "priority".into(),
        },
        max_attempts: m.max_attempts,
        lease_duration_ms: m.lease_duration.as_millis() as u64,
        max_payload_bytes: m.max_payload_bytes as u64,
        dlq_retention_ms: m.dlq_retention.map(|d| d.as_millis() as u64).unwrap_or(0),
    }
}

fn parse_ulid(s: &str) -> Result<Ulid, Status> {
    Ulid::from_string(s).map_err(|_| Status::invalid_argument(format!("bad ulid: {s}")))
}

#[tonic::async_trait]
impl PostboxService for PostboxGrpc {
    type StreamClaimStream =
        Pin<Box<dyn Stream<Item = Result<GrpcClaimResponse, Status>> + Send>>;

    async fn ensure_mailbox(
        &self,
        request: Request<GrpcEnsureMailboxRequest>,
    ) -> Result<Response<GrpcEnsureMailboxResponse>, Status> {
        let req = request.into_inner();
        validate_agent_id(&req.agent_id).map_err(core_err_to_grpc)?;
        let ordering_mode = match req.ordering_mode.as_str() {
            "unordered" => postbox_core::OrderingMode::Unordered,
            "priority" => postbox_core::OrderingMode::Priority,
            _ => postbox_core::OrderingMode::Fifo,
        };
        let dlq_retention = if req.dlq_retention_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(req.dlq_retention_ms))
        };
        let m = self
            .store
            .ensure_mailbox(postbox_core::MailboxConfig {
                agent_id: req.agent_id.clone(),
                capacity: req.capacity as usize,
                ordering_mode,
                max_attempts: req.max_attempts,
                lease_duration: Duration::from_millis(req.lease_duration_ms),
                max_payload_bytes: req.max_payload_bytes as usize,
                dlq_retention,
            })
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcEnsureMailboxResponse {
            mailbox: Some(mailbox_to_grpc(m)),
        }))
    }

    async fn get_mailbox(
        &self,
        request: Request<GrpcGetMailboxRequest>,
    ) -> Result<Response<GrpcGetMailboxResponse>, Status> {
        let req = request.into_inner();
        validate_agent_id(&req.agent_id).map_err(core_err_to_grpc)?;
        let m = self
            .store
            .get_mailbox(&req.agent_id)
            .await
            .map_err(core_err_to_grpc)?
            .ok_or_else(|| {
                Status::not_found(format!("mailbox not found: {}", req.agent_id))
            })?;
        Ok(Response::new(GrpcGetMailboxResponse {
            mailbox: Some(mailbox_to_grpc(m)),
        }))
    }

    async fn send_message(
        &self,
        request: Request<GrpcSendMessageRequest>,
    ) -> Result<Response<GrpcSendMessageResponse>, Status> {
        let req = request.into_inner();
        validate_agent_id(&req.to_agent).map_err(core_err_to_grpc)?;
        validate_agent_id(&req.from_agent).map_err(core_err_to_grpc)?;
        let headers: BTreeMap<String, String> = req
            .headers
            .map(|h| h.entries.into_iter().collect())
            .unwrap_or_default();
        let m = self
            .store
            .send(SendRequest {
                target_mailbox: req.to_agent,
                sender_id: req.from_agent,
                payload: Bytes::from(req.payload),
                headers,
                priority: req.priority,
                delay: if req.delay_ms == 0 {
                    None
                } else {
                    Some(Duration::from_millis(req.delay_ms))
                },
                ttl: if req.ttl_ms == 0 {
                    None
                } else {
                    Some(Duration::from_millis(req.ttl_ms))
                },
            })
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcSendMessageResponse {
            message: Some(message_to_grpc(m)),
        }))
    }

    async fn peek(
        &self,
        request: Request<GrpcPeekRequest>,
    ) -> Result<Response<GrpcPeekResponse>, Status> {
        let req = request.into_inner();
        validate_agent_id(&req.agent_id).map_err(core_err_to_grpc)?;
        let max = if req.max == 0 { 100 } else { req.max as usize };
        let messages = self
            .store
            .peek(&req.agent_id, max)
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcPeekResponse {
            messages: messages.into_iter().map(message_to_grpc).collect(),
        }))
    }

    async fn claim(
        &self,
        request: Request<GrpcClaimRequest>,
    ) -> Result<Response<GrpcClaimResponse>, Status> {
        let req = request.into_inner();
        validate_agent_id(&req.agent_id).map_err(core_err_to_grpc)?;
        validate_agent_id(&req.claimer_id).map_err(core_err_to_grpc)?;
        let lease = if req.lease_duration_ms == 0 {
            Duration::from_secs(60)
        } else {
            Duration::from_millis(req.lease_duration_ms)
        };
        let claim = self
            .store
            .claim(&req.agent_id, &req.claimer_id, lease)
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcClaimResponse {
            claim: claim.map(|c| ClaimResponseMessage {
                message: Some(message_to_grpc(c.message)),
                lease_expires_at_ms: st_to_ms(c.lease_expires_at),
            }),
        }))
    }

    async fn commit(
        &self,
        request: Request<GrpcCommitRequest>,
    ) -> Result<Response<GrpcCommitResponse>, Status> {
        let req = request.into_inner();
        let mid = parse_ulid(&req.message_id)?;
        validate_agent_id(&req.claimer_id).map_err(core_err_to_grpc)?;
        self.store
            .commit(mid, &req.claimer_id, &req.checkpoint_token)
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcCommitResponse {}))
    }

    async fn release(
        &self,
        request: Request<GrpcReleaseRequest>,
    ) -> Result<Response<GrpcReleaseResponse>, Status> {
        let req = request.into_inner();
        let mid = parse_ulid(&req.message_id)?;
        validate_agent_id(&req.claimer_id).map_err(core_err_to_grpc)?;
        let kind = match req.kind.as_str() {
            "transient" => FailureKind::Transient,
            "permanent" => FailureKind::Permanent,
            other => {
                return Err(Status::invalid_argument(format!("unknown kind: {other}")))
            }
        };
        self.store
            .release(mid, &req.claimer_id, kind, Some(&req.note))
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcReleaseResponse {}))
    }

    async fn reject_validation(
        &self,
        request: Request<GrpcRejectValidationRequest>,
    ) -> Result<Response<GrpcRejectValidationResponse>, Status> {
        let req = request.into_inner();
        let mid = parse_ulid(&req.message_id)?;
        self.store
            .reject_validation(mid, &req.note)
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcRejectValidationResponse {}))
    }

    async fn is_committed(
        &self,
        request: Request<GrpcIsCommittedRequest>,
    ) -> Result<Response<GrpcIsCommittedResponse>, Status> {
        let req = request.into_inner();
        validate_agent_id(&req.agent_id).map_err(core_err_to_grpc)?;
        let mid = parse_ulid(&req.message_id)?;
        let v = self
            .store
            .is_committed(&req.agent_id, mid)
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcIsCommittedResponse { committed: v }))
    }

    async fn list_dead_letters(
        &self,
        request: Request<GrpcListDeadLettersRequest>,
    ) -> Result<Response<GrpcListDeadLettersResponse>, Status> {
        let req = request.into_inner();
        validate_agent_id(&req.agent_id).map_err(core_err_to_grpc)?;
        let filter = match req.reason.as_str() {
            "" => None,
            "max_attempts_exceeded" => Some(PoisonReason::MaxAttemptsExceeded),
            "permanent_failure" => Some(PoisonReason::PermanentFailure),
            "validation_failed" => Some(PoisonReason::ValidationFailed),
            "expired" => Some(PoisonReason::Expired),
            other => {
                return Err(Status::invalid_argument(format!("unknown reason: {other}")))
            }
        };
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let dlq = self
            .store
            .list_dead_letters(&req.agent_id, filter, limit)
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcListDeadLettersResponse {
            dead_letters: dlq
                .into_iter()
                .map(|d| GrpcDeadLetter {
                    message_id: d.message_id.to_string(),
                    mailbox_id: d.mailbox_id,
                    sender_id: d.sender_id,
                    payload: d.payload.to_vec(),
                    headers: Some(GrpcHeaders {
                        entries: d.headers.into_iter().collect(),
                    }),
                    priority: d.priority,
                    created_at_ms: st_to_ms(d.created_at),
                    attempt_count: d.attempt_count,
                    poison_reason: match d.poison_reason {
                        PoisonReason::MaxAttemptsExceeded => "max_attempts_exceeded".into(),
                        PoisonReason::PermanentFailure => "permanent_failure".into(),
                        PoisonReason::ValidationFailed => "validation_failed".into(),
                        PoisonReason::Expired => "expired".into(),
                    },
                    dead_lettered_at_ms: st_to_ms(d.dead_lettered_at),
                })
                .collect(),
        }))
    }

    async fn replay_dead_letter(
        &self,
        request: Request<GrpcReplayDeadLetterRequest>,
    ) -> Result<Response<GrpcReplayDeadLetterResponse>, Status> {
        let req = request.into_inner();
        let mid = parse_ulid(&req.message_id)?;
        validate_agent_id(&req.replayed_by).map_err(core_err_to_grpc)?;
        let target = if req.target_mailbox.is_empty() {
            None
        } else {
            Some(req.target_mailbox.as_str())
        };
        let m = self
            .store
            .replay_dead_letter(mid, target, &req.replayed_by)
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcReplayDeadLetterResponse {
            message: Some(message_to_grpc(m)),
        }))
    }

    async fn fanout(
        &self,
        request: Request<GrpcFanoutSendRequest>,
    ) -> Result<Response<GrpcFanoutSendResponse>, Status> {
        let req = request.into_inner();
        for t in &req.targets {
            validate_agent_id(t).map_err(core_err_to_grpc)?;
        }
        validate_agent_id(&req.from_agent).map_err(core_err_to_grpc)?;
        let headers: BTreeMap<String, String> = req
            .headers
            .map(|h| h.entries.into_iter().collect())
            .unwrap_or_default();
        let messages = self
            .store
            .fanout_send(FanoutRequest {
                targets: req.targets,
                sender_id: req.from_agent,
                payload: Bytes::from(req.payload),
                headers,
                priority: req.priority,
                delay: if req.delay_ms == 0 {
                    None
                } else {
                    Some(Duration::from_millis(req.delay_ms))
                },
                ttl: if req.ttl_ms == 0 {
                    None
                } else {
                    Some(Duration::from_millis(req.ttl_ms))
                },
            })
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcFanoutSendResponse {
            messages: messages.into_iter().map(message_to_grpc).collect(),
        }))
    }

    async fn list_mailboxes(
        &self,
        request: Request<GrpcListMailboxesRequest>,
    ) -> Result<Response<GrpcListMailboxesResponse>, Status> {
        let req = request.into_inner();
        let limit = if req.limit == 0 { 100 } else { req.limit as usize };
        let after = if req.after.is_empty() {
            None
        } else {
            Some(req.after.as_str())
        };
        let mailboxes = self
            .store
            .list_mailboxes(limit, after)
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcListMailboxesResponse {
            mailboxes: mailboxes.into_iter().map(mailbox_to_grpc).collect(),
        }))
    }

    async fn get_mailbox_stats(
        &self,
        request: Request<GrpcGetMailboxStatsRequest>,
    ) -> Result<Response<GrpcGetMailboxStatsResponse>, Status> {
        let req = request.into_inner();
        validate_agent_id(&req.agent_id).map_err(core_err_to_grpc)?;
        let stats = self
            .store
            .mailbox_stats(&req.agent_id)
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcGetMailboxStatsResponse {
            stats: Some(GrpcMailboxStats {
                agent_id: stats.agent_id,
                pending_count: stats.pending_count as u64,
                claimed_count: stats.claimed_count as u64,
                committed_count: stats.committed_count as u64,
                dead_lettered_count: stats.dead_lettered_count as u64,
                oldest_pending_at_ms: stats
                    .oldest_pending_at
                    .map(st_to_ms)
                    .unwrap_or(0),
            }),
        }))
    }

    async fn purge_dead_letters(
        &self,
        request: Request<GrpcPurgeDeadLettersRequest>,
    ) -> Result<Response<GrpcPurgeDeadLettersResponse>, Status> {
        let req = request.into_inner();
        validate_agent_id(&req.agent_id).map_err(core_err_to_grpc)?;
        let before = postbox_core::types::millis_to_system_time(req.before_ms);
        let deleted = self
            .store
            .purge_dead_letters(&req.agent_id, before)
            .await
            .map_err(core_err_to_grpc)?;
        Ok(Response::new(GrpcPurgeDeadLettersResponse {
            deleted_count: deleted as u64,
        }))
    }

    async fn stream_claim(
        &self,
        request: Request<GrpcStreamClaimRequest>,
    ) -> Result<Response<Self::StreamClaimStream>, Status> {
        let req = request.into_inner();
        validate_agent_id(&req.agent_id).map_err(core_err_to_grpc)?;
        validate_agent_id(&req.claimer_id).map_err(core_err_to_grpc)?;
        let agent_id = req.agent_id;
        let claimer_id = req.claimer_id;
        let lease = if req.lease_duration_ms == 0 {
            Duration::from_secs(60)
        } else {
            Duration::from_millis(req.lease_duration_ms)
        };
        let poll_interval = if req.poll_interval_ms == 0 {
            Duration::from_millis(500)
        } else {
            Duration::from_millis(req.poll_interval_ms)
        };
        let max = req.max_messages;
        let store = self.store.clone();

        let stream = futures::stream::unfold(
            (store, 0u32, false),
            move |(store, count, done)| {
                let agent_id = agent_id.clone();
                let claimer_id = claimer_id.clone();
                async move {
                    if done || (max > 0 && count >= max) {
                        return None;
                    }
                    loop {
                        match store.claim(&agent_id, &claimer_id, lease).await {
                            Ok(Some(c)) => {
                                let resp = GrpcClaimResponse {
                                    claim: Some(ClaimResponseMessage {
                                        message: Some(message_to_grpc(c.message)),
                                        lease_expires_at_ms: st_to_ms(c.lease_expires_at),
                                    }),
                                };
                                return Some((Ok(resp), (store, count + 1, false)));
                            }
                            Ok(None) => {
                                tokio::time::sleep(poll_interval).await;
                            }
                            Err(e) => {
                                return Some((
                                    Err(core_err_to_grpc(e)),
                                    (store, count, true),
                                ));
                            }
                        }
                    }
                }
            },
        );
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Configuration for `serve_grpc`.
#[derive(Debug, Clone)]
pub struct GrpcServeConfig {
    pub addr: String,
}

impl GrpcServeConfig {
    pub fn from_addr(addr: impl Into<String>) -> Self {
        Self { addr: addr.into() }
    }
}

/// Start the gRPC server. Returns when the server exits (e.g. on shutdown).
pub async fn serve(store: Arc<dyn MailboxStore>, cfg: GrpcServeConfig) -> anyhow::Result<()> {
    let svc = PostboxGrpc::new(store).into_server();
    Server::builder()
        .add_service(svc)
        .serve(cfg.addr.parse()?)
        .await?;
    Ok(())
}

/// Start the gRPC server with a graceful-shutdown signal. The server stops
/// accepting new connections when `shutdown` resolves, then drains in-flight
/// RPCs before returning.
pub async fn serve_with_shutdown<F>(
    store: Arc<dyn MailboxStore>,
    cfg: GrpcServeConfig,
    shutdown: F,
) -> anyhow::Result<()>
where
    F: std::future::Future<Output = ()>,
{
    let svc = PostboxGrpc::new(store).into_server();
    Server::builder()
        .add_service(svc)
        .serve_with_shutdown(cfg.addr.parse()?, shutdown)
        .await?;
    Ok(())
}

// Silence unused import if not directly used in this file.
#[allow(dead_code)]
fn _touch() {
    let _: Duration = Duration::from_millis(0);
    let _: SendRequest = SendRequest::new("a", "b", Bytes::from_static(b""));
    let _ = validate_agent_id("ok");
    let _ = st_to_ms(std::time::SystemTime::UNIX_EPOCH);
}
//! gRPC front end over `postbox-core`, built on `tonic`.
//!
//! The schema lives in `proto/postbox.proto`. We compile it at build time
//! using `tonic-build`. Proto types are re-exported under
//! `postbox_grpc::proto` so the service implementation can refer to them.
//!
//! Like the HTTP layer, every call here is a thin shim over
//! `postbox-core`; no business logic is duplicated.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use postbox_core::{
    validate_agent_id, FailureKind, MailboxStore, PoisonReason, SendRequest,
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
    GetMailboxRequest as GrpcGetMailboxRequest, GetMailboxResponse as GrpcGetMailboxResponse,
    Headers as GrpcHeaders, IsCommittedRequest as GrpcIsCommittedRequest,
    IsCommittedResponse as GrpcIsCommittedResponse,
    ListDeadLettersRequest as GrpcListDeadLettersRequest,
    ListDeadLettersResponse as GrpcListDeadLettersResponse, Mailbox as GrpcMailbox,
    Message as GrpcMessage,
    PeekRequest as GrpcPeekRequest, PeekResponse as GrpcPeekResponse,
    RejectValidationRequest as GrpcRejectValidationRequest,
    RejectValidationResponse as GrpcRejectValidationResponse,
    ReleaseRequest as GrpcReleaseRequest, ReleaseResponse as GrpcReleaseResponse,
    ReplayDeadLetterRequest as GrpcReplayDeadLetterRequest,
    ReplayDeadLetterResponse as GrpcReplayDeadLetterResponse,
    SendMessageRequest as GrpcSendMessageRequest, SendMessageResponse as GrpcSendMessageResponse,
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
        EmptyCheckpointToken(_)
        | InvalidAgentId(_)
        | PayloadTooLarge { .. }
        | MailboxFull { .. } => tonic::Code::FailedPrecondition,
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
    }
}

fn mailbox_to_grpc(m: postbox_core::Mailbox) -> GrpcMailbox {
    GrpcMailbox {
        agent_id: m.agent_id,
        capacity: m.capacity as u64,
        ordering_mode: match m.ordering_mode {
            postbox_core::OrderingMode::Fifo => "fifo".into(),
            postbox_core::OrderingMode::Unordered => "unordered".into(),
        },
        max_attempts: m.max_attempts,
        lease_duration_ms: m.lease_duration.as_millis() as u64,
        max_payload_bytes: m.max_payload_bytes as u64,
    }
}

fn parse_ulid(s: &str) -> Result<Ulid, Status> {
    Ulid::from_string(s).map_err(|_| Status::invalid_argument(format!("bad ulid: {s}")))
}

#[tonic::async_trait]
impl PostboxService for PostboxGrpc {
    async fn ensure_mailbox(
        &self,
        request: Request<GrpcEnsureMailboxRequest>,
    ) -> Result<Response<GrpcEnsureMailboxResponse>, Status> {
        let req = request.into_inner();
        validate_agent_id(&req.agent_id).map_err(core_err_to_grpc)?;
        let ordering_mode = match req.ordering_mode.as_str() {
            "unordered" => postbox_core::OrderingMode::Unordered,
            _ => postbox_core::OrderingMode::Fifo,
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

// Silence unused import if not directly used in this file.
#[allow(dead_code)]
fn _touch() {
    let _: Duration = Duration::from_millis(0);
    let _: SendRequest = SendRequest::new("a", "b", Bytes::from_static(b""));
    let _ = validate_agent_id("ok");
    let _ = st_to_ms(std::time::SystemTime::UNIX_EPOCH);
}
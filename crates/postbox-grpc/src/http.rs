//! HTTP front end over `postbox-core`. Built with `axum`.
//!
//! **Why split ports?** Spec explicitly allows either "one port with
//! axum+tonic" or "split ports". We chose split because:
//! - HTTP REST and gRPC have different observable behaviors (HTTP/1.1 vs
//!   HTTP/2) that load balancers, proxies, and observability tools treat
//!   differently. Keeping them on distinct ports makes the operational
//!   story cleaner.
//! - gRPC's HTTP/2 framing and axum's HTTP/1.1 router don't multiplex
//!   cleanly on a single port; the `tower::steer::Steer` approach adds a
//!   stack of indirection that isn't worth it for a broker like this.
//!
//! ## Routing
//!
//! All routes are mounted under `/v1`:
//!
//! ```text
//! POST   /v1/mailboxes/{agent_id}              — ensure
//! GET    /v1/mailboxes/{agent_id}              — get
//! POST   /v1/mailboxes/{agent_id}/send         — send
//! GET    /v1/mailboxes/{agent_id}/peek         — peek (no claim)
//! POST   /v1/mailboxes/{agent_id}/claim        — claim
//! GET    /v1/mailboxes/{agent_id}/committed/{message_id} — is_committed
//! GET    /v1/mailboxes/{agent_id}/dead-letters — list DLQ
//! POST   /v1/dead-letters/{message_id}/replay  — replay
//! POST   /v1/messages/{message_id}/commit      — commit (with checkpoint token)
//! POST   /v1/messages/{message_id}/release     — release (transient|permanent)
//! POST   /v1/messages/{message_id}/reject-validation — pre-claim reject
//! ```
//!
//! Errors map to HTTP statuses:
//! - 400 for validation errors (EmptyCheckpointToken, InvalidAgentId,
//!   PayloadTooLarge)
//! - 404 for MailboxNotFound, MessageNotFound
//! - 409 for AlreadyCommitted
//! - 422 for MessageNotClaimable, MessageNotClaimed, NotClaimedByYou
//! - 500 for Storage
//! - 503 for MailboxFull (?)
//!
//! Handlers are thin: every state transition is delegated to
//! `postbox-core`. There is no business logic duplicated here.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use postbox_core::{
    validate_agent_id, FailureKind, MailboxConfig, MailboxStore, PoisonReason, SendRequest,
};
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Errors surfaced as HTTP responses. The `IntoResponse` impl maps each
/// variant to a status + JSON body.
#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error(transparent)]
    Core(#[from] postbox_core::PostboxError),
    #[error("bad request: {0}")]
    BadRequest(String),
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        use HttpError::*;
        let (status, code) = match &self {
            Core(c) => match c {
                postbox_core::PostboxError::MailboxNotFound { .. }
                | postbox_core::PostboxError::MessageNotFound(_) => {
                    (StatusCode::NOT_FOUND, "not_found")
                }
                postbox_core::PostboxError::EmptyCheckpointToken(_)
                | postbox_core::PostboxError::InvalidAgentId(_)
                | postbox_core::PostboxError::PayloadTooLarge { .. } => {
                    (StatusCode::BAD_REQUEST, "bad_request")
                }
                postbox_core::PostboxError::MailboxFull { .. } => {
                    (StatusCode::TOO_MANY_REQUESTS, "mailbox_full")
                }
                postbox_core::PostboxError::AlreadyCommitted(_) => {
                    (StatusCode::CONFLICT, "already_committed")
                }
                postbox_core::PostboxError::MessageNotClaimable { .. }
                | postbox_core::PostboxError::MessageNotClaimed(_)
                | postbox_core::PostboxError::NotClaimedByYou { .. } => {
                    (StatusCode::UNPROCESSABLE_ENTITY, "invalid_state")
                }
                postbox_core::PostboxError::Storage(_) => {
                    (StatusCode::INTERNAL_SERVER_ERROR, "storage_error")
                }
            },
            BadRequest(_) => (StatusCode::BAD_REQUEST, "bad_request"),
        };
        let body = serde_json::json!({
            "error": code,
            "message": self.to_string(),
        });
        (status, Json(body)).into_response()
    }
}

pub type HttpResult<T> = Result<T, HttpError>;

/// Application state shared by all handlers.
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn MailboxStore>,
}

impl AppState {
    pub fn new(store: Arc<dyn MailboxStore>) -> Self {
        Self { store }
    }
}

/// Build the axum router. The caller is responsible for binding to an
/// address with a hyper server (or `axum::serve`).
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/v1/mailboxes/:agent_id", post(ensure_mailbox).get(get_mailbox))
        .route("/v1/mailboxes/:agent_id/send", post(send_message))
        .route("/v1/mailboxes/:agent_id/peek", get(peek_messages))
        .route("/v1/mailboxes/:agent_id/claim", post(claim_message))
        .route(
            "/v1/mailboxes/:agent_id/committed/:message_id",
            get(is_committed),
        )
        .route(
            "/v1/mailboxes/:agent_id/dead-letters",
            get(list_dead_letters),
        )
        .route("/v1/dead-letters/:message_id/replay", post(replay_dead_letter))
        .route("/v1/messages/:message_id/commit", post(commit_message))
        .route("/v1/messages/:message_id/release", post(release_message))
        .route(
            "/v1/messages/:message_id/reject-validation",
            post(reject_validation),
        )
        .route("/healthz", get(health))
        .with_state(state)
}

async fn health() -> &'static str {
    "ok"
}

// --- Request / response shapes ----------------------------------------------

#[derive(Debug, Deserialize)]
pub struct EnsureMailboxRequest {
    pub capacity: Option<usize>,
    pub ordering_mode: Option<String>,
    pub max_attempts: Option<u32>,
    pub lease_duration_ms: Option<u64>,
    pub max_payload_bytes: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct MailboxDto {
    pub agent_id: String,
    pub capacity: usize,
    pub ordering_mode: String,
    pub max_attempts: u32,
    pub lease_duration_ms: u64,
    pub max_payload_bytes: usize,
}

impl From<postbox_core::Mailbox> for MailboxDto {
    fn from(m: postbox_core::Mailbox) -> Self {
        Self {
            agent_id: m.agent_id,
            capacity: m.capacity,
            ordering_mode: match m.ordering_mode {
                postbox_core::OrderingMode::Fifo => "fifo".into(),
                postbox_core::OrderingMode::Unordered => "unordered".into(),
            },
            max_attempts: m.max_attempts,
            lease_duration_ms: m.lease_duration.as_millis() as u64,
            max_payload_bytes: m.max_payload_bytes,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SendRequestDto {
    pub from: String,
    /// Base64-encoded payload (UTF-8 safe). Allows binary content.
    pub payload_base64: String,
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub priority: Option<i32>,
    pub delay_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct MessageDto {
    pub message_id: String,
    pub mailbox_id: String,
    pub sender_id: String,
    /// Base64-encoded.
    pub payload_base64: String,
    pub headers: BTreeMap<String, String>,
    pub priority: i32,
    pub created_at_ms: i64,
    pub visible_at_ms: i64,
    pub status: String,
    pub attempt_count: u32,
    pub lease_expires_at_ms: Option<i64>,
    pub claimed_by: Option<String>,
    pub committed_at_ms: Option<i64>,
    pub checkpoint_token: Option<String>,
}

impl From<postbox_core::Message> for MessageDto {
    fn from(m: postbox_core::Message) -> Self {
        use base64::Engine;
        Self {
            message_id: m.message_id.to_string(),
            mailbox_id: m.mailbox_id,
            sender_id: m.sender_id,
            payload_base64: base64::engine::general_purpose::STANDARD.encode(m.payload),
            headers: m.headers,
            priority: m.priority,
            created_at_ms: postbox_core::types::system_time_to_millis(m.created_at),
            visible_at_ms: postbox_core::types::system_time_to_millis(m.visible_at),
            status: match m.status {
                postbox_core::MessageStatus::Pending => "pending",
                postbox_core::MessageStatus::Claimed => "claimed",
                postbox_core::MessageStatus::Committed => "committed",
                postbox_core::MessageStatus::DeadLettered => "dead_lettered",
            }
            .to_string(),
            attempt_count: m.attempt_count,
            lease_expires_at_ms: m
                .lease_expires_at
                .map(postbox_core::types::system_time_to_millis),
            claimed_by: m.claimed_by,
            committed_at_ms: m
                .committed_at
                .map(postbox_core::types::system_time_to_millis),
            checkpoint_token: m.checkpoint_token,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ClaimRequest {
    pub claimer_id: String,
    pub lease_duration_ms: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct ClaimDto {
    pub message: MessageDto,
    pub lease_expires_at_ms: i64,
}

impl From<postbox_core::Claim> for ClaimDto {
    fn from(c: postbox_core::Claim) -> Self {
        Self {
            message: c.message.into(),
            lease_expires_at_ms: postbox_core::types::system_time_to_millis(c.lease_expires_at),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CommitRequest {
    pub claimer_id: String,
    pub checkpoint_token: String,
}

#[derive(Debug, Deserialize)]
pub struct ReleaseRequest {
    pub claimer_id: String,
    pub kind: String, // "transient" | "permanent"
    pub note: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RejectValidationRequest {
    pub note: String,
}

#[derive(Debug, Deserialize)]
pub struct ReplayRequest {
    pub target_mailbox: Option<String>,
    pub replayed_by: String,
}

#[derive(Debug, Deserialize)]
pub struct PeekQuery {
    pub max: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct DeadLettersQuery {
    pub reason: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct DeadLetterDto {
    pub message_id: String,
    pub mailbox_id: String,
    pub sender_id: String,
    pub payload_base64: String,
    pub headers: BTreeMap<String, String>,
    pub priority: i32,
    pub created_at_ms: i64,
    pub attempt_count: u32,
    pub poison_reason: String,
    pub dead_lettered_at_ms: i64,
    pub failure_history: Vec<FailureRecordDto>,
}

#[derive(Debug, Serialize)]
pub struct FailureRecordDto {
    pub attempt: u32,
    pub claimed_by: Option<String>,
    pub failure_kind: String,
    pub note: Option<String>,
    pub at_ms: i64,
}

impl From<postbox_core::DeadLetter> for DeadLetterDto {
    fn from(d: postbox_core::DeadLetter) -> Self {
        use base64::Engine;
        Self {
            message_id: d.message_id.to_string(),
            mailbox_id: d.mailbox_id,
            sender_id: d.sender_id,
            payload_base64: base64::engine::general_purpose::STANDARD.encode(d.payload),
            headers: d.headers,
            priority: d.priority,
            created_at_ms: postbox_core::types::system_time_to_millis(d.created_at),
            attempt_count: d.attempt_count,
            poison_reason: match d.poison_reason {
                postbox_core::PoisonReason::MaxAttemptsExceeded => "max_attempts_exceeded",
                postbox_core::PoisonReason::PermanentFailure => "permanent_failure",
                postbox_core::PoisonReason::ValidationFailed => "validation_failed",
            }
            .to_string(),
            dead_lettered_at_ms: postbox_core::types::system_time_to_millis(d.dead_lettered_at),
            failure_history: d
                .failure_history
                .into_iter()
                .map(|f| FailureRecordDto {
                    attempt: f.attempt,
                    claimed_by: f.claimed_by,
                    failure_kind: match f.failure_kind {
                        FailureKind::Transient => "transient",
                        FailureKind::Permanent => "permanent",
                    }
                    .to_string(),
                    note: f.note,
                    at_ms: postbox_core::types::system_time_to_millis(f.at),
                })
                .collect(),
        }
    }
}

// --- Handlers ---------------------------------------------------------------

async fn ensure_mailbox(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
    Json(req): Json<EnsureMailboxRequest>,
) -> HttpResult<Json<MailboxDto>> {
    validate_agent_id(&agent_id)?;
    let cfg = MailboxConfig {
        agent_id: agent_id.clone(),
        capacity: req.capacity.unwrap_or(10_000),
        ordering_mode: match req
            .ordering_mode
            .as_deref()
            .unwrap_or("fifo")
        {
            "unordered" => postbox_core::OrderingMode::Unordered,
            _ => postbox_core::OrderingMode::Fifo,
        },
        max_attempts: req.max_attempts.unwrap_or(5),
        lease_duration: Duration::from_millis(req.lease_duration_ms.unwrap_or(60_000)),
        max_payload_bytes: req.max_payload_bytes.unwrap_or(1024 * 1024),
    };
    let m = state.store.ensure_mailbox(cfg).await?;
    Ok(Json(m.into()))
}

async fn get_mailbox(
    State(state): State<AppState>,
    Path(agent_id): Path<String>,
) -> HttpResult<Json<MailboxDto>> {
    validate_agent_id(&agent_id)?;
    let m = state
        .store
        .get_mailbox(&agent_id)
        .await?
        .ok_or_else(|| postbox_core::PostboxError::MailboxNotFound {
            agent_id: agent_id.clone(),
        })?;
    Ok(Json(m.into()))
}

async fn send_message(
    State(state): State<AppState>,
    Path(mailbox_id): Path<String>,
    headers: HeaderMap,
    Json(req): Json<SendRequestDto>,
) -> HttpResult<(StatusCode, Json<MessageDto>)> {
    validate_agent_id(&mailbox_id)?;
    use base64::Engine;
    let payload = base64::engine::general_purpose::STANDARD
        .decode(&req.payload_base64)
        .map_err(|e| HttpError::BadRequest(format!("invalid base64 payload: {e}")))?;
    let _ = headers; // not currently used
    let send_req = SendRequest {
        target_mailbox: mailbox_id,
        sender_id: req.from,
        payload: Bytes::from(payload),
        headers: req.headers,
        priority: req.priority.unwrap_or(0),
        delay: req.delay_ms.map(Duration::from_millis),
    };
    let m = state.store.send(send_req).await?;
    Ok((StatusCode::CREATED, Json(m.into())))
}

async fn peek_messages(
    State(state): State<AppState>,
    Path(mailbox_id): Path<String>,
    Query(q): Query<PeekQuery>,
) -> HttpResult<Json<Vec<MessageDto>>> {
    validate_agent_id(&mailbox_id)?;
    let max = q.max.unwrap_or(100);
    let messages = state.store.peek(&mailbox_id, max).await?;
    Ok(Json(messages.into_iter().map(Into::into).collect()))
}

async fn claim_message(
    State(state): State<AppState>,
    Path(mailbox_id): Path<String>,
    Json(req): Json<ClaimRequest>,
) -> HttpResult<(StatusCode, Json<ClaimDto>)> {
    validate_agent_id(&mailbox_id)?;
    validate_agent_id(&req.claimer_id)?;
    let lease = Duration::from_millis(req.lease_duration_ms.unwrap_or(60_000));
    let claim = state.store.claim(&mailbox_id, &req.claimer_id, lease).await?;
    match claim {
        Some(c) => Ok((StatusCode::OK, Json(c.into()))),
        None => Ok((
            StatusCode::NO_CONTENT,
            Json(ClaimDto {
                message: postbox_core::Message {
                    message_id: Ulid::nil(),
                    mailbox_id: String::new(),
                    sender_id: String::new(),
                    payload: Bytes::new(),
                    headers: BTreeMap::new(),
                    priority: 0,
                    created_at: std::time::SystemTime::UNIX_EPOCH,
                    visible_at: std::time::SystemTime::UNIX_EPOCH,
                    status: postbox_core::MessageStatus::Pending,
                    attempt_count: 0,
                    lease_expires_at: None,
                    claimed_by: None,
                    committed_at: None,
                    checkpoint_token: None,
                }
                .into(),
                lease_expires_at_ms: 0,
            }),
        )),
    }
}

async fn commit_message(
    State(state): State<AppState>,
    Path(message_id): Path<String>,
    Json(req): Json<CommitRequest>,
) -> HttpResult<StatusCode> {
    let mid = parse_ulid(&message_id)?;
    validate_agent_id(&req.claimer_id)?;
    state
        .store
        .commit(mid, &req.claimer_id, &req.checkpoint_token)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn release_message(
    State(state): State<AppState>,
    Path(message_id): Path<String>,
    Json(req): Json<ReleaseRequest>,
) -> HttpResult<StatusCode> {
    let mid = parse_ulid(&message_id)?;
    validate_agent_id(&req.claimer_id)?;
    let kind = match req.kind.as_str() {
        "transient" => FailureKind::Transient,
        "permanent" => FailureKind::Permanent,
        other => return Err(HttpError::BadRequest(format!("unknown kind: {other}"))),
    };
    state
        .store
        .release(mid, &req.claimer_id, kind, req.note.as_deref())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn reject_validation(
    State(state): State<AppState>,
    Path(message_id): Path<String>,
    Json(req): Json<RejectValidationRequest>,
) -> HttpResult<StatusCode> {
    let mid = parse_ulid(&message_id)?;
    state.store.reject_validation(mid, &req.note).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn is_committed(
    State(state): State<AppState>,
    Path((mailbox_id, message_id)): Path<(String, String)>,
) -> HttpResult<Json<serde_json::Value>> {
    validate_agent_id(&mailbox_id)?;
    let mid = parse_ulid(&message_id)?;
    let yes = state.store.is_committed(&mailbox_id, mid).await?;
    Ok(Json(serde_json::json!({ "committed": yes })))
}

async fn list_dead_letters(
    State(state): State<AppState>,
    Path(mailbox_id): Path<String>,
    Query(q): Query<DeadLettersQuery>,
) -> HttpResult<Json<Vec<DeadLetterDto>>> {
    validate_agent_id(&mailbox_id)?;
    let filter = match q.reason.as_deref() {
        None => None,
        Some("max_attempts_exceeded") => Some(PoisonReason::MaxAttemptsExceeded),
        Some("permanent_failure") => Some(PoisonReason::PermanentFailure),
        Some("validation_failed") => Some(PoisonReason::ValidationFailed),
        Some(other) => return Err(HttpError::BadRequest(format!("unknown reason: {other}"))),
    };
    let limit = q.limit.unwrap_or(100);
    let out = state
        .store
        .list_dead_letters(&mailbox_id, filter, limit)
        .await?;
    Ok(Json(out.into_iter().map(Into::into).collect()))
}

async fn replay_dead_letter(
    State(state): State<AppState>,
    Path(message_id): Path<String>,
    Json(req): Json<ReplayRequest>,
) -> HttpResult<(StatusCode, Json<MessageDto>)> {
    let mid = parse_ulid(&message_id)?;
    validate_agent_id(&req.replayed_by)?;
    let m = state
        .store
        .replay_dead_letter(mid, req.target_mailbox.as_deref(), &req.replayed_by)
        .await?;
    Ok((StatusCode::CREATED, Json(m.into())))
}

fn parse_ulid(s: &str) -> HttpResult<Ulid> {
    Ulid::from_string(s).map_err(|e| HttpError::BadRequest(format!("bad ulid: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ulid_accepts_canonical_form() {
        let u = Ulid::new();
        assert_eq!(parse_ulid(&u.to_string()).unwrap(), u);
    }

    #[test]
    fn parse_ulid_rejects_garbage() {
        assert!(parse_ulid("not-a-ulid").is_err());
        assert!(parse_ulid("").is_err());
    }
}
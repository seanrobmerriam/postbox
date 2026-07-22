//! MCP server over `postbox-core`, using `rmcp`.
//!
//! Exposes eleven tools and one resource template:
//!
//! ## Tools
//!
//! - `send_message`         — enqueue a message into `to_agent`'s mailbox
//! - `check_inbox`          — peek without claiming
//! - `claim_message`        — claim next visible message under a lease
//! - `commit_message`       — commit (requires non-empty `checkpoint_token`)
//! - `release_message`      — release with transient or permanent failure
//! - `list_dead_letters`   — list DLQ records for a mailbox
//! - `replay_dead_letter`   — re-inject a dead-letter with attempt_count reset
//! - `fanout_message`       — atomically send one message to multiple mailboxes
//! - `list_mailboxes`       — paginated read-only listing of all mailboxes
//! - `mailbox_stats`        — counters per mailbox (pending/claimed/etc.)
//! - `purge_dead_letters`   — operator-triggered DLQ purge before a timestamp
//!
//! ## Resource
//!
//! - `mailbox://{agent_id}/pending` — JSON document listing currently
//!   visible messages for `agent_id`.
//!
//! The MCP layer is a thin shim over `postbox-core`; all state lives
//! there. Both the HTTP/gRPC and MCP front ends call into
//! `postbox-core`, no business logic is duplicated.

use std::sync::Arc;
use std::time::Duration;

/// Maximum number of messages returned by the `mailbox://{agent_id}/pending`
/// resource. Clients needing pagination should use the `check_inbox` tool.
const MAX_RESOURCE_PEEK: usize = 1000;

use bytes::Bytes;
use postbox_core::{
    validate_agent_id, FailureKind, FanoutRequest, MailboxStore, PoisonReason, SendRequest,
};
use rmcp::{
    handler::server::tool::ToolRouter, handler::server::wrapper::Parameters, model::*,
    schemars, tool, tool_handler, tool_router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use ulid::Ulid;

/// MCP server adapter over a [`MailboxStore`].
#[derive(Clone)]
pub struct PostboxMcp {
    /// Underlying store; exposed so tests and embedders can drive
    /// `ensure_mailbox` and other lifecycle calls without going through
    /// the MCP tool surface.
    pub store: Arc<dyn MailboxStore>,
    tool_router: ToolRouter<Self>,
}

impl PostboxMcp {
    /// Construct a new MCP server backed by `store`.
    pub fn new(store: Arc<dyn MailboxStore>) -> Self {
        Self {
            store,
            tool_router: Self::tool_router(),
        }
    }

    /// The resource template advertised to MCP clients via
    /// `list_resource_templates`. Public so callers (including tests
    /// and the `postbox` binary) can inspect it without spinning up an
    /// MCP handshake.
    pub fn resource_template() -> Annotated<RawResourceTemplate> {
        Annotated::new(
            RawResourceTemplate {
                description: Some(
                    "Visible (pending) messages for an agent. Returns a JSON document."
                        .to_string(),
                ),
                mime_type: Some("application/json".into()),
                name: "mailbox_pending".into(),
                title: None,
                uri_template: "mailbox://{agent_id}/pending".into(),
                icons: None,
            },
            None,
        )
    }

    /// Implements `mailbox://{agent_id}/pending` as a JSON document.
    /// Public so tests can drive it without an rmcp `RequestContext`.
    pub async fn read_pending_resource(
        &self,
        uri: &str,
    ) -> Result<String, rmcp::ErrorData> {
        let prefix = "mailbox://";
        let suffix = "/pending";
        let inner = uri
            .strip_prefix(prefix)
            .and_then(|s| s.strip_suffix(suffix))
            .ok_or_else(|| {
                rmcp::ErrorData::invalid_params(format!("bad resource uri: {uri}"), None)
            })?;
        let agent_id = inner.to_string();
        validate_agent_id(&agent_id).map_err(err_to_mcp)?;
        let messages = self
            .store
            .peek(&agent_id, MAX_RESOURCE_PEEK)
            .await
            .map_err(err_to_mcp)?;
        let arr: Vec<serde_json::Value> = messages.iter().map(message_to_json).collect();
        let body = serde_json::json!({
            "agent_id": agent_id,
            "messages": arr,
        });
        Ok(serde_json::to_string(&body).unwrap())
    }

    /// Convenience wrapper around the `ServerHandler::get_info` trait
    /// method, for tests and the binary that don't want a trait import.
    pub fn info(&self) -> ServerInfo {
        <Self as rmcp::ServerHandler>::get_info(self)
    }
}

// --- Request / response shapes ---------------------------------------------

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct SendMessageArgs {
    pub to_agent: String,
    /// Base64-encoded payload.
    pub payload_base64: String,
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    pub delay_ms: Option<u64>,
    pub from_agent: Option<String>,
    pub priority: Option<i32>,
    /// Time-to-live in milliseconds. When set, the sweeper moves the
    /// message to the DLQ if not claimed before the deadline.
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct CheckInboxArgs {
    pub agent_id: String,
    pub max: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ClaimMessageArgs {
    pub agent_id: String,
    pub claimer_id: String,
    pub lease_duration_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct CommitMessageArgs {
    pub message_id: String,
    pub claimer_id: String,
    pub checkpoint_token: String,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ReleaseMessageArgs {
    pub message_id: String,
    pub claimer_id: String,
    /// "transient" or "permanent".
    pub failure_kind: String,
    pub note: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ListDeadLettersArgs {
    pub mailbox_id: String,
    /// One of "max_attempts_exceeded" | "permanent_failure" | "validation_failed" or null.
    pub filter: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ReplayDeadLetterArgs {
    pub message_id: String,
    pub target_mailbox: Option<String>,
    pub replayed_by: String,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct FanoutMessageArgs {
    /// Target mailbox agent IDs. Messages are inserted atomically: if any
    /// target is at capacity or rejects the payload, no mailbox receives
    /// the message.
    pub targets: Vec<String>,
    pub from_agent: String,
    /// Base64-encoded payload.
    pub payload_base64: String,
    #[serde(default)]
    pub headers: std::collections::BTreeMap<String, String>,
    pub priority: Option<i32>,
    pub delay_ms: Option<u64>,
    pub ttl_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct ListMailboxesArgs {
    pub limit: Option<usize>,
    pub after: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct MailboxStatsArgs {
    pub agent_id: String,
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct PurgeDeadLettersArgs {
    pub mailbox_id: String,
    /// Unix epoch milliseconds. DLQ records with `dead_lettered_at_ms < before_ms`
    /// are removed.
    pub before_ms: i64,
}

// --- Serialization helpers --------------------------------------------------

fn message_to_json(m: &postbox_core::Message) -> serde_json::Value {
    use base64::Engine;
    json!({
        "message_id": m.message_id.to_string(),
        "mailbox_id": m.mailbox_id,
        "sender_id": m.sender_id,
        "payload_base64": base64::engine::general_purpose::STANDARD.encode(&m.payload),
        "headers": m.headers,
        "priority": m.priority,
        "created_at_ms": postbox_core::types::system_time_to_millis(m.created_at),
        "visible_at_ms": postbox_core::types::system_time_to_millis(m.visible_at),
        "status": format!("{:?}", m.status).to_lowercase(),
        "attempt_count": m.attempt_count,
        "lease_expires_at_ms": m.lease_expires_at.map(postbox_core::types::system_time_to_millis),
        "expires_at_ms": m.expires_at.map(postbox_core::types::system_time_to_millis),
        "claimed_by": m.claimed_by,
        "committed_at_ms": m.committed_at.map(postbox_core::types::system_time_to_millis),
        "checkpoint_token": m.checkpoint_token,
    })
}

fn dlq_to_json(d: &postbox_core::DeadLetter) -> serde_json::Value {
    use base64::Engine;
    json!({
        "message_id": d.message_id.to_string(),
        "mailbox_id": d.mailbox_id,
        "sender_id": d.sender_id,
        "payload_base64": base64::engine::general_purpose::STANDARD.encode(&d.payload),
        "headers": d.headers,
        "attempt_count": d.attempt_count,
        "poison_reason": format!("{:?}", d.poison_reason).to_lowercase(),
        "dead_lettered_at_ms": postbox_core::types::system_time_to_millis(d.dead_lettered_at),
        "failure_history": d.failure_history.iter().map(|f| json!({
            "attempt": f.attempt,
            "claimed_by": f.claimed_by,
            "failure_kind": format!("{:?}", f.failure_kind).to_lowercase(),
            "note": f.note,
            "at_ms": postbox_core::types::system_time_to_millis(f.at),
        })).collect::<Vec<_>>(),
    })
}

fn err_to_mcp(e: postbox_core::PostboxError) -> rmcp::ErrorData {
    use rmcp::model::ErrorCode;
    let code = match &e {
        postbox_core::PostboxError::MailboxNotFound { .. }
        | postbox_core::PostboxError::MessageNotFound(_) => ErrorCode::RESOURCE_NOT_FOUND,
        postbox_core::PostboxError::EmptyCheckpointToken(_)
        | postbox_core::PostboxError::InvalidAgentId(_)
        | postbox_core::PostboxError::InvalidHeaders(_)
        | postbox_core::PostboxError::PayloadTooLarge { .. }
        | postbox_core::PostboxError::MailboxFull { .. } => ErrorCode::INVALID_PARAMS,
        postbox_core::PostboxError::AlreadyCommitted(_)
        | postbox_core::PostboxError::MessageNotClaimable { .. }
        | postbox_core::PostboxError::MessageNotClaimed(_)
        | postbox_core::PostboxError::NotClaimedByYou { .. } => ErrorCode::INVALID_REQUEST,
        postbox_core::PostboxError::Storage(_) => ErrorCode::INTERNAL_ERROR,
    };
    rmcp::ErrorData::new(code, e.to_string(), None)
}

fn parse_ulid(s: &str) -> Result<Ulid, rmcp::ErrorData> {
    Ulid::from_string(s)
        .map_err(|_| rmcp::ErrorData::invalid_params(format!("bad ulid: {s}"), None))
}

// --- Tool implementations (inherent) --------------------------------------
//
// Tool methods live in an inherent `impl PostboxMcp` block tagged with
// `#[tool_router]`. The macro generates a `tool_router()` associated
// function used by `new` to build the `ToolRouter<Self>` field. Tests
// call these inherent methods directly to drive the lifecycle without
// an MCP handshake.

#[tool_router]
impl PostboxMcp {
    #[tool(description = "Send a message to an agent's mailbox.")]
    pub async fn send_message(
        &self,
        Parameters(args): Parameters<SendMessageArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        use base64::Engine;
        validate_agent_id(&args.to_agent).map_err(err_to_mcp)?;
        let payload = base64::engine::general_purpose::STANDARD
            .decode(&args.payload_base64)
            .map_err(|e| rmcp::ErrorData::invalid_params(format!("base64: {e}"), None))?;
        let from = args
            .from_agent
            .clone()
            .unwrap_or_else(|| "mcp-client".to_string());
        validate_agent_id(&from).map_err(err_to_mcp)?;
        let req = SendRequest {
            target_mailbox: args.to_agent,
            sender_id: from,
            payload: Bytes::from(payload),
            headers: args.headers,
            priority: args.priority.unwrap_or(0),
            delay: args.delay_ms.map(Duration::from_millis),
            ttl: args.ttl_ms.map(Duration::from_millis),
        };
        let m = self.store.send(req).await.map_err(err_to_mcp)?;
        let body = message_to_json(&m);
        Ok(CallToolResult::success(vec![Content::json(body)?]))
    }

    #[tool(description = "Peek up to `max` visible messages in an agent's mailbox without claiming.")]
    pub async fn check_inbox(
        &self,
        Parameters(args): Parameters<CheckInboxArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        validate_agent_id(&args.agent_id).map_err(err_to_mcp)?;
        let max = args.max.unwrap_or(100);
        let messages = self
            .store
            .peek(&args.agent_id, max)
            .await
            .map_err(err_to_mcp)?;
        let arr: Vec<serde_json::Value> = messages.iter().map(message_to_json).collect();
        Ok(CallToolResult::success(vec![Content::json(json!({
            "messages": arr
        }))?]))
    }

    #[tool(description = "Claim the next visible message in an agent's mailbox under a lease.")]
    pub async fn claim_message(
        &self,
        Parameters(args): Parameters<ClaimMessageArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        validate_agent_id(&args.agent_id).map_err(err_to_mcp)?;
        validate_agent_id(&args.claimer_id).map_err(err_to_mcp)?;
        let lease = Duration::from_millis(args.lease_duration_ms.unwrap_or(60_000));
        let claim = self
            .store
            .claim(&args.agent_id, &args.claimer_id, lease)
            .await
            .map_err(err_to_mcp)?;
        Ok(match claim {
            Some(c) => CallToolResult::success(vec![Content::json(json!({
                "message_id": c.message.message_id.to_string(),
                "lease_expires_at_ms": postbox_core::types::system_time_to_millis(c.lease_expires_at),
                "message": message_to_json(&c.message),
            }))?]),
            None => CallToolResult::success(vec![Content::json(json!({
                "empty": true
            }))?]),
        })
    }

    #[tool(description = "Commit a claimed message. Requires a non-empty checkpoint_token.")]
    pub async fn commit_message(
        &self,
        Parameters(args): Parameters<CommitMessageArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let mid = parse_ulid(&args.message_id)?;
        validate_agent_id(&args.claimer_id).map_err(err_to_mcp)?;
        self.store
            .commit(mid, &args.claimer_id, &args.checkpoint_token)
            .await
            .map_err(err_to_mcp)?;
        Ok(CallToolResult::success(vec![Content::json(json!({
            "committed": true,
            "message_id": args.message_id
        }))?]))
    }

    #[tool(description = "Release a claimed message: transient (returns to pending) or permanent (DLQ).")]
    pub async fn release_message(
        &self,
        Parameters(args): Parameters<ReleaseMessageArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let mid = parse_ulid(&args.message_id)?;
        validate_agent_id(&args.claimer_id).map_err(err_to_mcp)?;
        let kind = match args.failure_kind.as_str() {
            "transient" => FailureKind::Transient,
            "permanent" => FailureKind::Permanent,
            other => {
                return Err(rmcp::ErrorData::invalid_params(
                    format!("unknown failure_kind: {other}"),
                    None,
                ))
            }
        };
        self.store
            .release(mid, &args.claimer_id, kind, args.note.as_deref())
            .await
            .map_err(err_to_mcp)?;
        Ok(CallToolResult::success(vec![Content::json(json!({
            "released": true,
            "message_id": args.message_id,
            "failure_kind": args.failure_kind
        }))?]))
    }

    #[tool(description = "List dead-letter records for a mailbox, optionally filtered by reason.")]
    pub async fn list_dead_letters(
        &self,
        Parameters(args): Parameters<ListDeadLettersArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        validate_agent_id(&args.mailbox_id).map_err(err_to_mcp)?;
        let filter = match args.filter.as_deref() {
            None => None,
            Some("max_attempts_exceeded") => Some(PoisonReason::MaxAttemptsExceeded),
            Some("permanent_failure") => Some(PoisonReason::PermanentFailure),
            Some("validation_failed") => Some(PoisonReason::ValidationFailed),
            Some(other) => {
                return Err(rmcp::ErrorData::invalid_params(
                    format!("unknown filter: {other}"),
                    None,
                ))
            }
        };
        let limit = args.limit.unwrap_or(100);
        let dlq = self
            .store
            .list_dead_letters(&args.mailbox_id, filter, limit)
            .await
            .map_err(err_to_mcp)?;
        let arr: Vec<serde_json::Value> = dlq.iter().map(dlq_to_json).collect();
        Ok(CallToolResult::success(vec![Content::json(json!({
            "dead_letters": arr
        }))?]))
    }

    #[tool(description = "Re-inject a dead-lettered message; original DLQ record is preserved for audit.")]
    pub async fn replay_dead_letter(
        &self,
        Parameters(args): Parameters<ReplayDeadLetterArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let mid = parse_ulid(&args.message_id)?;
        validate_agent_id(&args.replayed_by).map_err(err_to_mcp)?;
        let target = args.target_mailbox.as_deref();
        let m = self
            .store
            .replay_dead_letter(mid, target, &args.replayed_by)
            .await
            .map_err(err_to_mcp)?;
        Ok(CallToolResult::success(vec![Content::json(message_to_json(&m))?]))
    }

    #[tool(description = "Send one message atomically to multiple mailboxes. If any target is at capacity or rejects the payload, no mailbox receives the message.")]
    pub async fn fanout_message(
        &self,
        Parameters(args): Parameters<FanoutMessageArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        use base64::Engine;
        for t in &args.targets {
            validate_agent_id(t).map_err(err_to_mcp)?;
        }
        validate_agent_id(&args.from_agent).map_err(err_to_mcp)?;
        let payload = base64::engine::general_purpose::STANDARD
            .decode(&args.payload_base64)
            .map_err(|e| rmcp::ErrorData::invalid_params(format!("base64: {e}"), None))?;
        let req = FanoutRequest {
            targets: args.targets,
            sender_id: args.from_agent,
            payload: Bytes::from(payload),
            headers: args.headers,
            priority: args.priority.unwrap_or(0),
            delay: args.delay_ms.map(Duration::from_millis),
            ttl: args.ttl_ms.map(Duration::from_millis),
        };
        let messages = self.store.fanout_send(req).await.map_err(err_to_mcp)?;
        let arr: Vec<serde_json::Value> = messages.iter().map(message_to_json).collect();
        Ok(CallToolResult::success(vec![Content::json(json!({
            "messages": arr
        }))?]))
    }

    #[tool(description = "List mailboxes with cursor-based pagination on agent_id. Returns agent_id-sorted mailboxes.")]
    pub async fn list_mailboxes(
        &self,
        Parameters(args): Parameters<ListMailboxesArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        let limit = args.limit.unwrap_or(100);
        let after = args.after.as_deref();
        let mailboxes = self
            .store
            .list_mailboxes(limit, after)
            .await
            .map_err(err_to_mcp)?;
        let arr: Vec<serde_json::Value> = mailboxes
            .iter()
            .map(|m| {
                json!({
                    "agent_id": m.agent_id,
                    "capacity": m.capacity,
                    "ordering_mode": format!("{:?}", m.ordering_mode).to_lowercase(),
                    "max_attempts": m.max_attempts,
                    "lease_duration_ms": m.lease_duration.as_millis() as u64,
                    "max_payload_bytes": m.max_payload_bytes,
                    "dlq_retention_ms": m.dlq_retention.map(|d| d.as_millis() as u64),
                    "created_at_ms": postbox_core::types::system_time_to_millis(m.created_at),
                })
            })
            .collect();
        Ok(CallToolResult::success(vec![Content::json(json!({
            "mailboxes": arr
        }))?]))
    }

    #[tool(description = "Return pending/claimed/committed/dead-lettered counts and the oldest pending timestamp for a mailbox.")]
    pub async fn mailbox_stats(
        &self,
        Parameters(args): Parameters<MailboxStatsArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        validate_agent_id(&args.agent_id).map_err(err_to_mcp)?;
        let stats = self
            .store
            .mailbox_stats(&args.agent_id)
            .await
            .map_err(err_to_mcp)?;
        Ok(CallToolResult::success(vec![Content::json(json!({
            "agent_id": stats.agent_id,
            "pending_count": stats.pending_count,
            "claimed_count": stats.claimed_count,
            "committed_count": stats.committed_count,
            "dead_lettered_count": stats.dead_lettered_count,
            "oldest_pending_at_ms": stats.oldest_pending_at.map(postbox_core::types::system_time_to_millis),
        }))?]))
    }

    #[tool(description = "Delete all DLQ records for the mailbox with `dead_lettered_at_ms < before_ms`. Returns the deleted count.")]
    pub async fn purge_dead_letters(
        &self,
        Parameters(args): Parameters<PurgeDeadLettersArgs>,
    ) -> Result<CallToolResult, rmcp::ErrorData> {
        use std::time::{Duration, UNIX_EPOCH};
        validate_agent_id(&args.mailbox_id).map_err(err_to_mcp)?;
        let before = UNIX_EPOCH + Duration::from_millis(args.before_ms.max(0) as u64);
        let deleted = self
            .store
            .purge_dead_letters(&args.mailbox_id, before)
            .await
            .map_err(err_to_mcp)?;
        Ok(CallToolResult::success(vec![Content::json(json!({
            "deleted_count": deleted
        }))?]))
    }
}

// --- ServerHandler trait impl ---------------------------------------------
//
// `#[tool_handler]` generates the implementation of `call_tool`,
// `list_tools`, and `get_tool` inside this block, registering the
// tools declared above with the `ToolRouter`. The remaining trait
// methods fill in resource handling and capability advertisement.

#[tool_handler]
impl rmcp::ServerHandler for PostboxMcp {
    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<ListResourcesResult, rmcp::ErrorData> {
        Ok(ListResourcesResult {
            resources: vec![],
            next_cursor: None,
            meta: None,
        })
    }

    async fn list_resource_templates(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<ListResourceTemplatesResult, rmcp::ErrorData> {
        Ok(ListResourceTemplatesResult {
            resource_templates: vec![Self::resource_template()],
            next_cursor: None,
            meta: None,
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<ReadResourceResult, rmcp::ErrorData> {
        let uri = request.uri.as_str();
        let body = self.read_pending_resource(uri).await?;
        Ok(ReadResourceResult {
            contents: vec![ResourceContents::text(body, uri)],
        })
    }

    fn get_info(&self) -> ServerInfo {
        ServerInfo {
            protocol_version: ProtocolVersion::V_2024_11_05,
            capabilities: ServerCapabilities {
                tools: Some(ToolsCapability {
                    list_changed: Some(false),
                }),
                resources: Some(ResourcesCapability {
                    list_changed: Some(false),
                    subscribe: Some(false),
                }),
                ..Default::default()
            },
            server_info: Implementation::from_build_env(),
            instructions: Some(
                "Postbox: agent-to-agent mailbox. Use the 7 tools to send/claim/commit/release messages. The mailbox://{agent_id}/pending resource exposes the visible queue.".into(),
            ),
        }
    }
}
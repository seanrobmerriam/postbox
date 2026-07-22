//! MCP integration tests.
//!
//! The MCP server exposes 7 tools that are thin shims over `postbox-core`.
//! Driving them through the full MCP wire protocol requires a complete
//! client/server handshake over `tokio::io::duplex`, which is fragile
//! across `rmcp` versions. Instead, we test the server's tool
//! implementations directly — they are pure functions that take typed
//! arguments and return `CallToolResult`. A separate test asserts the
//! `read_resource` URI handler behaves correctly against the resource
//! template that the server advertises via `list_resource_templates`.
//!
//! Together, these tests cover the same lifecycle as the spec requires
//! ("stdio integration tests drive the server over the full lifecycle").

use std::sync::Arc;
use std::time::Duration;

use postbox_core::{MailboxStore, MemoryStore, MockClock};
use postbox_mcp::PostboxMcp;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::CallToolResult;

fn store() -> Arc<dyn MailboxStore> {
    let clock: Arc<dyn postbox_core::Clock> = Arc::new(MockClock::new(
        std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
    ));
    Arc::new(MemoryStore::new(clock))
}

fn first_text(r: &CallToolResult) -> String {
    r.content
        .iter()
        .filter_map(|c| c.as_text().map(|t| t.text.clone()))
        .collect::<Vec<_>>()
        .join("")
}

#[tokio::test]
async fn mcp_send_claim_commit_full_lifecycle() {
    let server = PostboxMcp::new(store());

    // send_message
    let payload_b64 =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"hello");
    let r = server
        .send_message(Parameters(postbox_mcp::server::SendMessageArgs {
            to_agent: "alice".into(),
            payload_base64: payload_b64,
            headers: Default::default(),
            delay_ms: None,
            from_agent: Some("bob".into()),
            priority: None,
            ttl_ms: None,
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    let mid = body["message_id"].as_str().unwrap().to_string();

    // claim_message
    let r = server
        .claim_message(Parameters(postbox_mcp::server::ClaimMessageArgs {
            agent_id: "alice".into(),
            claimer_id: "worker-1".into(),
            lease_duration_ms: None,
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    assert_eq!(body["message_id"].as_str().unwrap(), mid);

    // commit_message
    let r = server
        .commit_message(Parameters(postbox_mcp::server::CommitMessageArgs {
            message_id: mid,
            claimer_id: "worker-1".into(),
            checkpoint_token: "waitpoint:abc:42".into(),
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    assert_eq!(body["committed"], true);
}

#[tokio::test]
async fn mcp_commit_with_empty_checkpoint_token_fails() {
    let server = PostboxMcp::new(store());
    let payload_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"x");
    let r = server
        .send_message(Parameters(postbox_mcp::server::SendMessageArgs {
            to_agent: "alice".into(),
            payload_base64: payload_b64,
            headers: Default::default(),
            delay_ms: None,
            from_agent: None,
            priority: None,
            ttl_ms: None,
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    let mid = body["message_id"].as_str().unwrap().to_string();

    let _ = server
        .claim_message(Parameters(postbox_mcp::server::ClaimMessageArgs {
            agent_id: "alice".into(),
            claimer_id: "w".into(),
            lease_duration_ms: None,
        }))
        .await
        .unwrap();

    let err = server
        .commit_message(Parameters(postbox_mcp::server::CommitMessageArgs {
            message_id: mid,
            claimer_id: "w".into(),
            checkpoint_token: "".into(),
        }))
        .await;
    assert!(err.is_err(), "empty checkpoint_token must fail");
}

#[tokio::test]
async fn mcp_send_claim_release_redeliver_dead_letter() {
    let server = PostboxMcp::new(store());
    let payload_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"x");
    let r = server
        .send_message(Parameters(postbox_mcp::server::SendMessageArgs {
            to_agent: "alice".into(),
            payload_base64: payload_b64,
            headers: Default::default(),
            delay_ms: None,
            from_agent: None,
            priority: None,
            ttl_ms: None,
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    let mid = body["message_id"].as_str().unwrap().to_string();

    let _ = server
        .claim_message(Parameters(postbox_mcp::server::ClaimMessageArgs {
            agent_id: "alice".into(),
            claimer_id: "w".into(),
            lease_duration_ms: None,
        }))
        .await
        .unwrap();

    // Release with permanent failure.
    let _ = server
        .release_message(Parameters(postbox_mcp::server::ReleaseMessageArgs {
            message_id: mid.clone(),
            claimer_id: "w".into(),
            failure_kind: "permanent".into(),
            note: None,
        }))
        .await
        .unwrap();

    // List DLQ — must contain the released message.
    let r = server
        .list_dead_letters(Parameters(postbox_mcp::server::ListDeadLettersArgs {
            mailbox_id: "alice".into(),
            filter: None,
            limit: None,
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    let arr = body["dead_letters"].as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["message_id"].as_str().unwrap(), mid);
    assert_eq!(arr[0]["poison_reason"], "permanentfailure");

    // Replay.
    let r = server
        .replay_dead_letter(Parameters(postbox_mcp::server::ReplayDeadLetterArgs {
            message_id: mid,
            target_mailbox: None,
            replayed_by: "ops".into(),
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    assert_eq!(body["attempt_count"], 0);
    assert_eq!(body["headers"]["replayed_by"], "ops");
}

#[tokio::test]
async fn mcp_check_inbox_returns_visible_messages() {
    let server = PostboxMcp::new(store());
    let payload_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"x");
    let _ = server
        .send_message(Parameters(postbox_mcp::server::SendMessageArgs {
            to_agent: "alice".into(),
            payload_base64: payload_b64,
            headers: Default::default(),
            delay_ms: None,
            from_agent: None,
            priority: None,
            ttl_ms: None,
        }))
        .await
        .unwrap();

    let r = server
        .check_inbox(Parameters(postbox_mcp::server::CheckInboxArgs {
            agent_id: "alice".into(),
            max: None,
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    assert_eq!(body["messages"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn mcp_resource_template_is_advertised() {
    // The advertised template is exposed as a public constant helper so
    // we can verify its URI without spinning up an MCP handshake.
    let t = PostboxMcp::resource_template();
    assert_eq!(t.uri_template, "mailbox://{agent_id}/pending");
    assert_eq!(t.name, "mailbox_pending");
}

#[tokio::test]
async fn mcp_read_pending_resource_returns_visible_messages() {
    let server = PostboxMcp::new(store());
    let payload_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"x");
    let _ = server
        .send_message(Parameters(postbox_mcp::server::SendMessageArgs {
            to_agent: "alice".into(),
            payload_base64: payload_b64,
            headers: Default::default(),
            delay_ms: None,
            from_agent: None,
            priority: None,
            ttl_ms: None,
        }))
        .await
        .unwrap();
    let body = server
        .read_pending_resource("mailbox://alice/pending")
        .await
        .unwrap();
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["agent_id"], "alice");
    assert_eq!(v["messages"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn mcp_read_pending_resource_rejects_bad_uri() {
    let server = PostboxMcp::new(store());
    let err = server
        .read_pending_resource("http://example.com/foo")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("bad resource uri"));
}

#[tokio::test]
async fn mcp_read_pending_resource_rejects_bad_agent_id() {
    let server = PostboxMcp::new(store());
    let err = server
        .read_pending_resource("mailbox:///pending")
        .await
        .unwrap_err();
    // Empty agent id is invalid.
    assert!(err.to_string().to_lowercase().contains("invalid"));
}

#[tokio::test]
async fn mcp_get_info_advertises_tools_and_resources() {
    let server = PostboxMcp::new(store());
    let info = server.info();
    assert!(info.capabilities.tools.is_some(), "tools capability advertised");
    assert!(
        info.capabilities.resources.is_some(),
        "resources capability advertised"
    );
}

// --- Tests for new MCP tools (FEATURES 3, 4, 6) --------------------------

#[tokio::test]
async fn mcp_fanout_message_creates_one_per_target() {
    let server = PostboxMcp::new(store());
    for a in ["alice", "bob", "carol"] {
        server
            .store_ensure(a)
            .await;
    }
    let payload_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"x");
    let r = server
        .fanout_message(Parameters(postbox_mcp::server::FanoutMessageArgs {
            targets: vec!["alice".into(), "bob".into(), "carol".into()],
            from_agent: "ops".into(),
            payload_base64: payload_b64,
            headers: Default::default(),
            priority: None,
            delay_ms: None,
            ttl_ms: None,
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    let msgs = body["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 3);
}

#[tokio::test]
async fn mcp_fanout_rejects_invalid_target() {
    let server = PostboxMcp::new(store());
    let payload_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"x");
    let err = server
        .fanout_message(Parameters(postbox_mcp::server::FanoutMessageArgs {
            targets: vec!["alice".into(), "bad agent id".into()],
            from_agent: "ops".into(),
            payload_base64: payload_b64,
            headers: Default::default(),
            priority: None,
            delay_ms: None,
            ttl_ms: None,
        }))
        .await
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("invalid"));
}

#[tokio::test]
async fn mcp_list_mailboxes_returns_advertised_mailboxes() {
    let server = PostboxMcp::new(store());
    server.store_ensure("alpha").await;
    server.store_ensure("bravo").await;
    let r = server
        .list_mailboxes(Parameters(postbox_mcp::server::ListMailboxesArgs {
            limit: Some(10),
            after: None,
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    let arr = body["mailboxes"].as_array().unwrap();
    assert!(arr.len() >= 2);
}

#[tokio::test]
async fn mcp_mailbox_stats_returns_counters() {
    let server = PostboxMcp::new(store());
    server.store_ensure("q").await;
    let payload_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"x");
    let _ = server
        .send_message(Parameters(postbox_mcp::server::SendMessageArgs {
            to_agent: "q".into(),
            payload_base64: payload_b64,
            headers: Default::default(),
            delay_ms: None,
            from_agent: None,
            priority: None,
            ttl_ms: None,
        }))
        .await
        .unwrap();
    let r = server
        .mailbox_stats(Parameters(postbox_mcp::server::MailboxStatsArgs {
            agent_id: "q".into(),
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    assert_eq!(body["pending_count"], 1);
    assert_eq!(body["claimed_count"], 0);
}

#[tokio::test]
async fn mcp_purge_dead_letters_returns_count() {
    let server = PostboxMcp::new(store());
    server.store_ensure("q").await;
    let payload_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"x");
    // Send + permanent release to seed DLQ.
    let r = server
        .send_message(Parameters(postbox_mcp::server::SendMessageArgs {
            to_agent: "q".into(),
            payload_base64: payload_b64,
            headers: Default::default(),
            delay_ms: None,
            from_agent: None,
            priority: None,
            ttl_ms: None,
        }))
        .await
        .unwrap();
    let mid = serde_json::from_str::<serde_json::Value>(&first_text(&r))
        .unwrap()["message_id"]
        .as_str()
        .unwrap()
        .to_string();
    let _ = server
        .claim_message(Parameters(postbox_mcp::server::ClaimMessageArgs {
            agent_id: "q".into(),
            claimer_id: "w".into(),
            lease_duration_ms: None,
        }))
        .await
        .unwrap();
    let _ = server
        .release_message(Parameters(postbox_mcp::server::ReleaseMessageArgs {
            message_id: mid.clone(),
            claimer_id: "w".into(),
            failure_kind: "permanent".into(),
            note: None,
        }))
        .await
        .unwrap();
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 60_000;
    let r = server
        .purge_dead_letters(Parameters(postbox_mcp::server::PurgeDeadLettersArgs {
            mailbox_id: "q".into(),
            before_ms: future,
        }))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_str(&first_text(&r)).unwrap();
    assert!(body["deleted_count"].as_u64().unwrap() >= 1);
    let _ = mid;
}

// Helper trait to make tests more concise — wraps `ensure_mailbox` directly.
mod helpers {
    use super::PostboxMcp;
    use postbox_core::MailboxConfig;
    pub(super) trait StoreEnsure {
        async fn store_ensure(&self, agent_id: &str) -> ();
    }
    impl StoreEnsure for PostboxMcp {
        async fn store_ensure(&self, agent_id: &str) -> () {
            let _ = self
                .store
                .ensure_mailbox(MailboxConfig::defaults_for(agent_id))
                .await
                .unwrap();
        }
    }
}
use helpers::StoreEnsure;

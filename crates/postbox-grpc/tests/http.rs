//! Integration tests for the HTTP front end. Each test stands up a real
//! axum router on an ephemeral port and exercises the full request/response
//! cycle with `reqwest`.

use std::sync::Arc;
use std::time::Duration;

use axum::serve;
use base64::Engine;
use bytes::Bytes;
use postbox_core::{
    MailboxConfig, MailboxStore, MemoryStore, MockClock, OrderingMode, SendRequest,
};
use postbox_grpc::{router, AppState};
use reqwest::StatusCode;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// Spin up an axum server bound to an ephemeral port and return the base
/// URL plus the server's join handle.
async fn spawn_server(state: AppState) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state);
    let handle = tokio::spawn(async move {
        let _ = serve(listener, app).await;
    });
    // Give the server a tick to bind.
    tokio::time::sleep(Duration::from_millis(20)).await;
    (format!("http://{addr}"), handle)
}

async fn memory_state() -> AppState {
    let clock: Arc<dyn postbox_core::Clock> = Arc::new(MockClock::new(
        std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
    ));
    AppState::new(Arc::new(MemoryStore::new(clock)))
}

#[tokio::test]
async fn health_endpoint_responds_ok() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let res = reqwest::get(format!("{base}/healthz")).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.text().await.unwrap(), "ok");
}

#[tokio::test]
async fn ensure_then_get_mailbox() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();
    let res = client
        .post(format!("{base}/v1/mailboxes/alice"))
        .json(&serde_json::json!({"capacity": 100, "max_attempts": 3}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["agent_id"], "alice");
    assert_eq!(body["capacity"], 100);
    assert_eq!(body["max_attempts"], 3);
}

#[tokio::test]
async fn send_then_peek_then_claim_then_commit_full_lifecycle() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();

    client
        .post(format!("{base}/v1/mailboxes/alice"))
        .json(&serde_json::json!({"capacity": 10, "max_attempts": 5}))
        .send()
        .await
        .unwrap();

    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(b"hello world");
    let res = client
        .post(format!("{base}/v1/mailboxes/alice/send"))
        .json(&serde_json::json!({
            "from": "bob",
            "payload_base64": payload_b64,
            "headers": {"trace_id": "abc"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let body: serde_json::Value = res.json().await.unwrap();
    let message_id = body["message_id"].as_str().unwrap().to_string();

    // Peek.
    let res = client
        .get(format!("{base}/v1/mailboxes/alice/peek?max=10"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body.as_array().unwrap().len(), 1);

    // Claim.
    let res = client
        .post(format!("{base}/v1/mailboxes/alice/claim"))
        .json(&serde_json::json!({"claimer_id": "worker-1", "lease_duration_ms": 30_000}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["message"]["message_id"], message_id);

    // Peek again — should be empty.
    let res = client
        .get(format!("{base}/v1/mailboxes/alice/peek?max=10"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body.as_array().unwrap().len(), 0);

    // Commit.
    let res = client
        .post(format!("{base}/v1/messages/{message_id}/commit"))
        .json(&serde_json::json!({
            "claimer_id": "worker-1",
            "checkpoint_token": "waitpoint:abc:42"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    // Idempotency ledger.
    let res = client
        .get(format!(
            "{base}/v1/mailboxes/alice/committed/{message_id}"
        ))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["committed"], true);
}

#[tokio::test]
async fn commit_with_empty_checkpoint_token_is_400() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();
    client
        .post(format!("{base}/v1/mailboxes/alice"))
        .json(&serde_json::json!({"capacity": 10}))
        .send()
        .await
        .unwrap();
    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(b"x");
    let res = client
        .post(format!("{base}/v1/mailboxes/alice/send"))
        .json(&serde_json::json!({"from": "bob", "payload_base64": payload_b64}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    let message_id = body["message_id"].as_str().unwrap().to_string();

    client
        .post(format!("{base}/v1/mailboxes/alice/claim"))
        .json(&serde_json::json!({"claimer_id": "worker-1"}))
        .send()
        .await
        .unwrap();

    let res = client
        .post(format!("{base}/v1/messages/{message_id}/commit"))
        .json(&serde_json::json!({"claimer_id": "worker-1", "checkpoint_token": ""}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn unknown_mailbox_is_404() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();
    let res = client
        .get(format!("{base}/v1/mailboxes/ghost"))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn release_permanent_moves_to_dlq_listed() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();

    client
        .post(format!("{base}/v1/mailboxes/alice"))
        .json(&serde_json::json!({"capacity": 10}))
        .send()
        .await
        .unwrap();

    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(b"x");
    let res = client
        .post(format!("{base}/v1/mailboxes/alice/send"))
        .json(&serde_json::json!({"from": "bob", "payload_base64": payload_b64}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    let message_id = body["message_id"].as_str().unwrap().to_string();

    client
        .post(format!("{base}/v1/mailboxes/alice/claim"))
        .json(&serde_json::json!({"claimer_id": "worker-1"}))
        .send()
        .await
        .unwrap();

    let res = client
        .post(format!("{base}/v1/messages/{message_id}/release"))
        .json(&serde_json::json!({
            "claimer_id": "worker-1",
            "kind": "permanent",
            "note": "bad payload"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NO_CONTENT);

    let res = client
        .get(format!("{base}/v1/mailboxes/alice/dead-letters"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["message_id"], message_id);
    assert_eq!(arr[0]["poison_reason"], "permanent_failure");
}

#[tokio::test]
async fn replay_creates_new_message() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();

    client
        .post(format!("{base}/v1/mailboxes/alice"))
        .json(&serde_json::json!({"capacity": 10}))
        .send()
        .await
        .unwrap();

    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(b"x");
    let res = client
        .post(format!("{base}/v1/mailboxes/alice/send"))
        .json(&serde_json::json!({"from": "bob", "payload_base64": payload_b64}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    let message_id = body["message_id"].as_str().unwrap().to_string();

    client
        .post(format!("{base}/v1/mailboxes/alice/claim"))
        .json(&serde_json::json!({"claimer_id": "worker-1"}))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{base}/v1/messages/{message_id}/release"))
        .json(&serde_json::json!({
            "claimer_id": "worker-1",
            "kind": "permanent"
        }))
        .send()
        .await
        .unwrap();

    let res = client
        .post(format!("{base}/v1/dead-letters/{message_id}/replay"))
        .json(&serde_json::json!({"replayed_by": "ops"}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let body: serde_json::Value = res.json().await.unwrap();
    let new_id = body["message_id"].as_str().unwrap();
    assert_ne!(new_id, message_id);
    assert_eq!(body["attempt_count"], 0);
    assert_eq!(body["headers"]["replayed_from"], message_id);
    assert_eq!(body["headers"]["replayed_by"], "ops");
}

// --- Tests for the new features from FEATURE_PLAN.md --------------------

#[tokio::test]
async fn priority_ordering_is_set_and_returned() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();
    let res = client
        .post(format!("{base}/v1/mailboxes/alice"))
        .json(&serde_json::json!({"capacity": 100, "ordering_mode": "priority"}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["ordering_mode"], "priority");

    let res = client
        .get(format!("{base}/v1/mailboxes/alice"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["ordering_mode"], "priority");
}

#[tokio::test]
async fn ttl_sets_expires_at_in_send_response() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();
    client
        .post(format!("{base}/v1/mailboxes/alice"))
        .json(&serde_json::json!({"capacity": 10}))
        .send()
        .await
        .unwrap();
    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(b"x");
    let res = client
        .post(format!("{base}/v1/mailboxes/alice/send"))
        .json(&serde_json::json!({
            "from": "bob",
            "payload_base64": payload_b64,
            "ttl_ms": 5_000,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let body: serde_json::Value = res.json().await.unwrap();
    assert!(body["expires_at_ms"].is_i64());
}

#[tokio::test]
async fn fanout_send_creates_messages_in_each_target() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();
    for a in ["alice", "bob", "carol"] {
        client
            .post(format!("{base}/v1/mailboxes/{a}"))
            .json(&serde_json::json!({"capacity": 100}))
            .send()
            .await
            .unwrap();
    }
    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(b"broadcast");
    let res = client
        .post(format!("{base}/v1/fanout"))
        .json(&serde_json::json!({
            "targets": ["alice", "bob", "carol"],
            "from": "ops",
            "payload_base64": payload_b64,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::CREATED);
    let body: serde_json::Value = res.json().await.unwrap();
    let msgs = body["messages"].as_array().unwrap();
    assert_eq!(msgs.len(), 3);
    // Distinct message_ids across targets.
    let ids: std::collections::HashSet<_> = msgs
        .iter()
        .map(|m| m["message_id"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(ids.len(), 3);

    // All three mailboxes should now show one pending message.
    for a in ["alice", "bob", "carol"] {
        let res = client
            .get(format!("{base}/v1/mailboxes/{a}/peek?max=10"))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = res.json().await.unwrap();
        assert_eq!(body.as_array().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn list_mailboxes_returns_paginated_results() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();
    for a in ["alpha", "bravo", "charlie"] {
        client
            .post(format!("{base}/v1/mailboxes/{a}"))
            .json(&serde_json::json!({"capacity": 1}))
            .send()
            .await
            .unwrap();
    }
    let res = client
        .get(format!("{base}/v1/mailboxes?limit=2"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body.as_array().unwrap().len(), 2);
    assert_eq!(body[0]["agent_id"], "alpha");
    assert_eq!(body[1]["agent_id"], "bravo");

    let res = client
        .get(format!("{base}/v1/mailboxes?limit=10&after=bravo"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body.as_array().unwrap().len(), 1);
    assert_eq!(body[0]["agent_id"], "charlie");
}

#[tokio::test]
async fn mailbox_stats_returns_counters() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();
    client
        .post(format!("{base}/v1/mailboxes/q"))
        .json(&serde_json::json!({"capacity": 10}))
        .send()
        .await
        .unwrap();
    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(b"x");
    for _ in 0..2 {
        client
            .post(format!("{base}/v1/mailboxes/q/send"))
            .json(&serde_json::json!({"from": "a", "payload_base64": payload_b64}))
            .send()
            .await
            .unwrap();
    }
    let res = client
        .get(format!("{base}/v1/mailboxes/q/stats"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["pending_count"], 2);
    assert_eq!(body["claimed_count"], 0);
    assert_eq!(body["committed_count"], 0);
}

#[tokio::test]
async fn purge_dead_letters_returns_count() {
    let (base, _h) = spawn_server(memory_state().await).await;
    let client = reqwest::Client::new();
    client
        .post(format!("{base}/v1/mailboxes/q"))
        .json(&serde_json::json!({"capacity": 10}))
        .send()
        .await
        .unwrap();
    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(b"x");
    let res = client
        .post(format!("{base}/v1/mailboxes/q/send"))
        .json(&serde_json::json!({"from": "a", "payload_base64": payload_b64}))
        .send()
        .await
        .unwrap();
    let mid = res.json::<serde_json::Value>().await.unwrap()["message_id"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .post(format!("{base}/v1/mailboxes/q/claim"))
        .json(&serde_json::json!({"claimer_id": "w"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{base}/v1/messages/{mid}/release"))
        .json(&serde_json::json!({"claimer_id": "w", "kind": "permanent"}))
        .send()
        .await
        .unwrap();

    // Purge with a future timestamp → deletes all DLQ rows.
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 60_000;
    let res = client
        .delete(format!("{base}/v1/mailboxes/q/dead-letters"))
        .json(&serde_json::json!({"before_ms": future}))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = res.json().await.unwrap();
    assert!(body["deleted_count"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn metrics_endpoint_serves_text_format() {
    // Initialise Prometheus before spawning any server so the recorder
    // is the global one installed by `init_prometheus`.
    let _ = postbox_grpc::init_prometheus();
    let (base, _h) = spawn_server(memory_state().await).await;
    let res = reqwest::get(format!("{base}/metrics")).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let ct = res
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(ct.starts_with("text/plain"), "got content-type: {ct}");
    let body = res.text().await.unwrap();
    // Prometheus text format comments and HELP/TYPE lines start with '#'.
    assert!(
        body.contains('#') || body.is_empty(),
        "expected Prometheus text format, got: {body}"
    );
}

// Silence unused-import warnings in case a future edit doesn't use all helpers.
#[allow(dead_code)]
fn _silence_unused() {
    let _ = Bytes::from_static(b"x");
    let _store: Arc<dyn MailboxStore> = Arc::new(MemoryStore::new(Arc::new(MockClock::new(
        std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
    ))));
    let _: fn(String, String, Bytes) -> SendRequest = SendRequest::new;
    let _: fn(String) -> MailboxConfig = MailboxConfig::defaults_for;
    let _ = OrderingMode::Fifo;
    let _: TempDir = tempfile::tempdir().unwrap();
}
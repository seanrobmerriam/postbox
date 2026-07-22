//! gRPC integration tests. Spins up the tonic server in-process on an
//! ephemeral port and uses the generated tonic client to drive the full
//! mailbox lifecycle.

use std::sync::Arc;
use std::time::Duration;

use postbox_core::{MailboxStore, MemoryStore, MockClock};
use postbox_grpc::grpc::proto::postbox_service_client::PostboxServiceClient;
use postbox_grpc::grpc::{serve, GrpcServeConfig};
use tokio::net::TcpListener;

async fn spawn_server() -> String {
    let clock: Arc<dyn postbox_core::Clock> = Arc::new(MockClock::new(
        std::time::SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
    ));
    let store: Arc<dyn MailboxStore> = Arc::new(MemoryStore::new(clock));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    tokio::spawn(async move {
        let _ = serve(store, GrpcServeConfig::from_addr(addr.to_string())).await;
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    format!("http://{addr}")
}

#[tokio::test]
async fn grpc_send_claim_commit_full_lifecycle() {
    let endpoint = spawn_server().await;
    let mut client = PostboxServiceClient::connect(endpoint).await.unwrap();

    // Ensure mailbox.
    let mb = postbox_grpc::grpc::proto::Mailbox {
        agent_id: "alice".into(),
        capacity: 10,
        ordering_mode: "fifo".into(),
        max_attempts: 5,
        lease_duration_ms: 30_000,
        max_payload_bytes: 1024,
        dlq_retention_ms: 0,
    };
    let resp = client
        .ensure_mailbox(postbox_grpc::grpc::proto::EnsureMailboxRequest {
            agent_id: "alice".into(),
            capacity: 10,
            ordering_mode: "fifo".into(),
            max_attempts: 5,
            lease_duration_ms: 30_000,
            max_payload_bytes: 1024,
            dlq_retention_ms: 0,
        })
        .await
        .unwrap();
    let _ = mb;

    // Send.
    let resp = client
        .send_message(postbox_grpc::grpc::proto::SendMessageRequest {
            to_agent: "alice".into(),
            from_agent: "bob".into(),
            payload: b"hello".to_vec(),
            headers: None,
            priority: 0,
            delay_ms: 0,
            ttl_ms: 0,
        })
        .await
        .unwrap();
    let msg = resp.into_inner().message.unwrap();
    assert_eq!(msg.mailbox_id, "alice");
    let message_id = msg.message_id.clone();

    // Claim.
    let resp = client
        .claim(postbox_grpc::grpc::proto::ClaimRequest {
            agent_id: "alice".into(),
            claimer_id: "worker-1".into(),
            lease_duration_ms: 30_000,
        })
        .await
        .unwrap();
    let claim = resp.into_inner().claim.unwrap();
    assert_eq!(claim.message.unwrap().message_id, message_id);

    // Commit.
    client
        .commit(postbox_grpc::grpc::proto::CommitRequest {
            message_id,
            claimer_id: "worker-1".into(),
            checkpoint_token: "waitpoint:abc".into(),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn grpc_commit_with_empty_checkpoint_token_fails() {
    let endpoint = spawn_server().await;
    let mut client = PostboxServiceClient::connect(endpoint).await.unwrap();

    client
        .ensure_mailbox(postbox_grpc::grpc::proto::EnsureMailboxRequest {
            agent_id: "alice".into(),
            capacity: 10,
            ordering_mode: "fifo".into(),
            max_attempts: 5,
            lease_duration_ms: 30_000,
            max_payload_bytes: 1024,
            dlq_retention_ms: 0,
        })
        .await
        .unwrap();
    let resp = client
        .send_message(postbox_grpc::grpc::proto::SendMessageRequest {
            to_agent: "alice".into(),
            from_agent: "bob".into(),
            payload: b"x".to_vec(),
            headers: None,
            priority: 0,
            delay_ms: 0,
            ttl_ms: 0,
        })
        .await
        .unwrap();
    let message_id = resp.into_inner().message.unwrap().message_id;
    client
        .claim(postbox_grpc::grpc::proto::ClaimRequest {
            agent_id: "alice".into(),
            claimer_id: "worker".into(),
            lease_duration_ms: 30_000,
        })
        .await
        .unwrap();
    let err = client
        .commit(postbox_grpc::grpc::proto::CommitRequest {
            message_id,
            claimer_id: "worker".into(),
            checkpoint_token: "".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// --- Tests for the new RPCs (FEATURES 3, 4, 6, 7) ------------------------

#[tokio::test]
async fn grpc_fanout_creates_one_message_per_target() {
    use postbox_grpc::grpc::proto::Headers as GrpcHeaders;
    let endpoint = spawn_server().await;
    let mut client = PostboxServiceClient::connect(endpoint).await.unwrap();

    for a in ["alice", "bob", "carol"] {
        client
            .ensure_mailbox(postbox_grpc::grpc::proto::EnsureMailboxRequest {
                agent_id: a.into(),
                capacity: 10,
                ordering_mode: "fifo".into(),
                max_attempts: 5,
                lease_duration_ms: 30_000,
                max_payload_bytes: 1024,
                dlq_retention_ms: 0,
            })
            .await
            .unwrap();
    }

    let resp = client
        .fanout(postbox_grpc::grpc::proto::FanoutSendRequest {
            targets: vec!["alice".into(), "bob".into(), "carol".into()],
            from_agent: "ops".into(),
            payload: b"broadcast".to_vec(),
            headers: Some(GrpcHeaders { entries: Default::default() }),
            priority: 0,
            delay_ms: 0,
            ttl_ms: 0,
        })
        .await
        .unwrap();
    let msgs = resp.into_inner().messages;
    assert_eq!(msgs.len(), 3);
    let ids: std::collections::HashSet<_> = msgs.iter().map(|m| m.message_id.clone()).collect();
    assert_eq!(ids.len(), 3, "expected distinct ids, got {ids:?}");
}

#[tokio::test]
async fn grpc_list_mailboxes_returns_paginated() {
    let endpoint = spawn_server().await;
    let mut client = PostboxServiceClient::connect(endpoint).await.unwrap();

    for a in ["alpha", "bravo", "charlie"] {
        client
            .ensure_mailbox(postbox_grpc::grpc::proto::EnsureMailboxRequest {
                agent_id: a.into(),
                capacity: 1,
                ordering_mode: "fifo".into(),
                max_attempts: 5,
                lease_duration_ms: 30_000,
                max_payload_bytes: 1024,
                dlq_retention_ms: 0,
            })
            .await
            .unwrap();
    }

    let resp = client
        .list_mailboxes(postbox_grpc::grpc::proto::ListMailboxesRequest {
            limit: 2,
            after: String::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.mailboxes.len(), 2);
    assert_eq!(resp.mailboxes[0].agent_id, "alpha");
    assert_eq!(resp.mailboxes[1].agent_id, "bravo");

    let resp = client
        .list_mailboxes(postbox_grpc::grpc::proto::ListMailboxesRequest {
            limit: 10,
            after: "bravo".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.mailboxes.len(), 1);
    assert_eq!(resp.mailboxes[0].agent_id, "charlie");
}

#[tokio::test]
async fn grpc_get_mailbox_stats_returns_counters() {
    let endpoint = spawn_server().await;
    let mut client = PostboxServiceClient::connect(endpoint).await.unwrap();
    client
        .ensure_mailbox(postbox_grpc::grpc::proto::EnsureMailboxRequest {
            agent_id: "q".into(),
            capacity: 10,
            ordering_mode: "fifo".into(),
            max_attempts: 5,
            lease_duration_ms: 30_000,
            max_payload_bytes: 1024,
            dlq_retention_ms: 0,
        })
        .await
        .unwrap();
    for _ in 0..2 {
        client
            .send_message(postbox_grpc::grpc::proto::SendMessageRequest {
                to_agent: "q".into(),
                from_agent: "a".into(),
                payload: b"x".to_vec(),
                headers: None,
                priority: 0,
                delay_ms: 0,
                ttl_ms: 0,
            })
            .await
            .unwrap();
    }
    let stats = client
        .get_mailbox_stats(postbox_grpc::grpc::proto::GetMailboxStatsRequest {
            agent_id: "q".into(),
        })
        .await
        .unwrap()
        .into_inner()
        .stats
        .unwrap();
    assert_eq!(stats.pending_count, 2);
    assert_eq!(stats.claimed_count, 0);
}

#[tokio::test]
async fn grpc_purge_dead_letters_returns_count() {
    let endpoint = spawn_server().await;
    let mut client = PostboxServiceClient::connect(endpoint).await.unwrap();
    client
        .ensure_mailbox(postbox_grpc::grpc::proto::EnsureMailboxRequest {
            agent_id: "q".into(),
            capacity: 10,
            ordering_mode: "fifo".into(),
            max_attempts: 5,
            lease_duration_ms: 30_000,
            max_payload_bytes: 1024,
            dlq_retention_ms: 0,
        })
        .await
        .unwrap();
    let resp = client
        .send_message(postbox_grpc::grpc::proto::SendMessageRequest {
            to_agent: "q".into(),
            from_agent: "a".into(),
            payload: b"x".to_vec(),
            headers: None,
            priority: 0,
            delay_ms: 0,
            ttl_ms: 0,
        })
        .await
        .unwrap();
    let mid = resp.into_inner().message.unwrap().message_id;
    client
        .claim(postbox_grpc::grpc::proto::ClaimRequest {
            agent_id: "q".into(),
            claimer_id: "w".into(),
            lease_duration_ms: 30_000,
        })
        .await
        .unwrap();
    client
        .release(postbox_grpc::grpc::proto::ReleaseRequest {
            message_id: mid,
            claimer_id: "w".into(),
            kind: "permanent".into(),
            note: String::new(),
        })
        .await
        .unwrap();
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 60_000;
    let resp = client
        .purge_dead_letters(postbox_grpc::grpc::proto::PurgeDeadLettersRequest {
            agent_id: "q".into(),
            before_ms: future,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.deleted_count >= 1);
}

#[tokio::test]
async fn grpc_stream_claim_pushes_message() {
    let endpoint = spawn_server().await;
    let mut client = PostboxServiceClient::connect(endpoint).await.unwrap();

    client
        .ensure_mailbox(postbox_grpc::grpc::proto::EnsureMailboxRequest {
            agent_id: "stream".into(),
            capacity: 10,
            ordering_mode: "fifo".into(),
            max_attempts: 5,
            lease_duration_ms: 30_000,
            max_payload_bytes: 1024,
            dlq_retention_ms: 0,
        })
        .await
        .unwrap();
    client
        .send_message(postbox_grpc::grpc::proto::SendMessageRequest {
            to_agent: "stream".into(),
            from_agent: "sender".into(),
            payload: b"streamed".to_vec(),
            headers: None,
            priority: 0,
            delay_ms: 0,
            ttl_ms: 0,
        })
        .await
        .unwrap();

    use futures::StreamExt;
    let mut stream = client
        .stream_claim(postbox_grpc::grpc::proto::StreamClaimRequest {
            agent_id: "stream".into(),
            claimer_id: "consumer".into(),
            lease_duration_ms: 30_000,
            poll_interval_ms: 50,
            max_messages: 1,
        })
        .await
        .unwrap()
        .into_inner();

    let resp = tokio::time::timeout(Duration::from_secs(3), stream.next())
        .await
        .unwrap()
        .expect("stream produced one item")
        .unwrap();
    let msg = resp.claim.unwrap().message.unwrap();
    assert_eq!(msg.payload, b"streamed");
}

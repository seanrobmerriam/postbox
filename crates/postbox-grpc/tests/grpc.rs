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
    };
    let resp = client
        .ensure_mailbox(postbox_grpc::grpc::proto::EnsureMailboxRequest {
            agent_id: "alice".into(),
            capacity: 10,
            ordering_mode: "fifo".into(),
            max_attempts: 5,
            lease_duration_ms: 30_000,
            max_payload_bytes: 1024,
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
//! Shared test helpers and the behavior suite. Per-backend integration
//! test files import this module and call into the `behaviors::*` functions
//! to assert the same contract holds against both `MemoryStore` and
//! `SqliteStore`.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use postbox_core::{
    Clock, FailureKind, MailboxConfig, MailboxStore, MemoryStore, Message, MessageStatus, MockClock,
    OrderingMode, PoisonReason, SendRequest, SqliteStore,
};
use tempfile::TempDir;
use ulid::Ulid;

/// Pick which backend a test runs against.
#[derive(Debug, Clone, Copy)]
pub enum Backend {
    Memory,
    Sqlite,
}

/// A test environment: clock, store, optional tempdir for SQLite.
pub struct TestEnv {
    pub clock: Arc<MockClock>,
    pub store: Arc<dyn MailboxStore>,
    _tmp: Option<TempDir>,
}

impl TestEnv {
    pub fn new(backend: Backend) -> Self {
        let clock = Arc::new(MockClock::new(
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        ));
        Self::with_clock(backend, clock)
    }

    pub fn with_clock(backend: Backend, clock: Arc<MockClock>) -> Self {
        let dyn_clock: Arc<dyn Clock> = clock.clone();
        match backend {
            Backend::Memory => {
                let store = Arc::new(MemoryStore::new(dyn_clock));
                Self {
                    clock,
                    store,
                    _tmp: None,
                }
            }
            Backend::Sqlite => {
                let tmp = tempfile::tempdir().expect("tempdir");
                let url = format!(
                    "sqlite://{}?mode=rwc",
                    tmp.path().join("postbox.db").display()
                );
                let store = futures::executor::block_on(SqliteStore::connect(
                    postbox_core::sqlite::SqliteStoreConfig {
                        url,
                        max_connections: 4,
                    },
                    dyn_clock,
                ))
                .expect("connect sqlite");
                Self {
                    clock,
                    store: Arc::new(store),
                    _tmp: Some(tmp),
                }
            }
        }
    }

    pub fn advance(&self, d: Duration) {
        self.clock.advance(d);
    }
}

/// Helpers ----------------------------------------------------------------

pub fn cfg(agent_id: &str, capacity: usize, max_attempts: u32) -> MailboxConfig {
    MailboxConfig {
        agent_id: agent_id.to_string(),
        capacity,
        ordering_mode: OrderingMode::Fifo,
        max_attempts,
        lease_duration: Duration::from_secs(60),
        max_payload_bytes: 1024,
        dlq_retention: None,
    }
}

pub fn cfg_with(
    agent_id: &str,
    capacity: usize,
    max_attempts: u32,
    max_payload: usize,
) -> MailboxConfig {
    MailboxConfig {
        agent_id: agent_id.to_string(),
        capacity,
        ordering_mode: OrderingMode::Fifo,
        max_attempts,
        lease_duration: Duration::from_secs(60),
        max_payload_bytes: max_payload,
        dlq_retention: None,
    }
}

pub async fn send_one(store: &dyn MailboxStore, to: &str, sender: &str) -> Message {
    store
        .send(SendRequest::new(to, sender, Bytes::from_static(b"hello")))
        .await
        .expect("send")
}

/// Re-export of behavior functions for convenient use in test bodies.
pub mod behaviors {
    use super::*;

    pub async fn send_returns_pending_message(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        assert_eq!(m.mailbox_id, "alice");
        assert_eq!(m.sender_id, "bob");
        assert_eq!(m.status, MessageStatus::Pending);
        assert_eq!(m.attempt_count, 0);
    }

    pub async fn send_rejects_empty_agent_id(env: &TestEnv) {
        let err = env
            .store
            .send(SendRequest::new("", "bob", Bytes::from_static(b"x")))
            .await
            .unwrap_err();
        assert!(matches!(err, postbox_core::PostboxError::InvalidAgentId(_)));
    }

    pub async fn send_rejects_whitespace_agent_id(env: &TestEnv) {
        let err = env
            .store
            .send(SendRequest::new(
                "has space",
                "bob",
                Bytes::from_static(b"x"),
            ))
            .await
            .unwrap_err();
        assert!(matches!(err, postbox_core::PostboxError::InvalidAgentId(_)));
    }

    pub async fn send_oversized_payload_is_rejected(env: &TestEnv) {
        env.store
            .ensure_mailbox(cfg_with("alice", 10, 5, 64))
            .await
            .unwrap();
        let err = env
            .store
            .send(SendRequest::new(
                "alice",
                "bob",
                Bytes::from(vec![0u8; 65]),
            ))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            postbox_core::PostboxError::PayloadTooLarge { .. }
        ));
    }

    pub async fn send_to_mailbox_at_capacity_is_rejected(env: &TestEnv) {
        env.store.ensure_mailbox(cfg("alice", 1, 5)).await.unwrap();
        send_one(&*env.store, "alice", "bob").await;
        let err = env
            .store
            .send(SendRequest::new("alice", "carol", Bytes::from_static(b"x")))
            .await
            .unwrap_err();
        assert!(matches!(err, postbox_core::PostboxError::MailboxFull { .. }));
    }

    pub async fn peek_does_not_claim(env: &TestEnv) {
        send_one(&*env.store, "alice", "bob").await;
        let a = env.store.peek("alice", 10).await.unwrap();
        let b = env.store.peek("alice", 10).await.unwrap();
        assert_eq!(a.len(), 1);
        assert_eq!(b.len(), 1);
        assert_eq!(a[0].message_id, b[0].message_id);
    }

    pub async fn peek_empty_mailbox_returns_empty(env: &TestEnv) {
        env.store.ensure_mailbox(cfg("alice", 10, 5)).await.unwrap();
        let v = env.store.peek("alice", 10).await.unwrap();
        assert!(v.is_empty());
    }

    pub async fn claim_from_empty_mailbox_returns_none(env: &TestEnv) {
        env.store.ensure_mailbox(cfg("alice", 10, 5)).await.unwrap();
        let c = env
            .store
            .claim("alice", "consumer", Duration::from_secs(30))
            .await
            .unwrap();
        assert!(c.is_none());
    }

    pub async fn claim_makes_message_invisible_to_others(env: &TestEnv) {
        send_one(&*env.store, "alice", "bob").await;
        let c1 = env
            .store
            .claim("alice", "c1", Duration::from_secs(30))
            .await
            .unwrap();
        assert!(c1.is_some());
        let c2 = env
            .store
            .claim("alice", "c2", Duration::from_secs(30))
            .await
            .unwrap();
        assert!(c2.is_none());
    }

    pub async fn lease_expiry_without_commit_makes_message_reclaimable_and_increments_attempt_count(
        env: &TestEnv,
    ) {
        send_one(&*env.store, "alice", "bob").await;
        let c1 = env
            .store
            .claim("alice", "c1", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c1.message.attempt_count, 1);
        env.advance(Duration::from_secs(1));
        let reclaimed = env
            .store
            .sweep_expired_leases(env.clock.now())
            .await
            .unwrap();
        assert!(reclaimed >= 1);
        let c2 = env
            .store
            .claim("alice", "c2", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c2.message.attempt_count, 2);
    }

    pub async fn lease_expiry_without_reclaim_does_not_increment_attempt_count(env: &TestEnv) {
        send_one(&*env.store, "alice", "bob").await;
        let c1 = env
            .store
            .claim("alice", "c1", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c1.message.attempt_count, 1);
        env.advance(Duration::from_secs(1));
        let peek = env.store.peek("alice", 10).await.unwrap();
        assert!(
            peek.is_empty(),
            "expired lease should still hide message until reclaimed"
        );
    }

    pub async fn commit_with_empty_checkpoint_token_is_rejected(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        let _c = env
            .store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap();
        let err = env.store.commit(m.message_id, "c", "").await.unwrap_err();
        assert!(matches!(
            err,
            postbox_core::PostboxError::EmptyCheckpointToken(_)
        ));
    }

    pub async fn commit_only_by_claimer_succeeds(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        env.store
            .claim("alice", "claimer", Duration::from_secs(10))
            .await
            .unwrap();
        let err = env
            .store
            .commit(m.message_id, "imposter", "tok")
            .await
            .unwrap_err();
        assert!(matches!(err, postbox_core::PostboxError::NotClaimedByYou { .. }));
    }

    pub async fn commit_makes_message_invisible(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        env.store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap();
        env.store.commit(m.message_id, "c", "tok").await.unwrap();
        let peek = env.store.peek("alice", 10).await.unwrap();
        assert!(peek.is_empty());
        let claimed = env
            .store
            .claim("alice", "c2", Duration::from_secs(10))
            .await
            .unwrap();
        assert!(claimed.is_none());
    }

    pub async fn commit_populates_idempotency_ledger(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        env.store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap();
        assert!(!env.store.is_committed("alice", m.message_id).await.unwrap());
        env.store.commit(m.message_id, "c", "tok").await.unwrap();
        assert!(env.store.is_committed("alice", m.message_id).await.unwrap());
    }

    pub async fn commit_twice_is_rejected(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        env.store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap();
        env.store.commit(m.message_id, "c", "tok").await.unwrap();
        let err = env
            .store
            .commit(m.message_id, "c", "tok2")
            .await
            .unwrap_err();
        assert!(matches!(err, postbox_core::PostboxError::AlreadyCommitted(_)));
    }

    pub async fn release_transient_returns_to_pending(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        env.store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap();
        env.store
            .release(m.message_id, "c", FailureKind::Transient, Some("blip"))
            .await
            .unwrap();
        let peek = env.store.peek("alice", 10).await.unwrap();
        assert_eq!(peek.len(), 1);
        assert_eq!(peek[0].status, MessageStatus::Pending);
    }

    pub async fn release_only_by_claimer(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        env.store
            .claim("alice", "claimer", Duration::from_secs(10))
            .await
            .unwrap();
        let err = env
            .store
            .release(m.message_id, "imposter", FailureKind::Transient, None)
            .await
            .unwrap_err();
        assert!(matches!(err, postbox_core::PostboxError::NotClaimedByYou { .. }));
    }

    pub async fn release_permanent_moves_to_dlq(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        env.store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap();
        env.store
            .release(
                m.message_id,
                "c",
                FailureKind::Permanent,
                Some("bad"),
            )
            .await
            .unwrap();
        let peek = env.store.peek("alice", 10).await.unwrap();
        assert!(peek.is_empty());
        let dlq = env
            .store
            .list_dead_letters("alice", None, 100)
            .await
            .unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].message_id, m.message_id);
        assert_eq!(dlq[0].poison_reason, PoisonReason::PermanentFailure);
    }

    pub async fn max_attempts_one_dead_letters_on_first_permanent(env: &TestEnv) {
        env.store.ensure_mailbox(cfg("alice", 10, 1)).await.unwrap();
        let m = send_one(&*env.store, "alice", "bob").await;
        env.store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap();
        env.store
            .release(m.message_id, "c", FailureKind::Permanent, None)
            .await
            .unwrap();
        let dlq = env
            .store
            .list_dead_letters("alice", None, 100)
            .await
            .unwrap();
        assert_eq!(dlq.len(), 1);
    }

    pub async fn reject_validation_moves_to_dlq(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        env.store
            .reject_validation(m.message_id, "schema mismatch")
            .await
            .unwrap();
        let dlq = env
            .store
            .list_dead_letters("alice", Some(PoisonReason::ValidationFailed), 100)
            .await
            .unwrap();
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].message_id, m.message_id);
    }

    pub async fn reject_validation_after_claim_is_rejected(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        env.store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap();
        let err = env
            .store
            .reject_validation(m.message_id, "too late")
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            postbox_core::PostboxError::MessageNotClaimable { .. }
        ));
    }

    pub async fn replay_dead_letter_creates_new_message_with_zero_attempts(env: &TestEnv) {
        let m = send_one(&*env.store, "alice", "bob").await;
        env.store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap();
        env.store
            .release(m.message_id, "c", FailureKind::Permanent, None)
            .await
            .unwrap();
        let replayed = env
            .store
            .replay_dead_letter(m.message_id, None, "ops")
            .await
            .unwrap();
        assert_ne!(replayed.message_id, m.message_id);
        assert_eq!(replayed.attempt_count, 0);
        assert_eq!(
            replayed.headers.get("replayed_from").unwrap(),
            &m.message_id.to_string()
        );
        assert_eq!(replayed.headers.get("replayed_by").unwrap(), "ops");
        let dlq = env
            .store
            .list_dead_letters("alice", None, 100)
            .await
            .unwrap();
        assert_eq!(dlq.len(), 1);
        let claim = env
            .store
            .claim("alice", "c2", Duration::from_secs(10))
            .await
            .unwrap();
        assert!(claim.is_some());
        assert_eq!(claim.unwrap().message.message_id, replayed.message_id);
    }

    pub async fn fifo_ordering_holds_across_redelivery(env: &TestEnv) {
        env.store.ensure_mailbox(cfg("alice", 100, 5)).await.unwrap();
        let m1 = send_one(&*env.store, "alice", "alice").await;
        let m2 = send_one(&*env.store, "alice", "alice").await;
        let m3 = send_one(&*env.store, "alice", "alice").await;
        let c1 = env
            .store
            .claim("alice", "c", Duration::from_millis(50))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c1.message.message_id, m1.message_id);
        env.advance(Duration::from_millis(1100));
        env.store
            .release(m1.message_id, "c", FailureKind::Transient, None)
            .await
            .unwrap();
        let c2 = env
            .store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c2.message.message_id, m1.message_id);
        env.store.commit(m1.message_id, "c", "tok1").await.unwrap();
        let c3 = env
            .store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c3.message.message_id, m2.message_id);
        env.store.commit(m2.message_id, "c", "tok2").await.unwrap();
        let c4 = env
            .store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c4.message.message_id, m3.message_id);
    }

    pub async fn unordered_mailbox_returns_all_messages(env: &TestEnv) {
        env.store
            .ensure_mailbox(MailboxConfig {
                ordering_mode: OrderingMode::Unordered,
                ..cfg("alice", 100, 5)
            })
            .await
            .unwrap();
        send_one(&*env.store, "alice", "alice").await;
        env.advance(Duration::from_millis(5));
        send_one(&*env.store, "alice", "alice").await;
        env.advance(Duration::from_millis(5));
        send_one(&*env.store, "alice", "alice").await;
        let claimed: Vec<Ulid> = futures::future::join_all((0..3).map(|_| async {
            env.store
                .claim("alice", "c", Duration::from_secs(10))
                .await
                .unwrap()
                .unwrap()
                .message
                .message_id
        }))
        .await;
        assert_eq!(claimed.len(), 3);
        let set: std::collections::HashSet<_> = claimed.iter().collect();
        assert_eq!(set.len(), 3);
    }

    pub async fn list_dead_letters_filters_by_reason(env: &TestEnv) {
        let m1 = send_one(&*env.store, "alice", "bob").await;
        env.store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap();
        env.store
            .release(m1.message_id, "c", FailureKind::Permanent, None)
            .await
            .unwrap();
        let m2 = send_one(&*env.store, "alice", "bob").await;
        env.store
            .reject_validation(m2.message_id, "bad")
            .await
            .unwrap();
        let all = env
            .store
            .list_dead_letters("alice", None, 100)
            .await
            .unwrap();
        assert_eq!(all.len(), 2);
        let perm = env
            .store
            .list_dead_letters("alice", Some(PoisonReason::PermanentFailure), 100)
            .await
            .unwrap();
        assert_eq!(perm.len(), 1);
        let val = env
            .store
            .list_dead_letters("alice", Some(PoisonReason::ValidationFailed), 100)
            .await
            .unwrap();
        assert_eq!(val.len(), 1);
    }

    pub async fn claim_unknown_mailbox_is_error(env: &TestEnv) {
        let err = env
            .store
            .claim("ghost", "c", Duration::from_secs(10))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            postbox_core::PostboxError::MailboxNotFound { .. }
        ));
    }

    pub async fn commit_unknown_message_is_error(env: &TestEnv) {
        let err = env
            .store
            .commit(Ulid::new(), "c", "tok")
            .await
            .unwrap_err();
        assert!(matches!(err, postbox_core::PostboxError::MessageNotFound(_)));
    }

    pub async fn lease_duration_zero_is_allowed(env: &TestEnv) {
        let _ = send_one(&*env.store, "alice", "bob").await;
        let c = env
            .store
            .claim("alice", "c", Duration::from_secs(0))
            .await
            .unwrap();
        assert!(c.is_some());
        let c2 = env
            .store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap();
        assert!(c2.is_some());
        assert_eq!(c.unwrap().message.message_id, c2.unwrap().message.message_id);
    }

    pub async fn payload_at_exact_cap_is_accepted(env: &TestEnv) {
        env.store
            .ensure_mailbox(cfg_with("alice", 10, 5, 64))
            .await
            .unwrap();
        let m = env
            .store
            .send(SendRequest::new(
                "alice",
                "bob",
                Bytes::from(vec![0u8; 64]),
            ))
            .await
            .unwrap();
        assert_eq!(m.payload.len(), 64);
        let err = env
            .store
            .send(SendRequest::new(
                "alice",
                "bob",
                Bytes::from(vec![0u8; 65]),
            ))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            postbox_core::PostboxError::PayloadTooLarge { .. }
        ));
    }

    pub async fn fifo_per_sender_round_robin(env: &TestEnv) {
        env.store.ensure_mailbox(cfg("alice", 100, 5)).await.unwrap();
        let a1 = send_one(&*env.store, "alice", "sender_a").await;
        let a2 = send_one(&*env.store, "alice", "sender_a").await;
        env.advance(Duration::from_millis(5));
        let b1 = send_one(&*env.store, "alice", "sender_b").await;
        let c1 = env
            .store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c1.message.sender_id, "sender_a");
        let c2 = env
            .store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c2.message.sender_id, "sender_a");
        let c3 = env
            .store
            .claim("alice", "c", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(c3.message.sender_id, "sender_b");
        env.store.commit(a1.message_id, "c", "tok").await.unwrap();
        env.store.commit(a2.message_id, "c", "tok").await.unwrap();
        env.store.commit(b1.message_id, "c", "tok").await.unwrap();
    }

    pub async fn unknown_message_release_is_error(env: &TestEnv) {
        let err = env
            .store
            .release(Ulid::new(), "c", FailureKind::Transient, None)
            .await
            .unwrap_err();
        assert!(matches!(err, postbox_core::PostboxError::MessageNotFound(_)));
    }

    /// Spawn many concurrent claimers against one mailbox. Assert every
    /// message is committed exactly once.
    pub async fn concurrent_claimers_never_double_claim_a_message_while_lease_is_active(
        env: Arc<TestEnv>,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};

        env.store.ensure_mailbox(cfg("alice", 1000, 5)).await.unwrap();

        let n_msgs = 100usize;
        let mut sent = Vec::with_capacity(n_msgs);
        for _ in 0..n_msgs {
            sent.push(send_one(&*env.store, "alice", "bob").await.message_id);
        }

        let n_claimers = 16usize;
        let committed_count = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..n_claimers {
            let env = env.clone();
            let count = committed_count.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    match env
                        .store
                        .claim("alice", "claimer", Duration::from_secs(30))
                        .await
                    {
                        Ok(Some(claim)) => {
                            tokio::task::yield_now().await;
                            env.store
                                .commit(claim.message.message_id, "claimer", "tok")
                                .await
                                .unwrap();
                            count.fetch_add(1, Ordering::SeqCst);
                        }
                        Ok(None) => break,
                        Err(_) => panic!("unexpected claim error"),
                    }
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(committed_count.load(Ordering::SeqCst), n_msgs);
        for id in &sent {
            assert!(env.store.is_committed("alice", *id).await.unwrap());
        }
    }
}

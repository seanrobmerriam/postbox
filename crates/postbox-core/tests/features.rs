//! Tests covering the new features from FEATURE_PLAN.md:
//! 1. Priority ordering
//! 2. TTL / message expiration
//! 3. Fanout send
//! 4. Admin / inspection API
//! 6. DLQ retention / purge

mod common;
use crate::common::{cfg_with, Backend, TestEnv};
use bytes::Bytes;
use postbox_core::{
    Clock, FailureKind, FanoutRequest, MailboxStore, MessageStatus, OrderingMode, PoisonReason,
    SendRequest,
};
use std::time::Duration;

fn mem_env() -> TestEnv {
    TestEnv::new(Backend::Memory)
}

// --- Feature 1: priority ordering ----------------------------------------

#[tokio::test]
async fn priority_mode_delivers_highest_priority_first_mem() {
    let env = mem_env();
    let mut cfg = cfg_with("alice", 100, 100, 1024);
    cfg.ordering_mode = OrderingMode::Priority;
    env.store.ensure_mailbox(cfg).await.unwrap();

    env.store
        .send(SendRequest::new("alice", "carol", Bytes::from_static(b"low-mid")).with_priority(2))
        .await
        .unwrap();
    env.store
        .send(SendRequest::new("alice", "bob", Bytes::from_static(b"high")).with_priority(10))
        .await
        .unwrap();
    env.store
        .send(SendRequest::new("alice", "alice", Bytes::from_static(b"low")).with_priority(1))
        .await
        .unwrap();

    let claimed = env
        .store
        .claim("alice", "worker", Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.message.payload.as_ref(), b"high");
}

#[tokio::test]
async fn priority_mode_equal_band_is_fifo_mem() {
    let env = mem_env();
    let mut cfg = cfg_with("queue", 100, 100, 1024);
    cfg.ordering_mode = OrderingMode::Priority;
    env.store.ensure_mailbox(cfg).await.unwrap();

    env.store
        .send(SendRequest::new("queue", "a", Bytes::from_static(b"first-equal")).with_priority(5))
        .await
        .unwrap();
    env.store
        .send(SendRequest::new("queue", "b", Bytes::from_static(b"second-equal")).with_priority(5))
        .await
        .unwrap();
    env.store
        .send(SendRequest::new("queue", "c", Bytes::from_static(b"lower")).with_priority(1))
        .await
        .unwrap();

    let m1 = env.store.claim("queue", "w", Duration::from_secs(5)).await.unwrap().unwrap();
    let m2 = env.store.claim("queue", "w", Duration::from_secs(5)).await.unwrap().unwrap();
    // The first claim should be the equal-band head, then the lower-priority one.
    assert_eq!(m1.message.payload.as_ref(), b"first-equal");
    assert_eq!(m2.message.payload.as_ref(), b"second-equal");
}

// --- Feature 2: TTL / expiration -----------------------------------------

#[tokio::test]
async fn ttl_none_means_no_expires_at() {
    let env = mem_env();
    env.store
        .ensure_mailbox(cfg_with("q", 100, 100, 1024))
        .await
        .unwrap();
    let m = env
        .store
        .send(SendRequest::new("q", "a", Bytes::from_static(b"data")))
        .await
        .unwrap();
    assert!(m.expires_at.is_none());
}

#[tokio::test]
async fn ttl_sets_expires_at() {
    let env = mem_env();
    env.store
        .ensure_mailbox(cfg_with("q", 100, 100, 1024))
        .await
        .unwrap();
    let m = env
        .store
        .send(
            SendRequest::new("q", "a", Bytes::from_static(b"data"))
                .with_ttl(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    assert!(m.expires_at.is_some());
}

#[tokio::test]
async fn sweep_expired_messages_moves_pending_to_dlq() {
    let env = mem_env();
    env.store
        .ensure_mailbox(cfg_with("q", 100, 100, 1024))
        .await
        .unwrap();
    // Send a message that expires almost immediately.
    let mut req = SendRequest::new("q", "a", Bytes::from_static(b"data"));
    req.ttl = Some(Duration::from_millis(50));
    env.store.send(req).await.unwrap();

    // Jump the mock clock well past the deadline.
    env.clock.advance(Duration::from_secs(120));
    let now = env.clock.now();
    let swept = env.store.sweep_expired_messages(now).await.unwrap();
    assert_eq!(swept, 1);

    let dlq = env.store.list_dead_letters("q", None, 10).await.unwrap();
    assert_eq!(dlq.len(), 1);
    assert!(matches!(dlq[0].poison_reason, PoisonReason::Expired));
}

#[tokio::test]
async fn claimed_messages_are_not_swept() {
    let env = mem_env();
    env.store
        .ensure_mailbox(cfg_with("q", 100, 100, 1024))
        .await
        .unwrap();
    let mut req = SendRequest::new("q", "a", Bytes::from_static(b"data"));
    req.ttl = Some(Duration::from_millis(50));
    env.store.send(req).await.unwrap();
    // Claim it so it leaves the pending state.
    env.store
        .claim("q", "w", Duration::from_secs(120))
        .await
        .unwrap()
        .unwrap();

    env.clock.advance(Duration::from_secs(120));
    let swept = env.store.sweep_expired_messages(env.clock.now()).await.unwrap();
    assert_eq!(swept, 0, "claimed messages must not be swept");
    // DLQ must be empty: the claimed message was not dead-lettered.
    let dlq = env.store.list_dead_letters("q", None, 10).await.unwrap();
    assert_eq!(dlq.len(), 0);
    // Mailbox still reports 1 active (claimed) message.
    let stats = env.store.mailbox_stats("q").await.unwrap();
    assert_eq!(stats.claimed_count, 1);
    assert_eq!(stats.pending_count, 0);
    assert_eq!(stats.dead_lettered_count, 0);
}

// --- Feature 3: fanout ---------------------------------------------------

#[tokio::test]
async fn fanout_to_multiple_mailboxes_mem() {
    let env = mem_env();
    for a in ["alice", "bob", "carol"] {
        env.store
            .ensure_mailbox(cfg_with(a, 100, 100, 1024))
            .await
            .unwrap();
    }
    let req = FanoutRequest {
        targets: vec!["alice".into(), "bob".into(), "carol".into()],
        sender_id: "ops".into(),
        payload: Bytes::from_static(b"broadcast"),
        headers: Default::default(),
        priority: 0,
        delay: None,
        ttl: None,
    };
    let messages = env.store.fanout_send(req).await.unwrap();
    assert_eq!(messages.len(), 3);
    // All targets received identical payloads (no implicit ordering across
    // targets). Distinct targets should produce distinct message_ids, so
    // even though the MockClock is frozen the monotonic ULID generator
    // yields three different ULIDs.
    let ids: std::collections::HashSet<_> = messages.iter().map(|m| m.message_id).collect();
    assert_eq!(ids.len(), 3, "expected 3 distinct ids, got {ids:?}");
    for m in &messages {
        assert_eq!(m.payload.as_ref(), b"broadcast");
    }
    for a in ["alice", "bob", "carol"] {
        let q = env.store.peek(a, 10).await.unwrap();
        assert_eq!(q.len(), 1);
    }
}

#[tokio::test]
async fn fanout_rolls_back_when_one_target_is_full_mem() {
    let env = mem_env();
    let mut alice_cfg = cfg_with("alice", 1, 100, 1024);
    alice_cfg.capacity = 1;
    env.store.ensure_mailbox(alice_cfg).await.unwrap();
    env.store
        .ensure_mailbox(cfg_with("bob", 100, 100, 1024))
        .await
        .unwrap();
    env.store
        .send(SendRequest::new("alice", "ops", Bytes::from_static(b"existing")))
        .await
        .unwrap();

    let req = FanoutRequest {
        targets: vec!["alice".into(), "bob".into()],
        sender_id: "ops".into(),
        payload: Bytes::from_static(b"x"),
        headers: Default::default(),
        priority: 0,
        delay: None,
        ttl: None,
    };
    let err = env.store.fanout_send(req).await.err().expect("must fail");
    let s = format!("{err:?}");
    assert!(s.contains("MailboxFull"), "got {s}");

    // Bob was not sent anything because the fanout is atomic.
    let bob = env.store.peek("bob", 10).await.unwrap();
    assert_eq!(bob.len(), 0);
}

// --- Feature 4: admin / inspection ---------------------------------------

#[tokio::test]
async fn list_mailboxes_returns_sorted_with_pagination_mem() {
    let env = mem_env();
    for a in ["charlie", "alpha", "bravo"] {
        env.store
            .ensure_mailbox(cfg_with(a, 100, 100, 1024))
            .await
            .unwrap();
    }

    let page1 = env.store.list_mailboxes(2, None).await.unwrap();
    assert_eq!(page1.len(), 2);
    assert_eq!(page1[0].agent_id, "alpha");
    assert_eq!(page1[1].agent_id, "bravo");

    let page2 = env.store.list_mailboxes(10, Some("bravo")).await.unwrap();
    assert_eq!(page2.len(), 1);
    assert_eq!(page2[0].agent_id, "charlie");
}

#[tokio::test]
async fn mailbox_stats_counts_match_state_mem() {
    let env = mem_env();
    env.store
        .ensure_mailbox(cfg_with("q", 100, 100, 1024))
        .await
        .unwrap();

    env.store
        .send(SendRequest::new("q", "a", Bytes::from_static(b"old")))
        .await
        .unwrap();
    env.store
        .send(SendRequest::new("q", "a", Bytes::from_static(b"new")))
        .await
        .unwrap();
    let stats = env.store.mailbox_stats("q").await.unwrap();
    assert_eq!(stats.agent_id, "q");
    assert_eq!(stats.pending_count, 2);
    assert_eq!(stats.claimed_count, 0);
    assert_eq!(stats.committed_count, 0);
    assert_eq!(stats.dead_lettered_count, 0);
    assert!(stats.oldest_pending_at.is_some());
}

// --- Feature 6: DLQ retention / purge -----------------------------------

#[tokio::test]
async fn purge_dead_letters_with_future_cutoff_deletes_all_mem() {
    let env = mem_env();
    env.store
        .ensure_mailbox(cfg_with("q", 100, 100, 1024))
        .await
        .unwrap();

    let m = env
        .store
        .send(SendRequest::new("q", "a", Bytes::from_static(b"x")))
        .await
        .unwrap();
    env.store
        .claim("q", "w", Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    env.store
        .release(m.message_id, "w", FailureKind::Permanent, None)
        .await
        .unwrap();

    // Use a clock-based future cutoff so the path is deterministic.
    let future = env.clock.now() + Duration::from_secs(60);
    let deleted = env.store.purge_dead_letters("q", future).await.unwrap();
    assert_eq!(deleted, 1);
}

#[tokio::test]
async fn purge_dead_letters_with_past_cutoff_is_noop_mem() {
    let env = mem_env();
    env.store
        .ensure_mailbox(cfg_with("q", 100, 100, 1024))
        .await
        .unwrap();

    let m = env
        .store
        .send(SendRequest::new("q", "a", Bytes::from_static(b"x")))
        .await
        .unwrap();
    env.store
        .claim("q", "w", Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    env.store
        .release(m.message_id, "w", FailureKind::Permanent, None)
        .await
        .unwrap();

    // Cutoff is in the past relative to when the row was dead-lettered:
    // nothing should be purged.
    let past = env.clock.now() - Duration::from_secs(60);
    let deleted = env.store.purge_dead_letters("q", past).await.unwrap();
    assert_eq!(deleted, 0);
}

// --- Mixed scenarios also exercised against Sqlite backend ---------------

fn sqlite_env() -> TestEnv {
    TestEnv::new(Backend::Sqlite)
}

#[tokio::test]
async fn priority_mode_delivers_highest_first_sqlite() {
    let env = sqlite_env();
    let mut cfg = cfg_with("alice", 100, 100, 1024);
    cfg.ordering_mode = OrderingMode::Priority;
    env.store.ensure_mailbox(cfg).await.unwrap();

    env.store
        .send(SendRequest::new("alice", "a", Bytes::from_static(b"low")).with_priority(1))
        .await
        .unwrap();
    env.store
        .send(SendRequest::new("alice", "b", Bytes::from_static(b"high")).with_priority(10))
        .await
        .unwrap();

    let claimed = env
        .store
        .claim("alice", "worker", Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.message.payload.as_ref(), b"high");
}

#[tokio::test]
async fn fanout_to_multiple_mailboxes_sqlite() {
    let env = sqlite_env();
    for a in ["alice", "bob"] {
        env.store
            .ensure_mailbox(cfg_with(a, 100, 100, 1024))
            .await
            .unwrap();
    }
    let req = FanoutRequest {
        targets: vec!["alice".into(), "bob".into()],
        sender_id: "ops".into(),
        payload: Bytes::from_static(b"broadcast"),
        headers: Default::default(),
        priority: 0,
        delay: None,
        ttl: None,
    };
    let messages = env.store.fanout_send(req).await.unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(env.store.peek("alice", 10).await.unwrap().len(), 1);
    assert_eq!(env.store.peek("bob", 10).await.unwrap().len(), 1);
}

#[tokio::test]
async fn list_mailboxes_sqlite() {
    let env = sqlite_env();
    for a in ["alpha", "bravo"] {
        env.store
            .ensure_mailbox(cfg_with(a, 100, 100, 1024))
            .await
            .unwrap();
    }
    let list = env.store.list_mailboxes(10, None).await.unwrap();
    assert!(list.len() >= 2);
    assert_eq!(list[0].agent_id, "alpha");
    assert_eq!(list[1].agent_id, "bravo");
}

#[tokio::test]
async fn mailbox_stats_sqlite() {
    let env = sqlite_env();
    env.store
        .ensure_mailbox(cfg_with("q", 100, 100, 1024))
        .await
        .unwrap();
    env.store
        .send(SendRequest::new("q", "a", Bytes::from_static(b"x")))
        .await
        .unwrap();
    let stats = env.store.mailbox_stats("q").await.unwrap();
    assert_eq!(stats.pending_count, 1);
}

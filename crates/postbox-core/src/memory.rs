//! In-memory [`MailboxStore`] used as the fast fake for unit tests and the
//! shared property-test harness.
//!
//! All state lives behind a `parking_lot::Mutex` so concurrent callers see
//! the same serialized view as SQLite would produce on a single connection.
//!
//! This implementation is the canonical reference for what the SQLite
//! backend must match. The proptest harness in `tests/properties.rs` drives
//! both backends and asserts the same invariants hold against either.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use parking_lot::Mutex;
use ulid::{Generator, Ulid};

use crate::clock::Clock;
use crate::error::PostboxError;
use crate::store::MailboxStore;
use crate::types::{
    validate_agent_id, Claim, DeadLetter, FailureKind, FailureRecord, Mailbox, MailboxConfig,
    Message, MessageStatus, OrderingMode, PoisonReason, SendRequest,
};

/// Convert the `Clock`'s `SystemTime` to a sortable `i64` millisecond epoch.
fn st_to_ms(t: SystemTime) -> i64 {
    crate::types::system_time_to_millis(t)
}

/// Convert back to `SystemTime`. Unused in current code path but kept as a
/// symmetric helper so callers/tests have a stable inverse at hand.
#[allow(dead_code)]
fn ms_to_st(ms: i64) -> SystemTime {
    crate::types::millis_to_system_time(ms)
}

/// Internal state for the in-memory backend.
#[derive(Debug, Default)]
struct State {
    mailboxes: BTreeMap<String, Mailbox>,
    /// Active messages keyed by `message_id`. Includes `pending`, `claimed`,
    /// and any historical message still in the active table. Committed and
    /// dead-lettered messages are removed from this map; their record lives
    /// in `idempotency` / `dead_letters`.
    messages: BTreeMap<Ulid, Message>,
    dead_letters: BTreeMap<Ulid, DeadLetter>,
    /// Idempotency ledger: which `(mailbox_id, message_id)` tuples have been
    /// committed at least once.
    idempotency: BTreeSet<(String, Ulid)>,
}

/// In-memory [`MailboxStore`].
pub struct MemoryStore {
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
    /// Monotonic ULID generator so successive messages in the same
    /// millisecond are strictly ordered — required for FIFO determinism.
    ulid_gen: Mutex<Generator>,
}

impl MemoryStore {
    /// Construct an empty store.
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            state: Mutex::new(State::default()),
            ulid_gen: Mutex::new(Generator::new()),
        }
    }

    /// Test-only helper: the current clock.
    pub fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }

    fn next_ulid(&self) -> Ulid {
        // Always use the clock time so the ULID reflects the "real" send
        // time; the generator's monotonic increment then guarantees
        // strictly increasing ULIDs even within the same millisecond.
        let now = self.clock.now();
        let mut gen = self.ulid_gen.lock();
        gen.generate_from_datetime(now)
            .expect("monotonic ULID generator exhausted random bits")
    }
}

#[async_trait]
impl MailboxStore for MemoryStore {
    async fn ensure_mailbox(&self, config: MailboxConfig) -> Result<Mailbox, PostboxError> {
        validate_agent_id(&config.agent_id)?;
        let mut state = self.state.lock();
        if let Some(existing) = state.mailboxes.get(&config.agent_id) {
            return Ok(existing.clone());
        }
        let m = Mailbox {
            agent_id: config.agent_id.clone(),
            capacity: config.capacity,
            ordering_mode: config.ordering_mode,
            max_attempts: config.max_attempts,
            lease_duration: config.lease_duration,
            max_payload_bytes: config.max_payload_bytes,
            created_at: self.clock.now(),
        };
        state.mailboxes.insert(m.agent_id.clone(), m.clone());
        Ok(m)
    }

    async fn get_mailbox(&self, agent_id: &str) -> Result<Option<Mailbox>, PostboxError> {
        validate_agent_id(agent_id)?;
        Ok(self.state.lock().mailboxes.get(agent_id).cloned())
    }

    async fn send(&self, req: SendRequest) -> Result<Message, PostboxError> {
        validate_agent_id(&req.target_mailbox)?;
        validate_agent_id(&req.sender_id)?;
        if req.payload.is_empty() {
            // We allow empty payloads but the storage does not depend on it.
        }

        let mut state = self.state.lock();

        // Implicit mailbox creation with defaults.
        let mailbox = if let Some(m) = state.mailboxes.get(&req.target_mailbox) {
            m.clone()
        } else {
            let cfg = MailboxConfig::defaults_for(req.target_mailbox.clone());
            let m = Mailbox {
                agent_id: cfg.agent_id.clone(),
                capacity: cfg.capacity,
                ordering_mode: cfg.ordering_mode,
                max_attempts: cfg.max_attempts,
                lease_duration: cfg.lease_duration,
                max_payload_bytes: cfg.max_payload_bytes,
                created_at: self.clock.now(),
            };
            state.mailboxes.insert(m.agent_id.clone(), m.clone());
            m
        };

        if req.payload.len() > mailbox.max_payload_bytes {
            return Err(PostboxError::PayloadTooLarge {
                size: req.payload.len(),
                max: mailbox.max_payload_bytes,
            });
        }

        // Capacity check: count of pending + claimed messages in this mailbox.
        let active = state
            .messages
            .values()
            .filter(|m| {
                m.mailbox_id == mailbox.agent_id
                    && matches!(
                        m.status,
                        MessageStatus::Pending | MessageStatus::Claimed
                    )
            })
            .count();
        if active >= mailbox.capacity {
            return Err(PostboxError::MailboxFull {
                agent_id: mailbox.agent_id.clone(),
                size: active,
                capacity: mailbox.capacity,
            });
        }

        let now = self.clock.now();
        let created_at = now;
        let visible_at = req.delay.map(|d| now + d).unwrap_or(now);
        let message_id = self.next_ulid();

        let msg = Message {
            message_id,
            mailbox_id: mailbox.agent_id.clone(),
            sender_id: req.sender_id,
            payload: req.payload,
            headers: req.headers,
            priority: req.priority,
            created_at,
            visible_at,
            status: MessageStatus::Pending,
            attempt_count: 0,
            lease_expires_at: None,
            claimed_by: None,
            committed_at: None,
            checkpoint_token: None,
        };
        state.messages.insert(message_id, msg.clone());
        Ok(msg)
    }

    async fn peek(
        &self,
        mailbox_id: &str,
        max: usize,
    ) -> Result<Vec<Message>, PostboxError> {
        validate_agent_id(mailbox_id)?;
        let now = self.clock.now();
        let state = self.state.lock();
        let mailbox = match state.mailboxes.get(mailbox_id) {
            Some(m) => m.clone(),
            None => return Err(PostboxError::MailboxNotFound {
                agent_id: mailbox_id.to_string(),
            }),
        };
        let mut out: Vec<Message> = state
            .messages
            .values()
            .filter(|m| {
                m.mailbox_id == mailbox.agent_id
                    && m.status == MessageStatus::Pending
                    && m.visible_at <= now
            })
            .cloned()
            .collect();
        // Order per the mailbox's mode. FIFO: oldest sender first, then
        // within sender by created_at. Unordered: globally oldest first.
        match mailbox.ordering_mode {
            OrderingMode::Fifo => {
                out.sort_by(|a, b| {
                    a.sender_id
                        .cmp(&b.sender_id)
                        .then(a.created_at.cmp(&b.created_at))
                        .then(a.message_id.cmp(&b.message_id))
                });
            }
            OrderingMode::Unordered => {
                out.sort_by(|a, b| {
                    a.created_at
                        .cmp(&b.created_at)
                        .then(a.message_id.cmp(&b.message_id))
                });
            }
        }
        out.truncate(max);
        Ok(out)
    }

    async fn claim(
        &self,
        mailbox_id: &str,
        claimer_id: &str,
        lease_duration: Duration,
    ) -> Result<Option<Claim>, PostboxError> {
        validate_agent_id(mailbox_id)?;
        validate_agent_id(claimer_id)?;
        if lease_duration.is_zero() {
            // Allowed but the claimer must immediately process; lease
            // expiration is checked in subsequent claims and sweeps.
        }
        let now = self.clock.now();
        let mut state = self.state.lock();
        let mailbox = state
            .mailboxes
            .get(mailbox_id)
            .cloned()
            .ok_or_else(|| PostboxError::MailboxNotFound {
                agent_id: mailbox_id.to_string(),
            })?;

        // First, walk over any stale claimed messages so they don't block
        // forever. This mirrors what the sweeper would do — but we keep the
        // critical section short, so doing it inline is fine and removes
        // the chance of a `claim()` returning nothing while an expired
        // lease still occupies the slot.
        let mut reclaimed: HashMap<Ulid, ()> = HashMap::new();
        for m in state.messages.values_mut() {
            if m.mailbox_id == mailbox.agent_id
                && m.status == MessageStatus::Claimed
                && m.lease_expires_at
                    .map(|e| e <= now)
                    .unwrap_or(false)
            {
                m.status = MessageStatus::Pending;
                m.lease_expires_at = None;
                m.claimed_by = None;
                reclaimed.insert(m.message_id, ());
            }
        }

        // Filter and order visible candidates.
        let mut candidates: Vec<Message> = state
            .messages
            .values()
            .filter(|m| {
                m.mailbox_id == mailbox.agent_id
                    && m.status == MessageStatus::Pending
                    && m.visible_at <= now
            })
            .cloned()
            .collect();
        if candidates.is_empty() {
            return Ok(None);
        }
        match mailbox.ordering_mode {
            OrderingMode::Fifo => {
                // Per-sender FIFO: pick the sender with the oldest earliest
                // message, then within that sender the oldest message.
                let sender_earliest: BTreeMap<String, (SystemTime, Ulid)> = {
                    let mut acc: BTreeMap<String, (SystemTime, Ulid)> = BTreeMap::new();
                    for m in &candidates {
                        let entry = acc
                            .entry(m.sender_id.clone())
                            .or_insert((m.created_at, m.message_id));
                        if (m.created_at, m.message_id) < *entry {
                            *entry = (m.created_at, m.message_id);
                        }
                    }
                    acc
                };
                let oldest_sender = sender_earliest
                    .iter()
                    .min_by(|a, b| a.1.cmp(b.1))
                    .map(|(k, _)| k.clone())
                    .unwrap();
                candidates.retain(|m| m.sender_id == oldest_sender);
                candidates.sort_by(|a, b| {
                    a.created_at
                        .cmp(&b.created_at)
                        .then(a.message_id.cmp(&b.message_id))
                });
            }
            OrderingMode::Unordered => {
                candidates.sort_by(|a, b| {
                    a.created_at
                        .cmp(&b.created_at)
                        .then(a.message_id.cmp(&b.message_id))
                });
            }
        }

        let target = candidates.into_iter().next().unwrap();
        let lease_expires_at = now + lease_duration;

        // Atomic write.
        let entry = state.messages.get_mut(&target.message_id).unwrap();
        entry.status = MessageStatus::Claimed;
        entry.attempt_count = entry.attempt_count.saturating_add(1);
        entry.lease_expires_at = Some(lease_expires_at);
        entry.claimed_by = Some(claimer_id.to_string());

        Ok(Some(Claim {
            message: entry.clone(),
            lease_expires_at,
        }))
    }

    async fn commit(
        &self,
        message_id: Ulid,
        claimer_id: &str,
        checkpoint_token: &str,
    ) -> Result<(), PostboxError> {
        validate_agent_id(claimer_id)?;
        if checkpoint_token.is_empty() {
            return Err(PostboxError::EmptyCheckpointToken(message_id));
        }
        let mut state = self.state.lock();
        let m = state
            .messages
            .get(&message_id)
            .cloned()
            .ok_or(PostboxError::MessageNotFound(message_id))?;
        match m.status {
            MessageStatus::Committed => {
                return Err(PostboxError::AlreadyCommitted(message_id));
            }
            MessageStatus::DeadLettered => {
                return Err(PostboxError::MessageNotFound(message_id));
            }
            _ => {}
        }
        if m.status != MessageStatus::Claimed {
            return Err(PostboxError::MessageNotClaimed(message_id));
        }
        match &m.claimed_by {
            Some(c) if c == claimer_id => {}
            Some(claimer) => {
                return Err(PostboxError::NotClaimedByYou {
                    message_id,
                    claimer: claimer.clone(),
                    caller: claimer_id.to_string(),
                });
            }
            None => return Err(PostboxError::MessageNotClaimed(message_id)),
        }
        let now = self.clock.now();
        let entry = state.messages.get_mut(&message_id).unwrap();
        entry.status = MessageStatus::Committed;
        entry.committed_at = Some(now);
        entry.checkpoint_token = Some(checkpoint_token.to_string());
        entry.lease_expires_at = None;
        entry.claimed_by = None;
        state
            .idempotency
            .insert((m.mailbox_id, m.message_id));
        Ok(())
    }

    async fn release(
        &self,
        message_id: Ulid,
        claimer_id: &str,
        failure: FailureKind,
        note: Option<&str>,
    ) -> Result<(), PostboxError> {
        validate_agent_id(claimer_id)?;
        let mut state = self.state.lock();
        let m = state
            .messages
            .get(&message_id)
            .cloned()
            .ok_or(PostboxError::MessageNotFound(message_id))?;
        if m.status != MessageStatus::Claimed {
            return Err(PostboxError::MessageNotClaimed(message_id));
        }
        match &m.claimed_by {
            Some(c) if c == claimer_id => {}
            Some(claimer) => {
                return Err(PostboxError::NotClaimedByYou {
                    message_id,
                    claimer: claimer.clone(),
                    caller: claimer_id.to_string(),
                });
            }
            None => return Err(PostboxError::MessageNotClaimed(message_id)),
        }

        let now = self.clock.now();
        match failure {
            FailureKind::Transient => {
                // Stash a failure record for DLQ if it ever ends up there,
                // but for now just return to pending.
                let entry = state.messages.get_mut(&message_id).unwrap();
                entry.status = MessageStatus::Pending;
                entry.lease_expires_at = None;
                entry.claimed_by = None;
                // Note: transient-failure history is not persisted as a
                // FailureRecord here. To preserve it we would attach it
                // to a side table keyed on `message_id`. Out of scope for
                // now; the DLQ record (when reached via permanent
                // failure / max attempts) carries the relevant history.
                let _ = note;
            }
            FailureKind::Permanent => {
                // Move to DLQ immediately.
                let entry = state.messages.remove(&message_id).unwrap();
                let record = FailureRecord {
                    attempt: entry.attempt_count,
                    claimed_by: Some(claimer_id.to_string()),
                    failure_kind: FailureKind::Permanent,
                    note: note.map(str::to_string),
                    at: now,
                };
                let dead = DeadLetter {
                    message_id: entry.message_id,
                    mailbox_id: entry.mailbox_id.clone(),
                    sender_id: entry.sender_id,
                    payload: entry.payload,
                    headers: entry.headers,
                    priority: entry.priority,
                    created_at: entry.created_at,
                    attempt_count: entry.attempt_count,
                    failure_history: vec![record],
                    poison_reason: PoisonReason::PermanentFailure,
                    dead_lettered_at: now,
                };
                state.dead_letters.insert(dead.message_id, dead);
            }
        }
        Ok(())
    }

    async fn reject_validation(
        &self,
        message_id: Ulid,
        note: &str,
    ) -> Result<(), PostboxError> {
        let mut state = self.state.lock();
        let m = state
            .messages
            .get(&message_id)
            .cloned()
            .ok_or(PostboxError::MessageNotFound(message_id))?;
        if m.status != MessageStatus::Pending {
            return Err(PostboxError::MessageNotClaimable {
                message_id,
                status: m.status,
            });
        }
        let now = self.clock.now();
        let entry = state.messages.remove(&message_id).unwrap();
        let record = FailureRecord {
            attempt: entry.attempt_count,
            claimed_by: None,
            failure_kind: FailureKind::Permanent,
            note: Some(note.to_string()),
            at: now,
        };
        let dead = DeadLetter {
            message_id: entry.message_id,
            mailbox_id: entry.mailbox_id.clone(),
            sender_id: entry.sender_id,
            payload: entry.payload,
            headers: entry.headers,
            priority: entry.priority,
            created_at: entry.created_at,
            attempt_count: entry.attempt_count,
            failure_history: vec![record],
            poison_reason: PoisonReason::ValidationFailed,
            dead_lettered_at: now,
        };
        state.dead_letters.insert(dead.message_id, dead);
        Ok(())
    }

    async fn list_dead_letters(
        &self,
        mailbox_id: &str,
        filter: Option<PoisonReason>,
        limit: usize,
    ) -> Result<Vec<DeadLetter>, PostboxError> {
        validate_agent_id(mailbox_id)?;
        let state = self.state.lock();
        let mut out: Vec<DeadLetter> = state
            .dead_letters
            .values()
            .filter(|d| {
                d.mailbox_id == mailbox_id && filter.map(|f| f == d.poison_reason).unwrap_or(true)
            })
            .cloned()
            .collect();
        out.sort_by(|a, b| a.dead_lettered_at.cmp(&b.dead_lettered_at));
        out.truncate(limit);
        Ok(out)
    }

    async fn replay_dead_letter(
        &self,
        message_id: Ulid,
        target_mailbox: Option<&str>,
        replayed_by: &str,
    ) -> Result<Message, PostboxError> {
        validate_agent_id(replayed_by)?;
        let mut state = self.state.lock();
        let dead = state
            .dead_letters
            .get(&message_id)
            .cloned()
            .ok_or(PostboxError::MessageNotFound(message_id))?;
        let target = target_mailbox
            .map(str::to_string)
            .unwrap_or_else(|| dead.mailbox_id.clone());
        validate_agent_id(&target)?;

        let mailbox = if let Some(m) = state.mailboxes.get(&target) {
            m.clone()
        } else {
            // Should not happen for missing target — but be defensive.
            return Err(PostboxError::MailboxNotFound {
                agent_id: target.clone(),
            });
        };
        if dead.payload.len() > mailbox.max_payload_bytes {
            return Err(PostboxError::PayloadTooLarge {
                size: dead.payload.len(),
                max: mailbox.max_payload_bytes,
            });
        }
        // Capacity check.
        let active = state
            .messages
            .values()
            .filter(|m| {
                m.mailbox_id == target
                    && matches!(
                        m.status,
                        MessageStatus::Pending | MessageStatus::Claimed
                    )
            })
            .count();
        if active >= mailbox.capacity {
            return Err(PostboxError::MailboxFull {
                agent_id: target.clone(),
                size: active,
                capacity: mailbox.capacity,
            });
        }

        let now = self.clock.now();
        let new_id = self.next_ulid();
        let mut headers = dead.headers.clone();
        headers.insert(
            "replayed_from".to_string(),
            dead.message_id.to_string(),
        );
        headers.insert("replayed_by".to_string(), replayed_by.to_string());
        headers.insert(
            "replayed_at".to_string(),
            st_to_ms(now).to_string(),
        );
        let replayed = Message {
            message_id: new_id,
            mailbox_id: target.clone(),
            sender_id: dead.sender_id.clone(),
            payload: dead.payload.clone(),
            headers,
            priority: dead.priority,
            created_at: now,
            visible_at: now,
            status: MessageStatus::Pending,
            attempt_count: 0,
            lease_expires_at: None,
            claimed_by: None,
            committed_at: None,
            checkpoint_token: None,
        };
        state.messages.insert(new_id, replayed.clone());
        Ok(replayed)
    }

    async fn is_committed(
        &self,
        mailbox_id: &str,
        message_id: Ulid,
    ) -> Result<bool, PostboxError> {
        validate_agent_id(mailbox_id)?;
        Ok(self
            .state
            .lock()
            .idempotency
            .contains(&(mailbox_id.to_string(), message_id)))
    }

    async fn sweep_expired_leases(
        &self,
        now: SystemTime,
    ) -> Result<usize, PostboxError> {
        let mut state = self.state.lock();
        let mut n = 0usize;
        for m in state.messages.values_mut() {
            if m.status == MessageStatus::Claimed
                && m.lease_expires_at.map(|e| e <= now).unwrap_or(false)
            {
                m.status = MessageStatus::Pending;
                m.lease_expires_at = None;
                m.claimed_by = None;
                n += 1;
            }
        }
        Ok(n)
    }

    async fn pending_count(&self, mailbox_id: &str) -> Result<usize, PostboxError> {
        validate_agent_id(mailbox_id)?;
        let state = self.state.lock();
        Ok(state
            .messages
            .values()
            .filter(|m| {
                m.mailbox_id == mailbox_id
                    && matches!(
                        m.status,
                        MessageStatus::Pending | MessageStatus::Claimed
                    )
            })
            .count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use bytes::Bytes;
    use std::time::Duration;

    fn store() -> (MemoryStore, MockClock) {
        let clock = MockClock::new(SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000));
        let s = MemoryStore::new(Arc::new(clock.clone()));
        (s, clock)
    }

    #[tokio::test]
    async fn send_creates_pending_message() {
        let (s, _c) = store();
        let m = s
            .send(SendRequest::new(
                "alice",
                "bob",
                Bytes::from_static(b"hello"),
            ))
            .await
            .unwrap();
        assert_eq!(m.mailbox_id, "alice");
        assert_eq!(m.sender_id, "bob");
        assert_eq!(m.status, MessageStatus::Pending);
        assert_eq!(m.attempt_count, 0);
    }

    #[tokio::test]
    async fn peek_does_not_claim() {
        let (s, _c) = store();
        s.send(SendRequest::new("a", "b", Bytes::from_static(b"x")))
            .await
            .unwrap();
        let peeked = s.peek("a", 10).await.unwrap();
        assert_eq!(peeked.len(), 1);
        let again = s.peek("a", 10).await.unwrap();
        assert_eq!(again.len(), 1);
    }

    #[tokio::test]
    async fn claim_makes_message_invisible_to_others() {
        let (s, _c) = store();
        s.send(SendRequest::new("a", "b", Bytes::from_static(b"x")))
            .await
            .unwrap();
        let claim1 = s.claim("a", "c1", Duration::from_secs(10)).await.unwrap();
        assert!(claim1.is_some());
        let claim2 = s.claim("a", "c2", Duration::from_secs(10)).await.unwrap();
        assert!(claim2.is_none());
    }

    #[tokio::test]
    async fn commit_with_empty_token_is_rejected() {
        let (s, _c) = store();
        let m = s
            .send(SendRequest::new("a", "b", Bytes::from_static(b"x")))
            .await
            .unwrap();
        let _c1 = s.claim("a", "c", Duration::from_secs(10)).await.unwrap();
        let err = s.commit(m.message_id, "c", "").await.unwrap_err();
        assert!(matches!(err, PostboxError::EmptyCheckpointToken(_)));
    }

    #[tokio::test]
    async fn commit_only_by_claimer() {
        let (s, _c) = store();
        let m = s
            .send(SendRequest::new("a", "b", Bytes::from_static(b"x")))
            .await
            .unwrap();
        let _c1 = s.claim("a", "c1", Duration::from_secs(10)).await.unwrap();
        let err = s.commit(m.message_id, "c2", "tok").await.unwrap_err();
        assert!(matches!(err, PostboxError::NotClaimedByYou { .. }));
    }

    #[tokio::test]
    async fn capacity_zero_rejects_send() {
        let (s, _c) = store();
        s.ensure_mailbox(MailboxConfig {
            agent_id: "a".into(),
            capacity: 0,
            ..MailboxConfig::defaults_for("a")
        })
        .await
        .unwrap();
        let err = s
            .send(SendRequest::new("a", "b", Bytes::from_static(b"x")))
            .await
            .unwrap_err();
        assert!(matches!(err, PostboxError::MailboxFull { .. }));
    }
}

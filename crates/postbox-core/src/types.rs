//! Domain types for Postbox.
//!
//! These types are deliberately framework-agnostic: no axum, no tonic, no
//! rmcp. Everything serializes through serde so front ends can project them
//! without losing information.

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Ordering mode for a mailbox.
///
/// `Fifo` preserves per-sender delivery order. `Unordered` makes no ordering
/// guarantee — useful for fire-and-forget workloads where order doesn't
/// matter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderingMode {
    Fifo,
    Unordered,
    /// Pick the highest-priority visible message first. Equal-priority
    /// messages are delivered FIFO within that priority band.
    Priority,
}

impl Default for OrderingMode {
    fn default() -> Self {
        OrderingMode::Fifo
    }
}

/// Lifecycle status of a [`Message`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageStatus {
    /// Visible and claimable.
    Pending,
    /// Held by a consumer under a lease.
    Claimed,
    /// Acknowledged and removed from active rotation.
    Committed,
    /// Moved to the dead-letter queue; never claimable again.
    DeadLettered,
}

/// The kind of failure a consumer reports on [`crate::MailboxStore::release`].
///
/// `Transient` releases the lease early so the message is reclaimable sooner.
/// `Permanent` triggers immediate dead-letter evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureKind {
    Transient,
    Permanent,
}

/// Why a message was dead-lettered. Distinguishing these three paths in the
/// DLQ record lets a developer tell "kept crashing the consumer" from
/// "was malformed on arrival" from "consumer looked at it and refused".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoisonReason {
    /// `attempt_count` reached `mailbox.max_attempts`.
    MaxAttemptsExceeded,
    /// A consumer called [`crate::MailboxStore::release`] with
    /// [`FailureKind::Permanent`].
    PermanentFailure,
    /// Rejected before reaching any consumer via
    /// [`crate::MailboxStore::reject_validation`].
    ValidationFailed,
    /// Message TTL expired before it was claimed.
    Expired,
}

/// Configuration for creating or updating a mailbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailboxConfig {
    pub agent_id: String,
    pub capacity: usize,
    pub ordering_mode: OrderingMode,
    pub max_attempts: u32,
    pub lease_duration: Duration,
    pub max_payload_bytes: usize,
    /// Optional DLQ retention window. Dead letters older than this are
    /// pruned by the sweeper. `None` means dead letters are kept forever.
    pub dlq_retention: Option<Duration>,
}

impl MailboxConfig {
    /// Defaults for an implicitly created mailbox on first send.
    pub fn defaults_for(agent_id: impl Into<String>) -> Self {
        Self {
            agent_id: agent_id.into(),
            capacity: 10_000,
            ordering_mode: OrderingMode::Fifo,
            max_attempts: 5,
            lease_duration: Duration::from_secs(60),
            max_payload_bytes: 1024 * 1024, // 1 MiB
            dlq_retention: None,
        }
    }
}

/// A mailbox belongs to one agent identity (`agent_id`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mailbox {
    pub agent_id: String,
    pub capacity: usize,
    pub ordering_mode: OrderingMode,
    pub max_attempts: u32,
    pub lease_duration: Duration,
    pub max_payload_bytes: usize,
    pub dlq_retention: Option<Duration>,
    pub created_at: SystemTime,
}

/// Aggregate statistics for a single mailbox.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MailboxStats {
    pub agent_id: String,
    pub pending_count: usize,
    pub claimed_count: usize,
    pub committed_count: usize,
    pub dead_lettered_count: usize,
    pub oldest_pending_at: Option<SystemTime>,
}

/// Request to send one message to multiple mailboxes atomically.
#[derive(Debug, Clone)]
pub struct FanoutRequest {
    pub targets: Vec<String>,
    pub sender_id: String,
    pub payload: Bytes,
    pub headers: BTreeMap<String, String>,
    pub priority: i32,
    pub delay: Option<Duration>,
    pub ttl: Option<Duration>,
}

/// One entry in a dead-letter failure history.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureRecord {
    pub attempt: u32,
    pub claimed_by: Option<String>,
    pub failure_kind: FailureKind,
    pub note: Option<String>,
    pub at: SystemTime,
}

/// A message that has been moved to the dead-letter queue.
///
/// The full payload and headers are preserved so the DLQ record is
/// self-contained for diagnosis.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub message_id: Ulid,
    pub mailbox_id: String,
    pub sender_id: String,
    pub payload: Bytes,
    pub headers: BTreeMap<String, String>,
    pub priority: i32,
    pub created_at: SystemTime,
    pub attempt_count: u32,
    pub failure_history: Vec<FailureRecord>,
    pub poison_reason: PoisonReason,
    pub dead_lettered_at: SystemTime,
}

impl Serialize for DeadLetter {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("DeadLetter", 10)?;
        st.serialize_field("message_id", &self.message_id)?;
        st.serialize_field("mailbox_id", &self.mailbox_id)?;
        st.serialize_field("sender_id", &self.sender_id)?;
        st.serialize_field("payload", &self.payload.as_ref())?;
        st.serialize_field("headers", &self.headers)?;
        st.serialize_field("priority", &self.priority)?;
        st.serialize_field("created_at", &self.created_at)?;
        st.serialize_field("attempt_count", &self.attempt_count)?;
        st.serialize_field("failure_history", &self.failure_history)?;
        st.serialize_field("poison_reason", &self.poison_reason)?;
        st.serialize_field("dead_lettered_at", &self.dead_lettered_at)?;
        st.end()
    }
}

impl<'de> Deserialize<'de> for DeadLetter {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = DeadLetter;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("DeadLetter")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<DeadLetter, A::Error> {
                let mut message_id: Option<Ulid> = None;
                let mut mailbox_id: Option<String> = None;
                let mut sender_id: Option<String> = None;
                let mut payload: Option<Vec<u8>> = None;
                let mut headers: Option<BTreeMap<String, String>> = None;
                let mut priority: Option<i32> = None;
                let mut created_at: Option<SystemTime> = None;
                let mut attempt_count: Option<u32> = None;
                let mut failure_history: Option<Vec<FailureRecord>> = None;
                let mut poison_reason: Option<PoisonReason> = None;
                let mut dead_lettered_at: Option<SystemTime> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "message_id" => message_id = Some(map.next_value()?),
                        "mailbox_id" => mailbox_id = Some(map.next_value()?),
                        "sender_id" => sender_id = Some(map.next_value()?),
                        "payload" => payload = Some(map.next_value()?),
                        "headers" => headers = Some(map.next_value()?),
                        "priority" => priority = Some(map.next_value()?),
                        "created_at" => created_at = Some(map.next_value()?),
                        "attempt_count" => attempt_count = Some(map.next_value()?),
                        "failure_history" => failure_history = Some(map.next_value()?),
                        "poison_reason" => poison_reason = Some(map.next_value()?),
                        "dead_lettered_at" => dead_lettered_at = Some(map.next_value()?),
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(DeadLetter {
                    message_id: message_id
                        .ok_or_else(|| de::Error::missing_field("message_id"))?,
                    mailbox_id: mailbox_id
                        .ok_or_else(|| de::Error::missing_field("mailbox_id"))?,
                    sender_id: sender_id
                        .ok_or_else(|| de::Error::missing_field("sender_id"))?,
                    payload: Bytes::from(
                        payload.ok_or_else(|| de::Error::missing_field("payload"))?,
                    ),
                    headers: headers.ok_or_else(|| de::Error::missing_field("headers"))?,
                    priority: priority
                        .ok_or_else(|| de::Error::missing_field("priority"))?,
                    created_at: created_at
                        .ok_or_else(|| de::Error::missing_field("created_at"))?,
                    attempt_count: attempt_count
                        .ok_or_else(|| de::Error::missing_field("attempt_count"))?,
                    failure_history: failure_history
                        .ok_or_else(|| de::Error::missing_field("failure_history"))?,
                    poison_reason: poison_reason
                        .ok_or_else(|| de::Error::missing_field("poison_reason"))?,
                    dead_lettered_at: dead_lettered_at
                        .ok_or_else(|| de::Error::missing_field("dead_lettered_at"))?,
                })
            }
        }
        d.deserialize_map(V)
    }
}

/// A message held in a mailbox.
///
/// `Serialize`/`Deserialize` are custom because `Bytes` has no serde impl.
/// On the wire, `payload` appears as a plain byte array.
#[derive(Debug, Clone)]
pub struct Message {
    pub message_id: Ulid,
    pub mailbox_id: String,
    pub sender_id: String,
    pub payload: Bytes,
    pub headers: BTreeMap<String, String>,
    pub priority: i32,
    pub created_at: SystemTime,
    pub visible_at: SystemTime,
    pub status: MessageStatus,
    pub attempt_count: u32,
    pub lease_expires_at: Option<SystemTime>,
    pub claimed_by: Option<String>,
    pub committed_at: Option<SystemTime>,
    pub checkpoint_token: Option<String>,
    /// Absolute deadline: if the message is still `pending` past this time,
    /// the sweeper dead-letters it with reason `Expired`. `None` = no TTL.
    pub expires_at: Option<SystemTime>,
}

impl Serialize for Message {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("Message", 14)?;
        st.serialize_field("message_id", &self.message_id)?;
        st.serialize_field("mailbox_id", &self.mailbox_id)?;
        st.serialize_field("sender_id", &self.sender_id)?;
        st.serialize_field("payload", &self.payload.as_ref())?;
        st.serialize_field("headers", &self.headers)?;
        st.serialize_field("priority", &self.priority)?;
        st.serialize_field("created_at", &self.created_at)?;
        st.serialize_field("visible_at", &self.visible_at)?;
        st.serialize_field("status", &self.status)?;
        st.serialize_field("attempt_count", &self.attempt_count)?;
        st.serialize_field("lease_expires_at", &self.lease_expires_at)?;
        st.serialize_field("claimed_by", &self.claimed_by)?;
        st.serialize_field("committed_at", &self.committed_at)?;
        st.serialize_field("checkpoint_token", &self.checkpoint_token)?;
        st.serialize_field("expires_at", &self.expires_at)?;
        st.end()
    }
}

impl<'de> Deserialize<'de> for Message {
    fn deserialize<D>(d: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Message;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("Message")
            }
            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Message, A::Error> {
                let mut message_id: Option<Ulid> = None;
                let mut mailbox_id: Option<String> = None;
                let mut sender_id: Option<String> = None;
                let mut payload: Option<Vec<u8>> = None;
                let mut headers: Option<BTreeMap<String, String>> = None;
                let mut priority: Option<i32> = None;
                let mut created_at: Option<SystemTime> = None;
                let mut visible_at: Option<SystemTime> = None;
                let mut status: Option<MessageStatus> = None;
                let mut attempt_count: Option<u32> = None;
                let mut lease_expires_at: Option<Option<SystemTime>> = None;
                let mut claimed_by: Option<Option<String>> = None;
                let mut committed_at: Option<Option<SystemTime>> = None;
                let mut checkpoint_token: Option<Option<String>> = None;
                let mut expires_at: Option<Option<SystemTime>> = None;
                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "message_id" => message_id = Some(map.next_value()?),
                        "mailbox_id" => mailbox_id = Some(map.next_value()?),
                        "sender_id" => sender_id = Some(map.next_value()?),
                        "payload" => payload = Some(map.next_value()?),
                        "headers" => headers = Some(map.next_value()?),
                        "priority" => priority = Some(map.next_value()?),
                        "created_at" => created_at = Some(map.next_value()?),
                        "visible_at" => visible_at = Some(map.next_value()?),
                        "status" => status = Some(map.next_value()?),
                        "attempt_count" => attempt_count = Some(map.next_value()?),
                        "lease_expires_at" => lease_expires_at = Some(map.next_value()?),
                        "claimed_by" => claimed_by = Some(map.next_value()?),
                        "committed_at" => committed_at = Some(map.next_value()?),
                        "checkpoint_token" => checkpoint_token = Some(map.next_value()?),
                        "expires_at" => expires_at = Some(map.next_value()?),
                        _ => {
                            let _: serde::de::IgnoredAny = map.next_value()?;
                        }
                    }
                }
                Ok(Message {
                    message_id: message_id
                        .ok_or_else(|| de::Error::missing_field("message_id"))?,
                    mailbox_id: mailbox_id
                        .ok_or_else(|| de::Error::missing_field("mailbox_id"))?,
                    sender_id: sender_id
                        .ok_or_else(|| de::Error::missing_field("sender_id"))?,
                    payload: Bytes::from(
                        payload.ok_or_else(|| de::Error::missing_field("payload"))?,
                    ),
                    headers: headers
                        .ok_or_else(|| de::Error::missing_field("headers"))?,
                    priority: priority
                        .ok_or_else(|| de::Error::missing_field("priority"))?,
                    created_at: created_at
                        .ok_or_else(|| de::Error::missing_field("created_at"))?,
                    visible_at: visible_at
                        .ok_or_else(|| de::Error::missing_field("visible_at"))?,
                    status: status.ok_or_else(|| de::Error::missing_field("status"))?,
                    attempt_count: attempt_count
                        .ok_or_else(|| de::Error::missing_field("attempt_count"))?,
                    lease_expires_at: lease_expires_at
                        .ok_or_else(|| de::Error::missing_field("lease_expires_at"))?,
                    claimed_by: claimed_by
                        .ok_or_else(|| de::Error::missing_field("claimed_by"))?,
                    committed_at: committed_at
                        .ok_or_else(|| de::Error::missing_field("committed_at"))?,
                    checkpoint_token: checkpoint_token
                        .ok_or_else(|| de::Error::missing_field("checkpoint_token"))?,
                    expires_at: expires_at
                        .unwrap_or(None),
                })
            }
        }
        d.deserialize_map(V)
    }
}

/// The result of a successful [`crate::MailboxStore::claim`].
///
/// The lease is the time window during which no other claimer will see this
/// message. After it expires the sweeper returns the message to `pending`
/// without bumping `attempt_count` (that only happens when it is re-claimed
/// and the consumer explicitly fails it).
#[derive(Debug, Clone)]
pub struct Claim {
    pub message: Message,
    pub lease_expires_at: SystemTime,
}

impl Serialize for Claim {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut st = s.serialize_struct("Claim", 2)?;
        st.serialize_field("message", &self.message)?;
        st.serialize_field("lease_expires_at", &self.lease_expires_at)?;
        st.end()
    }
}

/// What a sender hands to [`crate::MailboxStore::send`].
#[derive(Debug, Clone)]
pub struct SendRequest {
    pub target_mailbox: String,
    pub sender_id: String,
    pub payload: Bytes,
    pub headers: BTreeMap<String, String>,
    pub priority: i32,
    pub delay: Option<Duration>,
    /// Optional time-to-live. If the message is not claimed before this
    /// duration elapses, the sweeper moves it to the DLQ with reason
    /// `Expired`.
    pub ttl: Option<Duration>,
}

impl SendRequest {
    pub fn new(
        target_mailbox: impl Into<String>,
        sender_id: impl Into<String>,
        payload: Bytes,
    ) -> Self {
        Self {
            target_mailbox: target_mailbox.into(),
            sender_id: sender_id.into(),
            payload,
            headers: BTreeMap::new(),
            priority: 0,
            delay: None,
            ttl: None,
        }
    }

    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

pub const MAX_HEADER_COUNT: usize = 100;
pub const MAX_HEADER_KEY_LEN: usize = 256;
pub const MAX_HEADER_VALUE_LEN: usize = 8192;

/// Validate message headers: count, key length, and value length limits.
pub fn validate_headers(headers: &BTreeMap<String, String>) -> Result<(), crate::PostboxError> {
    if headers.len() > MAX_HEADER_COUNT {
        return Err(crate::PostboxError::InvalidHeaders(format!(
            "too many headers: {} (max {})",
            headers.len(),
            MAX_HEADER_COUNT
        )));
    }
    for (k, v) in headers {
        if k.len() > MAX_HEADER_KEY_LEN {
            return Err(crate::PostboxError::InvalidHeaders(format!(
                "header key too long: {} bytes (max {})",
                k.len(),
                MAX_HEADER_KEY_LEN
            )));
        }
        if v.len() > MAX_HEADER_VALUE_LEN {
            return Err(crate::PostboxError::InvalidHeaders(format!(
                "header value too long: {} bytes (max {})",
                v.len(),
                MAX_HEADER_VALUE_LEN
            )));
        }
    }
    Ok(())
}

/// Small utility used to validate an agent identifier. We refuse empty
/// strings and any whitespace, control characters, or absurd lengths so that
/// storage keys can't be abused.
pub fn validate_agent_id(id: &str) -> Result<(), crate::PostboxError> {
    if id.is_empty() {
        return Err(crate::PostboxError::InvalidAgentId(id.to_string()));
    }
    if id.len() > 256 {
        return Err(crate::PostboxError::InvalidAgentId(id.to_string()));
    }
    if id
        .chars()
        .any(|c| c.is_whitespace() || c.is_control() || c == '\0')
    {
        return Err(crate::PostboxError::InvalidAgentId(id.to_string()));
    }
    Ok(())
}

/// Helper: convert between [`SystemTime`] and the millisecond Unix-epoch
/// representation used by the SQLite backend.
pub fn system_time_to_millis(t: SystemTime) -> i64 {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => d.as_millis() as i64,
        Err(e) => -(e.duration().as_millis() as i64),
    }
}

/// Inverse of [`system_time_to_millis`].
pub fn millis_to_system_time(ms: i64) -> SystemTime {
    if ms >= 0 {
        SystemTime::UNIX_EPOCH + Duration::from_millis(ms as u64)
    } else {
        SystemTime::UNIX_EPOCH - Duration::from_millis((-ms) as u64)
    }
}
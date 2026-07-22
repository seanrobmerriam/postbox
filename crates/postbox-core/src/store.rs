//! The single [`MailboxStore`] trait that every storage backend implements.
//!
//! All state transitions are observable through this trait. Every method
//! takes a `&self` reference and is async; backends serialize the relevant
//! critical sections internally (in-process for `MemoryStore`, single SQL
//! transaction per transition for `SqliteStore`).
//!
//! Contract summary:
//! - `send` either returns the persisted `Message` or fails with an error
//!   that does not commit any state.
//! - `claim` returns at most one message per call and only if there is a
//!   visible message; the message becomes invisible to subsequent claimers
//!   until the lease expires or the message is committed/released.
//! - `commit` and `release` only succeed for the current claimer; the lease
//!   token is the claimer's `claimed_by` identity (a per-call opaque string).
//! - `sweep_expired_leases` is the single source of truth for restoring
//!   abandoned leases to `pending`. It does not bump `attempt_count`.

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use ulid::Ulid;

use crate::error::PostboxError;
use crate::types::{
    Claim, DeadLetter, FailureKind, FanoutRequest, Mailbox, MailboxConfig, MailboxStats, Message,
    PoisonReason, SendRequest,
};

/// Single storage interface every backend implements.
#[async_trait]
pub trait MailboxStore: Send + Sync + 'static {
    /// Idempotent: creates the mailbox if it doesn't exist, otherwise
    /// returns the existing row. Always returns the final mailbox state.
    async fn ensure_mailbox(&self, config: MailboxConfig) -> Result<Mailbox, PostboxError>;

    /// Lookup a mailbox by agent id.
    async fn get_mailbox(&self, agent_id: &str) -> Result<Option<Mailbox>, PostboxError>;

    /// Persist a message addressed to `req.target_mailbox`. If the mailbox
    /// does not exist yet, it is implicitly created with default
    /// configuration. Returns the persisted message with its server-assigned
    /// `message_id`, `created_at`, and `visible_at`.
    async fn send(&self, req: SendRequest) -> Result<Message, PostboxError>;

    /// Peek up to `max` visible messages without claiming.
    async fn peek(
        &self,
        mailbox_id: &str,
        max: usize,
    ) -> Result<Vec<Message>, PostboxError>;

    /// Atomically claim the next visible message for `claimer_id`. Returns
    /// `None` when the mailbox is empty or every message is currently held
    /// under another active lease.
    async fn claim(
        &self,
        mailbox_id: &str,
        claimer_id: &str,
        lease_duration: Duration,
    ) -> Result<Option<Claim>, PostboxError>;

    /// Commit a claimed message. `checkpoint_token` must be a non-empty
    /// opaque string supplied by the caller; Postbox does not interpret it,
    /// only audits that the caller pointed to *something* durable.
    async fn commit(
        &self,
        message_id: Ulid,
        claimer_id: &str,
        checkpoint_token: &str,
    ) -> Result<(), PostboxError>;

    /// Release a claimed message with a failure classification.
    /// - `Transient`: the lease is cleared and the message returns to
    ///   `pending`. `attempt_count` is not changed here — it was already
    ///   incremented when the message was claimed. The count therefore
    ///   accurately reflects "number of completed claim cycles" including
    ///   this one.
    /// - `Permanent`: message is moved to the DLQ immediately, regardless of
    ///   `max_attempts`.
    async fn release(
        &self,
        message_id: Ulid,
        claimer_id: &str,
        failure: FailureKind,
        note: Option<&str>,
    ) -> Result<(), PostboxError>;

    /// Reject a message before it is ever claimable. Used for the third
    /// poison-classification path (`ValidationFailed`). Note: only valid
    /// for messages that have not yet been claimed by any consumer.
    async fn reject_validation(
        &self,
        message_id: Ulid,
        note: &str,
    ) -> Result<(), PostboxError>;

    /// List dead-letter records, optionally filtered by reason.
    async fn list_dead_letters(
        &self,
        mailbox_id: &str,
        filter: Option<PoisonReason>,
        limit: usize,
    ) -> Result<Vec<DeadLetter>, PostboxError>;

    /// Re-inject a dead-lettered message. `target_mailbox` defaults to the
    /// original mailbox when `None`. The result is a fresh message: the
    /// original is left in the DLQ for audit, a new message is created with
    /// `attempt_count = 0` and an audit note `replayed_from=<old_message_id>`.
    async fn replay_dead_letter(
        &self,
        message_id: Ulid,
        target_mailbox: Option<&str>,
        replayed_by: &str,
    ) -> Result<Message, PostboxError>;

    /// Idempotency helper: returns true if `(mailbox_id, message_id)` was
    /// committed at any point in the past. Used by consumers to dedupe work
    /// after redelivery.
    async fn is_committed(
        &self,
        mailbox_id: &str,
        message_id: Ulid,
    ) -> Result<bool, PostboxError>;

    /// Recover abandoned leases. Should be called on startup (to handle
    /// crashes with live leases) and periodically thereafter. Messages whose
    /// lease has expired but which were not explicitly released are
    /// returned to `pending`; their `attempt_count` is preserved. Messages
    /// with live leases are untouched. Returns the count of messages
    /// reclaimed.
    async fn sweep_expired_leases(
        &self,
        now: SystemTime,
    ) -> Result<usize, PostboxError>;

    /// Diagnostic: how many messages are currently pending in `mailbox_id`.
    /// Used by capacity checks and observable in admin tools.
    async fn pending_count(&self, mailbox_id: &str) -> Result<usize, PostboxError>;

    /// Send one message to multiple target mailboxes atomically. If any
    /// insert fails (capacity, validation) the entire fanout is rolled back.
    /// Returns the persisted messages (one per target), each with a distinct
    /// `message_id`.
    async fn fanout_send(
        &self,
        req: FanoutRequest,
    ) -> Result<Vec<Message>, PostboxError>;

    /// List all mailboxes with cursor-based pagination on `agent_id`.
    /// `after` is the exclusive cursor (pass the last `agent_id` from the
    /// previous page, or `None` for the first page).
    async fn list_mailboxes(
        &self,
        limit: usize,
        after: Option<&str>,
    ) -> Result<Vec<Mailbox>, PostboxError>;

    /// Return aggregate counters for a single mailbox.
    async fn mailbox_stats(
        &self,
        agent_id: &str,
    ) -> Result<MailboxStats, PostboxError>;

    /// Dead-letter all `pending` messages whose `expires_at` has passed.
    /// Returns the count of messages moved to the DLQ.
    async fn sweep_expired_messages(
        &self,
        now: SystemTime,
    ) -> Result<usize, PostboxError>;

    /// Delete dead-letter records older than `before` for the given mailbox.
    /// Returns the number of rows deleted.
    async fn purge_dead_letters(
        &self,
        mailbox_id: &str,
        before: SystemTime,
    ) -> Result<usize, PostboxError>;
}

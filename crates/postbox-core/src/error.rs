//! All fallible paths return [`PostboxError`]. No `unwrap`, no `expect`,
//! no surprise panics.

use thiserror::Error;
use ulid::Ulid;

/// Top-level error type for the Postbox core API.
#[derive(Debug, Error)]
pub enum PostboxError {
    #[error("mailbox not found: {agent_id}")]
    MailboxNotFound { agent_id: String },

    #[error("mailbox full: {agent_id} ({size} messages, capacity {capacity})")]
    MailboxFull {
        agent_id: String,
        size: usize,
        capacity: usize,
    },

    #[error("payload too large: {size} bytes (max {max})")]
    PayloadTooLarge { size: usize, max: usize },

    #[error("message not found: {0}")]
    MessageNotFound(Ulid),

    #[error("message {message_id} is in status {status:?} and cannot be claimed")]
    MessageNotClaimable {
        message_id: Ulid,
        status: crate::MessageStatus,
    },

    #[error("message {0} is not currently claimed by anyone")]
    MessageNotClaimed(Ulid),

    #[error("message {message_id} is claimed by {claimer}, not {caller}")]
    NotClaimedByYou {
        message_id: Ulid,
        claimer: String,
        caller: String,
    },

    #[error("message {0} is already committed")]
    AlreadyCommitted(Ulid),

    #[error("message {0} has no checkpoint token; commit requires a non-empty token")]
    EmptyCheckpointToken(Ulid),

    #[error("invalid agent id: {0}")]
    InvalidAgentId(String),

    #[error("invalid headers: {0}")]
    InvalidHeaders(String),

    #[error("storage error: {0}")]
    Storage(String),
}

impl PostboxError {
    /// Whether this error indicates the caller violated a precondition and
    /// should be surfaced as a 4xx in HTTP front ends.
    pub fn is_client_error(&self) -> bool {
        matches!(
            self,
            PostboxError::MailboxNotFound { .. }
                | PostboxError::MailboxFull { .. }
                | PostboxError::PayloadTooLarge { .. }
                | PostboxError::MessageNotFound(_)
                | PostboxError::MessageNotClaimable { .. }
                | PostboxError::MessageNotClaimed(_)
                | PostboxError::NotClaimedByYou { .. }
                | PostboxError::AlreadyCommitted(_)
                | PostboxError::EmptyCheckpointToken(_)
                | PostboxError::InvalidAgentId(_)
                | PostboxError::InvalidHeaders(_)
        )
    }
}
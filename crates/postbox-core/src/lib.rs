//! Postbox core: domain model, errors, the [`MailboxStore`] trait, and the
//! storage backends that implement it (`memory`, `sqlite`). The front ends
//! (`postbox-grpc`, `postbox-mcp`) call into this crate only.
//!
//! See `types.rs` for the domain model and `store.rs` for the trait
//! contract.

#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod clock;
pub mod error;
pub mod store;
pub mod types;

pub mod memory;
pub mod sqlite;
pub mod sweeper;

pub use clock::{Clock, MockClock, SystemClock};
pub use error::PostboxError;
pub use memory::MemoryStore;
pub use store::MailboxStore;
pub use sqlite::SqliteStore;
pub use types::{
    validate_agent_id, validate_headers, Claim, DeadLetter, FailureKind, FailureRecord, Mailbox,
    MailboxConfig, Message, MessageStatus, OrderingMode, PoisonReason, SendRequest,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn agent_id_validation_rejects_empty_and_blank() {
        assert!(validate_agent_id("").is_err());
        assert!(validate_agent_id("has space").is_err());
        assert!(validate_agent_id("with\ttab").is_err());
        assert!(validate_agent_id("with\nnewline").is_err());
        assert!(validate_agent_id("with\0null").is_err());
        assert!(validate_agent_id(&"x".repeat(1024)).is_err());
    }

    #[test]
    fn agent_id_validation_accepts_normal_ids() {
        assert!(validate_agent_id("alice").is_ok());
        assert!(validate_agent_id("agent-123").is_ok());
        assert!(validate_agent_id("agent_456").is_ok());
        assert!(validate_agent_id("svc/orders.api").is_ok());
    }

    #[test]
    fn system_time_round_trip_through_millis() {
        use std::time::{Duration, SystemTime};
        let t = SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        let ms = types::system_time_to_millis(t);
        let t2 = types::millis_to_system_time(ms);
        assert_eq!(t, t2);
    }
}

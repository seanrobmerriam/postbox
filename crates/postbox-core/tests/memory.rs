//! In-memory backend. Drives the full behavior suite against `MemoryStore`.

mod common;
use crate::common::{behaviors, Backend, TestEnv};
use std::sync::Arc;

fn mem() -> TestEnv {
    TestEnv::new(Backend::Memory)
}

#[tokio::test]
async fn send_returns_pending_message() {
    behaviors::send_returns_pending_message(&mem()).await;
}
#[tokio::test]
async fn send_rejects_empty_agent_id() {
    behaviors::send_rejects_empty_agent_id(&mem()).await;
}
#[tokio::test]
async fn send_rejects_whitespace_agent_id() {
    behaviors::send_rejects_whitespace_agent_id(&mem()).await;
}
#[tokio::test]
async fn send_oversized_payload_is_rejected() {
    behaviors::send_oversized_payload_is_rejected(&mem()).await;
}
#[tokio::test]
async fn send_to_mailbox_at_capacity_is_rejected() {
    behaviors::send_to_mailbox_at_capacity_is_rejected(&mem()).await;
}
#[tokio::test]
async fn peek_does_not_claim() {
    behaviors::peek_does_not_claim(&mem()).await;
}
#[tokio::test]
async fn peek_empty_mailbox_returns_empty() {
    behaviors::peek_empty_mailbox_returns_empty(&mem()).await;
}
#[tokio::test]
async fn claim_from_empty_mailbox_returns_none() {
    behaviors::claim_from_empty_mailbox_returns_none(&mem()).await;
}
#[tokio::test]
async fn claim_makes_message_invisible_to_others() {
    behaviors::claim_makes_message_invisible_to_others(&mem()).await;
}
#[tokio::test]
async fn lease_expiry_without_commit_makes_message_reclaimable_and_increments_attempt_count() {
    behaviors::lease_expiry_without_commit_makes_message_reclaimable_and_increments_attempt_count(
        &mem(),
    )
    .await;
}
#[tokio::test]
async fn lease_expiry_without_reclaim_does_not_increment_attempt_count() {
    behaviors::lease_expiry_without_reclaim_does_not_increment_attempt_count(&mem()).await;
}
#[tokio::test]
async fn commit_with_empty_checkpoint_token_is_rejected() {
    behaviors::commit_with_empty_checkpoint_token_is_rejected(&mem()).await;
}
#[tokio::test]
async fn commit_only_by_claimer_succeeds() {
    behaviors::commit_only_by_claimer_succeeds(&mem()).await;
}
#[tokio::test]
async fn commit_makes_message_invisible() {
    behaviors::commit_makes_message_invisible(&mem()).await;
}
#[tokio::test]
async fn commit_populates_idempotency_ledger() {
    behaviors::commit_populates_idempotency_ledger(&mem()).await;
}
#[tokio::test]
async fn commit_twice_is_rejected() {
    behaviors::commit_twice_is_rejected(&mem()).await;
}
#[tokio::test]
async fn release_transient_returns_to_pending() {
    behaviors::release_transient_returns_to_pending(&mem()).await;
}
#[tokio::test]
async fn release_only_by_claimer() {
    behaviors::release_only_by_claimer(&mem()).await;
}
#[tokio::test]
async fn release_permanent_moves_to_dlq() {
    behaviors::release_permanent_moves_to_dlq(&mem()).await;
}
#[tokio::test]
async fn max_attempts_one_dead_letters_on_first_permanent() {
    behaviors::max_attempts_one_dead_letters_on_first_permanent(&mem()).await;
}
#[tokio::test]
async fn reject_validation_moves_to_dlq() {
    behaviors::reject_validation_moves_to_dlq(&mem()).await;
}
#[tokio::test]
async fn reject_validation_after_claim_is_rejected() {
    behaviors::reject_validation_after_claim_is_rejected(&mem()).await;
}
#[tokio::test]
async fn replay_dead_letter_creates_new_message_with_zero_attempts() {
    behaviors::replay_dead_letter_creates_new_message_with_zero_attempts(&mem()).await;
}
#[tokio::test]
async fn fifo_ordering_holds_across_redelivery() {
    behaviors::fifo_ordering_holds_across_redelivery(&mem()).await;
}
#[tokio::test]
async fn unordered_mailbox_returns_all_messages() {
    behaviors::unordered_mailbox_returns_all_messages(&mem()).await;
}
#[tokio::test]
async fn list_dead_letters_filters_by_reason() {
    behaviors::list_dead_letters_filters_by_reason(&mem()).await;
}
#[tokio::test]
async fn claim_unknown_mailbox_is_error() {
    behaviors::claim_unknown_mailbox_is_error(&mem()).await;
}
#[tokio::test]
async fn commit_unknown_message_is_error() {
    behaviors::commit_unknown_message_is_error(&mem()).await;
}
#[tokio::test]
async fn unknown_message_release_is_error() {
    behaviors::unknown_message_release_is_error(&mem()).await;
}
#[tokio::test]
async fn lease_duration_zero_is_allowed() {
    behaviors::lease_duration_zero_is_allowed(&mem()).await;
}
#[tokio::test]
async fn payload_at_exact_cap_is_accepted() {
    behaviors::payload_at_exact_cap_is_accepted(&mem()).await;
}
#[tokio::test]
async fn fifo_per_sender_round_robin() {
    behaviors::fifo_per_sender_round_robin(&mem()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claimers_never_double_claim_a_message_while_lease_is_active() {
    behaviors::concurrent_claimers_never_double_claim_a_message_while_lease_is_active(Arc::new(
        mem(),
    ))
    .await;
}
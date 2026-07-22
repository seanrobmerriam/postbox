//! Prometheus metrics instrumentation for Postbox.
//!
//! Key metrics:
//! - `postbox_messages_sent_total{mailbox_id}` — counter
//! - `postbox_messages_claimed_total{mailbox_id}` — counter
//! - `postbox_messages_committed_total{mailbox_id}` — counter
//! - `postbox_messages_released_total{mailbox_id, kind}` — counter
//! - `postbox_messages_dead_lettered_total{mailbox_id, reason}` — counter
//! - `postbox_leases_swept_total` — counter
//! - `postbox_messages_expired_total{mailbox_id}` — counter

/// Record that a message was sent to a mailbox.
pub fn record_send(mailbox_id: &str) {
    metrics::counter!("postbox_messages_sent_total", "mailbox_id" => mailbox_id.to_string())
        .increment(1);
}

/// Record that a message was claimed from a mailbox.
pub fn record_claim(mailbox_id: &str) {
    metrics::counter!("postbox_messages_claimed_total", "mailbox_id" => mailbox_id.to_string())
        .increment(1);
}

/// Record that a message was committed.
pub fn record_commit(mailbox_id: &str) {
    metrics::counter!("postbox_messages_committed_total", "mailbox_id" => mailbox_id.to_string())
        .increment(1);
}

/// Record that a message was released.
pub fn record_release(mailbox_id: &str, kind: &str) {
    metrics::counter!(
        "postbox_messages_released_total",
        "mailbox_id" => mailbox_id.to_string(),
        "kind" => kind.to_string()
    )
    .increment(1);
}

/// Record that messages were dead-lettered.
pub fn record_dead_letter(mailbox_id: &str, reason: &str) {
    metrics::counter!(
        "postbox_messages_dead_lettered_total",
        "mailbox_id" => mailbox_id.to_string(),
        "reason" => reason.to_string()
    )
    .increment(1);
}

/// Record that leases were swept (recovered).
pub fn record_leases_swept(count: u64) {
    metrics::counter!("postbox_leases_swept_total").increment(count);
}

/// Record that messages were expired (TTL) and dead-lettered.
pub fn record_expired(mailbox_id: &str, count: u64) {
    metrics::counter!("postbox_messages_expired_total", "mailbox_id" => mailbox_id.to_string())
        .increment(count);
}

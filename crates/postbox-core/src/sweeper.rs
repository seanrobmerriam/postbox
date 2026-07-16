//! Background sweeper task. Periodic lease-expiry recovery so abandoned
//! claims become reclaimable again without bumping `attempt_count`.
//!
//! The sweeper is a single `tokio` task that wakes up at a configurable
//! interval and runs [`crate::MailboxStore::sweep_expired_leases`]. A single
//! task is fine because we bound memory by scanning state instead of
//! holding per-message timers, and we get crash-recovery for free: a fresh
//! sweeper on startup will reclaim whatever expired while the process was
//! down.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{debug, info};

use crate::clock::Clock;
use crate::store::MailboxStore;

/// Default sweep interval.
pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(5);

/// Handle to a running sweeper. Drop or call [`SweeperHandle::stop`] to
/// shut the sweeper down cleanly.
pub struct SweeperHandle {
    handle: JoinHandle<()>,
    stop: Arc<tokio::sync::Notify>,
}

impl SweeperHandle {
    /// Stop the sweeper and wait for it to exit.
    pub async fn stop(self) {
        self.stop.notify_one();
        let _ = self.handle.await;
    }
}

/// Spawn the sweeper against a typed backend. It runs until
/// [`SweeperHandle::stop`] is called.
pub fn spawn<S: MailboxStore + 'static>(
    store: Arc<S>,
    clock: Arc<dyn Clock>,
    interval: Duration,
) -> SweeperHandle {
    let stop = Arc::new(tokio::sync::Notify::new());
    let stop_clone = stop.clone();
    let handle = tokio::spawn(async move {
        info!(?interval, "postbox sweeper started");
        loop {
            tokio::select! {
                _ = stop_clone.notified() => {
                    debug!("postbox sweeper stop signal received");
                    break;
                }
                _ = tokio::time::sleep(interval) => {
                    let now = clock.now();
                    match store.sweep_expired_leases(now).await {
                        Ok(0) => {}
                        Ok(n) => debug!(reclaimed = n, "postbox sweeper reclaimed expired leases"),
                        Err(e) => tracing::warn!(error = %e, "postbox sweeper error"),
                    }
                }
            }
        }
        debug!("postbox sweeper exited");
    });
    SweeperHandle { handle, stop }
}

/// Same as [`spawn`] but takes a type-erased `Arc<dyn MailboxStore>`.
/// Useful from the binary where the store is held as a trait object.
pub fn spawn_arc(
    store: Arc<dyn MailboxStore>,
    clock: Arc<dyn Clock>,
    interval: Duration,
) -> SweeperHandle {
    let stop = Arc::new(tokio::sync::Notify::new());
    let stop_clone = stop.clone();
    let handle = tokio::spawn(async move {
        info!(?interval, "postbox sweeper started");
        loop {
            tokio::select! {
                _ = stop_clone.notified() => {
                    debug!("postbox sweeper stop signal received");
                    break;
                }
                _ = tokio::time::sleep(interval) => {
                    let now = clock.now();
                    match store.sweep_expired_leases(now).await {
                        Ok(0) => {}
                        Ok(n) => debug!(reclaimed = n, "postbox sweeper reclaimed expired leases"),
                        Err(e) => tracing::warn!(error = %e, "postbox sweeper error"),
                    }
                }
            }
        }
        debug!("postbox sweeper exited");
    });
    SweeperHandle { handle, stop }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clock::MockClock;
    use crate::memory::MemoryStore;
    use crate::types::{OrderingMode, SendRequest};
    use bytes::Bytes;
    use std::time::{Duration, SystemTime};

    #[tokio::test]
    async fn sweeper_reclaims_expired_leases() {
        let clock = Arc::new(MockClock::new(
            SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000),
        ));
        let store = Arc::new(MemoryStore::new(clock.clone()));
        // Configure a mailbox with max_attempts = 10 to avoid dead-lettering.
        let _ = store
            .ensure_mailbox(crate::types::MailboxConfig {
                agent_id: "alice".into(),
                capacity: 10,
                ordering_mode: OrderingMode::Unordered,
                max_attempts: 10,
                lease_duration: Duration::from_secs(60),
                max_payload_bytes: 1024,
            })
            .await
            .unwrap();
        store
            .send(SendRequest::new("alice", "bob", Bytes::from_static(b"x")))
            .await
            .unwrap();
        let claim = store
            .claim("alice", "consumer", Duration::from_millis(100))
            .await
            .unwrap();
        assert!(claim.is_some());
        // Time travel past the lease.
        clock.advance(Duration::from_secs(1));
        let swept = store.sweep_expired_leases(clock.now()).await.unwrap();
        assert_eq!(swept, 1);
        // Re-claimable.
        let claim_again = store
            .claim("alice", "consumer", Duration::from_millis(100))
            .await
            .unwrap();
        assert!(claim_again.is_some());
    }
}
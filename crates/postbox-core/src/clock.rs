//! Abstract clock so storage backends and the sweeper are deterministic in
//! tests.
//!
//! Production code uses [`SystemClock`]. Tests use [`MockClock`] to step
//! time forward at a known rate and observe lease-expiry behavior exactly.

use std::sync::Arc;
use std::time::SystemTime;

use parking_lot::Mutex;

/// Indirection over the wall clock.
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> SystemTime;
}

/// Production clock: just asks the OS.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Test clock. Wraps a `SystemTime` behind a mutex so callers can step time
/// forward arbitrarily.
#[derive(Debug, Clone)]
pub struct MockClock {
    inner: Arc<Mutex<SystemTime>>,
}

impl MockClock {
    pub fn new(start: SystemTime) -> Self {
        Self {
            inner: Arc::new(Mutex::new(start)),
        }
    }

    /// Advance time by `delta`.
    pub fn advance(&self, delta: std::time::Duration) {
        let mut guard = self.inner.lock();
        *guard = guard
            .checked_add(delta)
            .expect("clock advanced past the heat-death of the universe");
    }

    /// Set the clock to an absolute value.
    pub fn set(&self, t: SystemTime) {
        *self.inner.lock() = t;
    }

    /// Return the current time.
    pub fn peek(&self) -> SystemTime {
        *self.inner.lock()
    }
}

impl Clock for MockClock {
    fn now(&self) -> SystemTime {
        self.peek()
    }
}
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Time source abstraction used by the engine.
///
/// All timestamps written to the event log come from this source so that tests
/// can control time deterministically without touching the host clock.
pub trait Clock: Send + Sync {
    /// Current time in milliseconds since the Unix epoch.
    fn now_ms(&self) -> u64;

    /// Advance the clock to `timestamp_ms`.
    ///
    /// Default implementation is a no-op for clocks that do not support
    /// manual advancement (e.g. the system clock).
    fn advance_clock_to(&self, _timestamp_ms: u64) {}
}

/// Production clock backed by the system monotonic wall-clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_millis() as u64
    }

    fn advance_clock_to(&self, _timestamp_ms: u64) {
        // System clock cannot be manually advanced.
    }
}

/// Test-only clock with explicit manual advancement.
#[derive(Debug)]
pub struct MockClock {
    now: AtomicU64,
}

impl MockClock {
    /// Create a clock frozen at the given timestamp.
    pub fn at(timestamp_ms: u64) -> Self {
        Self {
            now: AtomicU64::new(timestamp_ms),
        }
    }

    /// Advance the clock to `timestamp_ms`.
    ///
    /// # Panics
    /// Panics if `timestamp_ms` is earlier than the current clock value.
    pub fn advance_clock_to(&self, timestamp_ms: u64) {
        let current = self.now.load(Ordering::SeqCst);
        assert!(
            timestamp_ms >= current,
            "clock cannot move backwards: {timestamp_ms} < {current}"
        );
        self.now.store(timestamp_ms, Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }

    fn advance_clock_to(&self, timestamp_ms: u64) {
        Self::advance_clock_to(self, timestamp_ms);
    }
}

impl Default for MockClock {
    fn default() -> Self {
        Self::at(0)
    }
}

/// Helper to create an `Arc<dyn Clock>` from a concrete clock value.
pub fn into_clock<C: Clock + 'static>(clock: C) -> Arc<dyn Clock> {
    Arc::new(clock)
}

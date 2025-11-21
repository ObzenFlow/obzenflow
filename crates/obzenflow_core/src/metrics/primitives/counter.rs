use std::sync::atomic::{AtomicU64, Ordering};

/// Monotonically increasing counter
///
/// Thread-safe atomic counter that can only increase or reset to zero.
/// Used as the foundation for rate metrics, error counting, etc.
#[derive(Debug)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    /// Create a new counter starting at zero
    pub fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }

    /// Increment the counter by 1
    pub fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Get the current counter value
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Reset the counter to zero
    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter_basic_operations() {
        let counter = Counter::new();
        assert_eq!(counter.get(), 0);

        counter.increment();
        assert_eq!(counter.get(), 1);

        counter.increment();
        counter.increment();
        assert_eq!(counter.get(), 3);

        counter.reset();
        assert_eq!(counter.get(), 0);
    }
}

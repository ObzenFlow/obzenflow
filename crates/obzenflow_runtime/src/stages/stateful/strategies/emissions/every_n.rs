// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: EveryN Emission Strategy
//
// Emits results periodically every N events processed.

use super::EmissionStrategy;
use std::time::Instant;

/// Emit every N events.
///
/// This strategy emits results periodically after processing a fixed number
/// of events. Useful for progress updates and periodic checkpoints.
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::emissions::EveryN;
///
/// let mut strategy = EveryN::new(10);
/// assert!(!strategy.should_emit(5, None));   // Not yet at 10
/// assert!(strategy.should_emit(10, None));   // At boundary, emit
/// strategy.reset();
/// assert!(!strategy.should_emit(5, None));   // Counter reset
/// assert!(strategy.should_emit(10, None));   // Next boundary
/// ```
#[derive(Debug, Clone)]
pub struct EveryN {
    count: u64,
}

impl EveryN {
    /// Create a new EveryN emission strategy.
    ///
    /// # Arguments
    ///
    /// * `count` - Emit results every `count` events
    ///
    /// # Panics
    ///
    /// Panics if `count` is 0.
    pub fn new(count: u64) -> Self {
        assert!(count > 0, "EveryN count must be greater than 0");
        Self { count }
    }
}

impl EmissionStrategy for EveryN {
    fn should_emit(&mut self, events_seen: u64, _last_emit: Option<Instant>) -> bool {
        // Emit at boundaries: events_seen is divisible by count
        // This handles both the initial emission and subsequent ones
        events_seen > 0 && events_seen.is_multiple_of(self.count)
    }

    fn reset(&mut self) {
        // Not needed for modulo-based approach
        // Reset is a no-op since we use modulo arithmetic on events_seen
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_every_n_emits_at_boundaries() {
        let mut strategy = EveryN::new(10);

        assert!(!strategy.should_emit(1, None));
        assert!(!strategy.should_emit(5, None));
        assert!(!strategy.should_emit(9, None));
        assert!(strategy.should_emit(10, None)); // At boundary

        strategy.reset();

        assert!(!strategy.should_emit(1, None));
        assert!(!strategy.should_emit(9, None));
        assert!(strategy.should_emit(10, None)); // Next boundary
    }

    #[test]
    fn test_every_n_small_count() {
        let mut strategy = EveryN::new(1);

        assert!(strategy.should_emit(1, None));
        strategy.reset();
        assert!(strategy.should_emit(1, None));
    }

    #[test]
    fn test_every_n_large_count() {
        let mut strategy = EveryN::new(1000);

        assert!(!strategy.should_emit(999, None));
        assert!(strategy.should_emit(1000, None));
    }

    #[test]
    #[should_panic(expected = "EveryN count must be greater than 0")]
    fn test_every_n_zero_count_panics() {
        EveryN::new(0);
    }

    #[test]
    fn test_every_n_reset() {
        let mut strategy = EveryN::new(5);

        assert!(strategy.should_emit(5, None));

        // Reset is a no-op for modulo-based approach
        strategy.reset();

        // Still need to be at a boundary (multiple of 5) to emit
        assert!(!strategy.should_emit(3, None));
        assert!(strategy.should_emit(5, None));
    }
}

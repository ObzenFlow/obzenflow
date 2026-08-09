// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: TimeWindow Emission Strategy
//
// Emits results periodically based on wall-clock time (processing time).
//
// This is a processing-time trigger, not an event-time/watermark window model.

use super::EmissionStrategy;
use std::time::Duration;

/// Emit every duration.
///
/// This strategy emits results periodically based on wall-clock time.
/// Creates tumbling windows that emit after a fixed duration.
///
/// This uses processing time (wall-clock time) and therefore has the usual
/// caveats: boundaries are driven by the runtime's scheduling and can be
/// tick-approximate. ObzenFlow does not currently model event-time windows or
/// watermark advancement.
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::emissions::TimeWindow;
/// use std::time::Duration;
///
/// let strategy = TimeWindow::new(Duration::from_secs(5));
/// // Will emit every 5 seconds
/// ```
#[derive(Debug, Clone)]
pub struct TimeWindow {
    duration: Duration,
}

impl TimeWindow {
    /// Create a new TimeWindow emission strategy.
    ///
    /// # Arguments
    ///
    /// * `duration` - Duration of each time window
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

impl EmissionStrategy for TimeWindow {
    fn should_emit(&self, events_seen: u64, period_elapsed: Option<Duration>) -> bool {
        events_seen > 0 && period_elapsed.is_some_and(|elapsed| elapsed >= self.duration)
    }

    fn resets_accumulator_on_emit(&self) -> bool {
        true
    }

    fn emit_interval_hint(&self) -> Option<Duration> {
        Some(self.duration)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_window_first_emission() {
        let strategy = TimeWindow::new(Duration::from_millis(100));

        // First call should not emit immediately (unless duration is 0)
        assert!(!strategy.should_emit(0, None));
    }

    #[test]
    fn test_time_window_emits_after_duration() {
        let strategy = TimeWindow::new(Duration::from_millis(10));
        assert!(!strategy.should_emit(1, Some(Duration::from_millis(9))));
        assert!(strategy.should_emit(1, Some(Duration::from_millis(10))));
    }

    #[test]
    fn test_time_window_requires_contributions() {
        let strategy = TimeWindow::new(Duration::from_millis(10));
        assert!(!strategy.should_emit(0, Some(Duration::from_secs(1))));
    }
}

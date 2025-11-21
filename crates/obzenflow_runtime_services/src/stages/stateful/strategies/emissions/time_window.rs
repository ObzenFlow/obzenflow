// FLOWIP-080c: TimeWindow Emission Strategy
//
// Emits results periodically based on wall-clock time.
// NOTE: This should only be used for observability, not correctness.
// For correctness, use causal ordering strategies.

use super::EmissionStrategy;
use std::time::{Duration, Instant};

/// Emit every duration.
///
/// This strategy emits results periodically based on wall-clock time.
/// Creates tumbling windows that emit after a fixed duration.
///
/// **WARNING**: This uses wall-clock time and should only be used for
/// observability purposes (dashboards, monitoring). For correctness in
/// distributed systems, use causal ordering strategies instead.
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime_services::stages::stateful::strategies::emissions::TimeWindow;
/// use std::time::Duration;
///
/// let mut strategy = TimeWindow::new(Duration::from_secs(5));
/// // Will emit every 5 seconds
/// ```
#[derive(Debug, Clone)]
pub struct TimeWindow {
    duration: Duration,
    window_start: Option<Instant>,
}

impl TimeWindow {
    /// Create a new TimeWindow emission strategy.
    ///
    /// # Arguments
    ///
    /// * `duration` - Duration of each time window
    pub fn new(duration: Duration) -> Self {
        Self {
            duration,
            window_start: None,
        }
    }
}

impl EmissionStrategy for TimeWindow {
    fn should_emit(&mut self, _events_seen: u64, last_emit: Option<Instant>) -> bool {
        let now = Instant::now();

        // Initialize window start on first call
        if self.window_start.is_none() {
            self.window_start = Some(last_emit.unwrap_or(now));
        }

        let window_start = self.window_start.unwrap();

        // Check if duration has elapsed since window start
        if now.duration_since(window_start) >= self.duration {
            return true;
        }

        false
    }

    fn reset(&mut self) {
        // Start new window from now
        self.window_start = Some(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_time_window_first_emission() {
        let mut strategy = TimeWindow::new(Duration::from_millis(100));

        // First call should not emit immediately (unless duration is 0)
        assert!(!strategy.should_emit(0, None));
    }

    #[test]
    fn test_time_window_emits_after_duration() {
        let mut strategy = TimeWindow::new(Duration::from_millis(10));

        let start = Instant::now();
        assert!(!strategy.should_emit(0, Some(start)));

        // Wait for duration to elapse
        thread::sleep(Duration::from_millis(15));

        assert!(strategy.should_emit(0, Some(start)));
    }

    #[test]
    fn test_time_window_reset() {
        let mut strategy = TimeWindow::new(Duration::from_millis(10));

        let start = Instant::now();
        strategy.window_start = Some(start);

        thread::sleep(Duration::from_millis(15));
        assert!(strategy.should_emit(0, Some(start)));

        strategy.reset();

        // After reset, should not emit immediately
        assert!(!strategy.should_emit(0, Some(Instant::now())));
    }

    #[test]
    fn test_time_window_multiple_windows() {
        let mut strategy = TimeWindow::new(Duration::from_millis(5));

        let start = Instant::now();

        // First window
        assert!(!strategy.should_emit(0, Some(start)));
        thread::sleep(Duration::from_millis(10));
        assert!(strategy.should_emit(0, Some(start)));

        strategy.reset();

        // Second window
        assert!(!strategy.should_emit(0, Some(Instant::now())));
        thread::sleep(Duration::from_millis(10));
        assert!(strategy.should_emit(0, Some(Instant::now())));
    }
}

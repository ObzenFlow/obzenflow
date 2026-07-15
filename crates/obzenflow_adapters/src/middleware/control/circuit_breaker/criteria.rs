// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Opening criteria for [`CircuitBreaker::opens_when`](super::CircuitBreaker).
//!
//! The criteria object makes the failure-detection configuration one coherent
//! value: slow-call contribution and minimum-call floors exist only on the
//! rate-based type, so misordered configuration cannot compile, let alone
//! panic.

use super::config::CircuitBreakerFailureMode;
use super::window::FailureWindow;
use std::num::NonZeroU32;
use std::time::Duration;

/// Rate-based opening: the circuit opens when the failure rate over a window
/// meets `threshold` (0 < threshold <= 1). Choose the window with
/// [`FailureRateCriteria::over_last_calls`] or
/// [`FailureRateCriteria::over_window`].
pub fn failure_rate(threshold: f32) -> FailureRateCriteria {
    assert!(
        (0.0..=1.0).contains(&threshold) && threshold > 0.0,
        "circuit-breaker failure_rate must be in (0.0, 1.0], got {threshold}"
    );
    FailureRateCriteria { threshold }
}

/// A failure-rate threshold awaiting its window.
#[derive(Debug, Clone, Copy)]
pub struct FailureRateCriteria {
    threshold: f32,
}

impl FailureRateCriteria {
    /// Evaluate the rate over a sliding window of the last `window` calls.
    /// The breaker waits for at least `window` calls before evaluating;
    /// override with [`RateCriteria::min_calls`].
    pub fn over_last_calls(self, window: u32) -> RateCriteria {
        assert!(window > 0, "circuit-breaker window must be > 0");
        RateCriteria {
            window: FailureWindow::Count { size: window },
            failure_rate_threshold: self.threshold,
            slow_call: None,
            minimum_calls: window,
        }
    }

    /// Evaluate the rate over a sliding time window. The breaker waits for at
    /// least 10 calls before evaluating; override with
    /// [`RateCriteria::min_calls`].
    pub fn over_window(self, duration: Duration) -> RateCriteria {
        assert!(
            !duration.is_zero(),
            "circuit-breaker window duration must be > 0"
        );
        const DEFAULT_MIN_CALLS: u32 = 10;
        RateCriteria {
            window: FailureWindow::Time { duration },
            failure_rate_threshold: self.threshold,
            slow_call: None,
            minimum_calls: DEFAULT_MIN_CALLS,
        }
    }
}

/// Complete rate-based opening criteria.
#[derive(Debug, Clone)]
pub struct RateCriteria {
    window: FailureWindow,
    failure_rate_threshold: f32,
    slow_call: Option<(Duration, f32)>,
    minimum_calls: u32,
}

impl RateCriteria {
    /// Also count calls at or over `duration` as slow, opening the circuit
    /// when the slow-call rate in the window meets `rate` (0 < rate <= 1).
    /// Under recovery, one logical invocation's whole session is one sample.
    pub fn or_slow_calls_over(mut self, duration: Duration, rate: f32) -> Self {
        assert!(
            !duration.is_zero(),
            "circuit-breaker slow-call duration must be > 0"
        );
        assert!(
            (0.0..=1.0).contains(&rate) && rate > 0.0,
            "circuit-breaker slow-call rate must be in (0.0, 1.0], got {rate}"
        );
        self.slow_call = Some((duration, rate));
        self
    }

    /// Minimum calls observed before the rate is evaluated.
    pub fn min_calls(mut self, calls: u32) -> Self {
        assert!(calls > 0, "circuit-breaker min_calls must be > 0");
        self.minimum_calls = calls;
        self
    }

    /// The window size reported to planning and configuration snapshots,
    /// standing in for the consecutive threshold those surfaces expect.
    pub(super) fn reporting_threshold(&self) -> usize {
        match self.window {
            FailureWindow::Count { size } => size as usize,
            FailureWindow::Time { .. } => self.minimum_calls as usize,
        }
    }

    pub(super) fn into_failure_mode(self) -> CircuitBreakerFailureMode {
        let (slow_call_duration_threshold, slow_call_rate_threshold) = match self.slow_call {
            Some((duration, rate)) => (Some(duration), Some(rate)),
            None => (None, None),
        };
        CircuitBreakerFailureMode::RateBased {
            window: self.window,
            failure_rate_threshold: self.failure_rate_threshold,
            slow_call_rate_threshold,
            slow_call_duration_threshold,
            minimum_calls: NonZeroU32::new(self.minimum_calls)
                .expect("min_calls is validated at construction"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_window_criteria_lower_to_rate_based_mode() {
        let criteria = failure_rate(0.6)
            .over_last_calls(5)
            .or_slow_calls_over(Duration::from_millis(250), 0.5)
            .min_calls(2);
        assert_eq!(criteria.reporting_threshold(), 5);
        let CircuitBreakerFailureMode::RateBased {
            window: FailureWindow::Count { size },
            failure_rate_threshold,
            slow_call_rate_threshold,
            slow_call_duration_threshold,
            minimum_calls,
        } = criteria.into_failure_mode()
        else {
            panic!("count-window criteria must lower to rate-based mode");
        };
        assert_eq!(size, 5);
        assert_eq!(failure_rate_threshold, 0.6);
        assert_eq!(slow_call_rate_threshold, Some(0.5));
        assert_eq!(
            slow_call_duration_threshold,
            Some(Duration::from_millis(250))
        );
        assert_eq!(minimum_calls.get(), 2);
    }

    #[test]
    fn time_window_defaults_minimum_calls() {
        let criteria = failure_rate(1.0).over_window(Duration::from_secs(30));
        assert_eq!(criteria.reporting_threshold(), 10);
    }

    #[test]
    #[should_panic(expected = "failure_rate must be in (0.0, 1.0]")]
    fn zero_failure_rate_is_rejected() {
        let _ = failure_rate(0.0);
    }

    #[test]
    #[should_panic(expected = "window must be > 0")]
    fn zero_call_window_is_rejected() {
        let _ = failure_rate(0.5).over_last_calls(0);
    }

    #[test]
    #[should_panic(expected = "slow-call rate must be in (0.0, 1.0]")]
    fn out_of_range_slow_rate_is_rejected() {
        let _ = failure_rate(0.5)
            .over_last_calls(5)
            .or_slow_calls_over(Duration::from_millis(1), 1.5);
    }
}

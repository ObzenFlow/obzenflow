// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use std::time::Duration;

/// Retry limits enforced by the circuit breaker.
#[derive(Debug, Clone)]
pub struct RetryLimits {
    /// Maximum breaker-generated delay between physical calls. A provider's
    /// rate-limit floor may be longer and is never shortened by this cap.
    pub max_single_delay: Duration,
    /// Maximum elapsed time at which another physical call may start.
    pub max_attempt_start_window: Duration,
}

impl Default for RetryLimits {
    fn default() -> Self {
        Self {
            max_single_delay: Duration::from_secs(30),
            max_attempt_start_window: Duration::from_secs(120),
        }
    }
}

/// Configuration for integrated per-event retry inside the circuit breaker.
#[derive(Debug, Clone)]
pub(crate) struct CircuitBreakerRetryPolicy {
    pub max_attempts: u32,
    pub backoff: BackoffStrategy,
    #[cfg(test)]
    pub(super) deterministic_jitter_samples: Option<Vec<f64>>,
}

impl CircuitBreakerRetryPolicy {
    pub(crate) fn calculate_delay(&self, attempt: usize) -> Duration {
        #[cfg(test)]
        if let Some(samples) = &self.deterministic_jitter_samples {
            let sample = samples
                .get(attempt)
                .copied()
                .expect("deterministic jitter sample for every tested continuation");
            return calculate_delay_with_jitter_sample(&self.backoff, attempt, sample);
        }

        self.backoff.calculate_delay(attempt)
    }

    #[cfg(test)]
    pub(crate) fn use_deterministic_jitter_samples(&mut self, samples: Vec<f64>) {
        assert!(samples.iter().all(|sample| (0.0..1.0).contains(sample)));
        self.deterministic_jitter_samples = Some(samples);
    }
}

#[cfg(test)]
fn calculate_delay_with_jitter_sample(
    backoff: &BackoffStrategy,
    attempt: usize,
    sample: f64,
) -> Duration {
    match backoff {
        BackoffStrategy::Fixed { delay } => *delay,
        BackoffStrategy::Exponential {
            initial,
            max,
            factor,
            jitter,
        } => {
            let base_delay = initial.as_millis() as f64 * factor.powi(attempt as i32);
            let capped_delay = base_delay.min(max.as_millis() as f64);
            let final_delay = if *jitter {
                capped_delay * (1.0 + (sample - 0.5) * 0.2)
            } else {
                capped_delay
            };
            Duration::from_millis(final_delay as u64)
        }
    }
}

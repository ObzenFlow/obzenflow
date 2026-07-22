// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use std::time::Duration;

/// Inert retry configuration for `EffectResilience`.
#[derive(Debug, Clone)]
pub struct Retry {
    pub(in crate::middleware::control) policy: CircuitBreakerRetryPolicy,
    pub(in crate::middleware::control) limits: RetryLimits,
}

impl Retry {
    pub fn fixed(delay: Duration) -> Self {
        Self::with_backoff(BackoffStrategy::Fixed { delay })
    }

    /// Default exponential backoff: 250 ms initial, factor 2, four-second
    /// strategy cap, and jitter.
    pub fn exponential() -> Self {
        Self::with_backoff(BackoffStrategy::Exponential {
            initial: Duration::from_millis(250),
            max: Duration::from_secs(4),
            factor: 2.0,
            jitter: true,
        })
    }

    fn with_backoff(backoff: BackoffStrategy) -> Self {
        Self {
            policy: CircuitBreakerRetryPolicy {
                max_attempts: 3,
                backoff,
            },
            limits: RetryLimits::default(),
        }
    }

    /// Total physical calls including the first. Validation occurs when the
    /// enclosing `EffectResilience` is built.
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.policy.max_attempts = attempts;
        self
    }

    /// Cap breaker-generated backoff. A typed provider delay floor is never
    /// shortened by this value.
    pub fn max_backoff(mut self, delay: Duration) -> Self {
        self.limits.max_single_delay = delay;
        self
    }

    /// Window measured from the first call in which another attempt may
    /// start. It never cancels an already in-flight call.
    pub fn attempt_start_window(mut self, window: Duration) -> Self {
        self.limits.max_attempt_start_window = window;
        self
    }
}

#[derive(Debug, Clone)]
pub(in crate::middleware::control) struct RetryLimits {
    pub(in crate::middleware::control) max_single_delay: Duration,
    pub(in crate::middleware::control) max_attempt_start_window: Duration,
}

impl Default for RetryLimits {
    fn default() -> Self {
        Self {
            max_single_delay: Duration::from_secs(30),
            max_attempt_start_window: Duration::from_secs(120),
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::middleware::control) struct CircuitBreakerRetryPolicy {
    pub(in crate::middleware::control) max_attempts: u32,
    pub(in crate::middleware::control) backoff: BackoffStrategy,
}

impl CircuitBreakerRetryPolicy {
    pub(in crate::middleware::control) fn calculate_delay(&self, attempt: usize) -> Duration {
        self.backoff.calculate_delay(attempt)
    }
}

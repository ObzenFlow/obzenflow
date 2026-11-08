// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use std::time::Duration;

/// Inert retry configuration for one eligible declared effect.
#[derive(Debug, Clone)]
pub struct Retry {
    pub(in crate::middleware::control) policy: RetryPolicy,
    pub(in crate::middleware::control) limits: RetryLimits,
}

/// What a retry does when the circuit breaker protecting its effect ends a
/// call: refuses it, opens on its attempt, or fails its probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnRefusal {
    /// Wait for the breaker to admit the call again, within the retry's
    /// outage bound.
    Wait,
    /// End the call at once.
    Fail,
}

impl OnRefusal {
    pub(in crate::middleware::control) fn as_str(self) -> &'static str {
        match self {
            Self::Wait => "wait",
            Self::Fail => "fail",
        }
    }
}

pub(in crate::middleware::control) const DEFAULT_MAX_OUTAGE_WAIT: Duration =
    Duration::from_millis(obzenflow_runtime::runtime_config::DEFAULT_RETRY_MAX_OUTAGE_WAIT_MS);

impl Retry {
    pub fn fixed_delay(mut self, delay: Duration) -> Self {
        self.policy.backoff = BackoffStrategy::Fixed { delay };
        self
    }

    /// Default exponential backoff: 250 ms initial, factor 2, four-second
    /// strategy cap, and jitter.
    pub fn exponential_backoff(mut self) -> Self {
        self.policy.backoff = Self::default_backoff();
        self
    }

    fn default_backoff() -> BackoffStrategy {
        BackoffStrategy::Exponential {
            initial: Duration::from_millis(250),
            max: Duration::from_secs(4),
            factor: 2.0,
            jitter: true,
        }
    }

    fn with_backoff(backoff: BackoffStrategy) -> Self {
        Self {
            policy: RetryPolicy {
                max_attempts: 3,
                backoff,
                on_refusal: None,
            },
            limits: RetryLimits::default(),
        }
    }

    /// Total physical calls including the first, counted again after each
    /// wait on an open circuit breaker. Validation occurs when the flow is
    /// built.
    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.policy.max_attempts = attempts;
        self
    }

    /// Cap generated backoff. A typed provider delay floor is never
    /// shortened by this value.
    pub fn max_backoff(mut self, delay: Duration) -> Self {
        self.limits.max_single_delay = delay;
        self
    }

    /// Window measured from the first call, and again from the first call
    /// after each wait on an open circuit breaker, in which another attempt
    /// may start. It never cancels an already in-flight call.
    pub fn attempt_start_window(mut self, window: Duration) -> Self {
        self.limits.max_attempt_start_window = window;
        self
    }

    /// Whether a call the protecting circuit breaker ends waits for it or
    /// fails at once. Waiting is the default. Requires a circuit breaker on
    /// the same effect.
    pub fn on_refusal(mut self, on_refusal: OnRefusal) -> Self {
        self.policy.on_refusal = Some(on_refusal);
        self
    }

    /// How far into the protecting circuit breaker's outage, measured from
    /// when it opened, a call keeps waiting. Two minutes by default.
    /// Requires a circuit breaker on the same effect.
    pub fn max_outage_wait(mut self, wait: Duration) -> Self {
        self.limits.max_outage_wait = Some(wait);
        self
    }
}

#[derive(Debug, Clone)]
pub(in crate::middleware::control) struct RetryLimits {
    pub(in crate::middleware::control) max_single_delay: Duration,
    pub(in crate::middleware::control) max_attempt_start_window: Duration,
    pub(in crate::middleware::control) max_outage_wait: Option<Duration>,
}

impl Default for RetryLimits {
    fn default() -> Self {
        Self {
            max_single_delay: Duration::from_secs(30),
            max_attempt_start_window: Duration::from_secs(120),
            max_outage_wait: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(in crate::middleware::control) struct RetryPolicy {
    pub(in crate::middleware::control) max_attempts: u32,
    pub(in crate::middleware::control) backoff: BackoffStrategy,
    pub(in crate::middleware::control) on_refusal: Option<OnRefusal>,
}

impl RetryPolicy {
    pub(in crate::middleware::control) fn calculate_delay(&self, attempt: usize) -> Duration {
        self.backoff.calculate_delay(attempt)
    }
}

/// Declare an inert retry policy using the default exponential schedule.
pub fn retry() -> Retry {
    Retry::with_backoff(Retry::default_backoff())
}

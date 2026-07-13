// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::middleware::BoundaryRetryPolicy;
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use std::num::NonZeroU32;
use std::time::Duration;

/// Retry limits enforced by the circuit breaker.
#[derive(Debug, Clone)]
pub struct RetryLimits {
    pub max_single_delay: Duration,
    pub max_total_wall_time: Duration,
}

impl Default for RetryLimits {
    fn default() -> Self {
        Self {
            max_single_delay: Duration::from_secs(30),
            max_total_wall_time: Duration::from_secs(120),
        }
    }
}

/// Circuit-breaker recovery policy executed by the live source, effect, or
/// sink boundary. The breaker owns the policy; the boundary owns reinvocation.
#[derive(Debug, Clone)]
pub struct CircuitBreakerRetryPolicy {
    /// Maximum total physical executions, including the initial attempt.
    pub max_attempts: u32,
    pub backoff: BackoffStrategy,
}

impl CircuitBreakerRetryPolicy {
    pub(crate) fn validated(
        policy: &CircuitBreakerRetryPolicy,
        limits: &RetryLimits,
    ) -> Option<BoundaryRetryPolicy> {
        Some(BoundaryRetryPolicy {
            max_attempts: NonZeroU32::new(policy.max_attempts)?,
            backoff: policy.backoff.clone(),
            max_single_delay: limits.max_single_delay,
            max_total_wall_time: limits.max_total_wall_time,
        })
    }
}

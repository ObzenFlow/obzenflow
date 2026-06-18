// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::FailureClassification;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use std::time::{Duration, Instant};

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

/// Configuration for integrated per-event retry inside the circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerRetryPolicy {
    pub max_attempts: u32,
    pub backoff: BackoffStrategy,
}

#[derive(Debug, Clone)]
pub(super) struct RetryState {
    pub(super) attempts: u32,
    pub(super) first_attempt: Instant,
    pub(super) last_attempt: Instant,
    pub(super) last_error: Option<String>,
    pub(super) last_kind: Option<ErrorKind>,
    pub(super) classification: FailureClassification,
}

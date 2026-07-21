// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::classifier::{FailureClassificationClassifier, FailureClassificationPolicy};
use super::FailureWindow;
use std::num::NonZeroU32;
use std::time::Duration;
use thiserror::Error;

/// How the circuit breaker decides when to open while in the Closed state.
#[derive(Debug, Clone)]
pub(in crate::middleware::control) enum CircuitBreakerFailureMode {
    /// Simple consecutive-failures threshold (current default behaviour).
    Consecutive { max_failures: NonZeroU32 },
    /// Rate-based mode over a sliding window of recent calls.
    RateBased {
        window: FailureWindow,
        failure_rate_threshold: f32,
        slow_call_rate_threshold: Option<f32>,
        slow_call_duration_threshold: Option<Duration>,
        minimum_calls: NonZeroU32,
    },
}

#[derive(Debug, Error, Clone, PartialEq)]
pub enum CircuitBreakerConfigError {
    #[error("circuit breaker requires exactly one mode: consecutive_failures or count_window")]
    MissingMode,
    #[error("circuit breaker cannot combine consecutive_failures with count_window")]
    MixedModes,
    #[error("count-window mode requires minimum_calls")]
    MissingMinimumCalls,
    #[error("count-window mode requires failure_rate_threshold or paired slow-call settings")]
    MissingRateTrigger,
    #[error("slow_call_duration and slow_call_rate_threshold must be configured together")]
    IncompleteSlowCallTrigger,
    #[error("{field} must be greater than zero")]
    Zero { field: &'static str },
    #[error("{field} must be finite and in (0, 1], got {value}")]
    InvalidRate { field: &'static str, value: f64 },
    #[error("minimum_calls ({minimum_calls}) must be <= count_window ({count_window})")]
    MinimumCallsExceedsWindow {
        minimum_calls: u32,
        count_window: u32,
    },
}

#[derive(Clone)]
pub(in crate::middleware::control) struct EffectCircuitBreakerConfig {
    pub(in crate::middleware::control) failure_mode: CircuitBreakerFailureMode,
    pub(in crate::middleware::control) open_for: Duration,
    pub(in crate::middleware::control) probes: NonZeroU32,
    pub(in crate::middleware::control) classifier: Option<FailureClassificationClassifier>,
    pub(in crate::middleware::control) failure_classification_policy: FailureClassificationPolicy,
}

/// Behaviour while the circuit breaker is in the Open state.
#[derive(Debug, Clone, Default)]
pub enum OpenPolicy {
    /// Emit degraded responses via fallback when configured; otherwise behave
    /// like a simple rejection/skip. This matches the existing default
    /// behaviour from 051b-part-2.
    #[default]
    EmitFallback,
    /// Fail fast with an explicit rejection instead of attempting to
    /// synthesize degraded responses.
    FailFast,
    /// Drop events while Open (best-effort fire-and-forget or sampling
    /// scenarios). This is powerful but should be used with care when
    /// contracts are strict.
    Skip,
}

/// Behaviour while the circuit breaker is in the HalfOpen state.
#[derive(Debug, Clone)]
pub struct HalfOpenPolicy {
    /// Maximum number of concurrent probe calls allowed while HalfOpen.
    pub(super) permitted_probes: NonZeroU32,
    /// Policy applied to non-probe calls that arrive while all probe slots
    /// are already in use.
    pub(super) on_rejected: OpenPolicy,
}

impl Default for HalfOpenPolicy {
    fn default() -> Self {
        Self {
            // Matches existing single-probe behaviour.
            permitted_probes: NonZeroU32::new(1)
                .expect("permitted_probes default must be non-zero"),
            // Non-probe calls behave like Open with EmitFallback by default.
            on_rejected: OpenPolicy::EmitFallback,
        }
    }
}

impl HalfOpenPolicy {
    /// Create a new HalfOpenPolicy with the given probe limit and
    /// behaviour for non-probe calls. Flows configure this through the
    /// builder's `probes` and `when_probe_rejected` methods.
    pub(crate) fn new(permitted_probes: NonZeroU32, on_rejected: OpenPolicy) -> Self {
        Self {
            permitted_probes,
            on_rejected,
        }
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub(super) enum CircuitBreakerThresholdError {
    #[error("CircuitBreaker threshold must be in 1..=u32::MAX, got {threshold}")]
    InvalidThreshold { threshold: usize },
}

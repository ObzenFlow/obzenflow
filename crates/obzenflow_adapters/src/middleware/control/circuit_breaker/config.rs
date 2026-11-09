// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::classifier::FailureClassificationClassifier;
use super::FailureWindow;
use std::num::NonZeroU32;
use std::time::Duration;
use thiserror::Error;

/// A validated circuit-breaker rate threshold in the public `(0.0, 1.0]`
/// ratio domain.
///
/// Keeping the authored `f64` representation prevents a valid positive value
/// from narrowing to zero before the breaker evaluates it.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::middleware::control) struct RateThreshold(f64);

impl RateThreshold {
    pub(in crate::middleware::control) fn checked(
        value: f64,
        field: &'static str,
    ) -> Result<Self, CircuitBreakerConfigError> {
        if value.is_finite() && 0.0 < value && value <= 1.0 {
            Ok(Self(value))
        } else {
            Err(CircuitBreakerConfigError::InvalidRate { field, value })
        }
    }

    pub(in crate::middleware::control) fn get(&self) -> f64 {
        self.0
    }
}

/// How the circuit breaker decides when to open while in the Closed state.
#[derive(Debug, Clone)]
pub(in crate::middleware::control) enum CircuitBreakerFailureMode {
    /// Simple consecutive-failures threshold (current default behaviour).
    Consecutive { max_failures: NonZeroU32 },
    /// Rate-based mode over a sliding window of recent calls.
    RateBased {
        window: FailureWindow,
        failure_rate_threshold: Option<RateThreshold>,
        slow_call_rate_threshold: Option<RateThreshold>,
        slow_call_duration_threshold: Option<Duration>,
        minimum_calls: NonZeroU32,
    },
}

#[derive(Debug, Error, Clone, PartialEq)]
pub enum CircuitBreakerConfigError {
    #[error("circuit breaker requires exactly one mode: consecutive_failures or count_window")]
    MissingMode,
    #[error("rate-based circuit breaker requires count_window(n)")]
    MissingCountWindow,
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
    #[error("{field} must be in 1..=4294967295, got {value}")]
    InvalidCount { field: &'static str, value: u64 },
    #[error("circuit breaker mode must be 'consecutive' or 'rate_based', got '{value}'")]
    UnknownMode { value: String },
    #[error("{field} must be finite and in (0, 1], got {value}")]
    InvalidRate { field: &'static str, value: f64 },
    #[error("minimum_calls ({minimum_calls}) must be <= count_window ({count_window})")]
    MinimumCallsExceedsWindow {
        minimum_calls: u32,
        count_window: u32,
    },
    #[error("exponential open_for_mode requires open_for_multiplier and max_open_for")]
    IncompleteExponentialOpenFor,
    #[error("open_for_multiplier must be finite and greater than 1.0, got {value:?}")]
    InvalidOpenForMultiplier { value: f64 },
    #[error(
        "max_open_for ({max_open_for_ms} ms) must be greater than open_for ({open_for_ms} ms)"
    )]
    MaxOpenForNotAboveOpenFor {
        max_open_for_ms: u64,
        open_for_ms: u64,
    },
    #[error("circuit breaker open_for_mode must be 'fixed' or 'exponential', got '{value}'")]
    UnknownOpenForMode { value: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::middleware::control) enum OpenForMode {
    Fixed,
    Exponential,
}

impl OpenForMode {
    pub(in crate::middleware::control) fn parse(
        value: &str,
    ) -> Result<Self, CircuitBreakerConfigError> {
        match value {
            "fixed" => Ok(Self::Fixed),
            "exponential" => Ok(Self::Exponential),
            other => Err(CircuitBreakerConfigError::UnknownOpenForMode {
                value: other.to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(in crate::middleware::control) enum OpenForCurve {
    Fixed,
    Exponential {
        multiplier: f64,
        max_open_for: Duration,
    },
}

impl OpenForCurve {
    pub(in crate::middleware::control) fn after_failed_probe(
        self,
        open_for: Duration,
        current: Duration,
    ) -> Duration {
        match self {
            Self::Fixed => open_for,
            Self::Exponential {
                multiplier,
                max_open_for,
            } => Duration::try_from_secs_f64(current.as_secs_f64() * multiplier)
                .map_or(max_open_for, |grown| grown.min(max_open_for)),
        }
    }
}

#[derive(Clone)]
pub(in crate::middleware::control) struct EffectCircuitBreakerConfig {
    pub(in crate::middleware::control) failure_mode: CircuitBreakerFailureMode,
    pub(in crate::middleware::control) open_for: Duration,
    pub(in crate::middleware::control) open_for_curve: OpenForCurve,
    pub(in crate::middleware::control) probes: NonZeroU32,
    pub(in crate::middleware::control) classifier: Option<FailureClassificationClassifier>,
    pub(in crate::middleware::control) rate_limited_counts_as_failure: bool,
}

/// Behaviour while the circuit breaker is in the HalfOpen state.
#[derive(Debug, Clone)]
pub struct HalfOpenPolicy {
    /// Maximum number of concurrent probe calls allowed while HalfOpen.
    pub(super) permitted_probes: NonZeroU32,
}

impl Default for HalfOpenPolicy {
    fn default() -> Self {
        Self {
            // Matches existing single-probe behaviour.
            permitted_probes: NonZeroU32::new(1)
                .expect("permitted_probes default must be non-zero"),
        }
    }
}

impl HalfOpenPolicy {
    pub(crate) fn new(permitted_probes: NonZeroU32) -> Self {
        Self { permitted_probes }
    }
}

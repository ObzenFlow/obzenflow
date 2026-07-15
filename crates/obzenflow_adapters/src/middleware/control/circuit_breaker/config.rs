// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::FailureWindow;
use std::num::NonZeroU32;
use std::time::Duration;
use thiserror::Error;

/// How the circuit breaker decides when to open while in the Closed state.
#[derive(Debug, Clone)]
pub(super) enum CircuitBreakerFailureMode {
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

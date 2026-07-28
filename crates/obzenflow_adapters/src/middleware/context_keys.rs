// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed keys for values stored inside `MiddlewareContext`.
//!
//! These keys replace string-based baggage like `"processing_start_nanos"` or
//! `"circuit_breaker.should_retry"`.

use obzenflow_core::MiddlewareContextKey;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

// ---- Timing / processing --------------------------------------------------

/// Wall-clock duration (nanoseconds) of the protected effect call
/// (`execute.await`), measured by the effect boundary and read by the circuit
/// breaker for slow-call detection (FLOWIP-115f). Replaces the breaker's old
/// heuristic of reading `processing_info.processing_time` off the handler
/// outputs, which is now stamped at commit time after the breaker observes.
pub(crate) struct EffectCallDurationNanos;
impl MiddlewareContextKey for EffectCallDurationNanos {
    type Value = u64;
    const LABEL: &'static str = "effect.call_duration_nanos";
}

pub(crate) struct CircuitBreakerIsProbe;
impl MiddlewareContextKey for CircuitBreakerIsProbe {
    type Value = bool;
    const LABEL: &'static str = "circuit_breaker.is_probe";
}

pub(crate) struct CircuitBreakerProbeGeneration;
impl MiddlewareContextKey for CircuitBreakerProbeGeneration {
    type Value = u64;
    const LABEL: &'static str = "circuit_breaker.probe_generation";
}

/// RAII guard for circuit-breaker half-open probe slots.
///
/// When a half-open probe is admitted, the circuit breaker increments its
/// `probe_in_flight` counter. Normal reverse-order policy observation settles
/// the slot; cancellation or a later policy rejection instead drops this
/// guard with the invocation-local `MiddlewareContext`.
#[derive(Debug)]
pub(crate) struct CircuitBreakerProbeSlotGuard {
    probe_in_flight: Arc<AtomicU32>,
    released: bool,
}

impl CircuitBreakerProbeSlotGuard {
    pub(crate) fn new(probe_in_flight: Arc<AtomicU32>) -> Self {
        Self {
            probe_in_flight,
            released: false,
        }
    }

    fn release_once(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        let _ = self
            .probe_in_flight
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                current.checked_sub(1)
            });
    }
}

impl Drop for CircuitBreakerProbeSlotGuard {
    fn drop(&mut self) {
        self.release_once();
    }
}

pub(crate) struct CircuitBreakerProbeSlot;
impl MiddlewareContextKey for CircuitBreakerProbeSlot {
    type Value = CircuitBreakerProbeSlotGuard;
    const LABEL: &'static str = "circuit_breaker.probe_slot_guard";
}

pub(crate) struct CircuitBreakerRetryAfterMs;
impl MiddlewareContextKey for CircuitBreakerRetryAfterMs {
    type Value = u64;
    const LABEL: &'static str = "circuit_breaker.retry_after_ms";
}

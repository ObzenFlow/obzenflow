// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed keys for values stored inside `MiddlewareContext`.
//!
//! These keys replace string-based baggage like `"processing_start_nanos"` or
//! `"circuit_breaker.should_retry"`.

use obzenflow_core::EventId;
use obzenflow_core::MiddlewareContextKey;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

// ---- Timing / processing --------------------------------------------------

pub(crate) struct ProcessingStartNanos;
impl MiddlewareContextKey for ProcessingStartNanos {
    type Value = u64;
    const LABEL: &'static str = "processing_start_nanos";
}

// ---- Circuit breaker integrated retry ------------------------------------

pub(crate) struct CircuitBreakerAttempt;
impl MiddlewareContextKey for CircuitBreakerAttempt {
    type Value = u32;
    const LABEL: &'static str = "circuit_breaker.attempt";
}

pub(crate) struct CircuitBreakerIsProbe;
impl MiddlewareContextKey for CircuitBreakerIsProbe {
    type Value = bool;
    const LABEL: &'static str = "circuit_breaker.is_probe";
}

/// RAII guard for circuit-breaker half-open probe slots.
///
/// When a half-open probe is admitted, the circuit breaker increments its
/// `probe_in_flight` counter. In the normal path the slot is released in
/// `post_handle`, but middleware short-circuiting (`Skip`/`Abort`) can bypass
/// that call. This guard ensures the slot is released when the per-pass
/// `MiddlewareContext` is dropped.
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
        self.probe_in_flight.fetch_sub(1, Ordering::SeqCst);
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

pub(crate) struct CircuitBreakerShouldRetry;
impl MiddlewareContextKey for CircuitBreakerShouldRetry {
    type Value = bool;
    const LABEL: &'static str = "circuit_breaker.should_retry";
}

pub(crate) struct CircuitBreakerRetryDelayMs;
impl MiddlewareContextKey for CircuitBreakerRetryDelayMs {
    type Value = u64;
    const LABEL: &'static str = "circuit_breaker.retry_delay_ms";
}

pub(crate) struct CircuitBreakerRetryAfterMs;
impl MiddlewareContextKey for CircuitBreakerRetryAfterMs {
    type Value = u64;
    const LABEL: &'static str = "circuit_breaker.retry_after_ms";
}

pub(crate) struct CircuitBreakerTotalRetryWallMs;
impl MiddlewareContextKey for CircuitBreakerTotalRetryWallMs {
    type Value = u64;
    const LABEL: &'static str = "circuit_breaker.total_retry_wall_ms";
}

// ---- Outcome enrichment ---------------------------------------------------

pub(crate) struct OutcomeProcessingFailed;
impl MiddlewareContextKey for OutcomeProcessingFailed {
    type Value = bool;
    const LABEL: &'static str = "processing_failed";
}

pub(crate) struct OutcomeFailedEventType;
impl MiddlewareContextKey for OutcomeFailedEventType {
    type Value = String;
    const LABEL: &'static str = "failed_event_type";
}

pub(crate) struct OutcomeRetryAttempt;
impl MiddlewareContextKey for OutcomeRetryAttempt {
    type Value = u32;
    const LABEL: &'static str = "retry_attempt";
}

// ---- AI map-reduce --------------------------------------------------------

#[derive(Debug, Clone, Copy)]
pub(crate) struct AiMapReduceChunkContext {
    pub job_key: EventId,
    pub chunk_index: usize,
    pub chunk_count: usize,
}

pub(crate) struct AiMapReduceChunkContextKey;
impl MiddlewareContextKey for AiMapReduceChunkContextKey {
    type Value = AiMapReduceChunkContext;
    const LABEL: &'static str = "ai.map_reduce.chunk_context";
}

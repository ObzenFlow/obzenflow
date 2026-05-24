// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed keys for values stored inside `MiddlewareContext`.
//!
//! These keys replace string-based baggage like `"processing_start_nanos"` or
//! `"circuit_breaker.should_retry"`.

use obzenflow_core::EventId;
use obzenflow_core::MiddlewareContextKey;

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

pub(crate) struct AiMapReduceJobKey;
impl MiddlewareContextKey for AiMapReduceJobKey {
    type Value = EventId;
    const LABEL: &'static str = "ai.map_reduce.job_key";
}

pub(crate) struct AiMapReduceChunkIndex;
impl MiddlewareContextKey for AiMapReduceChunkIndex {
    type Value = usize;
    const LABEL: &'static str = "ai.map_reduce.chunk_index";
}

pub(crate) struct AiMapReduceChunkCount;
impl MiddlewareContextKey for AiMapReduceChunkCount {
    type Value = usize;
    const LABEL: &'static str = "ai.map_reduce.chunk_count";
}


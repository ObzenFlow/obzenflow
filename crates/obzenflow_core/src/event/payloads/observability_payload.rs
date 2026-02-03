// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Observability event payloads
//!
//! Replaces the old string‐prefixed "system.*" events with
//! structured enums.  Aligned with the four‑bucket model introduced
//! in `chain_event.rs` (Data / FlowControl / Delivery / Observability).
//!
//! Tag names are now consistent:
//! • Top‑level enum uses `observability_type` (mirrors `content_type` in ChainEvent).
//! • Sub‑enums use `stage_state`, `metrics_event`, `middleware_event`, and `action`.

use crate::id::StageId;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// =============================================================================
//  Top‑level wrapper: what kind of observability fact is this?
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "observability_type", rename_all = "snake_case")]
pub enum ObservabilityPayload {
    Stage(StageLifecycle),
    Metrics(MetricsLifecycle),
    Middleware(MiddlewareLifecycle),
}

// =============================================================================
//  Stage lifecycle
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "stage_state", rename_all = "snake_case")]
pub enum StageLifecycle {
    Running {
        stage_id: StageId,
        #[serde(skip_serializing_if = "Option::is_none")]
        metadata: Option<Value>,
    },
    Draining {
        stage_id: StageId,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    Drained {
        stage_id: StageId,
        #[serde(skip_serializing_if = "Option::is_none")]
        events_processed: Option<u64>,
    },
    Completed {
        stage_id: StageId,
        #[serde(skip_serializing_if = "Option::is_none")]
        final_metrics: Option<Value>,
    },
    Failed {
        stage_id: StageId,
        error: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        recoverable: Option<bool>,
    },
}

// =============================================================================
//  Metrics lifecycle
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "metrics_event", rename_all = "snake_case")]
pub enum MetricsLifecycle {
    Ready {
        #[serde(skip_serializing_if = "Option::is_none")]
        exporter_count: Option<usize>,
    },
    StateSnapshot {
        metrics: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        window_duration_ms: Option<u64>,
    },
    ResourceUsage {
        cpu_percent: f64,
        memory_bytes: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        thread_count: Option<u32>,
    },
    Custom {
        name: String,
        value: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        tags: Option<Value>,
    },
    DrainRequested,
    Drained {
        #[serde(skip_serializing_if = "Option::is_none")]
        final_flush_count: Option<u64>,
    },
}

// =============================================================================
//  Middleware lifecycle (wrapper)
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "middleware_event",
    content = "details",
    rename_all = "snake_case"
)]
pub enum MiddlewareLifecycle {
    CircuitBreaker(CircuitBreakerEvent),
    RateLimiter(RateLimiterEvent),
    Backpressure(BackpressureEvent),
    Retry(RetryEvent),
    Sli(SliEvent),
}

// ---- Circuit breaker ------------------------------------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum CircuitBreakerEvent {
    Opened {
        error_rate: f64,
        failure_count: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_error: Option<String>,
    },
    Closed {
        success_count: u64,
        recovery_duration_ms: u64,
    },
    Rejected {
        #[serde(skip_serializing_if = "Option::is_none")]
        circuit_open_duration_ms: Option<u64>,
    },
    HalfOpen {
        test_request_count: u32,
    },
    Summary {
        window_duration_s: u64,
        requests_processed: u64,
        requests_rejected: u64,
        state: String,
        consecutive_failures: usize,
        rejection_rate: f64,
        // ---- Cumulative circuit breaker metrics (FLOWIP-059a-2) ----
        //
        // These fields are monotonic totals captured as wide-event snapshots so
        // downstream metrics exports remain scrape-resilient. They default to 0
        // for backwards compatibility with older journal entries.
        #[serde(default)]
        successes_total: u64,
        #[serde(default)]
        failures_total: u64,
        #[serde(default)]
        opened_total: u64,
        #[serde(default)]
        time_in_closed_seconds: f64,
        #[serde(default)]
        time_in_open_seconds: f64,
        #[serde(default)]
        time_in_half_open_seconds: f64,
    },
}

// ---- Rate limiter ---------------------------------------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum RateLimiterEvent {
    Delayed {
        delay_ms: u64,
        current_rate: f64,
        limit_rate: f64,
    },
    ActivityPulse {
        window_ms: u64,
        delayed_events: u64,
        delay_ms_total: u64,
        delay_ms_max: u64,
        limit_rate: f64,
    },
    ModeChange {
        mode_from: String,
        mode_to: String,
        limit_rate: f64,
    },
    WindowUtilization {
        utilization_percent: f64,
        events_in_window: u64,
        window_size_ms: u64,
    },
    ConfigChanged {
        old_rate: f64,
        new_rate: f64,
    },
}

// ---- Backpressure --------------------------------------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum BackpressureEvent {
    /// Low-volume, fixed-cadence pulse used as a UI animation driver (FLOWIP-086k).
    ///
    /// Mirrors the semantics of `RateLimiterEvent::ActivityPulse`: one event per second
    /// when delay activity occurred within the window. This prevents per-block flooding
    /// while still providing responsive real-time feedback.
    ActivityPulse {
        window_ms: u64,
        delayed_events: u64,
        delay_ms_total: u64,
        delay_ms_max: u64,

        /// Optional debug context: minimum downstream credit observed at pulse time.
        #[serde(skip_serializing_if = "Option::is_none")]
        min_credit: Option<u64>,

        /// Optional debug context: downstream stage ID that currently limits the writer.
        #[serde(skip_serializing_if = "Option::is_none")]
        limiting_downstream_stage_id: Option<StageId>,
    },
}

// ---- Retry ---------------------------------------------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum RetryEvent {
    AttemptStarted {
        attempt_number: u32,
        max_attempts: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        backoff_ms: Option<u64>,
    },
    SucceededAfterRetry {
        total_attempts: u32,
        total_duration_ms: u64,
    },
    Exhausted {
        total_attempts: u32,
        last_error: String,
        total_duration_ms: u64,
    },
}

// ---- SLI / SLO -----------------------------------------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "indicator", rename_all = "snake_case")]
pub enum SliEvent {
    LatencyPercentiles {
        p50_ms: f64,
        p90_ms: f64,
        p95_ms: f64,
        p99_ms: f64,
        p999_ms: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        sample_count: Option<u64>,
    },
    Availability {
        success_rate: f64,
        error_rate: f64,
        total_requests: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        window_duration_ms: Option<u64>,
    },
    ErrorBudget {
        remaining_percent: f64,
        consumed_percent: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        time_window_hours: Option<u32>,
    },
}

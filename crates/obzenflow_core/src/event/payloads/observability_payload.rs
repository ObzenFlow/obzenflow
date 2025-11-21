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

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

use crate::event::payloads::effect_payload::{EffectCursor, EffectType};
use crate::event::status::processing_status::ErrorKind;
use crate::event::types::EventId;
use crate::id::StageId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use ulid::Ulid;

// =============================================================================
//  Top‑level wrapper: what kind of observability fact is this?
// =============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "observability_type", rename_all = "snake_case")]
pub enum ObservabilityPayload {
    Stage(StageLifecycle),
    Metrics(MetricsLifecycle),
    Middleware(MiddlewareLifecycle),
    /// Runtime flow-control observability (FLOWIP-115e). Backpressure is not
    /// middleware, so its pulses and stall facts are a sibling of
    /// `Middleware`, never nested under it: the middleware machinery
    /// (system-journal mirror, framework-middleware classifier) matches only
    /// `Middleware(..)` and structurally never sees these rows.
    Backpressure(BackpressureEvent),
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
    /// One per-execution service-level-indicator sample (FLOWIP-115f).
    ///
    /// An `Indicator` row is a single observe-only sample of one operation
    /// execution. Aggregation (percentiles, error budgets, windowed rates) and
    /// any objective evaluation are FLOWIP-115l's job, reading these rows.
    Indicator(IndicatorSample),
    User(UserMiddlewareEvent),
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
        #[serde(default)]
        reason: CircuitBreakerRejectionReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cooldown_remaining_ms: Option<u64>,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitBreakerRejectionReason {
    CircuitOpen,
    ProbeInProgress,
    #[default]
    Unknown,
}

// ---- Rate limiter ---------------------------------------------------------
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum RateLimiterEvent {
    Delayed {
        /// Last predicted gate wait for the delayed admission attempt, in milliseconds.
        ///
        /// Not cumulative; see `rate_limiter_delay_seconds_total` for cumulative actual waited time.
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

    /// `backpressure.stalled` (FLOWIP-115e): a continuous credit stall
    /// exceeded the limiting edge's ceiling. Authored live only, immediately
    /// before the stage's terminal transition; replays as any recorded fact.
    Stalled {
        upstream: StageId,
        /// The limiting edge's downstream (minimum credit at expiry, ties by
        /// lowest downstream stage id).
        downstream: StageId,
        window: u64,
        stall_timeout_ms: u64,
        elapsed_ms: u64,
        in_flight: u64,
    },
}

// ---- Retry ---------------------------------------------------------------

/// Stable identity shared by every lifecycle row for one logical retry invocation.
///
/// `attachment_id` is the core ULID projection of the middleware attachment identity.
/// Adapter-owned attachment and protected-unit carrier types deliberately do not cross
/// into this durable payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryLifecycleContext {
    pub stage_id: StageId,
    pub attachment_id: Ulid,
    pub protected_unit: RetryProtectedUnit,
    pub invocation: RetryInvocation,
}

/// Surface-native identity of one logical retry invocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetryInvocation {
    SourcePoll { poll_id: EventId },
    Effect { cursor: EffectCursor },
    SinkDelivery { parent_event_id: EventId },
}

/// Durable identity of the live I/O unit protected by retry recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RetryProtectedUnit {
    SourcePoll,
    Effect { effect_type: EffectType },
    SinkDelivery { configured_target_id: Option<Ulid> },
}

/// Bounded reason that a logical retry invocation could not continue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryExhaustionCause {
    MaxAttempts,
    TotalWallTime,
    DrainRequested,
    RetryHintExceedsLimit,
    PolicyRejected,
    TerminalFailure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum RetryEvent {
    /// Legacy archive shape. New retry recovery does not construct or emit this action.
    AttemptStarted {
        attempt_number: u32,
        max_attempts: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        backoff_ms: Option<u64>,
    },
    AttemptFailed {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context: Option<RetryLifecycleContext>,
        attempt_number: u32,
        max_attempts: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error_kind: Option<ErrorKind>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delay_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        elapsed_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        remaining_wall_ms: Option<u64>,
    },
    SucceededAfterRetry {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context: Option<RetryLifecycleContext>,
        total_attempts: u32,
        total_duration_ms: u64,
    },
    Exhausted {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context: Option<RetryLifecycleContext>,
        total_attempts: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        exhaustion_cause: Option<RetryExhaustionCause>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_error_kind: Option<ErrorKind>,
        /// Legacy unbounded error text retained only so old archive rows decode.
        /// New constructors always leave this field absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        last_error: Option<String>,
        total_duration_ms: u64,
    },
}

// ---- Service-level indicator sample (FLOWIP-115f) ------------------------
//
// A per-execution SLI *sample*: the observe-only raw input an SLI is computed
// from. `value_ms` is the raw observation a distribution is built from. The
// sample records the measurement only; the objective (threshold) and the
// good/bad classification are deliberately not embedded in the durable event,
// because the objective can change while the measurement cannot. Applying a
// threshold and computing ratios/percentiles/windows/error budgets belong to
// FLOWIP-115l, which reads these rows. Samples never steer control.

/// The family of service-level indicator a sample measures.
///
/// Only `Latency` ships today. Additional kinds (availability, consistency,
/// throughput) require their own FLOWIP with a producer, example, and tests
/// before they become callable public API (no dead indicator surface). This
/// enum is intentionally not `#[non_exhaustive]` so adding a kind later forces
/// every match to be revisited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndicatorKind {
    /// Wall-clock duration of one operation execution.
    Latency,
}

/// One per-execution service-level-indicator sample: the raw measurement
/// (`value_ms`) plus its identity and context. The objective (threshold) and the
/// good/bad evaluation are read-side (FLOWIP-115l), not baked into the event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorSample {
    /// The kind of indicator this sample measures.
    pub kind: IndicatorKind,
    /// The named operation being measured, e.g. `"payment.authorization"`.
    pub operation: String,
    /// The indicator name within the operation, e.g. `"authorization.latency"`.
    pub indicator: String,
    /// The measured sample value in milliseconds.
    pub value_ms: u64,
    /// Static authoring-time tags (dependency, region, ...).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<IndicatorTag>,
}

/// A static key/value tag attached to an indicator sample at authoring time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndicatorTag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserMiddlewareEvent {
    pub event_type: String,
    pub payload: Value,
}

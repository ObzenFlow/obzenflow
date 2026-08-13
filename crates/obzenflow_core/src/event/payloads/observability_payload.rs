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

use crate::event::observability::HttpPullTelemetry;
use crate::event::payloads::effect_payload::EffectCursor;
use crate::event::status::processing_status::ErrorKind;
use crate::event::types::EventId;
use crate::event::vector_clock::VectorClock;
use crate::id::StageId;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashSet;

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
    /// Runtime-authored snapshot for an HTTP pull source (FLOWIP-134g).
    ///
    /// Source handlers report this typed value through the closed source
    /// observation capability. They never construct the surrounding event.
    HttpPullSnapshot {
        snapshot: HttpPullTelemetry,
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
    /// One per-execution service-level-indicator sample (FLOWIP-115f).
    ///
    /// An `Indicator` row is a single observe-only sample of one operation
    /// execution. Aggregation (percentiles, error budgets, windowed rates) and
    /// any objective evaluation are FLOWIP-115l's job, reading these rows.
    Indicator(IndicatorSample),
    /// One typed, payload-opaque logging occurrence (FLOWIP-115m).
    Logging(LoggingEvidence),
    User(UserMiddlewareEvent),
}

// ---- Typed logging evidence (FLOWIP-115m) --------------------------------

const LOGGING_EVENT_NAME_PATTERN: &str = "[a-z][a-z0-9_]*(\\.[a-z][a-z0-9_]*)+";
const LOGGING_ATTRIBUTE_KEY_PATTERN: &str = "[a-z][a-z0-9_]*(\\.[a-z][a-z0-9_]*)*";
pub const LOGGING_MAX_ATTRIBUTES: usize = 16;
pub const LOGGING_MAX_ATTRIBUTE_KEY_BYTES: usize = 64;
pub const LOGGING_MAX_ATTRIBUTE_VALUE_BYTES: usize = 256;

/// Why a logging declaration or decoded logging row violates the v1 schema.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LoggingSchemaError {
    #[error("logging event name '{value}' must match {LOGGING_EVENT_NAME_PATTERN}")]
    InvalidEventName { value: String },
    #[error("logging attribute key '{value}' must match {LOGGING_ATTRIBUTE_KEY_PATTERN}")]
    InvalidAttributeKey { value: String },
    #[error("logging attribute '{key}' is duplicated")]
    DuplicateAttribute { key: String },
    #[error("logging accepts at most 16 attributes")]
    TooManyAttributes,
    #[error("logging attribute key '{key}' exceeds 64 bytes")]
    AttributeKeyTooLong { key: String },
    #[error("logging attribute '{key}' value exceeds 256 bytes")]
    AttributeValueTooLong { key: String },
    #[error("logging attribute '{key}' value contains a control character")]
    AttributeValueContainsControl { key: String },
    #[error("logging body does not match the canonical rendering for its event and occurrence")]
    NonCanonicalBody,
}

fn matches_segmented_ascii_name(value: &str, require_namespace: bool) -> bool {
    let mut segments = value.split('.').peekable();
    let mut count = 0usize;
    while let Some(segment) = segments.next() {
        count += 1;
        let mut bytes = segment.bytes();
        if !bytes.next().is_some_and(|byte| byte.is_ascii_lowercase()) {
            return false;
        }
        if !bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_') {
            return false;
        }
        if segments.peek().is_none() {
            break;
        }
    }
    count > usize::from(require_namespace)
}

/// Validated semantic identity for a logging occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct LoggingEventName(String);

impl LoggingEventName {
    pub fn new(value: impl Into<String>) -> Result<Self, LoggingSchemaError> {
        let value = value.into();
        if !matches_segmented_ascii_name(&value, true) {
            return Err(LoggingSchemaError::InvalidEventName { value });
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for LoggingEventName {
    type Error = LoggingSchemaError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl<'de> Deserialize<'de> for LoggingEventName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::new(String::deserialize(deserializer)?).map_err(D::Error::custom)
    }
}

impl std::fmt::Display for LoggingEventName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Severity attached to typed logging evidence and its optional local mirror.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoggingLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

/// One bounded, materialisation-time user attribute.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LoggingAttribute {
    key: String,
    value: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoggingAttributeWire {
    key: String,
    value: String,
}

impl LoggingAttribute {
    pub fn new(
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, LoggingSchemaError> {
        let key = key.into();
        let value = value.into();
        if !matches_segmented_ascii_name(&key, false) {
            return Err(LoggingSchemaError::InvalidAttributeKey { value: key });
        }
        if key.len() > LOGGING_MAX_ATTRIBUTE_KEY_BYTES {
            return Err(LoggingSchemaError::AttributeKeyTooLong { key });
        }
        if value.len() > LOGGING_MAX_ATTRIBUTE_VALUE_BYTES {
            return Err(LoggingSchemaError::AttributeValueTooLong { key });
        }
        if value.chars().any(char::is_control) {
            return Err(LoggingSchemaError::AttributeValueContainsControl { key });
        }
        Ok(Self { key, value })
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl<'de> Deserialize<'de> for LoggingAttribute {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LoggingAttributeWire::deserialize(deserializer)?;
        Self::new(wire.key, wire.value).map_err(D::Error::custom)
    }
}

/// Opaque reference to one delivered input. The event payload is never copied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingInputReference {
    pub event_id: EventId,
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_input_position: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoggingJoinSide {
    Reference,
    Stream,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingJoinCanonicalMerge {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub selected_feed: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reader_index: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingJoinDelivery {
    pub side: LoggingJoinSide,
    pub source_stage_id: StageId,
    pub stage_input_position: u64,
    pub reference_high_water: VectorClock,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_merge: Option<LoggingJoinCanonicalMerge>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "details",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum LoggingSourceOutcome {
    Batch {
        events: u64,
    },
    Eof,
    Error {
        kind: ErrorKind,
    },
    Rejected {
        #[serde(skip_serializing_if = "Option::is_none")]
        policy: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "details",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum LoggingSinkAttemptResult {
    ReportedSuccess,
    ReportedPartial {
        successful_count: u64,
        failed_count: u64,
    },
    ReportedBuffered,
    ReportedFailure {
        final_attempt: bool,
    },
    HandlerError {
        kind: ErrorKind,
    },
    HandlerPanicked,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LoggingSinkOutcome {
    Attempted {
        result: LoggingSinkAttemptResult,
    },
    Rejected {
        #[serde(skip_serializing_if = "Option::is_none")]
        policy: Option<String>,
    },
}

/// The closed runtime join point observed by one logging row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum LoggingOccurrence {
    HandlerInputObserved {
        input: LoggingInputReference,
    },
    HandlerOutputObserved {
        input: LoggingInputReference,
        output_count: u64,
    },
    StatefulInputObserved {
        input: LoggingInputReference,
    },
    StatefulOutputObserved {
        input: LoggingInputReference,
        output_count: u64,
    },
    JoinInputObserved {
        input: LoggingInputReference,
        delivery: LoggingJoinDelivery,
    },
    JoinOutputObserved {
        input: LoggingInputReference,
        delivery: LoggingJoinDelivery,
        output_count: u64,
    },
    SourcePollObserved {
        poll_duration_ms: u64,
        output_count: u64,
        data_event_count: u64,
        outcome: LoggingSourceOutcome,
    },
    SinkDeliveryBoundaryObserved {
        input: LoggingInputReference,
        outcome: LoggingSinkOutcome,
    },
}

impl LoggingOccurrence {
    /// The opaque input reference for input-bound occurrences. Source-poll
    /// observations are true roots and therefore have no single input.
    pub fn input_reference(&self) -> Option<&LoggingInputReference> {
        match self {
            Self::HandlerInputObserved { input }
            | Self::HandlerOutputObserved { input, .. }
            | Self::StatefulInputObserved { input }
            | Self::StatefulOutputObserved { input, .. }
            | Self::JoinInputObserved { input, .. }
            | Self::JoinOutputObserved { input, .. }
            | Self::SinkDeliveryBoundaryObserved { input, .. } => Some(input),
            Self::SourcePollObserved { .. } => None,
        }
    }

    fn canonical_body(&self, event: &LoggingEventName) -> Option<String> {
        let subject = logging_event_subject(event);
        let body = match self {
            Self::HandlerInputObserved { .. } => format!("{subject} handler input observed"),
            Self::HandlerOutputObserved { output_count, .. } => {
                format!("{subject} handler output observed ({output_count} outputs)")
            }
            Self::StatefulInputObserved { .. } => {
                format!("{subject} stateful input observed")
            }
            Self::StatefulOutputObserved { output_count, .. } => {
                format!("{subject} stateful output observed ({output_count} outputs)")
            }
            Self::JoinInputObserved { .. } => format!("{subject} join input observed"),
            Self::JoinOutputObserved { output_count, .. } => {
                format!("{subject} join output observed ({output_count} outputs)")
            }
            Self::SourcePollObserved { outcome, .. } => match outcome {
                LoggingSourceOutcome::Batch { events } => {
                    format!("{subject} source poll returned {events} events")
                }
                LoggingSourceOutcome::Eof => format!("{subject} source poll reached eof"),
                LoggingSourceOutcome::Error { kind } => {
                    format!("{subject} source poll reported {}", error_kind_label(kind))
                }
                LoggingSourceOutcome::Rejected { policy } => policy.as_ref().map_or_else(
                    || format!("{subject} source poll rejected"),
                    |policy| format!("{subject} source poll rejected by {policy}"),
                ),
            },
            Self::SinkDeliveryBoundaryObserved { outcome, .. } => match outcome {
                LoggingSinkOutcome::Attempted { result } => match result {
                    LoggingSinkAttemptResult::ReportedSuccess => {
                        format!("{subject} attempt reported success")
                    }
                    LoggingSinkAttemptResult::ReportedPartial {
                        successful_count,
                        failed_count,
                    } => format!(
                        "{subject} attempt reported partial success ({successful_count} succeeded, {failed_count} failed)"
                    ),
                    LoggingSinkAttemptResult::ReportedBuffered => {
                        format!("{subject} attempt reported buffered")
                    }
                    LoggingSinkAttemptResult::ReportedFailure { final_attempt } => format!(
                        "{subject} attempt reported failure (final_attempt={final_attempt})"
                    ),
                    LoggingSinkAttemptResult::HandlerError { kind } => {
                        format!("{subject} attempt returned {}", error_kind_label(kind))
                    }
                    LoggingSinkAttemptResult::HandlerPanicked => {
                        format!("{subject} attempt panicked")
                    }
                },
                LoggingSinkOutcome::Rejected { policy } => policy.as_ref().map_or_else(
                    || format!("{subject} rejected before attempt"),
                    |policy| format!("{subject} rejected by {policy} before attempt"),
                ),
            },
        };
        Some(body)
    }
}

fn logging_event_subject(event: &LoggingEventName) -> String {
    let final_segment = event.as_str().rsplit('.').next().unwrap_or(event.as_str());
    let words: Vec<_> = final_segment.split('_').collect();
    match words.split_last() {
        None => final_segment.to_string(),
        Some((last, [])) => (*last).to_string(),
        Some((last, prefix)) => format!("{} {last}", prefix.join("-")),
    }
}

fn error_kind_label(kind: &ErrorKind) -> &'static str {
    match kind {
        ErrorKind::Timeout => "timeout",
        ErrorKind::Remote => "remote error",
        ErrorKind::RateLimited => "rate limited",
        ErrorKind::PermanentFailure => "permanent failure",
        ErrorKind::Deserialization => "deserialization error",
        ErrorKind::Validation => "validation error",
        ErrorKind::Domain => "domain error",
        ErrorKind::Unknown => "unknown error",
    }
}

/// Typed, payload-opaque evidence emitted by the built-in logging observer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct LoggingEvidence {
    event: LoggingEventName,
    level: LoggingLevel,
    occurrence: LoggingOccurrence,
    attributes: Vec<LoggingAttribute>,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoggingEvidenceWire {
    event: LoggingEventName,
    level: LoggingLevel,
    occurrence: LoggingOccurrence,
    #[serde(default)]
    attributes: Vec<LoggingAttribute>,
    #[serde(default)]
    body: Option<String>,
}

impl LoggingEvidence {
    /// Validate collection-wide attribute constraints once at middleware
    /// materialisation. Individual key/value constraints are enforced by
    /// [`LoggingAttribute::new`].
    pub fn validate_attributes(attributes: &[LoggingAttribute]) -> Result<(), LoggingSchemaError> {
        if attributes.len() > LOGGING_MAX_ATTRIBUTES {
            return Err(LoggingSchemaError::TooManyAttributes);
        }
        let mut keys = HashSet::with_capacity(attributes.len());
        for attribute in attributes {
            if !keys.insert(attribute.key()) {
                return Err(LoggingSchemaError::DuplicateAttribute {
                    key: attribute.key().to_string(),
                });
            }
        }
        Ok(())
    }

    pub fn new(
        event: LoggingEventName,
        level: LoggingLevel,
        occurrence: LoggingOccurrence,
        attributes: Vec<LoggingAttribute>,
    ) -> Result<Self, LoggingSchemaError> {
        Self::validate_attributes(&attributes)?;
        let body = occurrence.canonical_body(&event);
        Ok(Self {
            event,
            level,
            occurrence,
            attributes,
            body,
        })
    }

    pub fn event(&self) -> &LoggingEventName {
        &self.event
    }

    pub fn level(&self) -> LoggingLevel {
        self.level
    }

    pub fn occurrence(&self) -> &LoggingOccurrence {
        &self.occurrence
    }

    pub fn attributes(&self) -> &[LoggingAttribute] {
        &self.attributes
    }

    pub fn body(&self) -> Option<&str> {
        self.body.as_deref()
    }
}

impl<'de> Deserialize<'de> for LoggingEvidence {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = LoggingEvidenceWire::deserialize(deserializer)?;
        let body = wire.body.clone();
        let evidence = Self::new(wire.event, wire.level, wire.occurrence, wire.attributes)
            .map_err(D::Error::custom)?;
        if body != evidence.body {
            return Err(D::Error::custom(LoggingSchemaError::NonCanonicalBody));
        }
        Ok(evidence)
    }
}

// ---- Circuit breaker ------------------------------------------------------
/// The condition whose observed evidence caused a circuit to open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitBreakerOpenTrigger {
    ConsecutiveFailures,
    FailureRate,
    SlowCallRate,
    FailureAndSlowCallRate,
    HalfOpenProbeFailure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum CircuitBreakerEvent {
    Opened {
        /// Failure rate in the population that caused this transition, not the
        /// breaker's cumulative lifetime failure rate.
        error_rate: f64,
        /// Failures in the population that caused this transition.
        failure_count: u64,
        trigger: CircuitBreakerOpenTrigger,
        observed_calls: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        slow_call_rate: Option<f64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        slow_call_count: Option<u64>,
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
    AttemptSettled {
        cursor: EffectCursor,
        attempt: u32,
        health_classification: CircuitBreakerHealthClassification,
        slow: bool,
        dependency_elapsed_ms: u64,
        admission_wait_ms: u64,
    },
    RetryScheduled {
        cursor: EffectCursor,
        next_attempt: u32,
        delay_ms: u64,
    },
    RetrySucceeded {
        cursor: EffectCursor,
        total_attempts: u32,
        terminal_classification: CircuitBreakerHealthClassification,
    },
    RetryExhausted {
        cursor: EffectCursor,
        total_attempts: u32,
        reason: CircuitBreakerRetryStopReason,
    },
    RetryStoppedNonRetryable {
        cursor: EffectCursor,
        total_attempts: u32,
    },
    RecoveryCompleted {
        cursor: EffectCursor,
        total_attempts: u32,
        backoff_elapsed_ms: u64,
        recovery_elapsed_ms: u64,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitBreakerHealthClassification {
    Success,
    TransientFailure,
    PermanentFailure,
    RateLimited,
    Ignored,
    NoObservation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitBreakerRetryStopReason {
    AttemptLimit,
    AttemptStartWindow,
    CircuitNoLongerClosed,
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

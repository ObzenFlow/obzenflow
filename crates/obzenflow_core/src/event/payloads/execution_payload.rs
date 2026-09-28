// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Closed, protected execution evidence. Measurements have no variant here.

use super::effect_payload::{
    EffectAttemptStarted, EffectCursor, EffectRecord, EffectRecoveryAbandoned,
};

use super::system_payload::{
    CommandDiscardDisposition, ContractName, ContractResultStatusLabel, SystemFeedRole,
};
use crate::ai::{ChunkExclusionReason, ChunkPlanningSummary, OversizePolicy};
use crate::event::observability::{HttpPullState, WaitReason};
use crate::event::provenance::ExecutionAccounting;
use crate::event::status::processing_status::ErrorKind;
use crate::event::types::EventType;
use crate::ingress::{IngressAttemptSeq, IngressKey, IngressRefusalReason};
use crate::{StageId, StageKey};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "execution_type", rename_all = "snake_case")]
pub enum ExecutionPayload {
    ReplayLifecycle(super::system_payload::ReplayLifecycleEvent),
    SupervisorRegistered {
        descriptor: super::supervisor_descriptor::SupervisorDescriptor,
    },
    SupervisorCommandDiscarded {
        supervisor: String,
        terminal_state: String,
        command: String,
        disposition: CommandDiscardDisposition,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    SourceCleanupFailed {
        stage_id: StageId,
        stage_name: String,
        error: String,
    },
    ContractStatus {
        upstream: StageId,
        reader: StageId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selected_event_type: Option<EventType>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        feed_role: Option<SystemFeedRole>,
        pass: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        reader_seq: Option<crate::event::types::SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        advertised_writer_seq: Option<crate::event::types::SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<crate::event::types::ViolationCause>,
    },
    ContractResult {
        upstream: StageId,
        reader: StageId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selected_event_type: Option<EventType>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        feed_role: Option<SystemFeedRole>,
        contract_name: ContractName,
        status: ContractResultStatusLabel,
        /// Stable category label (e.g. "seq_divergence", "content_mismatch", "other")
        #[serde(skip_serializing_if = "Option::is_none")]
        cause: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reader_seq: Option<crate::event::types::SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        advertised_writer_seq: Option<crate::event::types::SeqNo>,
    },
    IngressRefusal {
        /// Protocol-neutral hosted ingress key; the per-surface metric projection key.
        ingress_key: IngressKey,
        /// Runtime id of the linked source stage.
        stage_id: StageId,
        /// Replay-stable source stage key (`run_manifest.json` key).
        stage_key: StageKey,
        reason: IngressRefusalReason,
        /// Per-attempt sequence; the merge key against accepted source rows.
        attempt_seq: IngressAttemptSeq,
        /// HTTP submission requests in this attempt (always 1 in 115D).
        request_count: u64,
        /// Events refused by this attempt (1 for `/events`; the refused subset
        /// size for `/batch`, so a batch refusal is one fact with a count).
        event_count: u64,
        /// Batches in this attempt (0 for `/events`, 1 for `/batch`).
        batch_count: u64,
        http_status: u16,
        #[serde(skip_serializing_if = "Option::is_none")]
        retry_after_ms_bucket: Option<u64>,
    },
    StageLifecycle(StageLifecycleFact),
    MetricsCoordination(MetricsCoordinationFact),
    CircuitBreaker(CircuitBreakerFact),
    RateLimiter(RateLimiterFact),
    Backpressure(BackpressureFact),
    SourcePollError(SourcePollErrorFact),
    HttpPullState(HttpPullStateFact),
    AiChunkingPlanned(AiChunkingPlannedFact),
    AccumulatorProgress {
        inputs_since_last_report: u64,
    },
    JoinReferenceProgress {
        reference_inputs_since_last_report: u64,
    },
    EffectRecord(EffectRecord),
    EffectAttemptStarted(EffectAttemptStarted),
    EffectRecoveryAbandoned(EffectRecoveryAbandoned),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "stage_state", rename_all = "snake_case")]
pub enum StageLifecycleFact {
    Running {
        stage_id: StageId,
    },
    Draining {
        stage_id: StageId,
        reason: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        accounting: Option<ExecutionAccounting>,
    },
    Drained {
        stage_id: StageId,
        events_processed: Option<u64>,
    },
    Completed {
        stage_id: StageId,
        accounting: Option<ExecutionAccounting>,
    },
    Cancelled {
        stage_id: StageId,
        reason: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        accounting: Option<ExecutionAccounting>,
    },
    Failed {
        stage_id: StageId,
        error: String,
        recoverable: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        accounting: Option<ExecutionAccounting>,
        #[serde(skip_serializing_if = "Option::is_none")]
        causal_event_id: Option<crate::EventId>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "metrics_event", rename_all = "snake_case")]
pub enum MetricsCoordinationFact {
    Ready { exporter_count: Option<usize> },
    DrainRequested,
    Drained { final_flush_count: Option<u64> },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum CircuitBreakerFact {
    Opened {
        /// Configured minimum wait before another probe is permitted.
        cooldown_ms: u64,
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
    StateChanged {
        from_state: CircuitState,
        to_state: CircuitState,
        timestamp: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum RateLimiterFact {
    Delayed {
        delay_ms: u64,
        current_rate: f64,
        limit_rate: f64,
    },
    ModeChange {
        mode_from: RateLimiterMode,
        mode_to: RateLimiterMode,
        limit_rate: f64,
    },
    ConfigChanged {
        old_rate: f64,
        new_rate: f64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RateLimiterMode {
    Normal,
    Limiting,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum BackpressureFact {
    Stalled {
        upstream: StageId,
        downstream: StageId,
        window: u64,
        stall_timeout_ms: u64,
        elapsed_ms: u64,
        in_flight: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourcePollKind {
    Finite,
    AsyncFinite,
    Infinite,
    AsyncInfinite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourcePollErrorKind {
    Timeout,
    Transport,
    Deserialization,
    Validation,
    Other,
}

impl SourcePollErrorKind {
    pub const fn processing_error_kind(self) -> ErrorKind {
        match self {
            Self::Timeout => ErrorKind::Timeout,
            Self::Transport => ErrorKind::Remote,
            Self::Deserialization => ErrorKind::Deserialization,
            Self::Validation => ErrorKind::Validation,
            Self::Other => ErrorKind::Unknown,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePollErrorFact {
    pub source_type: SourcePollKind,
    pub error_type: SourcePollErrorKind,
    pub message: String,
    pub timestamp_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpPullStateFact {
    pub state: HttpPullState,
    pub wait_reason: Option<WaitReason>,
    pub next_wake_unix_secs: Option<u64>,
    pub last_success_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AiChunkingPlannedFact {
    pub planning: ChunkPlanningSummary,
    pub chunk_count: usize,
    pub oversize_policy: OversizePolicy,
    pub exclusions_by_reason: HashMap<ChunkExclusionReason, u64>,
}

impl ExecutionPayload {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::ReplayLifecycle(_) => "execution.replay.lifecycle",
            Self::SupervisorRegistered { .. } => "execution.supervisor.registered",
            Self::SupervisorCommandDiscarded { .. } => "execution.supervisor.command_discarded",
            Self::SourceCleanupFailed { .. } => "execution.source.cleanup_failed",
            Self::IngressRefusal { .. } => "execution.ingress.refusal",
            Self::ContractStatus { pass, .. } => {
                if *pass {
                    "execution.contract.pass"
                } else {
                    "execution.contract.fail"
                }
            }
            Self::ContractResult { status, .. } => match status {
                ContractResultStatusLabel::Passed => "execution.contract.result.passed",
                ContractResultStatusLabel::Failed => "execution.contract.result.failed",
                ContractResultStatusLabel::Pending => "execution.contract.result.pending",
                ContractResultStatusLabel::Healthy => "execution.contract.result",
            },
            Self::StageLifecycle(fact) => match fact {
                StageLifecycleFact::Running { .. } => "lifecycle.stage.running",
                StageLifecycleFact::Draining { .. } => "lifecycle.stage.draining",
                StageLifecycleFact::Drained { .. } => "lifecycle.stage.drained",
                StageLifecycleFact::Completed { .. } => "lifecycle.stage.completed",
                StageLifecycleFact::Cancelled { .. } => "lifecycle.stage.cancelled",
                StageLifecycleFact::Failed { .. } => "lifecycle.stage.failed",
            },
            Self::MetricsCoordination(fact) => match fact {
                MetricsCoordinationFact::Ready { .. } => "lifecycle.metrics.ready",
                MetricsCoordinationFact::DrainRequested => "lifecycle.metrics.drain",
                MetricsCoordinationFact::Drained { .. } => "lifecycle.metrics.drained",
            },
            Self::CircuitBreaker(CircuitBreakerFact::StateChanged { .. }) => {
                "lifecycle.middleware.circuit_breaker.state_changed"
            }
            Self::CircuitBreaker(_) => "lifecycle.middleware.circuit_breaker",
            Self::RateLimiter(_) => "lifecycle.middleware.rate_limiter",
            Self::Backpressure(_) => "lifecycle.backpressure",
            Self::SourcePollError(_) => "source.poll_error",
            Self::HttpPullState(_) => "source.http_pull_state",
            Self::AiChunkingPlanned(_) => "ai.chunking.planned",
            Self::AccumulatorProgress { .. } => "stateful.accumulation_progress",
            Self::JoinReferenceProgress { .. } => "join.reference_progress",
            Self::EffectRecord(record) => {
                super::effect_payload::framework_effect_event_type(&record.descriptor.effect_type)
            }
            Self::EffectAttemptStarted(_) => {
                super::effect_payload::EFFECT_ATTEMPT_STARTED_EVENT_TYPE
            }
            Self::EffectRecoveryAbandoned(_) => {
                super::effect_payload::EFFECT_RECOVERY_ABANDONED_EVENT_TYPE
            }
        }
    }

    /// The existing effect protocol charges these physical rows. Other execution
    /// facts came from transport-excluded lifecycle records and remain uncharged.
    pub const fn consumes_data_credit(&self) -> bool {
        match self {
            Self::EffectRecord(_)
            | Self::EffectAttemptStarted(_)
            | Self::EffectRecoveryAbandoned(_) => true,
            Self::ReplayLifecycle(_)
            | Self::SupervisorRegistered { .. }
            | Self::SupervisorCommandDiscarded { .. }
            | Self::SourceCleanupFailed { .. }
            | Self::ContractStatus { .. }
            | Self::ContractResult { .. }
            | Self::IngressRefusal { .. }
            | Self::StageLifecycle(_)
            | Self::MetricsCoordination(_)
            | Self::CircuitBreaker(_)
            | Self::RateLimiter(_)
            | Self::Backpressure(_)
            | Self::SourcePollError(_)
            | Self::HttpPullState(_)
            | Self::AiChunkingPlanned(_)
            | Self::AccumulatorProgress { .. }
            | Self::JoinReferenceProgress { .. } => false,
        }
    }
}

/// System mirrors retain their nested middleware discriminator, but accept only
/// the same protected decisions as the originating stage journal.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(
    tag = "middleware_event",
    content = "details",
    rename_all = "snake_case"
)]
pub enum MiddlewareFact {
    CircuitBreaker(CircuitBreakerFact),
    RateLimiter(RateLimiterFact),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CircuitBreakerOpenTrigger {
    ConsecutiveFailures,
    FailureRate,
    SlowCallRate,
    FailureAndSlowCallRate,
    HalfOpenProbeFailure,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opened_fact_requires_and_round_trips_its_cooldown() {
        let mut opening_json = serde_json::json!({
            "action": "opened", "error_rate": 1.0, "failure_count": 3,
            "trigger": "consecutive_failures", "observed_calls": 3,
            "cooldown_ms": 5_000
        });
        let opening: CircuitBreakerFact = serde_json::from_value(opening_json.clone()).unwrap();
        assert!(matches!(
            &opening,
            CircuitBreakerFact::Opened {
                cooldown_ms: 5_000,
                ..
            }
        ));
        assert_eq!(serde_json::to_value(opening).unwrap()["cooldown_ms"], 5_000);

        opening_json.as_object_mut().unwrap().remove("cooldown_ms");
        let missing_cooldown =
            serde_json::from_value::<CircuitBreakerFact>(opening_json.clone()).unwrap_err();
        assert!(missing_cooldown.to_string().contains("cooldown_ms"));
        opening_json["cooldown_ms"] = serde_json::Value::Null;
        assert!(serde_json::from_value::<CircuitBreakerFact>(opening_json).is_err());
    }
}

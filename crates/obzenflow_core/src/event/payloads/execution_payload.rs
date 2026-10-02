// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Closed, protected execution evidence. Measurements have no variant here.

use super::effect_payload::{
    EffectAttemptStarted, EffectCursor, EffectRecord, EffectRecoveryAbandoned,
};

use super::flow_control_payload::EofKind;
use super::system_payload::{
    CommandDiscardDisposition, ContractName, ContractResultStatusLabel, SystemFeedRole,
};
use crate::ai::{ChunkExclusionReason, ChunkPlanningSummary, OversizePolicy};
use crate::event::observability::{HttpPullState, WaitReason};
use crate::event::provenance::ExecutionAccounting;
use crate::event::status::processing_status::ErrorKind;
use crate::event::types::{Count, DurationMs};
use crate::journal::{ArchiveStatus, StatusDerivation};
use crate::StageId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "execution_type", rename_all = "snake_case")]
pub enum ExecutionPayload {
    ReplayLifecycle(ReplayLifecycleEvent),
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
        selected_event_type: Option<crate::EventDescriptor>,
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
        selected_event_type: Option<crate::EventDescriptor>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        feed_role: Option<SystemFeedRole>,
        contract_name: ContractName,
        status: ContractResultStatusLabel,
        phase: crate::contracts::ContractPhase,
        result: Box<crate::ContractResult>,
        /// Stable category label (e.g. "seq_divergence", "content_mismatch", "other")
        #[serde(skip_serializing_if = "Option::is_none")]
        cause: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reader_seq: Option<crate::event::types::SeqNo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        advertised_writer_seq: Option<crate::event::types::SeqNo>,
    },
    StageLifecycle(StageLifecycleFact),
    #[serde(rename = "resilience_occurrence")]
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
    SinkAudit(super::delivery_payload::SinkAuditPayload),
    StageFatalRecorded(super::stage_fatal_payload::StageFatalRecorded),
    SinkOperationFailed(super::sink_operation_payload::SinkOperationFailed),
    EffectRecord(EffectRecord),
    EffectAttemptStarted(EffectAttemptStarted),
    EffectRecoveryAbandoned(EffectRecoveryAbandoned),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "replay_event", rename_all = "snake_case")]
pub enum ReplayLifecycleEvent {
    Started {
        archive_path: PathBuf,
        archive_flow_id: String,
        archive_status: ArchiveStatus,
        archive_status_derivation: StatusDerivation,
        allow_incomplete: bool,
        source_stages: Vec<String>,
    },
    Completed {
        replayed_count: Count,
        skipped_count: Count,
        duration_ms: DurationMs,
        /// The terminal EOF kind synthesized at exhaustion (FLOWIP-095k).
        /// `None` on the resume handoff, which synthesizes no terminal EOF.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        synthesized_eof_kind: Option<EofKind>,
    },
    /// Resume handoff (FLOWIP-120n): the source finished its catch-up and
    /// continues live at `generation`. The transition announcement the
    /// presentation layer surfaces.
    ResumedLive {
        archive_flow_id: String,
        replayed_count: Count,
        generation: u64,
    },
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

impl StageLifecycleFact {
    pub fn stage_id(&self) -> StageId {
        match self {
            Self::Running { stage_id }
            | Self::Draining { stage_id, .. }
            | Self::Drained { stage_id, .. }
            | Self::Completed { stage_id, .. }
            | Self::Cancelled { stage_id, .. }
            | Self::Failed { stage_id, .. } => *stage_id,
        }
    }
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
    pub const fn payload_schema_version(&self) -> std::num::NonZeroU32 {
        match self {
            Self::SinkOperationFailed(_) => std::num::NonZeroU32::new(2).unwrap(),
            _ => std::num::NonZeroU32::MIN,
        }
    }

    pub fn event_type(&self, stage_name: &str) -> std::borrow::Cow<'static, str> {
        use super::supervisor_descriptor::{supervisor_event_type, SupervisionMode};
        let name = match self {
            Self::ReplayLifecycle(_) => "execution.replay.lifecycle",
            Self::SupervisorRegistered { descriptor } => {
                return format!("{}.registered", descriptor.event_prefix()).into()
            }
            Self::SupervisorCommandDiscarded { .. } => "execution.supervisor.command_discarded",
            Self::SourceCleanupFailed { .. } => "execution.source.cleanup_failed",
            Self::ContractStatus { pass, .. } => {
                if *pass {
                    "runtime.contract.policy_accepted"
                } else {
                    "runtime.contract.policy_rejected"
                }
            }
            Self::ContractResult { status, .. } => match status {
                ContractResultStatusLabel::Passed => "runtime.contract.verification_passed",
                ContractResultStatusLabel::Failed => "runtime.contract.verification_failed",
                ContractResultStatusLabel::Pending => "runtime.contract.verification_pending",
                ContractResultStatusLabel::Skipped => "runtime.contract.verification_skipped",
            },
            Self::StageLifecycle(fact) => {
                let occurrence = match fact {
                    StageLifecycleFact::Running { .. } => "milestone.ready",
                    StageLifecycleFact::Draining { .. } => "milestone.drain_started",
                    StageLifecycleFact::Drained { .. } => "milestone.drain_completed",
                    StageLifecycleFact::Completed { .. } => "outcome.completed",
                    StageLifecycleFact::Cancelled { .. } => "outcome.cancelled",
                    StageLifecycleFact::Failed { .. } => "outcome.failed",
                };
                return supervisor_event_type(
                    stage_name,
                    SupervisionMode::HandlerSupervised,
                    occurrence,
                )
                .into();
            }
            Self::CircuitBreaker(fact) => match fact {
                CircuitBreakerFact::Opened { .. }
                | CircuitBreakerFact::StateChanged {
                    to_state: CircuitState::Open,
                    ..
                } => "runtime.circuit_breaker.opened",
                CircuitBreakerFact::Closed { .. }
                | CircuitBreakerFact::StateChanged {
                    to_state: CircuitState::Closed,
                    ..
                } => "runtime.circuit_breaker.closed",
                CircuitBreakerFact::HalfOpen { .. }
                | CircuitBreakerFact::StateChanged {
                    to_state: CircuitState::HalfOpen,
                    ..
                } => "runtime.circuit_breaker.half_open_entered",
                CircuitBreakerFact::Rejected { .. } => "runtime.circuit_breaker.admission_rejected",
                CircuitBreakerFact::AttemptSettled { .. } => {
                    "runtime.circuit_breaker.attempt_assessed"
                }
                CircuitBreakerFact::RetryScheduled { .. } => "runtime.retry.scheduled",
                CircuitBreakerFact::RetrySucceeded { .. } => "runtime.retry.succeeded",
                CircuitBreakerFact::RetryExhausted { .. } => "runtime.retry.exhausted",
                CircuitBreakerFact::RetryStoppedNonRetryable { .. } => {
                    "runtime.retry.stopped_non_retryable"
                }
                CircuitBreakerFact::RecoveryCompleted { .. } => {
                    "runtime.resilience.evaluation_finished"
                }
            },
            Self::RateLimiter(fact) => match fact {
                RateLimiterFact::Delayed { .. } => "runtime.rate_limiter.wait_started",
                RateLimiterFact::ModeChange { .. } => "runtime.rate_limiter.mode_changed",
                RateLimiterFact::ConfigChanged { .. } => {
                    "runtime.rate_limiter.configuration_changed"
                }
            },
            Self::Backpressure(_) => "runtime.backpressure.stall_detected",
            Self::SourcePollError(_) => "source.poll_error",
            Self::HttpPullState(_) => "source.http_pull_state",
            Self::AiChunkingPlanned(_) => "ai.chunking.planned",
            Self::AccumulatorProgress { .. } => "stateful.accumulation_progress",
            Self::JoinReferenceProgress { .. } => "join.reference_progress",
            Self::SinkAudit(audit) => audit.event_type(),
            Self::StageFatalRecorded(_) => "obzenflow.stage_fatal_recorded",
            Self::SinkOperationFailed(_) => "obzenflow.sink_operation_failed",
            Self::EffectRecord(record) => record.event_type(),
            Self::EffectAttemptStarted(_) => {
                super::effect_payload::EFFECT_ATTEMPT_STARTED_EVENT_TYPE
            }
            Self::EffectRecoveryAbandoned(_) => {
                super::effect_payload::EFFECT_RECOVERY_ABANDONED_EVENT_TYPE
            }
        };
        name.into()
    }

    /// The existing effect protocol charges these physical rows. Other execution
    /// facts came from transport-excluded lifecycle records and remain uncharged.
    pub const fn consumes_data_credit(&self) -> bool {
        match self {
            Self::StageFatalRecorded(_)
            | Self::SinkOperationFailed(_)
            | Self::EffectRecord(_)
            | Self::EffectAttemptStarted(_)
            | Self::EffectRecoveryAbandoned(_) => true,
            Self::SinkAudit(_)
            | Self::ReplayLifecycle(_)
            | Self::SupervisorRegistered { .. }
            | Self::SupervisorCommandDiscarded { .. }
            | Self::SourceCleanupFailed { .. }
            | Self::ContractStatus { .. }
            | Self::ContractResult { .. }
            | Self::StageLifecycle(_)
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
    #[test]
    fn contract_result_feed_fields_are_typed_but_serialize_as_labels() {
        use crate::event::types::SeqNo;
        use serde_json::json;
        let upstream = StageId::new();
        let reader = StageId::new();
        let payload = ExecutionPayload::ContractResult {
            upstream,
            reader,
            selected_event_type: Some(crate::EventDescriptor {
                event_kind: crate::event::payloads::chain_payload::EventKind::Fact,
                event_type: "test.selected".into(),
                payload_schema_version: std::num::NonZeroU32::MIN,
            }),
            feed_role: Some(SystemFeedRole::Reference),
            contract_name: ContractName::from("TransportContract"),
            status: ContractResultStatusLabel::Pending,
            phase: crate::contracts::ContractPhase::Progress,
            result: Box::new(crate::ContractResult::Pending {
                reason: crate::contracts::PendingReason::ProgressOnly,
                evidence: crate::ContractEvidence {
                    contract_name: ContractName::from("TransportContract"),
                    upstream_stage: upstream,
                    downstream_stage: reader,
                    evaluated_at: chrono::Utc::now(),
                    details: crate::contracts::ContractEvidenceDetails::Progress,
                },
            }),
            cause: None,
            reader_seq: Some(SeqNo(3)),
            advertised_writer_seq: Some(SeqNo(5)),
        };

        let serialized = serde_json::to_value(&payload).expect("execution fact should serialize");
        assert_eq!(
            serialized["selected_event_type"],
            json!({"event_kind":"fact", "event_type":"test.selected", "payload_schema_version":1})
        );
        assert_eq!(serialized["feed_role"], "reference");
        assert_eq!(serialized["contract_name"], "TransportContract");
        assert_eq!(serialized["status"], "pending");

        let record =
            crate::event::ChainEventFactory::execution_event(reader.into(), payload.clone());
        assert_eq!(record.event_type(), "runtime.contract.verification_pending");
        let encoded = serde_json::to_value(record).unwrap();
        assert!(serde_json::from_value::<crate::ChainEvent>(encoded.clone()).is_ok());
        for (field, value) in [
            ("status", json!("passed")),
            ("contract_name", json!("OtherContract")),
            ("reader", json!(StageId::new())),
        ] {
            let mut invalid = encoded.clone();
            invalid["payload"][field] = value;
            assert!(serde_json::from_value::<crate::ChainEvent>(invalid).is_err());
        }

        let decoded: ExecutionPayload = serde_json::from_value(json!({
            "execution_type": "contract_result",
            "upstream": serialized["upstream"].clone(),
            "reader": serialized["reader"].clone(),
            "selected_event_type": serialized["selected_event_type"].clone(),
            "feed_role": "reference",
            "contract_name": "TransportContract",
            "status": "pending",
            "phase": "progress",
            "result": serialized["result"].clone(),
            "reader_seq": 3,
            "advertised_writer_seq": 5
        }))
        .expect("string-label execution fact should deserialize");

        match decoded {
            ExecutionPayload::ContractResult {
                selected_event_type,
                feed_role,
                contract_name,
                status,
                ..
            } => {
                assert_eq!(
                    selected_event_type,
                    Some(crate::EventDescriptor {
                        event_kind: crate::event::payloads::chain_payload::EventKind::Fact,
                        event_type: "test.selected".into(),
                        payload_schema_version: std::num::NonZeroU32::MIN
                    })
                );
                assert_eq!(feed_role, Some(SystemFeedRole::Reference));
                assert_eq!(contract_name.as_str(), "TransportContract");
                assert_eq!(status, ContractResultStatusLabel::Pending);
            }
            other => panic!("expected ContractResult, got {other:?}"),
        }
    }
}

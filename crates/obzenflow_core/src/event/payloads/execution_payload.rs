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
use crate::event::vocabulary;
use crate::journal::{ArchiveStatus, StatusDerivation};
use crate::{StageId, StageKey};
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
    Recovery(RecoveryFact),
    RateLimiter(RateLimiterFact),
    Backpressure(BackpressureFact),
    SourceRecordRejected(SourceRecordRejectedFact),
    SourcePollFailed(SourcePollFailedFact),
    SourceOpenFailed(SourceOpenFailedFact),
    SourceReaderAcquired(SourceReaderAcquiredFact),
    SourceCleanupSucceeded(SourceCleanupSucceededFact),
    SourceCleanupFailed(SourceCleanupFailedFact),
    SourceInputPinned(SourceInputPinnedFact),
    SourcePositioned(SourceResumeFact),
    SourceStartedOnResume(SourceResumeFact),
    SourceReattached(SourceReattachedFact),
    SourceContinuationRefused(SourceContinuationRefusedFact),
    SourceSeekFailed(SourceSeekFailedFact),
    HttpPullState(HttpPullStateFact),
    AiChunkingPlanned(AiChunkingPlannedFact),
    AccumulatorProgress {
        inputs_since_last_report: u64,
    },
    JoinReferenceProgress {
        reference_inputs_since_last_report: u64,
    },
    /// A join left a state that holds inputs.
    JoinPhase(JoinPhaseFact),
    SinkAudit(super::delivery_payload::SinkAuditPayload),
    StageFatalRecorded(super::stage_fatal_payload::StageFatalRecorded),
    StageProcessingFailed(super::stage_processing_failed_payload::StageProcessingFailed),
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

impl ReplayLifecycleEvent {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::Started { .. } => vocabulary::replay::STARTED,
            Self::Completed { .. } => vocabulary::replay::COMPLETED,
            Self::ResumedLive { .. } => vocabulary::replay::LIVE_RESUMED,
        }
    }
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
    pub fn event_type(&self, stage_name: &str) -> String {
        use super::supervisor_descriptor::{supervisor_event_type, SupervisionMode};

        let occurrence = match self {
            Self::Running { .. } => vocabulary::supervisor::READY,
            Self::Draining { .. } => vocabulary::supervisor::DRAIN_STARTED,
            Self::Drained { .. } => vocabulary::supervisor::DRAIN_COMPLETED,
            Self::Completed { .. } => vocabulary::supervisor::COMPLETED,
            Self::Cancelled { .. } => vocabulary::supervisor::CANCELLED,
            Self::Failed { .. } => vocabulary::supervisor::FAILED,
        };
        supervisor_event_type(stage_name, SupervisionMode::HandlerSupervised, occurrence)
    }

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
        unit: StageKey,
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
        unit: StageKey,
        success_count: u64,
        recovery_duration_ms: u64,
    },
    Rejected {
        unit: StageKey,
        #[serde(default)]
        reason: CircuitBreakerRejectionReason,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cooldown_remaining_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        circuit_open_duration_ms: Option<u64>,
    },
    HalfOpen {
        unit: StageKey,
        test_request_count: u32,
    },
    AttemptSettled {
        unit: StageKey,
        cursor: EffectCursor,
        attempt: u32,
        health_classification: CircuitBreakerHealthClassification,
        slow: bool,
        dependency_elapsed_ms: u64,
        admission_wait_ms: u64,
    },
    StateChanged {
        unit: StageKey,
        from_state: CircuitState,
        to_state: CircuitState,
        timestamp: u64,
    },
}

impl CircuitBreakerFact {
    /// The authored stage whose breaker wrote this fact.
    pub fn unit(&self) -> &StageKey {
        match self {
            Self::Opened { unit, .. }
            | Self::Closed { unit, .. }
            | Self::Rejected { unit, .. }
            | Self::HalfOpen { unit, .. }
            | Self::AttemptSettled { unit, .. }
            | Self::StateChanged { unit, .. } => unit,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            Self::Opened { .. }
            | Self::StateChanged {
                to_state: CircuitState::Open,
                ..
            } => vocabulary::circuit_breaker::OPENED,
            Self::Closed { .. }
            | Self::StateChanged {
                to_state: CircuitState::Closed,
                ..
            } => vocabulary::circuit_breaker::CLOSED,
            Self::HalfOpen { .. }
            | Self::StateChanged {
                to_state: CircuitState::HalfOpen,
                ..
            } => vocabulary::circuit_breaker::HALF_OPEN_ENTERED,
            Self::Rejected { .. } => vocabulary::circuit_breaker::ADMISSION_REJECTED,
            Self::AttemptSettled { .. } => vocabulary::circuit_breaker::CALL_CLASSIFIED,
        }
    }
}

/// The phase a join entered when a state that holds inputs completed. The
/// variant selects the occurrence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "phase", rename_all = "snake_case")]
pub enum JoinPhaseFact {
    EnrichingEntered {
        /// Generation of the delivery that ended the previous phase.
        generation: u64,
        /// Stage keys whose natural EOFs ended it.
        ended_by: Vec<crate::StageKey>,
    },
    LiveEntered {
        /// Generation of the delivery that ended the previous phase.
        generation: u64,
        /// Stage keys whose natural EOFs ended it.
        ended_by: Vec<crate::StageKey>,
    },
}

impl JoinPhaseFact {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::EnrichingEntered { .. } => vocabulary::join::ENRICHING_ENTERED,
            Self::LiveEntered { .. } => vocabulary::join::LIVE_ENTERED,
        }
    }

    /// The generation of the delivery that ended the previous phase.
    pub fn generation(&self) -> u64 {
        match self {
            Self::EnrichingEntered { generation, .. } | Self::LiveEntered { generation, .. } => {
                *generation
            }
        }
    }

    /// The stage keys whose natural EOFs ended the previous phase.
    pub fn ended_by(&self) -> &[crate::StageKey] {
        match self {
            Self::EnrichingEntered { ended_by, .. } | Self::LiveEntered { ended_by, .. } => {
                ended_by
            }
        }
    }
}

/// Invocation recovery evidence independent of optional breaker health policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum RecoveryFact {
    AttemptCompleted {
        cursor: EffectCursor,
        attempt: u32,
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
    },
    RetryExhausted {
        cursor: EffectCursor,
        total_attempts: u32,
        reason: RetryStopReason,
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
}

impl RecoveryFact {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::AttemptCompleted { .. } => vocabulary::resilience::ATTEMPT_COMPLETED,
            Self::RetryScheduled { .. } => vocabulary::retry::SCHEDULED,
            Self::RetrySucceeded { .. } => vocabulary::retry::SUCCEEDED,
            Self::RetryExhausted { .. } => vocabulary::retry::EXHAUSTED,
            Self::RetryStoppedNonRetryable { .. } => vocabulary::retry::STOPPED_NON_RETRYABLE,
            Self::RecoveryCompleted { .. } => vocabulary::resilience::ATTEMPTS_REPORTED,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum RateLimiterFact {
    Delayed {
        unit: StageKey,
        delay_ms: u64,
        current_rate: f64,
        limit_rate: f64,
    },
    ModeChange {
        unit: StageKey,
        mode_from: RateLimiterMode,
        mode_to: RateLimiterMode,
        limit_rate: f64,
    },
    ConfigChanged {
        unit: StageKey,
        old_rate: f64,
        new_rate: f64,
    },
}

impl RateLimiterFact {
    /// The authored stage whose rate limiter wrote this fact.
    pub fn unit(&self) -> &StageKey {
        match self {
            Self::Delayed { unit, .. }
            | Self::ModeChange { unit, .. }
            | Self::ConfigChanged { unit, .. } => unit,
        }
    }

    pub fn event_type(&self) -> &'static str {
        match self {
            Self::Delayed { .. } => vocabulary::rate_limiter::WAIT_STARTED,
            Self::ModeChange { .. } => vocabulary::rate_limiter::MODE_CHANGED,
            Self::ConfigChanged { .. } => vocabulary::rate_limiter::CONFIGURATION_CHANGED,
        }
    }
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

impl SourcePollErrorKind {
    /// Stable lower-case label used in rendered diagnostics.
    pub const fn label(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::Deserialization => "deserialization",
            Self::Validation => "validation",
            Self::Other => "other",
        }
    }
}

/// Whether the reader could continue after reporting a poll error (084n B1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourcePollContinuation {
    Recoverable,
    Terminal,
}

/// Why the supervisor was opening a reader when acquisition failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceOpenIntent {
    Start,
    Resume,
}

/// A row a source refused and read past.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRecordRejectedFact {
    pub kind: ErrorKind,
    pub detail: String,
    pub diagnostic: super::source_diagnostic::SourceDiagnostic,
}

/// Any other source poll error, including one that stops the source.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePollFailedFact {
    pub kind: ErrorKind,
    pub detail: String,
    pub continuation: SourcePollContinuation,
    pub diagnostic: super::source_diagnostic::SourceDiagnostic,
}

/// Opening evidence, distinct from a poll failure; no poll occurred.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceOpenFailedFact {
    pub source_type: SourcePollKind,
    pub error_type: SourcePollErrorKind,
    pub intent: SourceOpenIntent,
    pub diagnostic: super::source_diagnostic::SourceDiagnostic,
    pub timestamp_ms: u64,
}

/// A supervisor opened a source reader (FLOWIP-134i B2).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReaderAcquiredFact {
    pub source_type: SourcePollKind,
    pub intent: SourceOpenIntent,
}

/// Where a source was when it released its reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceCleanupPhase {
    /// Before live admission, while a resume positioned the reader.
    Recovery,
    Live,
}

/// Why a source released its reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceCleanupTrigger {
    Exhausted,
    Stopped,
    Failed,
}

/// A source released the reader it acquired (FLOWIP-134i B2).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCleanupSucceededFact {
    pub source_type: SourcePollKind,
    pub phase: SourceCleanupPhase,
    pub trigger: SourceCleanupTrigger,
}

/// A source failed to release its reader. The failure is secondary: it never
/// replaces the stage's primary failure, which it names.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCleanupFailedFact {
    pub source_type: SourcePollKind,
    pub phase: SourceCleanupPhase,
    pub trigger: SourceCleanupTrigger,
    pub error_type: SourcePollErrorKind,
    pub diagnostic: super::source_diagnostic::SourceDiagnostic,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_failure: Option<crate::EventId>,
}

/// What a positioned source's following positions index, committed before its
/// first record (FLOWIP-134i B1, B11).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceInputPinnedFact {
    pub identity: crate::source::SourceInputIdentity,
    pub beginning: crate::source::EncodedSourcePosition,
}

/// How a positioned source continued on resume: positioned after its committed
/// prefix, or started because it never pinned (FLOWIP-134i B15). It names the
/// input by digest and the generation it entered; never a position.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceResumeFact {
    pub identity_digest: String,
    pub generation: u64,
}

/// What a source's live reattachment can cost the input it read but had not
/// committed when the run stopped (FLOWIP-122i).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReattachGuarantee {
    /// The sender redelivers what was never acknowledged; only hosted ingress,
    /// which acknowledges after commit, records it.
    SenderRedelivers,
    /// Each observation supersedes the last; re-seen rows are expected.
    LatestState,
    /// No input is admitted twice. Input not committed at the interruption, and
    /// input that arrived while no reader was attached, may be lost.
    AtMostOnce,
}

/// A source reattached live in `generation`, committed after its catch-up
/// watermark and before its first live record (FLOWIP-122i).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReattachedFact {
    pub generation: u64,
    pub guarantee: ReattachGuarantee,
}

/// Why a resume cannot continue a source's interrupted input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "refusal", rename_all = "snake_case")]
pub enum SourceContinuationRefusal {
    /// A named part of the input identity differs from the pin (B10).
    InputChanged { part: String },
    /// Admitted records lack their recorded positions or pin (B11).
    MissingEvidence,
    /// The recorded position has another version or does not decode.
    PositionUnreadable,
    /// The source's connector does not position it.
    Unpositioned,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceContinuationRefusedFact {
    pub refusal: SourceContinuationRefusal,
}

/// A positioned source failed to seek after its committed prefix.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSeekFailedFact {
    pub identity_digest: String,
    pub diagnostic: super::source_diagnostic::SourceDiagnostic,
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
            Self::SinkOperationFailed(_) | Self::SourceCleanupFailed(_) => {
                std::num::NonZeroU32::new(2).unwrap()
            }
            _ => std::num::NonZeroU32::MIN,
        }
    }

    pub fn event_type(&self, stage_name: &str) -> std::borrow::Cow<'static, str> {
        let name = match self {
            Self::ReplayLifecycle(event) => event.event_type(),
            Self::SupervisorRegistered { descriptor } => {
                return descriptor.registered_event_type().into()
            }
            Self::SupervisorCommandDiscarded { .. } => "execution.supervisor.command_discarded",
            Self::ContractStatus { pass, .. } => {
                if *pass {
                    vocabulary::contract::CONTINUATION_ALLOWED
                } else {
                    vocabulary::contract::CONTINUATION_DENIED
                }
            }
            Self::ContractResult { status, .. } => status.event_type(),
            Self::StageLifecycle(fact) => return fact.event_type(stage_name).into(),
            Self::CircuitBreaker(fact) => fact.event_type(),
            Self::Recovery(fact) => fact.event_type(),
            Self::RateLimiter(fact) => fact.event_type(),
            Self::Backpressure(_) => vocabulary::backpressure::STALL_DETECTED,
            Self::SourceRecordRejected(_) => vocabulary::source::RECORD_REJECTED,
            Self::SourcePollFailed(_) => vocabulary::source::POLL_FAILED,
            Self::SourceOpenFailed(_) => vocabulary::source::OPEN_FAILED,
            Self::SourceReaderAcquired(_) => vocabulary::source::READER_ACQUIRED,
            Self::SourceCleanupSucceeded(_) => vocabulary::source::CLEANUP_SUCCEEDED,
            Self::SourceCleanupFailed(_) => vocabulary::source::CLEANUP_FAILED,
            Self::SourceInputPinned(_) => vocabulary::source::INPUT_PINNED,
            Self::SourcePositioned(_) => vocabulary::source::POSITIONED,
            Self::SourceStartedOnResume(_) => vocabulary::source::STARTED_ON_RESUME,
            Self::SourceReattached(_) => vocabulary::source::REATTACHED,
            Self::SourceContinuationRefused(_) => vocabulary::source::CONTINUATION_REFUSED,
            Self::SourceSeekFailed(_) => vocabulary::source::SEEK_FAILED,
            Self::HttpPullState(_) => "source.http_pull_state",
            Self::AiChunkingPlanned(_) => "ai.chunking.planned",
            Self::AccumulatorProgress { .. } => "stateful.accumulation_progress",
            Self::JoinReferenceProgress { .. } => "join.reference_progress",
            Self::JoinPhase(fact) => fact.event_type(),
            Self::SinkAudit(audit) => audit.event_type(),
            Self::StageFatalRecorded(_) => "obzenflow.stage_fatal_recorded",
            Self::StageProcessingFailed(_) => vocabulary::stage::PROCESSING_FAILED,
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
            | Self::StageProcessingFailed(_)
            | Self::SinkOperationFailed(_)
            | Self::EffectRecord(_)
            | Self::EffectAttemptStarted(_)
            | Self::EffectRecoveryAbandoned(_) => true,
            Self::SinkAudit(_)
            | Self::ReplayLifecycle(_)
            | Self::SupervisorRegistered { .. }
            | Self::SupervisorCommandDiscarded { .. }
            | Self::ContractStatus { .. }
            | Self::ContractResult { .. }
            | Self::StageLifecycle(_)
            | Self::CircuitBreaker(_)
            | Self::Recovery(_)
            | Self::RateLimiter(_)
            | Self::Backpressure(_)
            | Self::SourceRecordRejected(_)
            | Self::SourcePollFailed(_)
            | Self::SourceOpenFailed(_)
            | Self::SourceReaderAcquired(_)
            | Self::SourceCleanupSucceeded(_)
            | Self::SourceCleanupFailed(_)
            | Self::SourceInputPinned(_)
            | Self::SourcePositioned(_)
            | Self::SourceStartedOnResume(_)
            | Self::SourceReattached(_)
            | Self::SourceContinuationRefused(_)
            | Self::SourceSeekFailed(_)
            | Self::HttpPullState(_)
            | Self::AiChunkingPlanned(_)
            | Self::AccumulatorProgress { .. }
            | Self::JoinReferenceProgress { .. }
            | Self::JoinPhase(_) => false,
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
pub enum RetryStopReason {
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
            "action": "opened", "unit": "orders", "error_rate": 1.0, "failure_count": 3,
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

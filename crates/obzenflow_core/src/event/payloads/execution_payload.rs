// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Closed, protected execution evidence. Measurements have no variant here.

use super::effect_payload::{
    EffectAttemptStarted, EffectCursor, EffectRecord, EffectRecoveryAbandoned,
};

use crate::ai::{ChunkExclusionReason, ChunkPlanningSummary, OversizePolicy};
use crate::event::context::ExecutionAccounting;
use crate::event::observability::{HttpPullState, WaitReason};
use crate::event::status::processing_status::ErrorKind;
use crate::StageId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "execution_type", rename_all = "snake_case")]
pub enum ExecutionPayload {
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
    },
    Drained {
        stage_id: StageId,
        events_processed: Option<u64>,
    },
    Completed {
        stage_id: StageId,
        accounting: ExecutionAccounting,
    },
    Failed {
        stage_id: StageId,
        error: String,
        recoverable: Option<bool>,
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
            Self::StageLifecycle(fact) => match fact {
                StageLifecycleFact::Running { .. } => "lifecycle.stage.running",
                StageLifecycleFact::Draining { .. } => "lifecycle.stage.draining",
                StageLifecycleFact::Drained { .. } => "lifecycle.stage.drained",
                StageLifecycleFact::Completed { .. } => "lifecycle.stage.completed",
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
            Self::StageLifecycle(_)
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

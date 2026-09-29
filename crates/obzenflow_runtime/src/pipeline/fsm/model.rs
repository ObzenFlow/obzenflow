// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FSM phases and the owner results which advance them.

use super::PipelineContext;
use crate::pipeline::termination::ExecutionOutcome;
use crate::pipeline::{FlowStopMode, PipelineControl, PipelineState};
use crate::stages::common::stage_handle::{StageAck, StageExit, StageFailure};
use obzenflow_fsm::{EventVariant, StateVariant};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PipelineFsmState {
    Created,
    Registering,
    InitializingStages,
    StartingConsumers,
    PublishingReady,
    ReadyForRun,
    PublishingStart,
    StartingSources,
    PublishingRunning,
    Running,
    Draining,
    CancellingChildren,
    FailingChildren { cause: String },
    PublishingTerminal,
    FinalisingMetrics,
    PublishingFinalMarker,
    Finished { outcome: ExecutionOutcome },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PublicationStep {
    Ready,
    Start,
    Running,
    Terminal,
    Stop,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum PipelineDeadline {
    GracefulStop,
    StageCleanup,
    Metrics,
}

#[derive(Clone, Debug)]
pub(crate) enum PipelineFsmEvent {
    Bootstrap,
    RegistrationCompleted,
    Start,
    GracefulStop {
        timeout: std::time::Duration,
    },
    Cancel,
    Abort {
        reason: String,
    },
    ChildAcknowledged(StageAck),
    ChildFailed(StageFailure),
    ChildExited(StageExit),
    MetricsReady(crate::stages::common::stage_lifecycle::MilestoneAck),
    MetricsExited(crate::stages::common::stage_lifecycle::LifecycleExit),
    ReadyPublished,
    StartPublished,
    RunningPublished,
    TerminalPublished,
    FinalisationCompleted {
        error: Option<String>,
    },
    GracefulStopExpired,
    StageCleanupExpired,
    MetricsExpired,
    /// The outstanding set for this phase is empty. The FSM checks it again.
    PhaseSatisfied,
    OperationalFailure {
        message: String,
    },
    ObservationEnded,
}

impl EventVariant for PipelineFsmEvent {
    fn variant_name(&self) -> &str {
        match self {
            Self::Bootstrap => "Bootstrap",
            Self::RegistrationCompleted => "RegistrationCompleted",
            Self::Start => "Start",
            Self::GracefulStop { .. } => "GracefulStop",
            Self::Cancel => "Cancel",
            Self::Abort { .. } => "Abort",
            Self::ChildAcknowledged(_) => "ChildAcknowledged",
            Self::ChildFailed(_) => "ChildFailed",
            Self::ChildExited(_) => "ChildExited",
            Self::MetricsReady(_) => "MetricsReady",
            Self::MetricsExited(_) => "MetricsExited",
            Self::ReadyPublished => "ReadyPublished",
            Self::StartPublished => "StartPublished",
            Self::RunningPublished => "RunningPublished",
            Self::TerminalPublished => "TerminalPublished",
            Self::FinalisationCompleted { .. } => "FinalisationCompleted",
            Self::GracefulStopExpired => "GracefulStopExpired",
            Self::StageCleanupExpired => "StageCleanupExpired",
            Self::MetricsExpired => "MetricsExpired",
            Self::PhaseSatisfied => "PhaseSatisfied",
            Self::OperationalFailure { .. } => "OperationalFailure",
            Self::ObservationEnded => "ObservationEnded",
        }
    }
}

impl From<PipelineControl> for PipelineFsmEvent {
    fn from(control: PipelineControl) -> Self {
        match control {
            PipelineControl::Start => Self::Start,
            PipelineControl::Stop {
                mode: FlowStopMode::Graceful { timeout },
            } => Self::GracefulStop { timeout },
            PipelineControl::Stop {
                mode: FlowStopMode::Cancel,
            } => Self::Cancel,
            PipelineControl::Abort { reason } => Self::Abort { reason },
        }
    }
}
impl From<PipelineDeadline> for PipelineFsmEvent {
    fn from(deadline: PipelineDeadline) -> Self {
        match deadline {
            PipelineDeadline::GracefulStop => Self::GracefulStopExpired,
            PipelineDeadline::StageCleanup => Self::StageCleanupExpired,
            PipelineDeadline::Metrics => Self::MetricsExpired,
        }
    }
}
impl StateVariant for PipelineFsmState {
    fn variant_name(&self) -> &str {
        match self {
            Self::Created => "Created",
            Self::Registering => "Registering",
            Self::InitializingStages => "InitializingStages",
            Self::StartingConsumers => "StartingConsumers",
            Self::PublishingReady => "PublishingReady",
            Self::ReadyForRun => "ReadyForRun",
            Self::PublishingStart => "PublishingStart",
            Self::StartingSources => "StartingSources",
            Self::PublishingRunning => "PublishingRunning",
            Self::Running => "Running",
            Self::Draining => "Draining",
            Self::CancellingChildren => "CancellingChildren",
            Self::PublishingTerminal => "PublishingTerminal",
            Self::FinalisingMetrics => "FinalisingMetrics",
            Self::PublishingFinalMarker => "PublishingFinalMarker",
            Self::FailingChildren { .. } => "FailingChildren",
            Self::Finished { .. } => "Finished",
        }
    }
}
impl PipelineFsmState {
    pub(crate) fn public_state(&self, _ctx: &PipelineContext) -> PipelineState {
        match self {
            Self::Created => PipelineState::Created,
            Self::Registering => PipelineState::Registering,
            Self::InitializingStages => PipelineState::InitializingStages,
            Self::StartingConsumers => PipelineState::StartingConsumers,
            Self::PublishingReady => PipelineState::PublishingReady,
            Self::ReadyForRun => PipelineState::ReadyForRun,
            Self::PublishingStart => PipelineState::PublishingStart,
            Self::StartingSources => PipelineState::StartingSources,
            Self::PublishingRunning => PipelineState::PublishingRunning,
            Self::Running => PipelineState::Running,
            Self::Draining => PipelineState::Draining,
            Self::CancellingChildren => PipelineState::CancellingChildren,
            Self::PublishingTerminal => PipelineState::PublishingTerminal,
            Self::FinalisingMetrics => PipelineState::FinalisingMetrics,
            Self::PublishingFinalMarker => PipelineState::PublishingFinalMarker,
            Self::FailingChildren { cause } => PipelineState::FailingChildren {
                cause: cause.clone(),
            },
            Self::Finished {
                outcome: ExecutionOutcome::Completed | ExecutionOutcome::NotStarted,
            } => PipelineState::Drained,
            Self::Finished {
                outcome: ExecutionOutcome::Cancelled { reason },
            } => PipelineState::Cancelled {
                reason: reason.clone(),
            },
            Self::Finished {
                outcome: ExecutionOutcome::Failed(failure),
            } => PipelineState::Failed {
                reason: failure.reason.clone(),
                failure_cause: failure.cause.clone(),
            },
        }
    }
}

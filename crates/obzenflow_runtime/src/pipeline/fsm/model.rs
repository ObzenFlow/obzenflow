// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Private execution phases, input events, deadlines and their public projection.

use super::PipelineContext;
use crate::pipeline::{FlowStopMode, PipelineControl, PipelineState};
use obzenflow_core::event::SystemEvent;
use obzenflow_fsm::{EventVariant, StateVariant};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum PipelineFsmState {
    Created,
    Materializing,
    AwaitingStageReadiness,
    ReadyForRun,
    StartingSources,
    Running,
    SourceCompleted,
    Draining,
    SettlingStages,
    CatchingUpProducers,
    PublishingTerminal,
    FinalisingMetrics,
    PublishingFinalMarker,
    Finished {
        outcome: crate::pipeline::termination::ExecutionOutcome,
    },
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
    Start,
    GracefulStop { timeout: std::time::Duration },
    Cancel,
    Abort { reason: String },
    Journal(Box<obzenflow_core::EventEnvelope<SystemEvent>>),
    GracefulStopExpired,
    StageCleanupExpired,
    MetricsExpired,
    PhysicalSettlementSatisfied,
    OperationalFailure { message: String },
}

impl EventVariant for PipelineFsmEvent {
    fn variant_name(&self) -> &str {
        match self {
            Self::Bootstrap => "Bootstrap",
            Self::Start => "Start",
            Self::GracefulStop { .. } => "GracefulStop",
            Self::Cancel => "Cancel",
            Self::Abort { .. } => "Abort",
            Self::Journal(_) => "Journal",
            Self::GracefulStopExpired => "GracefulStopExpired",
            Self::StageCleanupExpired => "StageCleanupExpired",
            Self::MetricsExpired => "MetricsExpired",
            Self::PhysicalSettlementSatisfied => "PhysicalSettlementSatisfied",
            Self::OperationalFailure { .. } => "OperationalFailure",
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
            Self::Materializing => "Materializing",
            Self::AwaitingStageReadiness => "AwaitingStageReadiness",
            Self::ReadyForRun => "ReadyForRun",
            Self::StartingSources => "StartingSources",
            Self::Running => "Running",
            Self::SourceCompleted => "SourceCompleted",
            Self::Draining => "Draining",
            Self::SettlingStages => "SettlingStages",
            Self::CatchingUpProducers => "CatchingUpProducers",
            Self::PublishingTerminal => "PublishingTerminal",
            Self::FinalisingMetrics => "FinalisingMetrics",
            Self::PublishingFinalMarker => "PublishingFinalMarker",
            Self::Finished { .. } => "Finished",
        }
    }
}

impl PipelineFsmState {
    pub(crate) fn public_state(&self, ctx: &PipelineContext) -> PipelineState {
        use crate::pipeline::termination::ExecutionOutcome;
        match self {
            Self::Created => PipelineState::Created,
            Self::Materializing => PipelineState::Materializing,
            Self::AwaitingStageReadiness => PipelineState::Materialized,
            Self::ReadyForRun | Self::StartingSources => PipelineState::ReadyForRun,
            Self::Running => PipelineState::Running,
            Self::SourceCompleted => PipelineState::SourceCompleted,
            Self::Draining
            | Self::SettlingStages
            | Self::CatchingUpProducers
            | Self::PublishingTerminal
            | Self::FinalisingMetrics
            | Self::PublishingFinalMarker => match &ctx.progress.abort_cause {
                Some((reason, upstream)) => PipelineState::AbortRequested {
                    reason: reason.clone(),
                    upstream: *upstream,
                },
                None => PipelineState::Draining,
            },
            Self::Finished {
                outcome: ExecutionOutcome::Failed(failure),
            } => PipelineState::Failed {
                reason: failure.reason.clone(),
                failure_cause: failure.cause.clone(),
            },
            Self::Finished { .. } => PipelineState::Drained,
        }
    }
}

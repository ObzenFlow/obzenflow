// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{PipelineContext, PipelineDeadline, PipelineFsmEvent, PipelineFsmState};
use obzenflow_fsm::{EventVariant, FsmError, StateVariant};
use tokio::time::Instant;

impl PipelineFsmState {
    pub(crate) fn phase_satisfied(&self, ctx: &PipelineContext) -> bool {
        match self {
            Self::InitializingStages | Self::StartingConsumers | Self::StartingSources => {
                ctx.outstanding_milestones.is_empty()
            }
            Self::Running => ctx
                .source_supervisors
                .keys()
                .all(|id| ctx.completed_stages.contains(id)),
            Self::Draining | Self::CancellingChildren | Self::FailingChildren { .. } => {
                ctx.outstanding_children.is_empty()
            }
            Self::FinalisingMetrics => ctx.resources.metrics_join.is_none(),
            _ => false,
        }
    }
    pub(crate) fn next_deadline(
        &self,
        ctx: &PipelineContext,
    ) -> Option<(Instant, PipelineDeadline)> {
        match self {
            Self::Draining => ctx
                .stop_intent
                .deadline
                .map(|at| (at, PipelineDeadline::GracefulStop)),
            Self::CancellingChildren | Self::FailingChildren { .. } => ctx
                .cleanup_deadline
                .map(|at| (at, PipelineDeadline::StageCleanup)),
            Self::FinalisingMetrics => ctx
                .metrics_deadline
                .filter(|_| ctx.resources.metrics_join.is_some())
                .map(|at| (at, PipelineDeadline::Metrics)),
            _ => None,
        }
    }
}

pub(super) fn require_phase(
    state: &PipelineFsmState,
    event: &PipelineFsmEvent,
    ctx: &PipelineContext,
) -> Result<(), FsmError> {
    if state.phase_satisfied(ctx) {
        Ok(())
    } else {
        Err(invalid_input(state, event))
    }
}
pub(super) fn invalid_input(state: &PipelineFsmState, event: &PipelineFsmEvent) -> FsmError {
    FsmError::InvalidTransition {
        from: state.variant_name().into(),
        event: event.variant_name().into(),
    }
}

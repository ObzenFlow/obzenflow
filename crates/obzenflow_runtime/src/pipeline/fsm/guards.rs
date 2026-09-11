// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FSM-owned eligibility shared with input polling. Readiness to dispatch an
//! internal input never replaces validation at the transition boundary.

use super::{PipelineContext, PipelineDeadline, PipelineFsmEvent, PipelineFsmState};
use crate::pipeline::{resources::ProducerTail, FlowStopMode};
use crate::supervised_base::SupervisorHandle;
use obzenflow_fsm::{EventVariant, FsmError, StateVariant};
use std::time::{Duration, Instant};

impl PipelineFsmState {
    pub(crate) fn settlement_satisfied(&self, ctx: &mut PipelineContext) -> bool {
        match self {
            Self::Materializing => ctx.resources.delivery.is_empty(),
            Self::SettlingStages => {
                ctx.resources.stages_joined && ctx.resources.publication_settlement.is_none()
            }
            Self::CatchingUpProducers => {
                matches!(ctx.resources.producer_tail, ProducerTail::Reached)
                    || ctx.progress.journal_failed
            }
            Self::FinalisingMetrics => {
                ctx.resources.metrics_joined
                    && ctx.resources.publication_settlement.is_none()
                    && (ctx.progress.metrics_drained
                        || ctx.progress.metrics_cancelled
                        || ctx.resources.metrics.handle().is_none()
                        || ctx.resources.metrics.handle().is_some_and(|handle| {
                            matches!(
                                handle.current_state(),
                                crate::metrics::MetricsAggregatorState::Failed { .. }
                            )
                        }))
            }
            Self::PublishingFinalMarker => {
                ctx.progress.final_marker_seen && ctx.resources.publication_settlement.is_none()
            }
            _ => false,
        }
    }

    pub(crate) fn deadline_at(
        &self,
        ctx: &PipelineContext,
        deadline: PipelineDeadline,
    ) -> Option<Instant> {
        match deadline {
            PipelineDeadline::GracefulStop
                if matches!(
                    self,
                    Self::Draining
                        | Self::SettlingStages
                        | Self::CatchingUpProducers
                        | Self::PublishingTerminal
                        | Self::FinalisingMetrics
                ) =>
            {
                ctx.stop_intent
                    .deadline
                    .filter(|_| matches!(ctx.stop_intent.mode, Some(FlowStopMode::Graceful { .. })))
            }
            PipelineDeadline::StageCleanup if matches!(self, Self::SettlingStages) => ctx
                .progress
                .cleanup_deadline
                .filter(|_| !ctx.progress.stages_cancelled),
            PipelineDeadline::Metrics
                if matches!(self, Self::PublishingTerminal | Self::FinalisingMetrics) =>
            {
                ctx.resources
                    .terminal_ack
                    .get()
                    .filter(|_| {
                        !ctx.progress.metrics_cancelled
                            && !ctx.resources.metrics_joined
                            && ctx.resources.metrics.handle().is_some()
                    })
                    .map(|at| *at + Duration::from_millis(ctx.metrics_drain_timeout_ms))
            }
            _ => None,
        }
    }

    pub(crate) fn next_deadline(
        &self,
        ctx: &PipelineContext,
    ) -> Option<(Instant, PipelineDeadline)> {
        [
            PipelineDeadline::GracefulStop,
            PipelineDeadline::StageCleanup,
            PipelineDeadline::Metrics,
        ]
        .into_iter()
        .filter_map(|deadline| self.deadline_at(ctx, deadline).map(|at| (at, deadline)))
        .min_by_key(|(at, _)| *at)
    }
}

pub(super) fn require_settlement(
    state: &PipelineFsmState,
    event: &PipelineFsmEvent,
    ctx: &mut PipelineContext,
) -> Result<(), FsmError> {
    if state.settlement_satisfied(ctx) {
        Ok(())
    } else {
        Err(invalid_input(state, event))
    }
}

pub(super) fn require_deadline(
    state: &PipelineFsmState,
    ctx: &PipelineContext,
    deadline: PipelineDeadline,
) -> Result<(), FsmError> {
    if state
        .deadline_at(ctx, deadline)
        .is_some_and(|at| Instant::now() >= at)
    {
        Ok(())
    } else {
        Err(invalid_input(state, &deadline.into()))
    }
}

pub(super) fn invalid_input(state: &PipelineFsmState, event: &PipelineFsmEvent) -> FsmError {
    FsmError::InvalidTransition {
        from: state.variant_name().into(),
        event: event.variant_name().into(),
    }
}

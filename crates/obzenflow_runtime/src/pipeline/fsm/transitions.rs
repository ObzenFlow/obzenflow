// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Named control, failure and settlement decisions. Source phases are declared
//! in the FSM map; journal decisions live in journal.rs.

use super::context::StopRequestOutcome;
use super::guards::{invalid_input, require_deadline, require_settlement};
use super::{
    PipelineAction as A, PipelineContext as C, PipelineDeadline, PipelineFsmEvent as E,
    PipelineFsmState as S,
};
use crate::pipeline::metrics::compute_flow_lifecycle_metrics;
use crate::pipeline::termination::ExecutionOutcome;
use crate::pipeline::FlowStopMode;
use futures::future::BoxFuture;
use obzenflow_core::event::types::DurationMs;
use obzenflow_core::event::{
    PipelineCancellationCause, PipelineStopAdmission, SystemEvent, SystemEventFactory,
};
use obzenflow_fsm::{FsmError, Transition};

pub(super) type Change = Transition<S, A>;
pub(super) type Decision<'a> = BoxFuture<'a, Result<Change, FsmError>>;
pub(super) type FailureDecision = fn(&mut C, String) -> Change;

pub(super) fn change(state: S, actions: Vec<A>) -> Change {
    Transition {
        next_state: state,
        actions,
    }
}
pub(super) fn decided<'a>(transition: Change) -> Decision<'a> {
    Box::pin(async move { Ok(transition) })
}
pub(super) fn publish(event: SystemEvent) -> A {
    A::Publish {
        event: Box::new(event),
        control: false,
    }
}
pub(super) fn factory(ctx: &C) -> SystemEventFactory {
    SystemEventFactory::new(ctx.system_id)
}

pub(super) fn unhandled<'a>(
    state: &'a S,
    event: &'a E,
    _: &'a mut C,
) -> BoxFuture<'a, Result<(), FsmError>> {
    Box::pin(async move {
        let stale_start = matches!(event, E::Start) && !matches!(state, S::Finished { .. });
        let closed_control = matches!(state, S::PublishingFinalMarker)
            && matches!(event, E::GracefulStop { .. } | E::Cancel | E::Abort { .. });
        if stale_start || closed_control {
            Ok(())
        } else {
            Err(invalid_input(state, event))
        }
    })
}

pub(super) fn bootstrap<'a>(_: &'a S, _: &'a E, ctx: &'a mut C) -> Decision<'a> {
    decided(
        if ctx.topology.num_stages() == 0
            || ctx.stage_supervisors.len() + ctx.source_supervisors.len()
                != ctx.topology.num_stages()
        {
            fail_stages(
                ctx,
                "Stage count mismatch between supervisors and topology".into(),
            )
        } else if ctx.stage_supervisors.is_empty() {
            fail_stages(
                ctx,
                "source-only topologies are unsupported by the readiness barrier".into(),
            )
        } else {
            change(S::Materializing, vec![A::InitialiseStages])
        },
    )
}

pub(super) fn start<'a>(state: &'a S, _: &'a E, ctx: &'a mut C) -> Decision<'a> {
    decided(if ctx.stop_intent.requested {
        change(state.clone(), vec![])
    } else {
        ctx.flow_start_time = Some(std::time::Instant::now());
        change(
            S::StartingSources,
            vec![
                publish(factory(ctx).pipeline_starting()),
                publish(factory(ctx).pipeline_running()),
            ],
        )
    })
}

// Coalesce requests and order effects. The registered phase handler selects
// the successor; this helper does not implement another lifecycle switch.
fn admit_stop(state: &S, event: &E, ctx: &mut C) -> Result<Option<Vec<A>>, FsmError> {
    let (mode, reason) = match event {
        E::GracefulStop { timeout } => (FlowStopMode::Graceful { timeout: *timeout }, None),
        E::Cancel => (FlowStopMode::Cancel, None),
        E::GracefulStopExpired => {
            require_deadline(state, ctx, PipelineDeadline::GracefulStop)?;
            (
                FlowStopMode::Cancel,
                Some(crate::stages::common::stage_handle::STOP_REASON_TIMEOUT.into()),
            )
        }
        _ => return Err(invalid_input(state, event)),
    };
    let StopRequestOutcome::Applied { mode, reason_label } =
        ctx.stop_intent.apply_request(mode, reason)
    else {
        return Ok(None);
    };
    tracing::info!(%reason_label, ?mode, "Pipeline stop admitted");
    let admission = match mode {
        FlowStopMode::Graceful { timeout } => PipelineStopAdmission::Graceful {
            timeout_ms: DurationMs(timeout.as_millis().min(u64::MAX as u128) as u64),
        },
        FlowStopMode::Cancel => PipelineStopAdmission::Cancel {
            cause: if ctx.stop_intent.reason.as_deref()
                == Some(crate::stages::common::stage_handle::STOP_REASON_TIMEOUT)
            {
                PipelineCancellationCause::GracefulTimeout
            } else {
                PipelineCancellationCause::Requested
            },
        },
    };
    let mut actions = vec![];
    if matches!(mode, FlowStopMode::Cancel) || ctx.flow_start_time.is_none() {
        actions.push(A::CancelStages {
            contract_abort: false,
        });
    }
    actions.push(A::Publish {
        event: Box::new(factory(ctx).pipeline_stop_admitted(admission)),
        control: true,
    });
    Ok(Some(actions))
}

pub(super) fn stop_before_start<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    cancel_and_settle(state, event, ctx)
}
pub(super) fn cancel_and_settle<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let Some(mut actions) = admit_stop(state, event, ctx)? else {
            return Ok(change(state.clone(), vec![]));
        };
        actions.extend([A::ObserveStages, A::DrainMetrics]);
        Ok(change(S::SettlingStages, actions))
    })
}
pub(super) fn begin_graceful_drain<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let Some(actions) = admit_stop(state, event, ctx)? else {
            return Ok(change(state.clone(), vec![]));
        };
        Ok(change(S::Draining, actions))
    })
}
pub(super) fn stop_during_settlement<'a>(
    state: &'a S,
    event: &'a E,
    ctx: &'a mut C,
) -> Decision<'a> {
    Box::pin(async move {
        Ok(change(
            state.clone(),
            admit_stop(state, event, ctx)?.unwrap_or_default(),
        ))
    })
}

pub(super) fn fail_stages(ctx: &mut C, message: String) -> Change {
    ctx.termination.fail(message, None);
    change(
        S::SettlingStages,
        vec![
            A::CancelStages {
                contract_abort: false,
            },
            A::ObserveStages,
            A::DrainMetrics,
        ],
    )
}
pub(super) fn fail_catchup(ctx: &mut C, message: String) -> Change {
    ctx.termination.fail(message, None);
    change(
        S::CatchingUpProducers,
        vec![A::CancelStages {
            contract_abort: false,
        }],
    )
}
pub(super) fn fail_finalisation(ctx: &mut C, message: String) -> Change {
    ctx.termination.fail(message, None);
    change(
        S::FinalisingMetrics,
        vec![
            A::CancelStages {
                contract_abort: false,
            },
            A::CancelMetrics,
            A::ObserveMetrics,
        ],
    )
}
fn failure_input<'a>(state: &S, event: &E, ctx: &mut C, fail: FailureDecision) -> Decision<'a> {
    let message = match event {
        E::Abort { .. } if ctx.termination.failure.is_some() => {
            return decided(change(state.clone(), vec![]))
        }
        E::Abort { reason } => format!("Force abort: {reason}"),
        E::OperationalFailure { message } => message.clone(),
        _ => unreachable!("failure inputs are constrained by the transition map"),
    };
    decided(fail(ctx, message))
}
pub(super) fn failure_before_terminal<'a>(
    state: &'a S,
    event: &'a E,
    ctx: &'a mut C,
) -> Decision<'a> {
    failure_input(state, event, ctx, fail_stages)
}
pub(super) fn failure_during_catchup<'a>(
    state: &'a S,
    event: &'a E,
    ctx: &'a mut C,
) -> Decision<'a> {
    failure_input(state, event, ctx, fail_catchup)
}
pub(super) fn failure_after_terminal<'a>(
    state: &'a S,
    event: &'a E,
    ctx: &'a mut C,
) -> Decision<'a> {
    failure_input(state, event, ctx, fail_finalisation)
}
pub(super) fn expire_stage_cleanup<'a>(state: &'a S, _: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        require_deadline(state, ctx, PipelineDeadline::StageCleanup)?;
        ctx.progress.cleanup_deadline = None;
        Ok(change(
            state.clone(),
            vec![A::CancelStages {
                contract_abort: false,
            }],
        ))
    })
}
pub(super) fn expire_metrics<'a>(state: &'a S, _: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        require_deadline(state, ctx, PipelineDeadline::Metrics)?;
        tracing::warn!(
            timeout_ms = ctx.metrics_drain_timeout_ms,
            "Metrics finalisation did not settle within its budget"
        );
        Ok(change(
            state.clone(),
            vec![A::CancelMetrics, A::ObserveMetrics],
        ))
    })
}

fn selected_outcome(ctx: &C) -> ExecutionOutcome {
    if let Some(failure) = &ctx.termination.failure {
        return ExecutionOutcome::Failed(failure.clone());
    }
    if ctx.flow_start_time.is_none() {
        return ExecutionOutcome::NotStarted;
    }
    if matches!(ctx.stop_intent.mode, Some(FlowStopMode::Cancel))
        || (ctx.stop_intent.requested
            && ctx.topology.stages().any(|stage| {
                matches!(
                    stage.stage_type,
                    obzenflow_topology::StageType::InfiniteSource
                )
            }))
    {
        ExecutionOutcome::Cancelled {
            reason: ctx.stop_intent.reason_label(),
        }
    } else {
        ExecutionOutcome::Completed
    }
}

fn finish(ctx: &C) -> Change {
    let outcome = ctx
        .progress
        .selected_terminal
        .as_ref()
        .map(|(_, outcome)| outcome.clone())
        .unwrap_or_else(|| selected_outcome(ctx));
    change(S::Finished { outcome }, vec![])
}

pub(super) fn initialisation_delivered<'a>(
    state: &'a S,
    event: &'a E,
    ctx: &'a mut C,
) -> Decision<'a> {
    Box::pin(async move {
        require_settlement(state, event, ctx)?;
        let mut actions = vec![A::StartMetricsAggregator, A::StartNonSources];
        super::journal::announce_readiness(ctx, &mut actions);
        Ok(change(S::AwaitingStageReadiness, actions))
    })
}
pub(super) fn stage_owners_settled<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        require_settlement(state, event, ctx)?;
        ctx.progress.cleanup_deadline = None;
        Ok(if ctx.progress.journal_failed {
            change(
                S::FinalisingMetrics,
                vec![A::CancelMetrics, A::ObserveMetrics],
            )
        } else {
            change(S::CatchingUpProducers, vec![A::CaptureProducerTail])
        })
    })
}
pub(super) fn producer_tail_reached<'a>(
    state: &'a S,
    event: &'a E,
    ctx: &'a mut C,
) -> Decision<'a> {
    Box::pin(async move {
        require_settlement(state, event, ctx)?;
        if ctx.progress.journal_failed {
            return Ok(change(
                S::FinalisingMetrics,
                vec![A::CancelMetrics, A::ObserveMetrics],
            ));
        }
        let outcome = selected_outcome(ctx);
        let duration = DurationMs(
            ctx.flow_start_time
                .map(|start| start.elapsed().as_millis() as u64)
                .unwrap_or(0),
        );
        let metrics = compute_flow_lifecycle_metrics(ctx);
        let event = match &outcome {
            ExecutionOutcome::Completed => factory(ctx).pipeline_completed(duration, metrics),
            ExecutionOutcome::Cancelled { reason } => {
                factory(ctx).pipeline_cancelled(reason.clone(), duration, Some(metrics), None)
            }
            ExecutionOutcome::Failed(failure) => factory(ctx).pipeline_failed(
                failure.reason.clone(),
                duration,
                Some(metrics),
                failure.cause.clone(),
            ),
            ExecutionOutcome::NotStarted => factory(ctx).pipeline_not_started(),
        };
        ctx.progress.selected_terminal = Some((event, outcome));
        Ok(change(S::PublishingTerminal, vec![A::PublishTerminal]))
    })
}
pub(super) fn metrics_owner_settled<'a>(
    state: &'a S,
    event: &'a E,
    ctx: &'a mut C,
) -> Decision<'a> {
    Box::pin(async move {
        require_settlement(state, event, ctx)?;
        Ok(
            if ctx.resources.failure.get().is_some()
                || ctx.progress.journal_failed
                || ctx.termination.published.get().is_none()
            {
                finish(ctx)
            } else {
                change(S::PublishingFinalMarker, vec![A::PublishFinalMarker])
            },
        )
    })
}
pub(super) fn final_marker_settled<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        require_settlement(state, event, ctx)?;
        Ok(finish(ctx))
    })
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Child results are authoritative. These decisions aggregate them and select
//! the pipeline's own work without inspecting child journals.

use super::context::StopRequestOutcome;
use super::guards::{invalid_input, require_phase};
use super::{
    PipelineAction as A, PipelineContext as C, PipelineFsmEvent as E, PipelineFsmState as S,
    PublicationStep,
};
use crate::pipeline::metrics::compute_flow_lifecycle_metrics;
use crate::pipeline::termination::ExecutionOutcome;
use crate::pipeline::FlowStopMode;
use crate::stages::common::stage_handle::{StageMilestone, STOP_REASON_TIMEOUT};
use crate::stages::common::stage_lifecycle::LifecycleExit;
use crate::supervised_base::handler_supervised::SupervisorAction as H;
use crate::supervised_base::publication;
use futures::future::BoxFuture;
use obzenflow_core::event::types::{DurationMs, ViolationCause};
use obzenflow_core::event::{
    PipelineCancellationCause, PipelineStopAdmission, SystemEvent, SystemEventFactory,
};
use obzenflow_fsm::{FsmError, Transition};

pub(super) type Change = Transition<S, A>;
pub(super) type Decision<'a> = BoxFuture<'a, Result<Change, FsmError>>;
fn change(state: S, actions: Vec<A>) -> Change {
    Transition {
        next_state: state,
        actions,
    }
}
fn decided<'a>(transition: Change) -> Decision<'a> {
    Box::pin(async move { Ok(transition) })
}
fn factory(ctx: &C) -> SystemEventFactory {
    SystemEventFactory::new(ctx.system_id)
}
fn publish(event: SystemEvent, step: PublicationStep) -> A {
    A::Publish {
        event: Box::new(event),
        step,
    }
}
fn observe(
    ctx: &mut C,
    stage: obzenflow_core::StageId,
    snapshot: &crate::stages::common::stage_lifecycle::StageSnapshot,
) -> Result<(), FsmError> {
    publication::incorporate(&snapshot.causal_context)
        .map_err(|error| FsmError::HandlerError(error.to_string()))?;
    ctx.stage_lifecycle_metrics
        .insert(stage, snapshot.accounting.clone());
    Ok(())
}

pub(super) fn unhandled<'a>(
    state: &'a S,
    event: &'a E,
    _: &'a mut C,
) -> BoxFuture<'a, Result<(), FsmError>> {
    Box::pin(async move {
        // Late observation and completed publication results do not replay work.
        // Duplicate commands are admitted and interpreted here by the owner FSM.
        match event {
            E::Start
            | E::Cancel
            | E::GracefulStop { .. }
            | E::Abort { .. }
            | E::ObservationEnded
            | E::ChildAcknowledged(_)
            | E::MetricsReady(_)
            | E::RegistrationCompleted
            | E::ReadyPublished
            | E::StartPublished
            | E::RunningPublished
            | E::TerminalPublished
            | E::MetricsExited(_) => Ok(()),
            _ => Err(invalid_input(state, event)),
        }
    })
}

pub(super) fn bootstrap<'a>(_: &'a S, _: &'a E, _: &'a mut C) -> Decision<'a> {
    decided(change(
        S::Registering,
        vec![A::ObserveChildren, A::Register],
    ))
}
pub(super) fn registered<'a>(_: &'a S, _: &'a E, ctx: &'a mut C) -> Decision<'a> {
    if ctx.topology.num_stages() == 0
        || ctx.stage_supervisors.len() + ctx.source_supervisors.len() != ctx.topology.num_stages()
    {
        return decided(fail_children(
            ctx,
            "Stage count mismatch between supervisors and topology".into(),
            None,
        ));
    }
    if ctx.stage_supervisors.is_empty() {
        return decided(fail_children(
            ctx,
            "source-only topologies are unsupported".into(),
            None,
        ));
    }
    decided(change(S::InitializingStages, vec![A::InitialiseStages]))
}
pub(super) fn initialized<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        require_phase(state, event, ctx)?;
        Ok(change(S::StartingConsumers, vec![A::StartConsumers]))
    })
}
pub(super) fn consumers_started<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        require_phase(state, event, ctx)?;
        Ok(change(
            S::PublishingReady,
            vec![publish(
                factory(ctx).pipeline_ready_for_run(Some(ctx.topology.num_stages())),
                PublicationStep::Ready,
            )],
        ))
    })
}
pub(super) fn ready_published<'a>(_: &'a S, _: &'a E, _: &'a mut C) -> Decision<'a> {
    decided(change(S::ReadyForRun, vec![]))
}
pub(super) fn start<'a>(_: &'a S, _: &'a E, ctx: &'a mut C) -> Decision<'a> {
    ctx.flow_start_time = Some(std::time::Instant::now());
    decided(change(
        S::PublishingStart,
        vec![publish(
            factory(ctx).pipeline_starting(),
            PublicationStep::Start,
        )],
    ))
}
pub(super) fn start_published<'a>(_: &'a S, _: &'a E, _: &'a mut C) -> Decision<'a> {
    decided(change(S::StartingSources, vec![A::StartSources]))
}
pub(super) fn sources_started<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        require_phase(state, event, ctx)?;
        Ok(change(
            S::PublishingRunning,
            vec![publish(
                factory(ctx).pipeline_running(),
                PublicationStep::Running,
            )],
        ))
    })
}
pub(super) fn running_published<'a>(_: &'a S, _: &'a E, _: &'a mut C) -> Decision<'a> {
    decided(change(S::Running, vec![]))
}
pub(super) fn sources_completed<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        require_phase(state, event, ctx)?;
        Ok(change(S::Draining, vec![]))
    })
}

pub(super) fn acknowledge<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let E::ChildAcknowledged(ack) = event else {
            return Err(invalid_input(state, event));
        };
        let expected = match state {
            S::InitializingStages => StageMilestone::Initialized,
            _ => StageMilestone::Started,
        };
        if ack.milestone == expected && ctx.outstanding_milestones.remove(&ack.stage_id.into()) {
            observe(ctx, ack.stage_id, &ack.snapshot)?;
        }
        Ok(change(state.clone(), vec![]))
    })
}
pub(super) fn metrics_ready<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let E::MetricsReady(ack) = event else {
            return Err(invalid_input(state, event));
        };
        if let Some(writer) = ctx.resources.metrics.writer_id() {
            ctx.outstanding_milestones.remove(&writer);
        }
        publication::incorporate(&ack.snapshot.causal_context)
            .map_err(|e| FsmError::HandlerError(e.to_string()))?;
        Ok(change(state.clone(), vec![]))
    })
}

fn fail_children(ctx: &mut C, message: String, violation: Option<ViolationCause>) -> Change {
    ctx.termination.fail(message, violation);
    let cause = ctx
        .termination
        .failure
        .as_ref()
        .expect("failure selected")
        .reason
        .clone();
    ctx.cleanup_deadline
        .get_or_insert_with(|| tokio::time::Instant::now() + super::context::stop_drain_timeout());
    change(S::FailingChildren { cause }, vec![A::CancelChildren])
}
pub(super) fn failure<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let (message, violation) = match event {
            E::ChildFailed(failure) => {
                observe(ctx, failure.stage_id, &failure.snapshot)?;
                if matches!(state, S::CancellingChildren | S::FailingChildren { .. })
                    && matches!(
                        failure.cause,
                        crate::stages::common::stage_handle::StageError::Aborted
                    )
                {
                    return Ok(change(state.clone(), vec![]));
                }
                (
                    format!("Stage {}: {}", failure.stage_id, failure.cause),
                    failure
                        .cause
                        .contract_failure()
                        .map(|failure| failure.cause.clone()),
                )
            }
            E::Abort { reason } => (format!("Force abort: {reason}"), None),
            E::OperationalFailure { message } => {
                ctx.resources
                    .retain_failure(Box::new(std::io::Error::other(message.clone())));
                (message.clone(), None)
            }
            _ => return Err(invalid_input(state, event)),
        };
        if matches!(state, S::FailingChildren { .. }) {
            return Ok(change(state.clone(), vec![]));
        }
        Ok(fail_children(ctx, message, violation))
    })
}
pub(super) fn child_exited<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let E::ChildExited(exit) = event else {
            return Err(invalid_input(state, event));
        };
        if !ctx.outstanding_children.remove(&exit.stage_id) {
            return Ok(change(state.clone(), vec![]));
        }
        observe(ctx, exit.stage_id, exit.outcome.snapshot())?;
        match &exit.outcome {
            LifecycleExit::Completed(_) => {
                ctx.completed_stages.insert(exit.stage_id);
            }
            LifecycleExit::Cancelled { .. }
                if matches!(state, S::CancellingChildren | S::FailingChildren { .. }) => {}
            _ if matches!(state, S::FailingChildren { .. }) => {}
            LifecycleExit::Failed(failure) => {
                return Ok(fail_children(
                    ctx,
                    format!("Stage {} terminated: {}", exit.stage_id, failure.cause),
                    failure
                        .cause
                        .contract_failure()
                        .map(|failure| failure.cause.clone()),
                ))
            }
            _ => {
                return Ok(fail_children(
                    ctx,
                    format!("Stage {} terminated: {:?}", exit.stage_id, exit.outcome),
                    None,
                ))
            }
        }
        Ok(change(state.clone(), vec![]))
    })
}

pub(super) fn stop<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let (mode, reason) = match event {
            E::GracefulStop { timeout } => (FlowStopMode::Graceful { timeout: *timeout }, None),
            E::Cancel => (FlowStopMode::Cancel, None),
            E::GracefulStopExpired if ctx.stop_intent.timeout_due() => {
                (FlowStopMode::Cancel, Some(STOP_REASON_TIMEOUT.to_owned()))
            }
            _ => return Err(invalid_input(state, event)),
        };
        let StopRequestOutcome::Applied { mode, reason_label } =
            ctx.stop_intent.apply_request(mode, reason)
        else {
            return Ok(change(state.clone(), vec![]));
        };
        tracing::info!(%reason_label, ?mode, "Pipeline stop admitted");
        let cancel =
            matches!(mode, FlowStopMode::Cancel) || !matches!(state, S::Running | S::Draining);
        let admission = match mode {
            FlowStopMode::Graceful { timeout } => PipelineStopAdmission::Graceful {
                timeout_ms: DurationMs(timeout.as_millis().min(u64::MAX as u128) as u64),
            },
            FlowStopMode::Cancel => PipelineStopAdmission::Cancel {
                cause: if ctx.stop_intent.reason.as_deref() == Some(STOP_REASON_TIMEOUT) {
                    PipelineCancellationCause::GracefulTimeout
                } else {
                    PipelineCancellationCause::Requested
                },
            },
        };
        let mut actions = vec![if cancel {
            A::CancelChildren
        } else {
            A::StopSources
        }];
        actions.push(publish(
            factory(ctx).pipeline_stop_admitted(admission),
            PublicationStep::Stop,
        ));
        if cancel {
            ctx.cleanup_deadline = Some(
                tokio::time::Instant::now()
                    + if matches!(event, E::GracefulStopExpired) {
                        std::time::Duration::ZERO
                    } else {
                        super::context::stop_drain_timeout()
                    },
            );
        }
        Ok(change(
            if cancel {
                S::CancellingChildren
            } else {
                S::Draining
            },
            actions,
        ))
    })
}
pub(super) fn expire_children<'a>(state: &'a S, _: &'a E, ctx: &'a mut C) -> Decision<'a> {
    ctx.cleanup_deadline = None;
    decided(change(state.clone(), vec![A::AbortRemainingChildren]))
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
pub(super) fn children_settled<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        require_phase(state, event, ctx)?;
        ctx.cleanup_deadline = None;
        if ctx.resources.publications.first_failure().is_some() {
            return Ok(change(
                S::FinalisingMetrics,
                vec![A::FinishChildren, A::FinaliseMetrics, A::CancelMetrics],
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
        Ok(change(
            S::PublishingTerminal,
            vec![
                A::FinishChildren,
                A::PublishTerminal {
                    event: Box::new(event),
                    outcome,
                },
            ],
        ))
    })
}
pub(super) fn terminal_published<'a>(_: &'a S, _: &'a E, _: &'a mut C) -> Decision<'a> {
    decided(change(S::FinalisingMetrics, vec![A::FinaliseMetrics]))
}
pub(super) fn late_failure<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    let message = match event {
        E::OperationalFailure { message } => message.clone(),
        E::Abort { reason } => reason.clone(),
        E::ChildFailed(failure) => failure.cause.to_string(),
        _ => "Finalisation failed".into(),
    };
    ctx.resources
        .retain_failure(Box::new(std::io::Error::other(message.clone())));
    let violation = match event {
        E::ChildFailed(failure) => failure
            .cause
            .contract_failure()
            .map(|failure| failure.cause.clone()),
        _ => None,
    };
    ctx.termination.fail(message, violation);
    decided(match state {
        S::PublishingTerminal => change(
            S::FinalisingMetrics,
            vec![A::FinaliseMetrics, A::CancelMetrics],
        ),
        S::FinalisingMetrics => change(state.clone(), vec![A::CancelMetrics]),
        _ => change(state.clone(), vec![]),
    })
}
pub(super) fn metrics_exited<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let E::MetricsExited(exit) = event else {
            return Err(invalid_input(state, event));
        };
        // The child's settled result includes its accepted final publications.
        // Incorporate that result before selecting the pipeline's final marker.
        publication::incorporate(&exit.snapshot().causal_context)
            .map_err(|error| FsmError::HandlerError(error.to_string()))?;
        match exit {
            LifecycleExit::Completed(_) => {}
            LifecycleExit::Cancelled { .. } if ctx.metrics_deadline.is_none() => {}
            _ => {
                let message = exit.result().unwrap_err().to_string();
                ctx.resources
                    .retain_failure(Box::new(std::io::Error::other(message.clone())));
                ctx.termination.fail(message, None);
            }
        }
        Ok(change(state.clone(), vec![]))
    })
}
pub(super) fn metrics_settled<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        require_phase(state, event, ctx)?;
        ctx.metrics_deadline = None;
        Ok(change(
            S::PublishingFinalMarker,
            vec![A::Host(H::CloseMailbox), A::PublishFinalMarker],
        ))
    })
}
pub(super) fn expire_metrics<'a>(state: &'a S, _: &'a E, ctx: &'a mut C) -> Decision<'a> {
    ctx.metrics_deadline = None;
    decided(change(state.clone(), vec![A::CancelMetrics]))
}
pub(super) fn finish<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    if let E::FinalisationCompleted {
        error: Some(message),
    } = event
    {
        ctx.resources
            .retain_failure(Box::new(std::io::Error::other(message.clone())));
        ctx.termination.fail(message.clone(), None);
    }
    decided(change(
        S::Finished {
            outcome: selected_outcome(ctx),
        },
        vec![],
    ))
}

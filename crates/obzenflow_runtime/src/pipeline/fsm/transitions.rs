// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Lifecycle policy. Journal envelopes are folded here, inside the canonical
//! FSM transition, before it authorises effects through named actions.

use super::context::{record_stage_completion, ContractEdgeStatus, StopRequestOutcome};
use super::{
    PipelineAction as A, PipelineContext as C, PipelineDeadline, PipelineFsmEvent as E,
    PipelineFsmState as S,
};
use crate::id_conversions::StageIdExt;
use crate::pipeline::config::SourceContractStrictMode;
use crate::pipeline::metrics::compute_flow_lifecycle_metrics;
use crate::pipeline::resources::ProducerTail;
use crate::pipeline::termination::ExecutionOutcome;
use crate::pipeline::{FlowStopMode, PipelineControl};
use futures::future::BoxFuture;
use obzenflow_core::event::types::{DurationMs, ViolationCause};
use obzenflow_core::event::{
    MetricsCoordinationEvent, PipelineCancellationCause, PipelineLifecycleEvent,
    PipelineStopAdmission, StageLifecycleEvent, SystemEvent, SystemEventFactory, SystemEventType,
};
use obzenflow_fsm::{FsmError, Transition};

type Change = Transition<S, A>;
type Decision<'a> = BoxFuture<'a, Result<Change, FsmError>>;

fn change(state: S, actions: Vec<A>) -> Change {
    Transition {
        next_state: state,
        actions,
    }
}
fn decided<'a>(transition: Change) -> Decision<'a> {
    Box::pin(async move { Ok(transition) })
}
fn publish(event: SystemEvent) -> A {
    A::Publish {
        event: Box::new(event),
        control: false,
    }
}
fn factory(ctx: &C) -> SystemEventFactory {
    SystemEventFactory::new(ctx.system_id)
}
fn settling(state: &S) -> bool {
    matches!(
        state,
        S::SettlingStages
            | S::CatchingUpProducers
            | S::PublishingTerminal
            | S::FinalisingMetrics
            | S::PublishingFinalMarker
            | S::Finished { .. }
    )
}

pub(super) fn bootstrap<'a>(state: &'a S, _: &'a E, ctx: &'a mut C) -> Decision<'a> {
    let transition = if matches!(state, S::Created) {
        if ctx.topology.num_stages() == 0
            || ctx.stage_supervisors.len() + ctx.source_supervisors.len()
                != ctx.topology.num_stages()
        {
            fail(
                state,
                ctx,
                "Stage count mismatch between supervisors and topology".into(),
            )
        } else if ctx.stage_supervisors.is_empty() {
            fail(
                state,
                ctx,
                "source-only topologies are unsupported by the readiness barrier".into(),
            )
        } else {
            change(S::Materializing, vec![A::InitialiseStages])
        }
    } else {
        change(state.clone(), vec![])
    };
    decided(transition)
}

fn stop(state: &S, ctx: &mut C, mode: FlowStopMode, reason: Option<String>) -> Change {
    if matches!(state, S::PublishingFinalMarker | S::Finished { .. }) {
        return change(state.clone(), vec![]);
    }
    let StopRequestOutcome::Applied { mode, reason_label } =
        ctx.stop_intent.apply_request(mode, reason)
    else {
        return change(state.clone(), vec![]);
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
    let cancel = matches!(mode, FlowStopMode::Cancel) || ctx.flow_start_time.is_none();
    if cancel {
        actions.push(A::CancelStages {
            contract_abort: false,
        });
    }
    actions.push(A::Publish {
        event: Box::new(factory(ctx).pipeline_stop_admitted(admission)),
        control: true,
    });
    let next = if settling(state) {
        state.clone()
    } else if cancel {
        actions.extend([A::ObserveStages, A::DrainMetrics]);
        S::SettlingStages
    } else {
        S::Draining
    };
    change(next, actions)
}

pub(super) fn control<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    // All execution resources have settled before the final marker is
    // authorised. A late control cannot admit work behind that marker or
    // restart finalisation; append failure still uses the failure gateway.
    if matches!(state, S::PublishingFinalMarker | S::Finished { .. }) {
        return decided(change(state.clone(), vec![]));
    }
    let E::Control(control) = event else {
        unreachable!()
    };
    let transition = match control {
        PipelineControl::Start if matches!(state, S::ReadyForRun) && !ctx.stop_intent.requested => {
            ctx.flow_start_time = Some(std::time::Instant::now());
            change(
                S::StartingSources,
                vec![
                    publish(factory(ctx).pipeline_starting()),
                    publish(factory(ctx).pipeline_running()),
                ],
            )
        }
        PipelineControl::Start => change(state.clone(), vec![]),
        PipelineControl::Stop { mode } => stop(state, ctx, mode.clone(), None),
        PipelineControl::Abort { .. } if ctx.termination.failure.is_some() => {
            change(state.clone(), vec![])
        }
        PipelineControl::Abort { reason } => fail(state, ctx, format!("Force abort: {reason}")),
    };
    decided(transition)
}

fn fail(state: &S, ctx: &mut C, message: String) -> Change {
    if matches!(state, S::Finished { .. }) {
        return change(state.clone(), vec![]);
    }
    ctx.termination.fail(message, None);
    if matches!(
        state,
        S::PublishingTerminal | S::FinalisingMetrics | S::PublishingFinalMarker
    ) {
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
    } else if matches!(state, S::CatchingUpProducers) {
        change(
            state.clone(),
            vec![A::CancelStages {
                contract_abort: false,
            }],
        )
    } else {
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
}

pub(super) fn failure<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    let E::OperationalFailure { message } = event else {
        unreachable!()
    };
    decided(fail(state, ctx, message.clone()))
}

pub(super) fn deadline<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    let E::Deadline(deadline) = event else {
        unreachable!()
    };
    let transition = match deadline {
        PipelineDeadline::GracefulStop => stop(
            state,
            ctx,
            FlowStopMode::Cancel,
            Some(crate::stages::common::stage_handle::STOP_REASON_TIMEOUT.into()),
        ),
        PipelineDeadline::StageCleanup => {
            ctx.progress.cleanup_deadline = None;
            change(
                state.clone(),
                vec![A::CancelStages {
                    contract_abort: false,
                }],
            )
        }
        PipelineDeadline::Metrics => {
            tracing::warn!(
                timeout_ms = ctx.metrics_drain_timeout_ms,
                "Metrics finalisation did not settle within its budget"
            );
            change(state.clone(), vec![A::CancelMetrics, A::ObserveMetrics])
        }
    };
    decided(transition)
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

pub(super) fn settled<'a>(state: &'a S, _: &'a E, ctx: &'a mut C) -> Decision<'a> {
    let transition =
        match state {
            S::Materializing => {
                let mut actions = vec![A::StartMetricsAggregator, A::StartNonSources];
                announce_readiness(ctx, &mut actions);
                change(S::AwaitingStageReadiness, actions)
            }
            S::SettlingStages => {
                ctx.progress.cleanup_deadline = None;
                if ctx.progress.journal_failed {
                    change(
                        S::FinalisingMetrics,
                        vec![A::CancelMetrics, A::ObserveMetrics],
                    )
                } else {
                    change(S::CatchingUpProducers, vec![A::CaptureProducerTail])
                }
            }
            S::CatchingUpProducers => {
                if ctx.progress.journal_failed {
                    change(
                        S::FinalisingMetrics,
                        vec![A::CancelMetrics, A::ObserveMetrics],
                    )
                } else {
                    let outcome = selected_outcome(ctx);
                    let duration = DurationMs(
                        ctx.flow_start_time
                            .map(|start| start.elapsed().as_millis() as u64)
                            .unwrap_or(0),
                    );
                    let metrics = compute_flow_lifecycle_metrics(ctx);
                    let event =
                        match &outcome {
                            ExecutionOutcome::Completed => {
                                factory(ctx).pipeline_completed(duration, metrics)
                            }
                            ExecutionOutcome::Cancelled { reason } => factory(ctx)
                                .pipeline_cancelled(reason.clone(), duration, Some(metrics), None),
                            ExecutionOutcome::Failed(failure) => factory(ctx).pipeline_failed(
                                failure.reason.clone(),
                                duration,
                                Some(metrics),
                                failure.cause.clone(),
                            ),
                            ExecutionOutcome::NotStarted => factory(ctx).pipeline_not_started(),
                        };
                    ctx.progress.selected_terminal = Some((event, outcome));
                    change(S::PublishingTerminal, vec![A::PublishTerminal])
                }
            }
            S::FinalisingMetrics => {
                if ctx.resources.failure.get().is_some()
                    || ctx.progress.journal_failed
                    || ctx.termination.published.get().is_none()
                {
                    finish(ctx)
                } else {
                    change(S::PublishingFinalMarker, vec![A::PublishFinalMarker])
                }
            }
            S::PublishingFinalMarker => finish(ctx),
            _ => change(state.clone(), vec![]),
        };
    decided(transition)
}

fn announce_readiness(ctx: &mut C, actions: &mut Vec<A>) {
    if !ctx.progress.ready_announced
        && !ctx.stage_supervisors.is_empty()
        && ctx
            .stage_supervisors
            .keys()
            .all(|id| ctx.running_stages.contains(id))
    {
        ctx.progress.ready_announced = true;
        actions.push(publish(
            factory(ctx).pipeline_ready_for_run(Some(ctx.topology.num_stages())),
        ));
    }
}

pub(super) fn journal<'a>(state: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    let E::Journal(envelope) = event else {
        unreachable!()
    };
    let row = &envelope.event;
    ctx.last_system_event_id_seen = Some(row.id);
    if matches!(ctx.resources.producer_tail, ProducerTail::Through(id) if id == row.id) {
        ctx.resources.producer_tail = ProducerTail::Reached;
    }
    let mut next = state.clone();
    let mut actions = Vec::new();
    match &row.event {
        SystemEventType::StageLifecycle { stage_id, event } => {
            // Unrelated journal writers cannot satisfy this topology's barriers.
            if ctx
                .topology
                .stages()
                .any(|stage| stage.id == stage_id.to_topology_id())
            {
                match event {
                    StageLifecycleEvent::Running => {
                        ctx.running_stages.insert(*stage_id);
                    }
                    StageLifecycleEvent::Completed { metrics } => {
                        if let Some(metrics) = metrics {
                            ctx.stage_lifecycle_metrics
                                .insert(*stage_id, metrics.clone());
                        }
                        record_stage_completion(
                            &mut ctx.completed_stages,
                            *stage_id,
                            ctx.topology.num_stages(),
                        );
                    }
                    StageLifecycleEvent::Draining { metrics } => {
                        if let Some(metrics) = metrics {
                            ctx.stage_lifecycle_metrics
                                .insert(*stage_id, metrics.clone());
                        }
                    }
                    StageLifecycleEvent::Failed { error, metrics, .. } => {
                        if let Some(metrics) = metrics {
                            ctx.stage_lifecycle_metrics
                                .insert(*stage_id, metrics.clone());
                        }
                        return decided(fail(
                            state,
                            ctx,
                            format!("Stage '{stage_id}' failed: {error}"),
                        ));
                    }
                    StageLifecycleEvent::Cancelled { reason, metrics } => {
                        if let Some(metrics) = metrics {
                            ctx.stage_lifecycle_metrics
                                .insert(*stage_id, metrics.clone());
                        }
                        if !ctx.progress.stages_cancelled && !ctx.stop_intent.requested {
                            return decided(fail(
                                state,
                                ctx,
                                format!("Stage '{stage_id}' cancelled: {reason}"),
                            ));
                        }
                    }
                    StageLifecycleEvent::Drained => {}
                }
            }
        }
        SystemEventType::ContractStatus {
            upstream,
            reader,
            selected_event_type,
            feed_role,
            pass,
            reader_seq,
            advertised_writer_seq,
            reason,
        } => {
            let status = if *pass {
                ContractEdgeStatus::passed(*reader_seq, *advertised_writer_seq)
            } else {
                ContractEdgeStatus::failed(reason.clone(), *reader_seq, *advertised_writer_seq)
            };
            for key in ctx.contract_keys_for_contract_event(
                *upstream,
                *reader,
                selected_event_type.as_ref().map(|kind| kind.as_str()),
                feed_role.as_ref().map(|role| role.as_str()),
            ) {
                ctx.contract_pairs.insert(key, status.clone());
            }
            let is_source = ctx.expected_sources.contains(upstream);
            let gating =
                !is_source || matches!(ctx.source_contract_strict, SourceContractStrictMode::Abort);
            if !status.is_passed() && gating {
                let cause = reason
                    .clone()
                    .unwrap_or_else(|| ViolationCause::Other("contract_failed".into()));
                ctx.termination
                    .fail(format!("{cause:?}"), Some(cause.clone()));
                let first_abort = ctx.progress.abort_cause.is_none();
                ctx.progress
                    .abort_cause
                    .get_or_insert((cause, Some(*upstream)));
                if !settling(state) {
                    next = S::SettlingStages;
                }
                if first_abort {
                    actions.push(A::CancelStages {
                        contract_abort: true,
                    });
                }
                if matches!(next, S::SettlingStages) {
                    actions.push(A::ObserveStages);
                }
                return decided(change(next, actions));
            }
            // Mid-flight passes are not source completion evidence.
            if is_source && advertised_writer_seq.is_some() {
                ctx.contract_status.insert(*upstream, true);
            }
            if matches!(state, S::Running)
                && !ctx.expected_sources.is_empty()
                && ctx
                    .expected_sources
                    .iter()
                    .all(|id| ctx.contract_status.get(id) == Some(&true))
            {
                next = S::SourceCompleted;
                actions.push(publish(factory(ctx).pipeline_draining()));
            }
        }
        SystemEventType::MetricsCoordination(event) => {
            let own_metrics = ctx.resources.metrics.writer_id() == Some(row.writer_id);
            if own_metrics {
                match event {
                    MetricsCoordinationEvent::Ready => ctx.progress.metrics_ready = true,
                    MetricsCoordinationEvent::Drained => ctx.progress.metrics_drained = true,
                    _ => {}
                }
            }
        }
        SystemEventType::PipelineLifecycle(event) if row.writer_id == ctx.system_id.into() => {
            match event {
                PipelineLifecycleEvent::ReadyForRun { .. }
                    if matches!(state, S::AwaitingStageReadiness)
                        && ctx.progress.ready_announced =>
                {
                    next = S::ReadyForRun
                }
                PipelineLifecycleEvent::Running { .. }
                    if matches!(state, S::StartingSources | S::Draining)
                        && ctx.flow_start_time.is_some()
                        && !ctx.progress.sources_authorised
                        && !ctx.progress.stages_cancelled =>
                {
                    ctx.progress.sources_authorised = true;
                    actions.push(A::StartSources);
                }
                PipelineLifecycleEvent::StopAdmitted {
                    admission: PipelineStopAdmission::Graceful { .. },
                } if !settling(state)
                    && !ctx.progress.stages_cancelled
                    && matches!(ctx.stop_intent.mode, Some(FlowStopMode::Graceful { .. })) =>
                {
                    actions.extend([A::StopSources, publish(factory(ctx).pipeline_draining())]);
                }
                PipelineLifecycleEvent::Draining { .. } if matches!(state, S::SourceCompleted) => {
                    next = S::Draining
                }
                PipelineLifecycleEvent::AllStagesCompleted { .. }
                    if !settling(state)
                        && ctx.completed_stages.len() == ctx.topology.num_stages() =>
                {
                    next = S::SettlingStages;
                    actions.extend([A::ObserveStages, A::DrainMetrics]);
                }
                PipelineLifecycleEvent::Completed { .. }
                | PipelineLifecycleEvent::Cancelled { .. }
                | PipelineLifecycleEvent::Failed { .. }
                | PipelineLifecycleEvent::NotStarted
                    if matches!(state, S::PublishingTerminal)
                        && ctx
                            .progress
                            .selected_terminal
                            .as_ref()
                            .is_some_and(|(selected, _)| selected.id == row.id) =>
                {
                    next = S::FinalisingMetrics;
                    actions.push(A::ObserveMetrics);
                }
                PipelineLifecycleEvent::Drained
                    if matches!(state, S::PublishingFinalMarker)
                        && ctx.progress.final_marker == Some(row.id) =>
                {
                    ctx.progress.final_marker_seen = true
                }
                _ => {}
            }
        }
        _ => {}
    }
    if matches!(next, S::AwaitingStageReadiness) {
        announce_readiness(ctx, &mut actions);
    }
    if matches!(next, S::StartingSources)
        && ctx.progress.sources_authorised
        && ctx
            .expected_sources
            .iter()
            .all(|id| ctx.running_stages.contains(id))
    {
        next = S::Running;
    }
    if !settling(&next)
        && !ctx.progress.all_stages_announced
        && ctx.completed_stages.len() == ctx.topology.num_stages()
    {
        // Each stage owns its EOF/quiescence/replay completion protocol.
        // Preserve the existing boundary derived from their Completed facts;
        // feedback and bounded replay do not necessarily emit a final status
        // for every logical feed. Producer catch-up still folds all committed
        // contract failures before the terminal outcome is selected.
        ctx.progress.all_stages_announced = true;
        actions.push(publish(factory(ctx).pipeline_all_stages_completed()));
    }
    decided(change(next, actions))
}

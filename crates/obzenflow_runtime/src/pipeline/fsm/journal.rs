// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Each live phase consumes original system-journal envelopes. Shared folding
//! retains evidence; phase handlers below name the lifecycle decisions it permits.

use super::context::{record_stage_completion, ContractEdgeStatus};
use super::transitions::{
    change, factory, fail_catchup, fail_finalisation, fail_stages, publish, Change, Decision,
    FailureDecision,
};
use super::{
    PipelineAction as A, PipelineContext as C, PipelineFsmEvent as E, PipelineFsmState as S,
};
use crate::id_conversions::StageIdExt;
use crate::pipeline::config::SourceContractStrictMode;
use crate::pipeline::resources::ProducerTail;
use crate::pipeline::FlowStopMode;
use obzenflow_core::event::types::ViolationCause;
use obzenflow_core::event::{
    MetricsCoordinationEvent, PipelineLifecycleEvent, PipelineStopAdmission, StageLifecycleEvent,
    SystemEvent, SystemEventType,
};

// Failure destinations are supplied explicitly by each phase. This fold does
// not select readiness, startup, completion, or finalisation transitions.
fn observe<'a>(
    event: &'a E,
    ctx: &mut C,
    fail: FailureDecision,
    contract_state: S,
) -> Result<&'a SystemEvent, Change> {
    let E::Journal(envelope) = event else {
        unreachable!("journal handler input");
    };
    let row = &envelope.event;
    ctx.last_system_event_id_seen = Some(row.id);
    if matches!(ctx.resources.producer_tail, ProducerTail::Through(id) if id == row.id) {
        ctx.resources.producer_tail = ProducerTail::Reached;
    }
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
                        return Err(fail(ctx, format!("Stage '{stage_id}' failed: {error}")));
                    }
                    StageLifecycleEvent::Cancelled { reason, metrics } => {
                        if let Some(metrics) = metrics {
                            ctx.stage_lifecycle_metrics
                                .insert(*stage_id, metrics.clone());
                        }
                        if !ctx.progress.stages_cancelled && !ctx.stop_intent.requested {
                            return Err(fail(
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
            let keys = ctx.contract_keys_for_contract_event(
                *upstream,
                *reader,
                selected_event_type.as_ref().map(|kind| kind.as_str()),
                feed_role.as_ref().map(|role| role.as_str()),
            );
            if keys.is_empty() {
                return Ok(row);
            }
            for key in keys {
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
                let mut actions = Vec::new();
                if first_abort {
                    actions.push(A::CancelStages {
                        contract_abort: true,
                    });
                }
                if matches!(contract_state, S::SettlingStages) {
                    actions.push(A::ObserveStages);
                }
                return Err(change(contract_state, actions));
            }
            // Mid-flight passes are not source completion evidence.
            if is_source && advertised_writer_seq.is_some() {
                ctx.contract_status.insert(*upstream, true);
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
        _ => {}
    }
    Ok(row)
}

pub(super) fn announce_readiness(ctx: &mut C, actions: &mut Vec<A>) {
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

fn all_stages_completed(ctx: &C) -> bool {
    ctx.topology.num_stages() > 0 && ctx.completed_stages.len() == ctx.topology.num_stages()
}

fn own_pipeline<'a>(row: &'a SystemEvent, ctx: &C) -> Option<&'a PipelineLifecycleEvent> {
    match &row.event {
        SystemEventType::PipelineLifecycle(event) if row.writer_id == ctx.system_id.into() => {
            Some(event)
        }
        _ => None,
    }
}

// Used only by phases after bootstrap and before settlement. Stage-owned
// EOF/quiescence/replay completion does not require all logical feed statuses.
fn completion_boundary(state: S, row: &SystemEvent, ctx: &mut C, mut actions: Vec<A>) -> Change {
    if matches!(
        own_pipeline(row, ctx),
        Some(PipelineLifecycleEvent::AllStagesCompleted { .. })
    ) && all_stages_completed(ctx)
    {
        actions.extend([A::ObserveStages, A::DrainMetrics]);
        return change(S::SettlingStages, actions);
    }
    if !ctx.progress.all_stages_announced && all_stages_completed(ctx) {
        ctx.progress.all_stages_announced = true;
        actions.push(publish(factory(ctx).pipeline_all_stages_completed()));
    }
    change(state, actions)
}

fn authorise_sources(row: &SystemEvent, ctx: &mut C, actions: &mut Vec<A>) {
    if matches!(
        own_pipeline(row, ctx),
        Some(PipelineLifecycleEvent::Running { .. })
    ) && ctx.flow_start_time.is_some()
        && !ctx.progress.sources_authorised
        && !ctx.progress.stages_cancelled
    {
        ctx.progress.sources_authorised = true;
        actions.push(A::StartSources);
    }
}

pub(super) fn created<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        match observe(event, ctx, fail_stages, S::SettlingStages) {
            Ok(_) => Ok(change(S::Created, vec![])),
            Err(failure) => Ok(failure),
        }
    })
}

pub(super) fn materializing<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let row = match observe(event, ctx, fail_stages, S::SettlingStages) {
            Ok(row) => row,
            Err(failure) => return Ok(failure),
        };
        Ok(completion_boundary(S::Materializing, row, ctx, vec![]))
    })
}

pub(super) fn awaiting_readiness<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let row = match observe(event, ctx, fail_stages, S::SettlingStages) {
            Ok(row) => row,
            Err(failure) => return Ok(failure),
        };
        let mut actions = vec![];
        let next = if matches!(
            own_pipeline(row, ctx),
            Some(PipelineLifecycleEvent::ReadyForRun { .. })
        ) && ctx.progress.ready_announced
        {
            S::ReadyForRun
        } else {
            announce_readiness(ctx, &mut actions);
            S::AwaitingStageReadiness
        };
        Ok(completion_boundary(next, row, ctx, actions))
    })
}

pub(super) fn ready_for_run<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let row = match observe(event, ctx, fail_stages, S::SettlingStages) {
            Ok(row) => row,
            Err(failure) => return Ok(failure),
        };
        Ok(completion_boundary(S::ReadyForRun, row, ctx, vec![]))
    })
}

pub(super) fn starting_sources<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let row = match observe(event, ctx, fail_stages, S::SettlingStages) {
            Ok(row) => row,
            Err(failure) => return Ok(failure),
        };
        let mut actions = vec![];
        authorise_sources(row, ctx, &mut actions);
        let next = if ctx.progress.sources_authorised
            && ctx
                .expected_sources
                .iter()
                .all(|id| ctx.running_stages.contains(id))
        {
            S::Running
        } else {
            S::StartingSources
        };
        Ok(completion_boundary(next, row, ctx, actions))
    })
}

pub(super) fn running<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let row = match observe(event, ctx, fail_stages, S::SettlingStages) {
            Ok(row) => row,
            Err(failure) => return Ok(failure),
        };
        let mut actions = vec![];
        let next = if matches!(row.event, SystemEventType::ContractStatus { .. })
            && !ctx.expected_sources.is_empty()
            && ctx
                .expected_sources
                .iter()
                .all(|id| ctx.contract_status.get(id) == Some(&true))
        {
            actions.push(publish(factory(ctx).pipeline_draining()));
            S::SourceCompleted
        } else {
            S::Running
        };
        Ok(completion_boundary(next, row, ctx, actions))
    })
}

pub(super) fn source_completed<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let row = match observe(event, ctx, fail_stages, S::SettlingStages) {
            Ok(row) => row,
            Err(failure) => return Ok(failure),
        };
        let next = if matches!(
            own_pipeline(row, ctx),
            Some(PipelineLifecycleEvent::Draining { .. })
        ) {
            S::Draining
        } else {
            S::SourceCompleted
        };
        Ok(completion_boundary(next, row, ctx, vec![]))
    })
}

pub(super) fn draining<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let row = match observe(event, ctx, fail_stages, S::SettlingStages) {
            Ok(row) => row,
            Err(failure) => return Ok(failure),
        };
        let mut actions = vec![];
        // Graceful stop may have changed phase before the earlier Running row
        // was consumed. Preserve its already-authorised source-control order.
        authorise_sources(row, ctx, &mut actions);
        if matches!(
            own_pipeline(row, ctx),
            Some(PipelineLifecycleEvent::StopAdmitted {
                admission: PipelineStopAdmission::Graceful { .. },
            })
        ) && !ctx.progress.stages_cancelled
            && matches!(ctx.stop_intent.mode, Some(FlowStopMode::Graceful { .. }))
        {
            actions.extend([A::StopSources, publish(factory(ctx).pipeline_draining())]);
        }
        Ok(completion_boundary(S::Draining, row, ctx, actions))
    })
}

pub(super) fn settling_stages<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        Ok(match observe(event, ctx, fail_stages, S::SettlingStages) {
            Ok(_) => change(S::SettlingStages, vec![]),
            Err(failure) => failure,
        })
    })
}

pub(super) fn catching_up_producers<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        Ok(
            match observe(event, ctx, fail_catchup, S::CatchingUpProducers) {
                Ok(_) => change(S::CatchingUpProducers, vec![]),
                Err(failure) => failure,
            },
        )
    })
}

pub(super) fn publishing_terminal<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let row = match observe(event, ctx, fail_finalisation, S::PublishingTerminal) {
            Ok(row) => row,
            Err(failure) => return Ok(failure),
        };
        let selected = ctx
            .progress
            .selected_terminal
            .as_ref()
            .is_some_and(|(event, _)| event.id == row.id);
        Ok(
            if selected
                && matches!(
                    own_pipeline(row, ctx),
                    Some(
                        PipelineLifecycleEvent::Completed { .. }
                            | PipelineLifecycleEvent::Cancelled { .. }
                            | PipelineLifecycleEvent::Failed { .. }
                            | PipelineLifecycleEvent::NotStarted
                    )
                )
            {
                change(S::FinalisingMetrics, vec![A::ObserveMetrics])
            } else {
                change(S::PublishingTerminal, vec![])
            },
        )
    })
}

pub(super) fn finalising_metrics<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        Ok(
            match observe(event, ctx, fail_finalisation, S::FinalisingMetrics) {
                Ok(_) => change(S::FinalisingMetrics, vec![]),
                Err(failure) => failure,
            },
        )
    })
}

pub(super) fn publishing_final_marker<'a>(_: &'a S, event: &'a E, ctx: &'a mut C) -> Decision<'a> {
    Box::pin(async move {
        let row = match observe(event, ctx, fail_finalisation, S::PublishingFinalMarker) {
            Ok(row) => row,
            Err(failure) => return Ok(failure),
        };
        if ctx.progress.final_marker == Some(row.id)
            && matches!(
                own_pipeline(row, ctx),
                Some(PipelineLifecycleEvent::Drained)
            )
        {
            ctx.progress.final_marker_seen = true;
        }
        Ok(change(S::PublishingFinalMarker, vec![]))
    })
}

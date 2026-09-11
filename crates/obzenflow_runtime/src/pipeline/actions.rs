// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Prompt handoffs authorised by the pipeline FSM.

use super::fsm::{PipelineAction, PipelineContext};
use super::resources::{ProducerTail, StageCommand};
use crate::supervised_base::publication::BoxError;
use crate::supervised_base::SupervisorHandle;
use futures::{stream::FuturesUnordered, FutureExt};
use obzenflow_core::event::{
    MetricsCoordinationEvent, SystemEvent, SystemEventFactory, SystemEventType,
};
use obzenflow_fsm::{FsmAction, FsmError};
use std::sync::Mutex;

fn publish(ctx: &mut PipelineContext, event: SystemEvent, control: bool) -> Result<(), BoxError> {
    let journal = ctx.system_journal.clone();
    let append = async move {
        journal.append(event, None).await?;
        Ok(())
    };
    let receipt = if control {
        ctx.resources.publications.enqueue_control(append)
    } else {
        ctx.resources.publications.enqueue(append)
    }?;
    drop(receipt);
    ctx.resources.refresh_publications();
    Ok(())
}

#[async_trait::async_trait]
impl FsmAction for PipelineAction {
    type Context = PipelineContext;

    async fn execute(&self, ctx: &mut PipelineContext) -> Result<(), FsmError> {
        // No await belongs in this path: the shared runner awaits actions with
        // borrowed context and must regain dispatch after each handoff.
        if let Err(error) = self.handoff(ctx) {
            let message = error.to_string();
            ctx.resources.retain_failure(error);
            return Err(FsmError::HandlerError(message));
        }
        Ok(())
    }
}

impl PipelineAction {
    fn handoff(&self, ctx: &mut PipelineContext) -> Result<(), BoxError> {
        match self {
            Self::InitialiseStages
            | Self::StartNonSources
            | Self::StartSources
            | Self::StopSources => {
                let (mut handles, commands): (Vec<_>, &[_]) = match self {
                    Self::InitialiseStages => (
                        ctx.stage_supervisors
                            .values()
                            .chain(ctx.source_supervisors.values())
                            .cloned()
                            .collect(),
                        &[StageCommand::Initialize],
                    ),
                    Self::StartNonSources => (
                        ctx.stage_supervisors.values().cloned().collect(),
                        &[StageCommand::Start],
                    ),
                    Self::StartSources => (
                        ctx.source_supervisors.values().cloned().collect(),
                        &[StageCommand::Ready, StageCommand::Start],
                    ),
                    Self::StopSources => (
                        ctx.source_supervisors.values().cloned().collect(),
                        &[StageCommand::Drain],
                    ),
                    _ => unreachable!(),
                };
                handles.sort_by_key(|handle| handle.stage_id());
                ctx.resources
                    .delivery
                    .enqueue(handles, commands, ctx.topology.num_stages())?;
            }
            Self::StartMetricsAggregator => {
                if let Some(prepared) = ctx.resources.prepared_metrics.take() {
                    ctx.resources.metrics.start(prepared)?;
                }
            }
            Self::Publish { event, control } => publish(ctx, event.as_ref().clone(), *control)?,
            Self::CancelStages { contract_abort } => {
                ctx.resources.delivery.cancel();
                ctx.progress.stages_cancelled = true;
                ctx.progress.cleanup_deadline = None;
                // A stage writer must accept its abort row before request_abort
                // closes admission. Failure cannot skip this or any sibling.
                let abort = if *contract_abort {
                    ctx.progress.abort_cause.as_ref().map(|(reason, upstream)| {
                        obzenflow_core::event::ChainEventFactory::pipeline_abort_event(
                            ctx.system_id.into(),
                            reason.clone(),
                            *upstream,
                        )
                    })
                } else {
                    None
                };
                let mut failure = None;
                for handle in ctx
                    .stage_supervisors
                    .values()
                    .chain(ctx.source_supervisors.values())
                {
                    if let Some(event) = &abort {
                        if let Some((_, journal)) = ctx
                            .stage_data_journals
                            .iter()
                            .find(|(id, _)| *id == handle.stage_id())
                        {
                            if let Err(error) =
                                handle.publish_pipeline_control(journal.clone(), event.clone())
                            {
                                failure.get_or_insert(Box::new(error) as BoxError);
                            }
                        }
                    }
                    handle.request_abort();
                }
                if let Some(error) = failure {
                    return Err(error);
                }
            }
            Self::ObserveStages => {
                if ctx.resources.stage_joins.is_none() && !ctx.resources.stages_joined {
                    let joins = FuturesUnordered::new();
                    for handle in ctx
                        .stage_supervisors
                        .values()
                        .chain(ctx.source_supervisors.values())
                    {
                        let handle = handle.clone();
                        joins.push(async move { handle.wait_for_completion().await }.boxed());
                    }
                    ctx.resources.stage_joins = Some(Mutex::new(joins));
                    if !ctx.progress.stages_cancelled {
                        ctx.progress.cleanup_deadline.get_or_insert_with(|| {
                            std::time::Instant::now() + super::fsm::stop_drain_timeout()
                        });
                    }
                }
            }
            Self::CaptureProducerTail => {
                let journal = ctx.system_journal.clone();
                ctx.resources.producer_tail = ProducerTail::Reading(Mutex::new(
                    async move {
                        Ok(journal
                            .read_last_n(1)
                            .await?
                            .first()
                            .map(|row| row.event.id))
                    }
                    .boxed(),
                ));
            }
            Self::PublishTerminal => {
                let (event, outcome) = ctx.progress.selected_terminal.clone().ok_or_else(|| {
                    std::io::Error::other("terminal publication without FSM selection")
                })?;
                let id = event.id;
                let published = ctx.termination.published.clone();
                let acknowledged_at = ctx.resources.terminal_ack.clone();
                let journal = ctx.system_journal.clone();
                drop(ctx.resources.publications.enqueue(async move {
                    journal.append(event, None).await?;
                    let at = std::time::Instant::now();
                    published
                        .set(super::termination::PublishedTermination {
                            outcome,
                            event_id: Some(id),
                        })
                        .map_err(|_| std::io::Error::other("terminal outcome already published"))?;
                    acknowledged_at.set(at).map_err(|_| {
                        std::io::Error::other("terminal acknowledgement already retained")
                    })?;
                    Ok(())
                })?);
                ctx.resources.refresh_publications();
            }
            Self::ObserveMetrics => {
                if ctx.resources.metrics_join.is_none() && !ctx.resources.metrics_joined {
                    match ctx.resources.metrics.handle() {
                        Some(handle) => {
                            ctx.resources.metrics_join = Some(Mutex::new(
                                async move { handle.wait_for_completion().await }.boxed(),
                            ))
                        }
                        None => ctx.resources.metrics_joined = true,
                    }
                }
            }
            Self::CancelMetrics => {
                ctx.progress.metrics_cancelled = true;
                ctx.resources.metrics.request_abort();
            }
            Self::PublishFinalMarker => {
                let event = SystemEventFactory::new(ctx.system_id).pipeline_drained();
                ctx.progress.final_marker = Some(event.id);
                publish(ctx, event, false)?;
            }
            Self::DrainMetrics => {
                if ctx.resources.metrics.handle().is_some() && !ctx.progress.metrics_drain_requested
                {
                    publish(
                        ctx,
                        SystemEvent::new(
                            ctx.system_id.into(),
                            SystemEventType::MetricsCoordination(
                                MetricsCoordinationEvent::DrainRequested,
                            ),
                        ),
                        false,
                    )?;
                    ctx.progress.metrics_drain_requested = true;
                }
            }
        }
        Ok(())
    }
}

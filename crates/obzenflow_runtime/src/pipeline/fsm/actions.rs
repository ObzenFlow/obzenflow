// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Operations selected on transition edges. Every publication result returns
//! to the FSM; child observations never read a journal.

use super::{PipelineContext, PipelineFsmEvent as E, PublicationStep};
use crate::pipeline::resources::StageCommand;
use crate::pipeline::termination::{ExecutionOutcome, PublishedTermination};
use crate::stages::common::stage_handle::{StageFailure, StageMilestone};
use crate::supervised_base::handler_supervised::SupervisorAction;
use crate::supervised_base::publication::{self, BoxError};
use crate::supervised_base::SupervisorHandle;
use futures::FutureExt;
use obzenflow_core::event::{SystemEvent, SystemEventFactory};
use obzenflow_fsm::{FsmAction, FsmError};
use std::sync::Mutex;

#[derive(Clone, Debug)]
pub(crate) enum PipelineAction {
    Host(SupervisorAction<E>),
    Register,
    ObserveChildren,
    InitialiseStages,
    StartConsumers,
    StartSources,
    StopSources,
    CancelChildren,
    AbortRemainingChildren,
    FinishChildren,
    Publish {
        event: Box<SystemEvent>,
        step: PublicationStep,
    },
    PublishTerminal {
        event: Box<SystemEvent>,
        outcome: ExecutionOutcome,
    },
    FinaliseMetrics,
    CancelMetrics,
    PublishFinalMarker,
}

fn publish(
    ctx: &mut PipelineContext,
    event: SystemEvent,
    step: PublicationStep,
    outcome: Option<ExecutionOutcome>,
) -> Result<(), BoxError> {
    let journal = ctx.system_journal.clone();
    let published = ctx.termination.published.clone();
    let append = async move {
        let id = event.id;
        publication::append_inline(&journal, event, Default::default()).await?;
        if let Some(outcome) = outcome {
            published
                .set(PublishedTermination {
                    outcome,
                    event_id: Some(id),
                })
                .map_err(|_| std::io::Error::other("terminal outcome already published"))?;
        }
        Ok(())
    };
    let receipt = if matches!(step, PublicationStep::Stop) {
        ctx.resources.publications.enqueue_control(append)?
    } else {
        ctx.resources.publications.enqueue(append)?
    };
    ctx.resources
        .publication_results
        .get_mut()
        .unwrap_or_else(|e| e.into_inner())
        .push(
            async move {
                let result = async {
                    receipt.await?;
                    Ok::<(), BoxError>(())
                }
                .await;
                match result {
                    Ok(()) => match step {
                        PublicationStep::Ready => E::ReadyPublished,
                        PublicationStep::Start => E::StartPublished,
                        PublicationStep::Running => E::RunningPublished,
                        PublicationStep::Terminal => E::TerminalPublished,
                        PublicationStep::Stop => E::ObservationEnded,
                    },
                    Err(error) => E::OperationalFailure {
                        message: error.to_string(),
                    },
                }
            }
            .boxed(),
        );
    Ok(())
}

#[async_trait::async_trait]
impl FsmAction for PipelineAction {
    type Context = PipelineContext;
    async fn execute(&self, ctx: &mut PipelineContext) -> Result<(), FsmError> {
        match self.handoff(ctx) {
            Ok(()) => Ok(()),
            Err(error) => {
                ctx.resources
                    .publications
                    .relinquish_cancelled_admission(error.as_ref())
                    .await;
                let message = error.to_string();
                ctx.resources.retain_failure(error);
                Err(FsmError::HandlerError(message))
            }
        }
    }
}

impl PipelineAction {
    fn handoff(&self, ctx: &mut PipelineContext) -> Result<(), BoxError> {
        match self {
            Self::Host(_) | Self::Register => {
                return Err(
                    std::io::Error::other("host action requires the supervised runner").into(),
                )
            }
            Self::ObserveChildren => {
                for handle in ctx
                    .stage_supervisors
                    .values()
                    .chain(ctx.source_supervisors.values())
                {
                    ctx.outstanding_children.insert(handle.stage_id());
                    let child = handle.clone();
                    ctx.resources
                        .failures
                        .get_mut()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(
                            async move {
                                match child.wait_for_failure().await {
                                    Some(failure) => E::ChildFailed(failure),
                                    None => E::ObservationEnded,
                                }
                            }
                            .boxed(),
                        );
                    let child = handle.clone();
                    ctx.resources
                        .exits
                        .get_mut()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(
                            async move { E::ChildExited(child.wait_for_completion().await) }
                                .boxed(),
                        );
                }
            }
            Self::InitialiseStages
            | Self::StartConsumers
            | Self::StartSources
            | Self::StopSources
            | Self::CancelChildren => {
                let (mut handles, commands, milestone): (Vec<_>, &[_], _) = match self {
                    Self::InitialiseStages => (
                        ctx.stage_supervisors
                            .values()
                            .chain(ctx.source_supervisors.values())
                            .cloned()
                            .collect(),
                        &[StageCommand::Initialize],
                        Some(StageMilestone::Initialized),
                    ),
                    Self::StartConsumers => (
                        ctx.stage_supervisors.values().cloned().collect(),
                        &[StageCommand::Ready],
                        Some(StageMilestone::Started),
                    ),
                    Self::StartSources => (
                        ctx.source_supervisors.values().cloned().collect(),
                        &[StageCommand::Ready, StageCommand::Start],
                        Some(StageMilestone::Started),
                    ),
                    Self::StopSources => (
                        ctx.source_supervisors
                            .values()
                            .filter(|h| ctx.outstanding_children.contains(&h.stage_id()))
                            .cloned()
                            .collect(),
                        &[StageCommand::Drain],
                        None,
                    ),
                    Self::CancelChildren => {
                        ctx.resources.delivery.cancel();
                        ctx.resources
                            .acknowledgements
                            .get_mut()
                            .unwrap_or_else(|e| e.into_inner())
                            .clear();
                        ctx.outstanding_milestones.clear();
                        (
                            ctx.stage_supervisors
                                .values()
                                .chain(ctx.source_supervisors.values())
                                .filter(|h| ctx.outstanding_children.contains(&h.stage_id()))
                                .cloned()
                                .collect(),
                            &[StageCommand::Cancel],
                            None,
                        )
                    }
                    _ => unreachable!(),
                };
                handles.sort_by_key(|h| h.stage_id());
                if let Some(milestone) = milestone {
                    ctx.outstanding_milestones.clear();
                    for child in &handles {
                        ctx.outstanding_milestones.insert(child.stage_id().into());
                        let child = child.clone();
                        ctx.resources
                            .acknowledgements
                            .get_mut()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(
                                async move {
                                    match child.wait_for_milestone(milestone).await {
                                        Ok(ack) => E::ChildAcknowledged(ack),
                                        Err(cause) => E::ChildFailed(StageFailure {
                                            stage_id: child.stage_id(),
                                            cause,
                                            snapshot: Default::default(),
                                        }),
                                    }
                                }
                                .boxed(),
                            );
                    }
                }
                ctx.resources
                    .delivery
                    .enqueue(handles, commands, ctx.topology.num_stages())?;
                if matches!(self, Self::StartConsumers) {
                    if let Some(prepared) = ctx.resources.prepared_metrics.take() {
                        ctx.resources.metrics.start(prepared)?;
                    }
                    if let Some(metrics) = ctx.resources.metrics.handle() {
                        let writer = ctx.resources.metrics.writer_id().ok_or_else(|| {
                            std::io::Error::other("metrics child has no identity")
                        })?;
                        ctx.outstanding_milestones.insert(writer);
                        let ready = metrics.clone();
                        ctx.resources
                            .acknowledgements
                            .get_mut()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(
                                async move {
                                    match ready.wait_for_milestone(StageMilestone::Started).await {
                                        Ok(ack) => E::MetricsReady(ack),
                                        Err(error) => E::OperationalFailure {
                                            message: error.to_string(),
                                        },
                                    }
                                }
                                .boxed(),
                            );
                        ctx.resources
                            .failures
                            .get_mut()
                            .unwrap_or_else(|e| e.into_inner())
                            .push(
                                async move {
                                    match metrics.wait_for_failure().await {
                                        Some(failure) => E::OperationalFailure {
                                            message: failure.cause.to_string(),
                                        },
                                        None => E::ObservationEnded,
                                    }
                                }
                                .boxed(),
                            );
                    }
                }
            }
            Self::FinishChildren => {
                ctx.resources.delivery.cancel();
                ctx.resources
                    .acknowledgements
                    .get_mut()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
                ctx.outstanding_milestones.clear();
            }
            Self::AbortRemainingChildren => {
                ctx.resources.delivery.cancel();
                for handle in ctx
                    .stage_supervisors
                    .values()
                    .chain(ctx.source_supervisors.values())
                {
                    if ctx.outstanding_children.contains(&handle.stage_id()) {
                        handle.request_abort();
                    }
                }
            }
            Self::Publish { event, step } => publish(ctx, event.as_ref().clone(), *step, None)?,
            Self::PublishTerminal { event, outcome } => publish(
                ctx,
                event.as_ref().clone(),
                PublicationStep::Terminal,
                Some(outcome.clone()),
            )?,
            Self::FinaliseMetrics => {
                ctx.metrics_deadline = Some(
                    tokio::time::Instant::now()
                        + std::time::Duration::from_millis(ctx.metrics_drain_timeout_ms),
                );
                if let Some(metrics) = ctx.resources.metrics.handle() {
                    let journal = ctx.system_journal.clone();
                    let event = SystemEventFactory::new(ctx.system_id).metrics_drain_requested();
                    let requested = match ctx.resources.publications.enqueue(async move {
                        publication::append_inline(&journal, event, Default::default()).await?;
                        Ok(())
                    }) {
                        Ok(receipt) => Some(receipt),
                        Err(error)
                            if ctx
                                .resources
                                .publications
                                .is_cancelled_admission(error.as_ref()) =>
                        {
                            return Err(error);
                        }
                        Err(error) => {
                            ctx.resources.retain_failure(error);
                            None
                        }
                    };
                    ctx.resources.metrics_join = Some(Mutex::new(
                        async move {
                            // Mailbox acceptance is separate from owned task completion.
                            // The pipeline owns its request fact and commits it before
                            // delivering the command with that causal context.
                            let committed = match requested {
                                Some(receipt) => receipt.await.is_ok(),
                                None => false,
                            };
                            if committed {
                                let _ = metrics
                                    .send_event(
                                        crate::metrics::MetricsAggregatorEvent::StartDraining,
                                    )
                                    .await;
                            }
                            metrics.wait_for_stage_exit().await
                        }
                        .boxed(),
                    ));
                }
            }
            Self::CancelMetrics => ctx.resources.metrics.request_abort(),
            Self::PublishFinalMarker => {
                let scope = ctx.resources.publications.clone();
                let journal = ctx.system_journal.clone();
                let event = SystemEventFactory::new(ctx.system_id).pipeline_drained();
                let receipt = if scope.first_failure().is_none()
                    && ctx.termination.published.get().is_some()
                {
                    Some(scope.enqueue(async move {
                        publication::append_inline(&journal, event, Default::default()).await?;
                        Ok(())
                    })?)
                } else {
                    None
                };
                ctx.resources
                    .publication_results
                    .get_mut()
                    .unwrap_or_else(|e| e.into_inner())
                    .push(
                        async move {
                            let append = match receipt {
                                Some(receipt) => receipt.await,
                                None => Ok(()),
                            };
                            let settlement = scope.join().await;
                            E::FinalisationCompleted {
                                error: append
                                    .err()
                                    .map(|error| error.to_string())
                                    .or_else(|| settlement.err().map(|error| error.to_string())),
                            }
                        }
                        .boxed(),
                    );
            }
        }
        Ok(())
    }
}

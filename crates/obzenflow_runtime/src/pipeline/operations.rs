// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Owned I/O for the pipeline FSM. Preparing and applying completions are the
//! only places allowed to borrow its context.

use super::fsm::{PipelineAction, PipelineContext};
use crate::id_conversions::StageIdExt;
use crate::messaging::system_subscription::SystemSubscription;
use crate::metrics::{MetricsAggregatorBuilder, MetricsHandle, MetricsInputs};
use crate::supervised_base::publication::{BoxError, PublicationScope};
use crate::supervised_base::{SupervisorBuilder, SupervisorHandle};
use futures::future::BoxFuture;
use futures::FutureExt;
use obzenflow_core::event::{SystemEvent, SystemEventFactory};
use std::sync::Arc;

pub(super) struct Cancellation(tokio::sync::watch::Sender<bool>);

impl Cancellation {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self(tokio::sync::watch::channel(false).0))
    }
    pub(super) fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    pub(super) fn cancel(&self) {
        self.0.send_replace(true);
    }
    async fn cancelled(&self) {
        let mut receiver = self.0.subscribe();
        let _ = receiver.wait_for(|cancelled| *cancelled).await;
    }
}

/// The context owns cancellation; the pipeline publication scope retains the
/// separate metrics join even when this lease or its delivery is dropped.
pub(crate) struct MetricsLease(Arc<MetricsHandle>);

#[cfg(test)]
impl From<MetricsHandle> for MetricsLease {
    fn from(handle: MetricsHandle) -> Self {
        Self(Arc::new(handle))
    }
}

impl std::ops::Deref for MetricsLease {
    type Target = MetricsHandle;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for MetricsLease {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) type Operation = BoxFuture<'static, Result<Completion, BoxError>>;

pub(super) enum Completion {
    Done,
    Subscription(Box<SystemSubscription<SystemEvent>>),
    Metrics(MetricsLease),
}

impl Completion {
    pub(super) fn apply(self, context: &mut PipelineContext) {
        match self {
            Self::Done => {}
            Self::Subscription(subscription) => {
                context.completion_subscription = Some(*subscription)
            }
            Self::Metrics(handle) => context.metrics_handle = Some(handle),
        }
    }
}

pub(super) fn publish(
    context: &PipelineContext,
    writer: Arc<PublicationScope>,
    event: SystemEvent,
) -> Operation {
    let journal = context.system_journal.clone();
    let receipt = writer.enqueue(async move {
        journal.append(event, None).await?;
        Ok(())
    });
    async move {
        receipt?.await?;
        Ok(Completion::Done)
    }
    .boxed()
}

pub(super) fn prepare(
    action: PipelineAction,
    context: &mut PipelineContext,
    cancelled: Arc<Cancellation>,
    writer: Arc<PublicationScope>,
) -> Operation {
    let factory = SystemEventFactory::new(context.system_id);
    match action {
        PipelineAction::WritePipelineStopRequested { mode } => {
            use obzenflow_core::event::{PipelineCancellationCause, PipelineStopAdmission};
            let admission = match mode {
                super::fsm::FlowStopMode::Graceful { timeout } => PipelineStopAdmission::Graceful {
                    timeout_ms: obzenflow_core::event::types::DurationMs(
                        timeout.as_millis().min(u64::MAX as u128) as u64,
                    ),
                },
                super::fsm::FlowStopMode::Cancel => PipelineStopAdmission::Cancel {
                    cause: if context.stop_intent.reason.as_deref()
                        == Some(crate::stages::common::stage_handle::STOP_REASON_TIMEOUT)
                    {
                        PipelineCancellationCause::GracefulTimeout
                    } else {
                        PipelineCancellationCause::Requested
                    },
                },
            };
            publish(context, writer, factory.pipeline_stop_admitted(admission))
        }
        PipelineAction::WritePipelineReadyForRun => publish(
            context,
            writer,
            factory.pipeline_ready_for_run(Some(context.topology.num_stages())),
        ),
        PipelineAction::BeginDrain => publish(context, writer, factory.pipeline_draining()),
        PipelineAction::DrainMetrics => {
            if context.metrics_handle.is_none() {
                return async { Ok(Completion::Done) }.boxed();
            }
            publish(
                context,
                writer,
                SystemEvent::new(
                    context.system_id.into(),
                    obzenflow_core::event::SystemEventType::MetricsCoordination(
                        obzenflow_core::event::MetricsCoordinationEvent::DrainRequested,
                    ),
                ),
            )
        }
        PipelineAction::StartCompletionSubscription => {
            let journal = context.system_journal.clone();
            async move {
                Ok(Completion::Subscription(Box::new(SystemSubscription::new(
                    journal.reader().await?,
                    "pipeline_supervisor".into(),
                ))))
            }
            .boxed()
        }
        PipelineAction::StartMetricsAggregator => {
            if context.metrics_handle.is_some() {
                return async { Ok(Completion::Done) }.boxed();
            }
            let Some(exporter) = context.metrics_exporter.clone() else {
                return async { Ok(Completion::Done) }.boxed();
            };
            let inputs = MetricsInputs::new(
                context.stage_data_journals.clone(),
                context.stage_error_journals.clone(),
            )
            .with_backpressure_registry_opt(context.backpressure_registry.clone());
            let metadata = context
                .stage_supervisors
                .iter()
                .chain(context.source_supervisors.iter())
                .filter_map(|(id, handle)| {
                    context
                        .topology
                        .stages()
                        .find(|stage| stage.id == id.to_topology_id())
                        .map(|stage| {
                            (
                                *id,
                                obzenflow_core::metrics::StageMetadata {
                                    name: stage.name.clone(),
                                    stage_type: handle.stage_type(),
                                    reference_mode: None,
                                    flow_name: context.flow_name.clone(),
                                    flow_id: Some(context.flow_id),
                                },
                            )
                        })
                })
                .collect();
            let builder =
                MetricsAggregatorBuilder::new(inputs, context.system_journal.clone(), exporter)
                    .with_pipeline_writer(context.system_id.into())
                    .with_stage_metadata(metadata)
                    .with_composite_boundaries(super::fsm::composite_boundaries_from_topology(
                        &context.topology,
                    ))
                    .with_export_interval(1);
            let scope = PublicationScope::current().unwrap_or_else(PublicationScope::concurrent);
            let (sender, receiver) = tokio::sync::oneshot::channel();
            let registration = scope.enqueue(async move {
                match builder.build().await {
                    Ok(handle) => {
                        let handle = Arc::new(handle);
                        // Failed delivery drops the lease and requests cancellation.
                        let _ = sender.send(Ok(MetricsLease(handle.clone())));
                        match handle.wait_for_completion().await {
                            Ok(()) => Ok(()),
                            Err(crate::supervised_base::HandleError::SupervisorAborted) => Ok(()),
                            Err(error) => Err(Box::new(error) as BoxError),
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(Err(Box::new(error) as BoxError));
                        Ok(())
                    }
                }
            });
            async move {
                // The scope owns the join; this action waits only for construction.
                drop(registration?);
                Ok(Completion::Metrics(receiver.await??))
            }
            .boxed()
        }
        PipelineAction::CreateStages
        | PipelineAction::NotifyStagesStart
        | PipelineAction::NotifySourceStart
        | PipelineAction::StopSources => {
            let handles: Vec<_> = match action {
                PipelineAction::CreateStages => context
                    .stage_supervisors
                    .values()
                    .chain(context.source_supervisors.values())
                    .cloned()
                    .collect(),
                PipelineAction::NotifyStagesStart => {
                    context.stage_supervisors.values().cloned().collect()
                }
                _ => context.source_supervisors.values().cloned().collect(),
            };
            let journal = context.system_journal.clone();
            if matches!(action, PipelineAction::NotifySourceStart)
                && context.flow_start_time.is_none()
            {
                context.flow_start_time = Some(std::time::Instant::now());
            }
            let starting = if matches!(action, PipelineAction::NotifySourceStart) {
                Some(writer.enqueue(async move {
                    journal.append(factory.pipeline_starting(), None).await?;
                    journal.append(factory.pipeline_running(), None).await?;
                    Ok(())
                }))
            } else {
                None
            };
            async move {
                if let Some(starting) = starting {
                    starting?.await?;
                }
                for handle in handles {
                    if cancelled.is_cancelled() {
                        break;
                    }
                    let control = async {
                        match action {
                            PipelineAction::CreateStages => handle.initialize().await,
                            PipelineAction::NotifyStagesStart => handle.start().await,
                            PipelineAction::NotifySourceStart => {
                                handle.ready().await?;
                                handle.start().await
                            }
                            PipelineAction::StopSources => handle.begin_drain().await,
                            _ => unreachable!(),
                        }
                    };
                    tokio::select! {
                        biased;
                        _ = cancelled.cancelled() => break,
                        result = control => result?,
                    }
                }
                Ok(Completion::Done)
            }
            .boxed()
        }
        PipelineAction::Cleanup => {
            let handles: Vec<_> = context
                .stage_supervisors
                .values()
                .chain(context.source_supervisors.values())
                .cloned()
                .collect();
            async move {
                let results = futures::future::join_all(handles.into_iter().map(|handle| {
                    let cancelled = cancelled.clone();
                    async move {
                        if !handle.is_drained() && !cancelled.is_cancelled() {
                            tokio::select! {
                                biased;
                                _ = cancelled.cancelled() => {},
                                _ = handle.force_shutdown() => {},
                            }
                        }
                        tokio::select! {
                            biased;
                            _ = cancelled.cancelled() => handle.abort_and_join().await,
                            result = handle.wait_for_completion() => result,
                        }
                    }
                }))
                .await;
                for result in results {
                    result?;
                }
                Ok(Completion::Done)
            }
            .boxed()
        }
        PipelineAction::AbortTeardown { reason, upstream } => {
            tracing::error!(?reason, ?upstream, "Pipeline abort selected");
            context.completion_subscription = None;
            context
                .termination
                .fail(format!("{reason:?}"), Some(reason));
            async { Ok(Completion::Done) }.boxed()
        }
        PipelineAction::WritePipelineAbort { reason, upstream } => {
            let journals: Vec<_> = context
                .stage_data_journals
                .iter()
                .filter_map(|(id, journal)| {
                    context
                        .stage_supervisors
                        .get(id)
                        .or_else(|| context.source_supervisors.get(id))
                        .map(|handle| (handle.clone(), journal.clone()))
                })
                .collect();
            let event = obzenflow_core::event::ChainEventFactory::pipeline_abort_event(
                context.system_id.into(),
                reason,
                upstream,
            );
            async move {
                for (handle, journal) in journals {
                    handle
                        .publish_pipeline_control(journal, event.clone())
                        .await?;
                }
                Ok(Completion::Done)
            }
            .boxed()
        }
        PipelineAction::HandleStageCompleted { envelope } => {
            if let obzenflow_core::event::SystemEventType::StageLifecycle { stage_id, .. } =
                envelope.event.event
            {
                let (_, complete) = super::fsm::record_stage_completion(
                    &mut context.completed_stages,
                    stage_id,
                    context.topology.num_stages(),
                );
                if complete {
                    return publish(context, writer, factory.pipeline_all_stages_completed());
                }
            }
            async { Ok(Completion::Done) }.boxed()
        }
    }
}

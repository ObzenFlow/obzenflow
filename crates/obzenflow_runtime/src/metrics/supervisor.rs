// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics aggregator supervisor - self-contained event loop
//!
//! The supervisor owns the FSM directly and runs autonomously.
//! Once started, all communication happens through journal events only.

use super::subscription::MetricsSubscription;
use crate::messaging::system_subscription::SystemSubscription;
use crate::messaging::{PollResult, SubscriptionPoller};
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, SelfSupervised, StateWatcher};
use futures::FutureExt;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::WriterId;
use obzenflow_core::id::SystemId;
use obzenflow_core::journal::Journal;
use std::sync::Arc;

use super::fsm::{
    MetricsAggregatorAction, MetricsAggregatorContext, MetricsAggregatorEvent,
    MetricsAggregatorState, MetricsJournalKind,
};

const IDLE_BACKOFF_MS: u64 = 10;

/// The supervisor that manages the metrics aggregator
pub(crate) struct MetricsAggregatorSupervisor {
    /// Supervisor name
    pub(crate) name: String,

    /// System journal for writing metrics ready event
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// System ID for metrics writer
    pub(crate) system_id: SystemId,

    pub(crate) data_subscription: Option<MetricsSubscription>,
    pub(crate) error_subscription: Option<MetricsSubscription>,
    pub(crate) system_subscription: Option<SystemSubscription<SystemEvent>>,
    pub(crate) export_timer: Option<tokio::time::Interval>,
    pub(crate) next_input: usize,

    pub(crate) state_watcher: StateWatcher<MetricsAggregatorState>,
    pub(crate) last_state: Option<MetricsAggregatorState>,
}

// Implement base Supervisor trait
impl Supervisor for MetricsAggregatorSupervisor {
    type State = MetricsAggregatorState;
    type Event = MetricsAggregatorEvent;
    type Context = MetricsAggregatorContext;
    type Action = MetricsAggregatorAction;

    fn build_state_machine(
        &self,
        _initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        // Reuse the typed DSL FSM defined in metrics/fsm.rs.
        crate::metrics::fsm::build_metrics_aggregator_fsm()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// Metrics dispatch and lifecycle hooks for the shared self-supervised runner.
#[async_trait::async_trait]
impl SelfSupervised for MetricsAggregatorSupervisor {
    fn writer_id(&self) -> WriterId {
        WriterId::from(self.system_id)
    }

    fn event_for_action_error(&self, msg: String) -> MetricsAggregatorEvent {
        MetricsAggregatorEvent::Error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = obzenflow_core::event::SystemEvent::new(
            self.writer_id(),
            obzenflow_core::event::SystemEventType::MetricsCoordination(
                obzenflow_core::event::MetricsCoordinationEvent::Shutdown,
            ),
        );

        if let Err(e) =
            crate::supervised_base::publication::append(&self.system_journal, event, None).await
        {
            tracing::error!(
                journal_error = %e,
                "Failed to write metrics shutdown event; continuing without system journal entry"
            );
        }
        Ok(())
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut MetricsAggregatorContext,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        // Update state for external observers only when it changes (FLOWIP-086i).
        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }

        match state {
            MetricsAggregatorState::Initializing => {
                // Publish ready event to system journal
                // Metrics aggregator creates SystemEvent directly
                let event = obzenflow_core::event::SystemEvent::new(
                    WriterId::from(self.system_id),
                    obzenflow_core::event::SystemEventType::MetricsCoordination(
                        obzenflow_core::event::MetricsCoordinationEvent::Ready,
                    ),
                );

                crate::supervised_base::publication::append(&self.system_journal, event, None)
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("Failed to write ready event: {e}"))?;

                tracing::info!("Metrics aggregator published ready event");

                // Transition to Running
                Ok(EventLoopDirective::Transition(
                    MetricsAggregatorEvent::StartRunning,
                ))
            }

            MetricsAggregatorState::Running | MetricsAggregatorState::Draining => {
                let timer = self.export_timer.get_or_insert_with(|| {
                    let mut timer = tokio::time::interval(std::time::Duration::from_secs(
                        ctx.export_interval_secs.max(1),
                    ));
                    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    timer
                });
                if timer.tick().now_or_never().is_some() {
                    return Ok(EventLoopDirective::Transition(
                        MetricsAggregatorEvent::ExportMetrics,
                    ));
                }
                // The current pipeline terminal fixes the system endpoint. All
                // stage producers have settled before it can be committed.
                for _ in 0..3 {
                    let input = self.next_input;
                    self.next_input = (input + 1) % 3;
                    if input == 0 {
                        if !ctx.metrics_store.pipeline_terminal() {
                            if let Some(subscription) = &mut self.system_subscription {
                                match subscription.poll_next().await {
                                    PollResult::Event(envelope) => {
                                        return Ok(EventLoopDirective::Transition(
                                            MetricsAggregatorEvent::ProcessSystemEvent {
                                                envelope: Box::new(envelope),
                                            },
                                        ))
                                    }
                                    PollResult::Error(error) => {
                                        return Ok(EventLoopDirective::Transition(
                                            MetricsAggregatorEvent::Error(error.to_string()),
                                        ))
                                    }
                                    _ => {}
                                }
                            }
                        }
                        continue;
                    }
                    let (subscription, journal_kind) = if input == 1 {
                        (&mut self.data_subscription, MetricsJournalKind::Data)
                    } else {
                        (&mut self.error_subscription, MetricsJournalKind::Error)
                    };
                    if let Some(subscription) = subscription {
                        match subscription.poll_next().await {
                            PollResult::Event(envelope) => {
                                return Ok(EventLoopDirective::Transition(
                                    MetricsAggregatorEvent::ProcessBatch {
                                        events: vec![envelope],
                                        journal_kind,
                                        journal_stage: subscription
                                            .last_delivered_upstream_stage()
                                            .expect("physical reader identity"),
                                    },
                                ))
                            }
                            PollResult::Error(error) => {
                                return Ok(EventLoopDirective::Transition(
                                    MetricsAggregatorEvent::Error(error.to_string()),
                                ))
                            }
                            _ => {}
                        }
                    }
                }
                if ctx.metrics_store.pipeline_terminal() {
                    return Ok(EventLoopDirective::Transition(
                        MetricsAggregatorEvent::FlowTerminal,
                    ));
                }
                tokio::select! {
                    _ = self.export_timer.as_mut().expect("initialised").tick() => Ok(EventLoopDirective::Transition(MetricsAggregatorEvent::ExportMetrics)),
                    _ = idle_backoff() => Ok(EventLoopDirective::Continue),
                }
            }

            MetricsAggregatorState::Drained { .. } => {
                // Terminal state
                tracing::info!("Metrics aggregator drained, terminating");
                Ok(EventLoopDirective::Terminate)
            }

            MetricsAggregatorState::Failed { error } => {
                // Terminal state - error occurred
                tracing::error!("Metrics aggregator failed: {}", error);
                Ok(EventLoopDirective::Terminate)
            }
        }
    }
}

#[inline]
async fn idle_backoff() {
    tokio::time::sleep(std::time::Duration::from_millis(IDLE_BACKOFF_MS)).await;
}
// All business logic has been moved to FSM actions - no free functions needed!

impl Drop for MetricsAggregatorSupervisor {
    fn drop(&mut self) {
        // Clean shutdown - subscription will be dropped automatically
        tracing::debug!("Metrics aggregator supervisor dropped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::fsm::MetricsStore;
    use crate::supervised_base::{ChannelBuilder, SelfSupervisedExt};
    use async_trait::async_trait;
    use obzenflow_core::event::types::EventId;
    use obzenflow_core::id::{JournalId, SystemId};
    use obzenflow_core::journal::{JournalError, JournalReader};
    use obzenflow_core::{EventEnvelope, Journal, JournalOwner};
    use std::collections::HashMap;
    use std::marker::PhantomData;

    struct EmptyReader<T> {
        position: u64,
        _phantom: PhantomData<T>,
    }

    #[async_trait]
    impl<T> JournalReader<T> for EmptyReader<T>
    where
        T: obzenflow_core::event::JournalEvent,
    {
        async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
            Ok(None)
        }

        fn position(&self) -> u64 {
            self.position
        }
    }

    struct FailAppendJournal<T> {
        id: JournalId,
        _phantom: PhantomData<T>,
    }

    impl<T> FailAppendJournal<T> {
        fn new() -> Self {
            Self {
                id: JournalId::new(),
                _phantom: PhantomData,
            }
        }
    }

    #[async_trait]
    impl<T> Journal<T> for FailAppendJournal<T>
    where
        T: obzenflow_core::event::JournalEvent + 'static,
    {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&JournalOwner> {
            None
        }

        async fn append(
            &self,
            _event: T,
            _parent: Option<&EventEnvelope<T>>,
        ) -> Result<EventEnvelope<T>, JournalError> {
            Err(JournalError::Implementation {
                message: "append failed".to_string(),
                source: Box::new(std::io::Error::other("append failed")),
            })
        }

        async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            Ok(None)
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(EmptyReader {
                position,
                _phantom: PhantomData,
            }))
        }

        async fn read_last_n(&self, _count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn state_watcher_reports_failed_on_dispatch_error() {
        let system_journal: Arc<dyn Journal<SystemEvent>> =
            Arc::new(FailAppendJournal::<SystemEvent>::new());
        let system_id = SystemId::new();

        let (_event_sender, _event_receiver, state_watcher) =
            ChannelBuilder::<MetricsAggregatorEvent, MetricsAggregatorState>::new()
                .with_event_buffer(1)
                .build(MetricsAggregatorState::Initializing);

        let supervisor = MetricsAggregatorSupervisor {
            name: "metrics_aggregator".to_string(),
            system_journal: system_journal.clone(),
            system_id,
            data_subscription: None,
            error_subscription: None,
            system_subscription: None,
            export_timer: None,
            next_input: 0,
            state_watcher: state_watcher.clone(),
            last_state: Some(MetricsAggregatorState::Initializing),
        };

        let ctx = MetricsAggregatorContext {
            system_journal,
            stage_data_journals: HashMap::new(),
            stage_error_journals: HashMap::new(),
            backpressure_registry: None,
            include_error_journals: true,
            pipeline_writer: None,
            metrics_exporter: Arc::new(crate::metrics::RecordingSnapshots::default()),
            metrics_store: MetricsStore::default(),
            export_interval_secs: 10,
            system_id,
            stage_metadata: HashMap::new(),
            composite_boundaries: Vec::new(),
            composite_durations: obzenflow_core::metrics::CompositeDurationAccumulator::default(),
        };

        let _ = SelfSupervisedExt::run(supervisor, MetricsAggregatorState::Initializing, ctx).await;

        assert!(
            matches!(
                state_watcher.current(),
                MetricsAggregatorState::Failed { .. }
            ),
            "expected state watcher to reflect Failed on error path"
        );
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics aggregator supervisor - self-contained event loop
//!
//! The supervisor owns the FSM directly and runs autonomously.
//! Owned tail readers overwrite buffers; publication never waits for refresh I/O.

use super::buffer::TailReaders;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::builder::EventReceiver;
use crate::supervised_base::{EventLoopDirective, SelfSupervised, StateWatcher};
use obzenflow_core::event::{SystemEvent, SystemPayload, WriterId};
use obzenflow_core::id::SystemId;
use obzenflow_core::journal::Journal;
use std::sync::Arc;

use super::fsm::{
    MetricsAggregatorAction, MetricsAggregatorContext, MetricsAggregatorEvent,
    MetricsAggregatorState,
};

/// The supervisor that manages the metrics aggregator
pub(crate) struct MetricsAggregatorSupervisor {
    /// Supervisor name
    pub(crate) name: String,

    /// System journal for writing metrics ready event
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// System ID for metrics writer
    pub(crate) system_id: SystemId,

    pub(crate) control: EventReceiver<MetricsAggregatorEvent>,
    pub(crate) readers: Option<TailReaders>,
    pub(crate) final_refresh: Option<tokio::time::Instant>,

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

    fn supervisor_kind(
        &self,
    ) -> obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind {
        obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind::MetricsAggregator
    }

    fn report_journal(
        &self,
        _context: &Self::Context,
    ) -> crate::supervised_base::SupervisorJournal {
        self.system_journal.clone().into()
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
            SystemPayload::MetricsCoordination(
                obzenflow_core::event::MetricsCoordinationEvent::Shutdown,
            ),
        );

        if let Err(e) = crate::supervised_base::publication::append(
            &self.system_journal,
            event,
            Default::default(),
        )
        .await
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
                self.readers = Some(TailReaders::start(ctx));
                // Publish ready event to system journal
                // Metrics aggregator creates SystemEvent directly
                let event = obzenflow_core::event::SystemEvent::new(
                    WriterId::from(self.system_id),
                    SystemPayload::MetricsCoordination(
                        obzenflow_core::event::MetricsCoordinationEvent::Ready,
                    ),
                );

                crate::supervised_base::publication::append(
                    &self.system_journal,
                    event,
                    Default::default(),
                )
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
                use tokio::time::{Duration, Instant};
                let buffer = ctx.metrics_store.buffer.clone();
                let terminal = buffer.terminal(ctx.pipeline_writer);
                if terminal && matches!(state, MetricsAggregatorState::Running) {
                    self.final_refresh = Some(Instant::now());
                    return Ok(EventLoopDirective::Transition(
                        MetricsAggregatorEvent::StartDraining,
                    ));
                }
                let mut wake_at = ctx
                    .metrics_store
                    .next_export_at
                    .unwrap_or_else(Instant::now);
                if terminal {
                    let since = *self.final_refresh.get_or_insert_with(Instant::now);
                    // One current refresh opportunity, independent of journal length.
                    // An unavailable journal retains its buffer and cannot become a
                    // historical-drain barrier. Periodic exports remain eligible.
                    let deadline = since + ctx.export_interval.min(Duration::from_millis(250));
                    if buffer
                        .refreshed_since(since, self.readers.as_ref().map_or(0, TailReaders::len))
                        || Instant::now() >= deadline
                    {
                        if let Some(readers) = &mut self.readers {
                            readers.stop().await;
                        }
                        return Ok(EventLoopDirective::Transition(
                            MetricsAggregatorEvent::FlowTerminal,
                        ));
                    }
                    wake_at = wake_at.min(deadline);
                }
                if ctx
                    .metrics_store
                    .next_export_at
                    .is_none_or(|at| at <= Instant::now())
                {
                    return Ok(EventLoopDirective::Transition(
                        MetricsAggregatorEvent::ExportMetrics,
                    ));
                }
                tokio::select! {
                    _ = tokio::time::sleep_until(wake_at) => {},
                    _ = buffer.updated.notified() => {},
                    Some(event) = self.control.recv() => return Ok(EventLoopDirective::Transition(event)),
                }
                Ok(EventLoopDirective::Continue)
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

// All business logic has been moved to FSM actions - no free functions needed!

impl Drop for MetricsAggregatorSupervisor {
    fn drop(&mut self) {
        // JoinSet aborts all owned refresh tasks if the supervisor is cancelled.
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
    use obzenflow_core::{Journal, JournalOwner, JournalRecord};
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
        async fn next(&mut self) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            Ok(None)
        }

        fn position(&self) -> u64 {
            self.position
        }
    }

    struct FailAppendJournal<T> {
        id: JournalId,
        owner: JournalOwner,
        _phantom: PhantomData<T>,
    }

    impl<T> FailAppendJournal<T> {
        fn new(system: SystemId) -> Self {
            Self {
                id: JournalId::new(),
                owner: JournalOwner::system(system),
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
            Some(&self.owner)
        }

        async fn append(
            &self,
            event: T,
            _options: obzenflow_core::journal::AppendOptions<T>,
        ) -> Result<JournalRecord<T::Payload>, JournalError> {
            // Exercise a dispatch failure after successful registration.
            if event.event_type_name() == "system.supervisor.registered" {
                return Ok(JournalRecord::new(self.id.into(), event));
            }
            Err(JournalError::Implementation {
                message: "append failed".to_string(),
                source: Box::new(std::io::Error::other("append failed")),
            })
        }

        async fn read_all_unordered(&self) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
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

        async fn read_last_n(
            &self,
            _count: usize,
        ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn state_watcher_reports_failed_on_dispatch_error() {
        let system_id = SystemId::new();
        let system_journal: Arc<dyn Journal<SystemEvent>> =
            Arc::new(FailAppendJournal::<SystemEvent>::new(system_id));

        let (_event_sender, _event_receiver, state_watcher) =
            ChannelBuilder::<MetricsAggregatorEvent, MetricsAggregatorState>::new()
                .with_event_buffer(1)
                .build(MetricsAggregatorState::Initializing);

        let supervisor = MetricsAggregatorSupervisor {
            name: "metrics_aggregator".to_string(),
            system_journal: system_journal.clone(),
            system_id,
            control: _event_receiver,
            readers: None,
            final_refresh: None,
            state_watcher: state_watcher.clone(),
            last_state: Some(MetricsAggregatorState::Initializing),
        };

        let ctx = MetricsAggregatorContext {
            journals: super::super::builder::MetricsJournals {
                system_id,
                coordination: system_journal.clone(),
                export: system_journal.clone(),
            },
            system_journal,
            stage_data_journals: HashMap::new(),
            stage_error_journals: HashMap::new(),
            backpressure_registry: None,
            include_error_journals: true,
            pipeline_writer: None,
            metrics_exporter: Arc::new(crate::metrics::RecordingSnapshots::default()),
            metrics_store: MetricsStore::default(),
            export_interval: std::time::Duration::from_secs(10),
            system_id,
            stage_metadata: HashMap::new(),
            composite_boundaries: Vec::new(),
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

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics aggregator supervisor - self-contained event loop
//!
//! The supervisor owns the FSM directly and runs autonomously.
//! Owned tail readers overwrite buffers; publication never waits for refresh I/O.

use super::buffer::TailReaders;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::with_external_events::CommandMailbox;
use crate::supervised_base::{EventLoopDirective, SelfSupervised, StateWatcher};
use obzenflow_core::event::{SystemEvent, WriterId};
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

    pub(crate) control: CommandMailbox<MetricsAggregatorEvent>,
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

    fn registration(
        &self,
        _context: &Self::Context,
        descriptor: obzenflow_core::event::payloads::supervisor_descriptor::SupervisorDescriptor,
    ) -> crate::supervised_base::base::Registration {
        crate::supervised_base::base::register_system(
            self.system_journal.clone(),
            self.writer_id(),
            descriptor,
        )
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// Every lifecycle operation is selected by the metrics FSM.
#[async_trait::async_trait]
impl SelfSupervised for MetricsAggregatorSupervisor {
    fn writer_id(&self) -> WriterId {
        self.system_id.into()
    }
    fn event_for_action_error(&self, msg: String) -> MetricsAggregatorEvent {
        MetricsAggregatorEvent::Error(msg)
    }
    fn supervisor_action(
        &self,
        action: &MetricsAggregatorAction,
    ) -> Option<crate::supervised_base::handler_supervised::SupervisorAction<MetricsAggregatorEvent>>
    {
        if let MetricsAggregatorAction::Host(action) = action {
            Some(action.clone())
        } else {
            None
        }
    }
    fn lifecycle_phase(
        &self,
        state: &MetricsAggregatorState,
    ) -> crate::stages::common::stage_lifecycle::LifecyclePhase {
        use crate::stages::common::stage_lifecycle::LifecyclePhase as L;
        match state {
            MetricsAggregatorState::Initializing => L::Initializing,
            MetricsAggregatorState::Starting => L::Initialized,
            MetricsAggregatorState::Running => L::Active,
            MetricsAggregatorState::Finalising => L::Finalising,
            MetricsAggregatorState::Failing { error } => L::Failing(error.clone()),
            MetricsAggregatorState::Cancelling { reason } => L::Cancelling(reason.clone()),
            MetricsAggregatorState::Drained { .. } => L::Completed,
            MetricsAggregatorState::Failed { error } => L::Failed(error.clone()),
            MetricsAggregatorState::Cancelled { reason } => L::Cancelled(reason.clone()),
            _ => L::Other,
        }
    }
    fn after_transition(
        &mut self,
        state: &MetricsAggregatorState,
        _ctx: &MetricsAggregatorContext,
    ) {
        if self.last_state.as_ref() != Some(state) {
            let _ = self.state_watcher.update(state.clone());
            self.last_state = Some(state.clone());
        }
    }
    async fn execute_action(
        &mut self,
        action: MetricsAggregatorAction,
        ctx: &mut MetricsAggregatorContext,
    ) -> Result<
        crate::supervised_base::handler_supervised::ActionExecution<
            MetricsAggregatorContext,
            MetricsAggregatorEvent,
        >,
        obzenflow_fsm::FsmError,
    > {
        use crate::supervised_base::handler_supervised::{ActionCompletion, ActionExecution};
        if matches!(action, MetricsAggregatorAction::BeginFinalRefresh) {
            self.final_refresh
                .get_or_insert_with(tokio::time::Instant::now);
            return Ok(ActionExecution::Completed);
        }
        let exported = matches!(action, MetricsAggregatorAction::ExportMetrics);
        if matches!(action, MetricsAggregatorAction::StartReaders) {
            self.readers = Some(TailReaders::start(ctx));
            return Ok(ActionExecution::Completed);
        }
        let mut resources = ctx
            .resources
            .take()
            .expect("previous metrics operation settled");
        Ok(ActionExecution::Pending(Box::pin(async move {
            let result = action.execute_resources(&mut resources).await;
            Box::new(move |ctx: &mut MetricsAggregatorContext| {
                ctx.resources = Some(resources);
                result.map(|()| exported.then_some(MetricsAggregatorEvent::ExportCompleted))
            }) as ActionCompletion<MetricsAggregatorContext, MetricsAggregatorEvent>
        })))
    }
    async fn execute_cleanup(
        &mut self,
        _ctx: &MetricsAggregatorContext,
    ) -> Result<
        crate::supervised_base::handler_supervised::ActionExecution<
            MetricsAggregatorContext,
            MetricsAggregatorEvent,
        >,
        obzenflow_fsm::FsmError,
    > {
        use crate::supervised_base::handler_supervised::{ActionCompletion, ActionExecution};
        let readers = self.readers.take();
        Ok(ActionExecution::Pending(Box::pin(async move {
            if let Some(mut readers) = readers {
                readers.stop().await;
            }
            Box::new(|_: &mut MetricsAggregatorContext| Ok(None))
                as ActionCompletion<MetricsAggregatorContext, MetricsAggregatorEvent>
        })))
    }
    async fn next_control(
        &mut self,
        state: &MetricsAggregatorState,
        _ctx: &mut MetricsAggregatorContext,
    ) -> Option<MetricsAggregatorEvent> {
        self.control
            .recv(|event| !defer_command(state, event))
            .await
    }
    fn close_mailbox(
        &mut self,
        state: &MetricsAggregatorState,
    ) -> futures::future::BoxFuture<'static, Result<(), Box<dyn std::error::Error + Send + Sync>>>
    {
        use obzenflow_fsm::StateVariant;
        self.control.close_and_record(
            crate::supervised_base::with_external_events::system_commands(
                self.system_journal.clone(),
                self.writer_id(),
            ),
            &self.name,
            state.variant_name(),
        )
    }
    async fn dispatch_state(
        &mut self,
        state: &MetricsAggregatorState,
        ctx: &mut MetricsAggregatorContext,
    ) -> Result<EventLoopDirective<MetricsAggregatorEvent>, Box<dyn std::error::Error + Send + Sync>>
    {
        use MetricsAggregatorEvent as E;
        use MetricsAggregatorState as S;
        if matches!(state, S::Created) {
            return Ok(EventLoopDirective::Transition(E::Initialize));
        }
        if matches!(
            state,
            S::Drained { .. } | S::Failed { .. } | S::Cancelled { .. }
        ) {
            return Ok(EventLoopDirective::Terminate);
        }
        if let Ok(event) = self.control.try_recv(|event| !defer_command(state, event)) {
            return Ok(EventLoopDirective::Transition(event));
        }
        match state {
            S::Running | S::Draining => {
                use tokio::time::{Duration, Instant};
                let buffer = ctx.metrics_store.buffer.clone();
                if buffer.terminal(ctx.pipeline_writer) && matches!(state, S::Running) {
                    return Ok(EventLoopDirective::Transition(E::StartDraining));
                }
                let mut wake_at = ctx
                    .metrics_store
                    .next_export_at
                    .unwrap_or_else(Instant::now);
                if matches!(state, S::Draining) {
                    let since = self
                        .final_refresh
                        .expect("drain transition starts the bounded refresh opportunity");
                    let deadline = since + ctx.export_interval.min(Duration::from_millis(250));
                    if buffer
                        .refreshed_since(since, self.readers.as_ref().map_or(0, TailReaders::len))
                        || Instant::now() >= deadline
                    {
                        return Ok(EventLoopDirective::Transition(E::FlowTerminal));
                    }
                    wake_at = wake_at.min(deadline);
                }
                if ctx
                    .metrics_store
                    .next_export_at
                    .is_none_or(|at| at <= Instant::now())
                {
                    return Ok(EventLoopDirective::Transition(E::ExportMetrics));
                }
                tokio::select! {
                    _ = tokio::time::sleep_until(wake_at) => {},
                    _ = buffer.updated.notified() => {},
                    Some(event) = self.control.recv(|event| !defer_command(state, event)) => return Ok(EventLoopDirective::Transition(event)),
                }
                Ok(EventLoopDirective::Continue)
            }
            _ => Ok(EventLoopDirective::Continue),
        }
    }
}

fn defer_command(state: &MetricsAggregatorState, event: &MetricsAggregatorEvent) -> bool {
    use MetricsAggregatorEvent as E;
    use MetricsAggregatorState as S;
    (matches!(state, S::Initializing | S::Starting)
        && matches!(event, E::StartDraining | E::ExportMetrics))
        || (matches!(state, S::Exporting | S::DrainingExport) && matches!(event, E::ExportMetrics))
}
impl crate::supervised_base::with_external_events::ExternalControlEvent for MetricsAggregatorEvent {
    fn discard_details(
        &self,
    ) -> (
        obzenflow_core::event::CommandDiscardDisposition,
        Option<String>,
    ) {
        crate::stages::common::stage_handle::discarded_control_details(match self {
            Self::Error(error) => Some(error),
            _ => None,
        })
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
    impl<T> obzenflow_core::journal::JournalStorageReader<T> for EmptyReader<T>
    where
        T: obzenflow_core::event::JournalEvent,
    {
        async fn storage_next(
            &mut self,
        ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            Ok(None)
        }

        fn storage_position(&self) -> u64 {
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
    impl<T> obzenflow_core::journal::JournalStorage<T> for FailAppendJournal<T>
    where
        T: obzenflow_core::event::JournalEvent + 'static,
    {
        fn storage_id(&self) -> &JournalId {
            &self.id
        }

        fn storage_owner(&self) -> Option<&JournalOwner> {
            Some(&self.owner)
        }

        async fn storage_append(
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

        async fn storage_read_all_unordered(
            &self,
        ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
            Ok(Vec::new())
        }

        async fn storage_read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            Ok(None)
        }

        async fn storage_reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(EmptyReader {
                position,
                _phantom: PhantomData,
            }))
        }

        async fn storage_read_last_n(
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
            control: _event_receiver.into(),
            readers: None,
            final_refresh: None,
            state_watcher: state_watcher.clone(),
            last_state: Some(MetricsAggregatorState::Initializing),
        };

        let ctx = crate::metrics::fsm::MetricsAggregatorResources {
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
        }
        .into();

        let _ = SelfSupervisedExt::run(supervisor, MetricsAggregatorState::Created, ctx).await;

        assert!(
            matches!(
                state_watcher.current(),
                MetricsAggregatorState::Failed { .. }
            ),
            "expected state watcher to reflect Failed on error path"
        );
    }
}

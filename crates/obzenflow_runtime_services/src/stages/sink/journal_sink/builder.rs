//! Builder for journal sink stages

use std::sync::Arc;

use crate::message_bus::FsmMessageBus;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::control_strategies::{ControlEventStrategy, JonestownStrategy};
use crate::stages::common::handlers::SinkHandler;
use crate::stages::resources_builder::StageResources;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{
    BuilderError, ChannelBuilder, EventLoopDirective, EventReceiver, HandleBuilder,
    HandlerSupervised, HandlerSupervisedExt, StateWatcher, SupervisorBuilder,
    SupervisorTaskBuilder,
};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};

use super::config::JournalSinkConfig;
use super::fsm::{JournalSinkAction, JournalSinkContext, JournalSinkEvent, JournalSinkState};
use super::handle::JournalSinkHandle;
use super::supervisor::JournalSinkSupervisor;

/// Builder for creating journal sink stages
pub struct JournalSinkBuilder<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    handler: H,
    config: JournalSinkConfig,
    resources: StageResources,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> JournalSinkBuilder<H> {
    /// Create a new journal sink builder
    pub fn new(handler: H, config: JournalSinkConfig, resources: StageResources) -> Self {
        Self {
            handler,
            config,
            resources,
            instrumentation: None,
        }
    }

    /// Set the instrumentation for this sink
    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }
}

#[async_trait::async_trait]
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder
    for JournalSinkBuilder<H>
{
    type Handle = JournalSinkHandle<H>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(JournalSinkState::<H>::Created);

        // Create instrumentation if not provided
        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

        // Create context
        let control_strategy: Arc<dyn ControlEventStrategy> = self
            .config
            .control_strategy
            .unwrap_or_else(|| Arc::new(JonestownStrategy));

        let context = JournalSinkContext::new(
            self.handler,
            self.config.stage_id,
            self.config.stage_name.clone(),
            self.config.flow_name.clone(),
            self.resources.flow_id.clone(),
            self.resources.data_journal.clone(),
            self.resources.error_journal.clone(),
            self.resources.system_journal.clone(),
            self.resources.upstream_journals.clone(),
            self.resources.message_bus.clone(),
            self.resources.upstream_stages.clone(),
            control_strategy.clone(),
            instrumentation,
        );

        // Create supervisor (private - not exposed)
        let supervisor = JournalSinkSupervisor {
            name: format!("sink_{}", self.config.stage_name),
            context: Arc::new(context.clone()),
            data_journal: self.resources.data_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            stage_id: self.config.stage_id,
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();

        // Spawn the supervisor task
        let supervisor_name = format!("sink_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<JournalSinkSupervisor<H>>::new(&supervisor_name).spawn(
            move || async move {
                // Create a wrapper that handles external events
                let supervisor_with_events = HandlerSupervisedWithExternalEvents {
                    supervisor,
                    external_events: event_receiver,
                    state_watcher: state_watcher_for_task,
                };

                // Run with the wrapper
                HandlerSupervisedExt::run(
                    supervisor_with_events,
                    JournalSinkState::<H>::Created,
                    context,
                )
                .await
            },
        );

        // Build and return handle
        HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))
    }
}

/// Internal wrapper that bridges external events with the handler-supervised supervisor
struct HandlerSupervisedWithExternalEvents<
    H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    supervisor: JournalSinkSupervisor<H>,
    external_events: EventReceiver<JournalSinkEvent<H>>,
    state_watcher: StateWatcher<JournalSinkState<H>>,
}

// Delegate trait implementations to the inner supervisor
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for HandlerSupervisedWithExternalEvents<H>
{
    type State = JournalSinkState<H>;
    type Event = JournalSinkEvent<H>;
    type Context = JournalSinkContext<H>;
    type Action = JournalSinkAction<H>;

    fn configure_fsm(
        &self,
        builder: obzenflow_fsm::FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>,
    ) -> obzenflow_fsm::FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
        self.supervisor.configure_fsm(builder)
    }

    fn name(&self) -> &str {
        self.supervisor.name()
    }
}

// Implement Sealed for the wrapper
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for HandlerSupervisedWithExternalEvents<H>
{
}

#[async_trait::async_trait]
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for HandlerSupervisedWithExternalEvents<H>
{
    type Handler = H;

    fn writer_id(&self) -> obzenflow_core::WriterId {
        self.supervisor.writer_id()
    }

    fn stage_id(&self) -> StageId {
        self.supervisor.stage_id()
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.supervisor.write_completion_event().await
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        // Update state for external observers
        let _ = self.state_watcher.update(state.clone());

        // Check for external events first
        match self.external_events.try_recv() {
            Ok(event) => {
                // Got an external event, transition to handle it
                return Ok(EventLoopDirective::Transition(event));
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                // No external events, proceed with normal dispatch
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                // Channel closed, initiate shutdown only if not already failed
                if !matches!(state, JournalSinkState::Failed(_)) {
                    return Ok(EventLoopDirective::Transition(JournalSinkEvent::Error(
                        "External control channel closed".to_string(),
                    )));
                }
            }
        }

        // Delegate to the actual supervisor
        self.supervisor.dispatch_state(state).await
    }
}

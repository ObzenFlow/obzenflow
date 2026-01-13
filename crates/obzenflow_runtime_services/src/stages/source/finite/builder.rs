//! Builder for finite source stages

use std::sync::Arc;
use std::time::Duration;

use crate::message_bus::FsmMessageBus;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::handlers::FiniteSourceHandler;
use crate::stages::resources_builder::StageResources;
use crate::stages::source::strategies::{JonestownSourceStrategy, SourceControlStrategy};
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{
    BuilderError, ChannelBuilder, EventLoopDirective, EventReceiver, HandleBuilder,
    HandlerSupervised, HandlerSupervisedExt, StateWatcher, SupervisorBuilder,
    SupervisorTaskBuilder,
};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId, WriterId};

use super::config::FiniteSourceConfig;
use super::fsm::{FiniteSourceAction, FiniteSourceContext, FiniteSourceEvent, FiniteSourceState};
use super::handle::FiniteSourceHandle;
use super::supervisor::FiniteSourceSupervisor;

/// Builder for creating finite source stages
pub struct FiniteSourceBuilder<
    H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    handler: H,
    config: FiniteSourceConfig,
    resources: StageResources,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    FiniteSourceBuilder<H>
{
    /// Create a new finite source builder
    pub fn new(handler: H, config: FiniteSourceConfig, resources: StageResources) -> Self {
        Self {
            handler,
            config,
            resources,
            instrumentation: None,
        }
    }

    /// Set the instrumentation for this source
    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }

    /// Set a custom source control strategy (defaults to JonestownSourceStrategy)
    pub fn with_control_strategy(mut self, strategy: Arc<dyn SourceControlStrategy>) -> Self {
        self.config.control_strategy = Some(strategy);
        self
    }
}

#[async_trait::async_trait]
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder
    for FiniteSourceBuilder<H>
{
    type Handle = FiniteSourceHandle<H>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(FiniteSourceState::<H>::Created);

        // Use provided strategy or default to JonestownSourceStrategy
        let control_strategy = self
            .config
            .control_strategy
            .unwrap_or_else(|| Arc::new(JonestownSourceStrategy));

        // Create instrumentation if not provided
        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

        // Create context
        let context = FiniteSourceContext::<H>::new(
            self.config.stage_id,
            self.config.stage_name.clone(),
            self.config.flow_name.clone(),
            self.resources.flow_id,
            self.resources.data_journal.clone(),
            self.resources.error_journal.clone(),
            self.resources.system_journal.clone(),
            self.resources.replay_archive.clone(),
            self.resources.message_bus.clone(),
            instrumentation,
            control_strategy,
            self.resources.backpressure_writer.clone(),
        );

        // Ensure the handler (and any wrappers) receive the stage writer id before running (FLOWIP-081).
        let mut handler = self.handler;
        handler.bind_writer_id(WriterId::from(self.config.stage_id));

        // Create supervisor (private - not exposed)
        let supervisor = FiniteSourceSupervisor {
            name: format!("finite_source_{}", self.config.stage_name),
            handler,
            context: Arc::new(context.clone()),
            data_journal: self.resources.data_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            stage_id: self.config.stage_id,
            idle_backoff: crate::supervised_base::idle_backoff::IdleBackoff::exponential_with_cap(
                Duration::from_millis(1),
                Duration::from_millis(10),
            ),
            replay_driver: None,
            replay_started_at: None,
            replay_completed_emitted: false,
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();

        // Spawn the supervisor task
        let supervisor_name = format!("finite_source_{}", self.config.stage_name);
        let stage_name_for_trace = self.config.stage_name.clone();
        let task = SupervisorTaskBuilder::<FiniteSourceSupervisor<H>>::new(&supervisor_name).spawn(
            move || async move {
                tracing::debug!("Spawned task for finite_source_{}", stage_name_for_trace);
                // Create a wrapper that handles external events
                let supervisor_with_events = HandlerSupervisedWithExternalEvents {
                    supervisor,
                    external_events: event_receiver,
                    state_watcher: state_watcher_for_task,
                    last_state: None,
                };

                // Run with the wrapper
                HandlerSupervisedExt::run(
                    supervisor_with_events,
                    FiniteSourceState::<H>::Created,
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
    H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    supervisor: FiniteSourceSupervisor<H>,
    external_events: EventReceiver<FiniteSourceEvent<H>>,
    state_watcher: StateWatcher<FiniteSourceState<H>>,
    last_state: Option<FiniteSourceState<H>>,
}

// Delegate trait implementations to the inner supervisor
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for HandlerSupervisedWithExternalEvents<H>
{
    type State = FiniteSourceState<H>;
    type Event = FiniteSourceEvent<H>;
    type Context = FiniteSourceContext<H>;
    type Action = FiniteSourceAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        // Delegate to the inner supervisor so we reuse its DSL-defined FSM.
        self.supervisor.build_state_machine(initial_state)
    }

    fn name(&self) -> &str {
        self.supervisor.name()
    }
}

// Implement Sealed for the wrapper
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for HandlerSupervisedWithExternalEvents<H>
{
}

#[async_trait::async_trait]
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for HandlerSupervisedWithExternalEvents<H>
{
    type Handler = H;

    fn writer_id(&self) -> obzenflow_core::WriterId {
        self.supervisor.writer_id()
    }

    fn stage_id(&self) -> StageId {
        self.supervisor.stage_id()
    }

    fn event_for_action_error(&self, msg: String) -> FiniteSourceEvent<H> {
        self.supervisor.event_for_action_error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.supervisor.write_completion_event().await
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        // Update state for external observers only when it changes (FLOWIP-086i).
        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }

        // In these states the supervisor is purely waiting for external control events.
        // Blocking here avoids a busy-spin loop (FLOWIP-086i).
        if matches!(
            state,
            FiniteSourceState::Created
                | FiniteSourceState::Initialized
                | FiniteSourceState::WaitingForGun
        ) {
            match self.external_events.recv().await {
                Some(event) => return Ok(EventLoopDirective::Transition(event)),
                None => {
                    if !matches!(state, FiniteSourceState::Failed(_)) {
                        return Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                            "External control channel closed".to_string(),
                        )));
                    }
                }
            }
        }

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
                if !matches!(state, FiniteSourceState::Failed(_)) {
                    return Ok(EventLoopDirective::Transition(FiniteSourceEvent::Error(
                        "External control channel closed".to_string(),
                    )));
                }
            }
        }
        // Delegate to the actual supervisor
        self.supervisor.dispatch_state(state, context).await
    }
}

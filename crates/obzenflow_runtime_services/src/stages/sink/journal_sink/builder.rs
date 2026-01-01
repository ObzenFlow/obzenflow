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
use obzenflow_fsm::{EventVariant, StateVariant};

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
            self.resources.message_bus.clone(),
            control_strategy,
            instrumentation,
            self.resources.upstream_subscription_factory,
        );

        // Create supervisor (private - not exposed)
        let supervisor = JournalSinkSupervisor {
            name: format!("sink_{}", self.config.stage_name),
            data_journal: self.resources.data_journal.clone(),
            system_journal: self.resources.system_journal.clone(),
            stage_id: self.config.stage_id,
            stage_name: self.config.stage_name.clone(),
            _marker: std::marker::PhantomData,
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
                    last_state: None,
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
    last_state: Option<JournalSinkState<H>>,
}

// Delegate trait implementations to the inner supervisor
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for HandlerSupervisedWithExternalEvents<H>
{
    type State = JournalSinkState<H>;
    type Event = JournalSinkEvent<H>;
    type Context = JournalSinkContext<H>;
    type Action = JournalSinkAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        // Delegate to the inner supervisor so we use its FSM definition
        // (implemented via the typed `fsm!` DSL) rather than rebuilding here.
        self.supervisor.build_state_machine(initial_state)
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

    fn event_for_action_error(&self, msg: String) -> JournalSinkEvent<H> {
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
        // Lightweight instrumentation to understand sink FSM behaviour.
        // Note: this runs on every loop; keep it at TRACE to avoid log spam.
        tracing::trace!(
            target: "flowip-080o",
            stage_name = %self.supervisor.stage_name,
            fsm_state = %state.variant_name(),
            "sink_fsm: dispatch_state entry"
        );

        // Update state for external observers only when it changes (FLOWIP-086i).
        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }

        // Created is a pure "wait for Initialize" state; block on control events to avoid spin.
        if matches!(state, JournalSinkState::Created) {
            match self.external_events.recv().await {
                Some(event) => {
                    tracing::debug!(
                        target: "flowip-080o",
                        stage_name = %self.supervisor.stage_name,
                        event = %event.variant_name(),
                        "sink_fsm: received external event"
                    );
                    return Ok(EventLoopDirective::Transition(event));
                }
                None => {
                    if !matches!(state, JournalSinkState::Failed(_)) {
                        return Ok(EventLoopDirective::Transition(JournalSinkEvent::Error(
                            "External control channel closed".to_string(),
                        )));
                    }
                }
            }
        }

        // Check for external events first
        match self.external_events.try_recv() {
            Ok(event) => {
                tracing::debug!(
                    target: "flowip-080o",
                    stage_name = %self.supervisor.stage_name,
                    event = %event.variant_name(),
                    "sink_fsm: received external event"
                );
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
        self.supervisor.dispatch_state(state, context).await
    }
}

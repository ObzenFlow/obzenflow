//! Builder for stateful stages

use std::sync::Arc;

use crate::message_bus::FsmMessageBus;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::control_strategies::{ControlEventStrategy, JonestownStrategy};
use crate::stages::common::handlers::StatefulHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{
    BuilderError, ChannelBuilder, EventLoopDirective, EventReceiver, HandleBuilder,
    HandlerSupervised, HandlerSupervisedExt, StateWatcher, SupervisorBuilder,
    SupervisorTaskBuilder,
};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};

use super::config::StatefulConfig;
use super::fsm::{StatefulAction, StatefulContext, StatefulEvent, StatefulState};
use super::handle::StatefulHandle;
use super::supervisor::StatefulSupervisor;

/// Builder for creating stateful stages
pub struct StatefulBuilder<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    handler: H,
    config: StatefulConfig,
    flow_id: obzenflow_core::FlowId,
    data_journal: Arc<dyn Journal<ChainEvent>>,
    error_journal: Arc<dyn Journal<ChainEvent>>,
    system_journal: Arc<dyn Journal<SystemEvent>>,
    upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    bus: Arc<FsmMessageBus>,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StatefulBuilder<H> {
    /// Create a new stateful builder
    pub fn new(
        handler: H,
        config: StatefulConfig,
        flow_id: obzenflow_core::FlowId,
        data_journal: Arc<dyn Journal<ChainEvent>>,
        error_journal: Arc<dyn Journal<ChainEvent>>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
        bus: Arc<FsmMessageBus>,
    ) -> Self {
        Self {
            handler,
            config,
            flow_id,
            data_journal,
            error_journal,
            system_journal,
            upstream_journals,
            bus,
            instrumentation: None,
        }
    }

    /// Set the instrumentation for this stateful stage
    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }

    /// Set a custom control strategy (defaults to JonestownStrategy)
    pub fn with_control_strategy(mut self, strategy: Arc<dyn ControlEventStrategy>) -> Self {
        self.config.control_strategy = Some(strategy);
        self
    }
}

#[async_trait::async_trait]
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder
    for StatefulBuilder<H>
{
    type Handle = StatefulHandle<H>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(StatefulState::<H>::Created);

        // Use provided strategy or default to JonestownStrategy
        let control_strategy = self
            .config
            .control_strategy
            .unwrap_or_else(|| Arc::new(JonestownStrategy));

        // Create instrumentation if not provided
        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

        // Create context
        let context = StatefulContext::new(
            self.handler,
            self.config.stage_id,
            self.config.stage_name.clone(),
            self.config.flow_name.clone(),
            self.flow_id.clone(),
            self.data_journal.clone(),
            self.error_journal.clone(),
            self.system_journal.clone(),
            self.upstream_journals.clone(),
            self.bus.clone(),
            self.config.upstream_stages.clone(),
            control_strategy,
            instrumentation,
        );

        // Create supervisor (private - not exposed)
        let supervisor = StatefulSupervisor {
            name: format!("stateful_{}", self.config.stage_name),
            context: Arc::new(context.clone()),
            data_journal: self.data_journal.clone(),
            system_journal: self.system_journal.clone(),
            stage_id: self.config.stage_id,
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();

        // Spawn the supervisor task
        let supervisor_name = format!("stateful_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<StatefulSupervisor<H>>::new(&supervisor_name).spawn(
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
                    StatefulState::<H>::Created,
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
    H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    supervisor: StatefulSupervisor<H>,
    external_events: EventReceiver<StatefulEvent<H>>,
    state_watcher: StateWatcher<StatefulState<H>>,
}

// Delegate trait implementations to the inner supervisor
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for HandlerSupervisedWithExternalEvents<H>
{
    type State = StatefulState<H>;
    type Event = StatefulEvent<H>;
    type Context = StatefulContext<H>;
    type Action = StatefulAction<H>;

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
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for HandlerSupervisedWithExternalEvents<H>
{
}

#[async_trait::async_trait]
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for HandlerSupervisedWithExternalEvents<H>
{
    type Handler = H;

    fn writer_id(&self) -> obzenflow_core::WriterId {
        self.supervisor.writer_id()
    }

    fn stage_id(&self) -> obzenflow_core::StageId {
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
                if !matches!(state, StatefulState::Failed(_)) {
                    return Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                        "External control channel closed".to_string(),
                    )));
                }
            }
        }

        // Delegate to the actual supervisor
        self.supervisor.dispatch_state(state).await
    }
}

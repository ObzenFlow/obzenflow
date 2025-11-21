//! Builder for join stages

use std::sync::Arc;

use crate::message_bus::FsmMessageBus;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::handlers::JoinHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{
    BuilderError, ChannelBuilder, EventLoopDirective, EventReceiver, HandleBuilder,
    HandlerSupervised, HandlerSupervisedExt, StateWatcher, SupervisorBuilder,
    SupervisorTaskBuilder,
};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};

use super::config::JoinConfig;
use super::fsm::{JoinAction, JoinContext, JoinEvent, JoinState};
use super::handle::JoinHandle;
use super::supervisor::JoinSupervisor;

/// Error type for join builder
#[derive(Debug, thiserror::Error)]
pub enum JoinBuilderError {
    #[error("Missing upstream journal for {0} source")]
    MissingUpstream(&'static str),
}

/// Builder for creating join stages
pub struct JoinBuilder<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    handler: H,
    config: JoinConfig,
    flow_id: obzenflow_core::FlowId,
    data_journal: Arc<dyn Journal<ChainEvent>>,
    error_journal: Arc<dyn Journal<ChainEvent>>,
    system_journal: Arc<dyn Journal<SystemEvent>>,
    reference_journal: Arc<dyn Journal<ChainEvent>>,
    stream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    bus: Arc<FsmMessageBus>,
    control_strategy: Arc<dyn crate::stages::common::control_strategies::ControlEventStrategy>,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> JoinBuilder<H> {
    /// Create a new join builder
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        handler: H,
        config: JoinConfig,
        flow_id: obzenflow_core::FlowId,
        data_journal: Arc<dyn Journal<ChainEvent>>,
        error_journal: Arc<dyn Journal<ChainEvent>>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        reference_journal: Arc<dyn Journal<ChainEvent>>,
        stream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
        bus: Arc<FsmMessageBus>,
        control_strategy: Arc<dyn crate::stages::common::control_strategies::ControlEventStrategy>,
    ) -> Result<Self, JoinBuilderError> {
        tracing::info!(
            "JoinBuilder: Creating join with reference_stage_id={:?}, stream_stages={:?}",
            config.reference_source_id,
            stream_journals.iter().map(|(id, _)| id).collect::<Vec<_>>()
        );

        Ok(Self {
            handler,
            config,
            flow_id,
            data_journal,
            error_journal,
            system_journal,
            reference_journal,
            stream_journals,
            bus,
            control_strategy,
            instrumentation: None,
        })
    }

    /// Set the instrumentation for this join
    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }
}

#[async_trait::async_trait]
impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder
    for JoinBuilder<H>
{
    type Handle = JoinHandle<H>;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::new().build(JoinState::<H>::Created);

        // Create instrumentation if not provided
        let instrumentation = self
            .instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));

        // Extract stream_stages from stream_journals
        let stream_stages: Vec<StageId> = self.stream_journals.iter().map(|(id, _)| *id).collect();

        // Create context
        let context = Arc::new(JoinContext::new(
            self.handler,
            self.config.stage_id,
            self.config.stage_name.clone(),
            self.config.flow_name.clone(),
            self.flow_id.clone(),
            self.config.reference_source_id,
            stream_stages,
            self.data_journal.clone(),
            self.error_journal.clone(),
            self.system_journal.clone(),
            self.reference_journal.clone(),
            self.stream_journals.clone(),
            self.bus.clone(),
            self.control_strategy.clone(),
            instrumentation.clone(),
        ));

        // Create supervisor
        let supervisor = JoinSupervisor {
            name: format!("join_{}", self.config.stage_name),
            context: context.clone(),
            data_journal: self.data_journal.clone(),
            system_journal: self.system_journal.clone(),
            stage_id: self.config.stage_id,
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();
        let context_for_task = (*context).clone();

        // Spawn the supervisor task
        let supervisor_name = format!("join_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<JoinSupervisor<H>>::new(&supervisor_name).spawn(
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
                    JoinState::<H>::Created,
                    context_for_task,
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
    H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    supervisor: JoinSupervisor<H>,
    external_events: EventReceiver<JoinEvent<H>>,
    state_watcher: StateWatcher<JoinState<H>>,
}

// Delegate trait implementations to the inner supervisor
impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for HandlerSupervisedWithExternalEvents<H>
{
    type State = JoinState<H>;
    type Event = JoinEvent<H>;
    type Context = JoinContext<H>;
    type Action = JoinAction<H>;

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

impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    crate::supervised_base::base::private::Sealed for HandlerSupervisedWithExternalEvents<H>
{
}

#[async_trait::async_trait]
impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
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
        // Update state watcher (following transform/stateful pattern with explicit ignore)
        let _ = self.state_watcher.update(state.clone());

        // Check for external events first
        if let Ok(event) = self.external_events.try_recv() {
            return Ok(EventLoopDirective::Transition(event));
        }

        // Otherwise delegate to supervisor
        self.supervisor.dispatch_state(state).await
    }
}

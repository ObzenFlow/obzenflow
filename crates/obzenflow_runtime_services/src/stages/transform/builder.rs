//! Builder for transform stages

use std::sync::Arc;
use obzenflow_core::WriterId;

use crate::messaging::reactive_journal::ReactiveJournal;
use crate::message_bus::FsmMessageBus;
use crate::stages::common::handlers::TransformHandler;
use crate::stages::common::control_strategies::{ControlEventStrategy, JonestownStrategy};
use crate::metrics::instrumentation::StageInstrumentation;
use crate::supervised_base::{
    SupervisorBuilder, BuilderError, ChannelBuilder, SupervisorTaskBuilder,
    HandlerSupervisedExt, HandleBuilder, EventReceiver, StateWatcher,
    EventLoopDirective, HandlerSupervised,
};
use crate::supervised_base::base::Supervisor;

use super::config::TransformConfig;
use super::handle::TransformHandle;
use super::supervisor::TransformSupervisor;
use super::fsm::{TransformState, TransformContext, TransformEvent, TransformAction};

/// Builder for creating transform stages
pub struct TransformBuilder<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    handler: H,
    config: TransformConfig,
    journal: Arc<ReactiveJournal>,
    bus: Arc<FsmMessageBus>,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> TransformBuilder<H> {
    /// Create a new transform builder
    pub fn new(
        handler: H,
        config: TransformConfig,
        journal: Arc<ReactiveJournal>,
        bus: Arc<FsmMessageBus>,
    ) -> Self {
        Self {
            handler,
            config,
            journal,
            bus,
            instrumentation: None,
        }
    }
    
    /// Set the instrumentation for this transform
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
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder for TransformBuilder<H> {
    type Handle = TransformHandle<H>;
    type Error = BuilderError;
    
    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) = 
            ChannelBuilder::new().build(TransformState::<H>::Created);
        
        // Use provided strategy or default to JonestownStrategy
        let control_strategy = self.config.control_strategy
            .unwrap_or_else(|| Arc::new(JonestownStrategy));
        
        // Create instrumentation if not provided
        let instrumentation = self.instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));
        
        // Create context
        let context = TransformContext::new(
            self.handler,
            self.config.stage_id,
            self.config.stage_name.clone(),
            self.config.flow_name.clone(),
            self.journal.clone(),
            self.bus.clone(),
            self.config.upstream_stages.clone(),
            control_strategy,
            instrumentation,
        );
        
        // Create supervisor (private - not exposed)
        let supervisor = TransformSupervisor {
            name: format!("transform_{}", self.config.stage_name),
            context: Arc::new(context.clone()),
            journal: self.journal.clone(),
            writer_id: WriterId::new(), // Will be replaced during init
            stage_id: self.config.stage_id,
        };
        
        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();
        
        // Spawn the supervisor task
        let supervisor_name = format!("transform_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<TransformSupervisor<H>>::new(&supervisor_name)
            .spawn(move || async move {
                // Create a wrapper that handles external events
                let supervisor_with_events = HandlerSupervisedWithExternalEvents {
                    supervisor,
                    external_events: event_receiver,
                    state_watcher: state_watcher_for_task,
                };
                
                // Run with the wrapper
                HandlerSupervisedExt::run(
                    supervisor_with_events,
                    TransformState::<H>::Created,
                    context,
                ).await
            });
        
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
struct HandlerSupervisedWithExternalEvents<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    supervisor: TransformSupervisor<H>,
    external_events: EventReceiver<TransformEvent<H>>,
    state_watcher: StateWatcher<TransformState<H>>,
}

// Delegate trait implementations to the inner supervisor
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor for HandlerSupervisedWithExternalEvents<H> {
    type State = TransformState<H>;
    type Event = TransformEvent<H>;
    type Context = TransformContext<H>;
    type Action = TransformAction<H>;

    fn configure_fsm(
        &self,
        builder: obzenflow_fsm::FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>,
    ) -> obzenflow_fsm::FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
        self.supervisor.configure_fsm(builder)
    }

    fn journal(&self) -> &Arc<ReactiveJournal> {
        self.supervisor.journal()
    }

    fn writer_id(&self) -> &WriterId {
        self.supervisor.writer_id()
    }

    fn name(&self) -> &str {
        self.supervisor.name()
    }
}

// Implement Sealed for the wrapper
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> crate::supervised_base::base::private::Sealed for HandlerSupervisedWithExternalEvents<H> {}

#[async_trait::async_trait]
impl<H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised for HandlerSupervisedWithExternalEvents<H> {
    type Handler = H;
    
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
                if !matches!(state, TransformState::Failed(_)) {
                    return Ok(EventLoopDirective::Transition(
                        TransformEvent::Error("External control channel closed".to_string()),
                    ));
                }
            }
        }
        
        // Delegate to the actual supervisor
        self.supervisor.dispatch_state(state).await
    }
}
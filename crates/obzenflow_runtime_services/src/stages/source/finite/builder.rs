//! Builder for finite source stages

use std::sync::Arc;
use obzenflow_core::WriterId;

use crate::messaging::reactive_journal::ReactiveJournal;
use crate::message_bus::FsmMessageBus;
use crate::stages::common::handlers::FiniteSourceHandler;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::supervised_base::{
    SupervisorBuilder, BuilderError, ChannelBuilder, SupervisorTaskBuilder,
    HandlerSupervisedExt, HandleBuilder, EventReceiver, StateWatcher,
    EventLoopDirective, HandlerSupervised,
};
use crate::supervised_base::base::Supervisor;

use super::config::FiniteSourceConfig;
use super::handle::FiniteSourceHandle;
use super::supervisor::FiniteSourceSupervisor;
use super::fsm::{FiniteSourceState, FiniteSourceContext, FiniteSourceEvent, FiniteSourceAction};

/// Builder for creating finite source stages
pub struct FiniteSourceBuilder<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    handler: H,
    config: FiniteSourceConfig,
    journal: Arc<ReactiveJournal>,
    bus: Arc<FsmMessageBus>,
    instrumentation: Option<Arc<StageInstrumentation>>,
}

impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> FiniteSourceBuilder<H> {
    /// Create a new finite source builder
    pub fn new(
        handler: H,
        config: FiniteSourceConfig,
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
    
    /// Set the instrumentation for this source
    pub fn with_instrumentation(mut self, instrumentation: Arc<StageInstrumentation>) -> Self {
        self.instrumentation = Some(instrumentation);
        self
    }
}

#[async_trait::async_trait]
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> SupervisorBuilder for FiniteSourceBuilder<H> {
    type Handle = FiniteSourceHandle<H>;
    type Error = BuilderError;
    
    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels for supervisor communication
        let (event_sender, event_receiver, state_watcher) = 
            ChannelBuilder::new().build(FiniteSourceState::<H>::Created);
        
        // Create instrumentation if not provided
        let instrumentation = self.instrumentation
            .unwrap_or_else(|| Arc::new(StageInstrumentation::new()));
        
        // Create context
        let context = FiniteSourceContext::new(
            self.handler,
            self.config.stage_id,
            self.config.stage_name.clone(),
            self.config.flow_name.clone(),
            self.journal.clone(),
            self.bus.clone(),
            instrumentation,
        );
        
        // Create supervisor (private - not exposed)
        let supervisor = FiniteSourceSupervisor {
            name: format!("finite_source_{}", self.config.stage_name),
            context: Arc::new(context.clone()),
            journal: self.journal.clone(),
            writer_id: WriterId::new(), // Will be replaced during init
            stage_id: self.config.stage_id,
        };
        
        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();
        
        // Spawn the supervisor task
        let supervisor_name = format!("finite_source_{}", self.config.stage_name);
        let task = SupervisorTaskBuilder::<FiniteSourceSupervisor<H>>::new(&supervisor_name)
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
                    FiniteSourceState::<H>::Created,
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
struct HandlerSupervisedWithExternalEvents<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    supervisor: FiniteSourceSupervisor<H>,
    external_events: EventReceiver<FiniteSourceEvent<H>>,
    state_watcher: StateWatcher<FiniteSourceState<H>>,
}

// Delegate trait implementations to the inner supervisor
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor for HandlerSupervisedWithExternalEvents<H> {
    type State = FiniteSourceState<H>;
    type Event = FiniteSourceEvent<H>;
    type Context = FiniteSourceContext<H>;
    type Action = FiniteSourceAction<H>;

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
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> crate::supervised_base::base::private::Sealed for HandlerSupervisedWithExternalEvents<H> {}

#[async_trait::async_trait]
impl<H: FiniteSourceHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised for HandlerSupervisedWithExternalEvents<H> {
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
                if !matches!(state, FiniteSourceState::Failed(_)) {
                    return Ok(EventLoopDirective::Transition(
                        FiniteSourceEvent::Error("External control channel closed".to_string()),
                    ));
                }
            }
        }
        
        // Delegate to the actual supervisor
        self.supervisor.dispatch_state(state).await
    }
}
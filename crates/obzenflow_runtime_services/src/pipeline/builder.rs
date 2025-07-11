//! Pipeline builder pattern for creating supervisors with proper FSM lifecycle
//!
//! This builder ensures supervisors are created and started correctly according
//! to the FSM architecture patterns, returning only a FlowHandle for control.

use super::{
    handle::FlowHandle,
    fsm::{PipelineAction, PipelineContext, PipelineEvent, PipelineState},
    supervisor::PipelineSupervisor,
};
use crate::{
    event_flow::reactive_journal::ReactiveJournal,
    message_bus::FsmMessageBus,
    stages::common::stage_handle::BoxedStageHandle,
    supervised_base::{
        SelfSupervisedExt, base::Supervisor, SupervisorBuilder, BuilderError,
        ChannelBuilder, EventReceiver, StateWatcher, SupervisorTaskBuilder, HandleBuilder,
    },
};
use obzenflow_core::{metrics::MetricsExporter, WriterId};
use obzenflow_topology_services::{stages::StageId, topology::Topology};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

/// Builder for creating a pipeline with proper FSM lifecycle
pub struct PipelineBuilder {
    topology: Arc<Topology>,
    reactive_journal: Arc<ReactiveJournal>,
    stages: Vec<BoxedStageHandle>,
    metrics_exporter: Option<Arc<dyn MetricsExporter>>,
}

impl PipelineBuilder {
    /// Create a new pipeline builder
    pub fn new(topology: Arc<Topology>, reactive_journal: Arc<ReactiveJournal>) -> Self {
        Self {
            topology,
            reactive_journal,
            stages: Vec::new(),
            metrics_exporter: None,
        }
    }

    /// Add stages to the pipeline
    pub fn with_stages(mut self, stages: Vec<BoxedStageHandle>) -> Self {
        self.stages = stages;
        self
    }

    /// Add metrics exporter
    pub fn with_metrics(mut self, exporter: Arc<dyn MetricsExporter>) -> Self {
        self.metrics_exporter = Some(exporter);
        self
    }
}

#[async_trait::async_trait]
impl SupervisorBuilder for PipelineBuilder {
    type Handle = FlowHandle;
    type Error = BuilderError;

    /// Build and start the pipeline, returning a FlowHandle
    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Register as a writer first
        let stage_id = StageId::new();
        let writer_id = self
            .reactive_journal
            .register_writer(stage_id, None)
            .await
            .map_err(|e| BuilderError::WriterRegistrationError(format!("Failed to register writer: {}", e)))?;

        // Create message bus
        let message_bus = Arc::new(FsmMessageBus::new());

        // Prepare stage supervisors map
        let mut stage_map = HashMap::new();
        for stage in self.stages {
            let stage_id = stage.stage_id();
            stage_map.insert(stage_id, stage);
        }

        // Create pipeline context with all mutable state
        let pipeline_context = Arc::new(PipelineContext {
            bus: message_bus.clone(),
            topology: self.topology.clone(),
            journal: self.reactive_journal.clone(),
            completed_stages: Arc::new(RwLock::new(Vec::new())),
            running_stages: Arc::new(RwLock::new(std::collections::HashSet::new())),
            stage_supervisors: Arc::new(RwLock::new(stage_map)),
            completion_subscription: Arc::new(RwLock::new(None)),
            metrics_exporter: self.metrics_exporter.clone(),
            writer_id: writer_id.clone(),
        });

        // Create channels using the common infrastructure
        let (event_sender, mut event_receiver, state_watcher) = 
            ChannelBuilder::<PipelineEvent, PipelineState>::new()
                .with_event_buffer(100)
                .build(PipelineState::Created);

        // Create supervisor (note: no public new() method)
        let supervisor = PipelineSupervisor {
            name: "pipeline_supervisor".to_string(),
            pipeline_context: pipeline_context.clone(),
            reactive_journal: self.reactive_journal.clone(),
            writer_id,
        };

        // Clone what we need for the task
        let context_for_task = pipeline_context.clone();
        let state_watcher_for_task = state_watcher.clone();
        let metrics_exporter = self.metrics_exporter.clone();

        // Spawn the supervisor task with proper FSM lifecycle
        let supervisor_task = SupervisorTaskBuilder::<PipelineSupervisor>::new("pipeline_supervisor")
            .spawn(move || async move {
                // Create a supervisor wrapper that handles external events
                let supervisor_with_events = SupervisorWithExternalEvents {
                    supervisor,
                    external_events: event_receiver,
                    state_watcher: state_watcher_for_task,
                };

                // Run the supervisor with FSM control
                SelfSupervisedExt::run(
                    supervisor_with_events,
                    PipelineState::Created,
                    context_for_task.as_ref().clone(),
                )
                .await
            });

        // Send initial Materialize event to bootstrap the pipeline
        event_sender.send(PipelineEvent::Materialize).await
            .map_err(|_| BuilderError::Other("Failed to send materialize event".to_string()))?;

        // Build the standard handle first
        let standard_handle = HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(supervisor_task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))?;
            
        // Wrap it in FlowHandle with pipeline-specific extras
        Ok(FlowHandle::new(standard_handle, metrics_exporter))
    }
}

/// Internal wrapper that bridges external events with the supervisor
struct SupervisorWithExternalEvents {
    supervisor: PipelineSupervisor,
    external_events: EventReceiver<PipelineEvent>,
    state_watcher: StateWatcher<PipelineState>,
}

// Delegate trait implementations to the inner supervisor
impl Supervisor for SupervisorWithExternalEvents {
    type State = PipelineState;
    type Event = PipelineEvent;
    type Context = PipelineContext;
    type Action = PipelineAction;

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

#[async_trait::async_trait]
impl crate::supervised_base::SelfSupervised for SupervisorWithExternalEvents {
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
    ) -> Result<
        crate::supervised_base::EventLoopDirective<Self::Event>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        // Update state for external observers
        let _ = self.state_watcher.update(state.clone());

        // Check for external events first
        match self.external_events.try_recv() {
            Ok(event) => {
                // Got an external event, transition to handle it
                return Ok(crate::supervised_base::EventLoopDirective::Transition(event));
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                // No external events, proceed with normal dispatch
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                // Channel closed, initiate shutdown
                return Ok(crate::supervised_base::EventLoopDirective::Transition(
                    PipelineEvent::Error {
                        message: "External control channel closed".to_string(),
                    },
                ));
            }
        }

        // Delegate to the actual supervisor
        self.supervisor.dispatch_state(state).await
    }
}
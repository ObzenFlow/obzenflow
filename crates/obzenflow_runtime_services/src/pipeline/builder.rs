//! Pipeline builder pattern for creating supervisors with proper FSM lifecycle
//!
//! This builder ensures supervisors are created and started correctly according
//! to the FSM architecture patterns, returning only a FlowHandle for control.

use super::{
    fsm::{PipelineAction, PipelineContext, PipelineEvent, PipelineState},
    handle::{FlowHandle, MiddlewareStackConfig},
    supervisor::PipelineSupervisor,
};
use crate::{
    id_conversions::StageIdExt,
    message_bus::FsmMessageBus,
    stages::common::stage_handle::BoxedStageHandle,
    supervised_base::{
        base::Supervisor, BuilderError, ChannelBuilder, EventReceiver, HandleBuilder,
        SelfSupervisedExt, StateWatcher, SupervisorBuilder, SupervisorTaskBuilder,
    },
};
use obzenflow_core::event::WriterId;
use obzenflow_core::event::{ChainEvent, SystemEvent};
use obzenflow_core::id::SystemId;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::metrics::MetricsExporter;
use obzenflow_core::StageId;
use obzenflow_topology::Topology;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

/// Builder for creating a pipeline with proper FSM lifecycle
pub struct PipelineBuilder {
    topology: Arc<Topology>,
    system_journal: Arc<dyn Journal<SystemEvent>>,
    stages: Vec<BoxedStageHandle>,
    sources: Vec<BoxedStageHandle>,
    metrics_exporter: Option<Arc<dyn MetricsExporter>>,
    stage_journals: Option<Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>>,
    error_journals: Option<Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>>,
    flow_name: Option<String>,
    middleware_stacks: Option<HashMap<StageId, MiddlewareStackConfig>>,
    contract_attachments: Option<HashMap<(StageId, StageId), Vec<String>>>,
}

impl PipelineBuilder {
    /// Create a new pipeline builder
    pub fn new(topology: Arc<Topology>, system_journal: Arc<dyn Journal<SystemEvent>>) -> Self {
        Self {
            topology,
            system_journal,
            stages: Vec::new(),
            sources: Vec::new(),
            metrics_exporter: None,
            stage_journals: None,
            error_journals: None,
            flow_name: None,
            middleware_stacks: None,
            contract_attachments: None,
        }
    }

    /// Add stages to the pipeline
    pub fn with_stages(mut self, stages: Vec<BoxedStageHandle>) -> Self {
        self.stages = stages;
        self
    }

    /// Add source stages to the pipeline
    pub fn with_sources(mut self, sources: Vec<BoxedStageHandle>) -> Self {
        self.sources = sources;
        self
    }

    /// Add metrics exporter
    pub fn with_metrics(mut self, exporter: Arc<dyn MetricsExporter>) -> Self {
        self.metrics_exporter = Some(exporter);
        self
    }

    /// Add stage journals for metrics aggregator
    pub fn with_stage_journals(
        mut self,
        journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    ) -> Self {
        self.stage_journals = Some(journals);
        self
    }

    /// Add error journals for error sink
    pub fn with_error_journals(
        mut self,
        journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    ) -> Self {
        self.error_journals = Some(journals);
        self
    }

    /// Set the user-specified flow name from the flow! macro
    pub fn with_flow_name(mut self, name: impl Into<String>) -> Self {
        self.flow_name = Some(name.into());
        self
    }

    /// Attach structural middleware stacks per stage (for topology observability, FLOWIP-059)
    pub fn with_middleware_stacks(
        mut self,
        stacks: HashMap<StageId, MiddlewareStackConfig>,
    ) -> Self {
        self.middleware_stacks = Some(stacks);
        self
    }

    /// Attach structural contract names per edge (for topology observability)
    pub fn with_contract_attachments(
        mut self,
        attachments: HashMap<(StageId, StageId), Vec<String>>,
    ) -> Self {
        self.contract_attachments = Some(attachments);
        self
    }
}

#[async_trait::async_trait]
impl SupervisorBuilder for PipelineBuilder {
    type Handle = FlowHandle;
    type Error = BuilderError;

    /// Build and start the pipeline, returning a FlowHandle
    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // ErrorSink will be automatically created by the flow DSL
        // similar to how MetricsAggregator is created

        // Create unique stage ID for the pipeline supervisor
        let _stage_id = StageId::new();

        // Create message bus
        let message_bus = Arc::new(FsmMessageBus::new());

        // Prepare stage supervisors map
        let mut stage_map = HashMap::new();
        for stage in self.stages {
            let stage_id = stage.stage_id();
            stage_map.insert(stage_id, stage);
        }

        // Prepare source supervisors map
        let mut source_map = HashMap::new();
        for source in self.sources {
            let stage_id = source.stage_id();
            source_map.insert(stage_id, source);
        }

        // Create pipeline context with all mutable state
        let system_id = SystemId::new();

        // DEBUG: Print topology information
        tracing::info!("=== TOPOLOGY DEBUG ===");
        let stages: Vec<_> = self.topology.stages().collect();
        tracing::info!("Topology stages count: {}", stages.len());
        for stage in stages {
            let upstreams = self.topology.upstream_stages(stage.id.clone());
            let downstreams = self.topology.downstream_stages(stage.id.clone());
            tracing::info!(
                "Stage '{}' (id={:?}): upstreams={:?}, downstreams={:?}",
                stage.name,
                stage.id,
                upstreams,
                downstreams
            );
        }
        tracing::info!("=== END TOPOLOGY DEBUG ===");

        // Identify source stages (no upstreams)
        let expected_sources: Vec<StageId> = self
            .topology
            .stages()
            .filter(|stage| self.topology.upstream_stages(stage.id.clone()).is_empty())
            .map(|stage| StageId::from_topology_id(stage.id))
            .collect();

        // Track every topology edge so we can require ContractStatus evidence for each upstream->reader pair
        let expected_contract_pairs: HashSet<(StageId, StageId)> = self
            .topology
            .edges()
            .iter()
            .map(|edge| {
                (
                    StageId::from_topology_id(edge.from),
                    StageId::from_topology_id(edge.to),
                )
            })
            .collect();

        // Structural contract attachments for topology observability:
        // - Every edge gets TransportContract.
        // - Edges whose upstream is a source stage also get SourceContract.
        let mut contract_attachments_map: HashMap<(StageId, StageId), Vec<String>> =
            self.contract_attachments.unwrap_or_default();
        for (upstream, downstream) in &expected_contract_pairs {
            let entry = contract_attachments_map
                .entry((*upstream, *downstream))
                .or_default();
            if !entry.iter().any(|n| n == "TransportContract") {
                entry.push("TransportContract".to_string());
            }
            if expected_sources.contains(upstream) {
                if !entry.iter().any(|n| n == "SourceContract") {
                    entry.push("SourceContract".to_string());
                }
            }
        }

        let pipeline_context = PipelineContext {
            system_id,
            bus: message_bus.clone(),
            topology: self.topology.clone(),
            system_journal: self.system_journal.clone(),
            stage_supervisors: stage_map,
            source_supervisors: source_map,
            completed_stages: Vec::new(),
            running_stages: std::collections::HashSet::new(),
            stage_data_journals: self
                .stage_journals
                .unwrap_or_else(|| Vec::<(StageId, Arc<dyn Journal<ChainEvent>>)>::new()),
            stage_error_journals: self
                .error_journals
                .unwrap_or_else(|| Vec::<(StageId, Arc<dyn Journal<ChainEvent>>)>::new()),
            completion_subscription: None,
            metrics_exporter: self.metrics_exporter.clone(),
            contract_status: HashMap::new(),
            contract_pairs: HashMap::new(),
            expected_contract_pairs,
            expected_sources,
            stage_lifecycle_metrics: HashMap::new(),
            flow_start_time: None,
            last_system_event_id_seen: None,
        };

        // Create channels using the common infrastructure
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::<PipelineEvent, PipelineState>::new()
                .with_event_buffer(100)
                .build(PipelineState::Created);

        // Create supervisor (note: no public new() method)
        let supervisor = PipelineSupervisor {
            name: "pipeline_supervisor".to_string(),
            system_id,
            system_journal: self.system_journal.clone(),
            last_barrier_log: None,
            drain_idle_iters: 0,
        };

        // Clone what we need for the task
        let state_watcher_for_task = state_watcher.clone();
        let metrics_exporter = self.metrics_exporter.clone();

        // Spawn the supervisor task with proper FSM lifecycle
        tracing::info!("About to create pipeline supervisor task");

        // Create the supervisor wrapper BEFORE the spawn to reduce closure size
        let supervisor_with_events = SupervisorWithExternalEvents {
            supervisor,
            external_events: event_receiver,
            state_watcher: state_watcher_for_task,
        };

        let supervisor_task = SupervisorTaskBuilder::<PipelineSupervisor>::new(
            "pipeline_supervisor",
        )
        .spawn(move || async move {
            tracing::info!("Pipeline supervisor task starting");

            // Run the supervisor with FSM control
            let result = SelfSupervisedExt::run(
                supervisor_with_events,
                PipelineState::Created,
                pipeline_context,
            )
            .await;

            match &result {
                Ok(()) => tracing::info!("Pipeline supervisor run() completed successfully"),
                Err(e) => tracing::error!("Pipeline supervisor run() failed: {}", e),
            }
            result
        });
        tracing::info!("Pipeline supervisor task handle created");

        // Give the supervisor task a chance to start before sending events
        tokio::task::yield_now().await;
        tracing::info!("Yielded to allow pipeline supervisor to start");

        // Send initial Materialize event to bootstrap the pipeline
        tracing::info!("About to send Materialize event");
        event_sender
            .send(PipelineEvent::Materialize)
            .await
            .map_err(|_| BuilderError::Other("Failed to send materialize event".to_string()))?;
        tracing::info!("Materialize event sent");

        // Build the standard handle first
        let standard_handle = HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(supervisor_task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))?;

        // Wrap it in FlowHandle with pipeline-specific extras
        // Clone topology for the handle (topology is Arc, so this is cheap)
        let topology = Some(self.topology.clone());
        let flow_name = self.flow_name.unwrap_or_else(|| "unnamed_flow".to_string());
        let middleware_stacks = self
            .middleware_stacks
            .map(|stacks| Arc::new(stacks) as Arc<HashMap<StageId, MiddlewareStackConfig>>);
        let contract_attachments =
            Some(Arc::new(contract_attachments_map) as Arc<HashMap<(StageId, StageId), Vec<String>>>);
        Ok(FlowHandle::new(
            standard_handle,
            metrics_exporter,
            topology,
            flow_name,
            middleware_stacks,
            contract_attachments,
            Some(self.system_journal.clone()),
        ))
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

#[async_trait::async_trait]
impl crate::supervised_base::SelfSupervised for SupervisorWithExternalEvents {
    fn writer_id(&self) -> WriterId {
        self.supervisor.writer_id()
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.supervisor.write_completion_event().await
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut PipelineContext,
    ) -> Result<
        crate::supervised_base::EventLoopDirective<Self::Event>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        // Update state for external observers
        let _ = self.state_watcher.update(state.clone());

        // During Materializing state, ignore external events to allow
        // supervisor's dispatch_state to complete materialization first
        let should_check_external = !matches!(state, PipelineState::Materializing);

        if should_check_external {
            // Check for external events first
            match self.external_events.try_recv() {
                Ok(event) => {
                    // Got an external event, transition to handle it
                    return Ok(crate::supervised_base::EventLoopDirective::Transition(
                        event,
                    ));
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
        }

        // Delegate to the actual supervisor
        self.supervisor.dispatch_state(state, context).await
    }
}

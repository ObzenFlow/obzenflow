//! Pipeline FSM using obzenflow_fsm
//!
//! This defines the pipeline state machine without the supervision logic

use crate::id_conversions::StageIdExt;
use crate::message_bus::FsmMessageBus;
use crate::messaging::system_subscription::SystemSubscription;
use crate::messaging::upstream_subscription::UpstreamSubscription;
use crate::supervised_base::SupervisorHandle;
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::{
    constants, context::StageType, ChainEvent, ChainEventContent, ChainEventFactory, SystemEvent,
    SystemEventFactory, WriterId,
};
use obzenflow_core::id::SystemId;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_fsm::{
    EventVariant, FsmAction, FsmBuilder, FsmContext, StateMachine, StateVariant, Transition,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Pipeline states
#[derive(Clone, Debug, PartialEq)]
pub enum PipelineState {
    Created,
    Materializing,
    Materialized,
    Running,
    SourceCompleted, // Source has finished, initiating Jonestown protocol
    AbortRequested {
        reason: obzenflow_core::event::types::ViolationCause,
        upstream: Option<StageId>,
    },
    Draining,
    Drained,
    Failed {
        reason: String,
    },
}

impl StateVariant for PipelineState {
    fn variant_name(&self) -> &str {
        match self {
            PipelineState::Created => "Created",
            PipelineState::Materializing => "Materializing",
            PipelineState::Materialized => "Materialized",
            PipelineState::Running => "Running",
            PipelineState::SourceCompleted => "SourceCompleted",
            PipelineState::AbortRequested { .. } => "AbortRequested",
            PipelineState::Draining => "Draining",
            PipelineState::Drained => "Drained",
            PipelineState::Failed { .. } => "Failed",
        }
    }
}

/// Pipeline events
#[derive(Clone, Debug)]
pub enum PipelineEvent {
    Materialize,
    MaterializationComplete,
    Run,
    Shutdown,   // Source has completed
    BeginDrain, // Start draining all stages
    Abort {
        reason: obzenflow_core::event::types::ViolationCause,
        upstream: Option<StageId>,
    },
    StageCompleted {
        envelope: obzenflow_core::EventEnvelope<SystemEvent>,
    },
    AllStagesCompleted,
    Error {
        message: String,
    },
}

impl EventVariant for PipelineEvent {
    fn variant_name(&self) -> &str {
        match self {
            PipelineEvent::Materialize => "Materialize",
            PipelineEvent::MaterializationComplete => "MaterializationComplete",
            PipelineEvent::Run => "Run",
            PipelineEvent::Shutdown => "Shutdown",
            PipelineEvent::BeginDrain => "BeginDrain",
            PipelineEvent::Abort { .. } => "Abort",
            PipelineEvent::StageCompleted { .. } => "StageCompleted",
            PipelineEvent::AllStagesCompleted => "AllStagesCompleted",
            PipelineEvent::Error { .. } => "Error",
        }
    }
}

/// Pipeline actions
#[derive(Clone, Debug)]
pub enum PipelineAction {
    CreateStages,
    NotifyStagesStart,
    NotifySourceReady,
    NotifySourceStart,
    BeginDrain,
    WritePipelineCompleted,
    Cleanup,
    StartMetricsAggregator,
    DrainMetrics,
    WritePipelineAbort {
        reason: obzenflow_core::event::types::ViolationCause,
        upstream: Option<StageId>,
    },
    AbortTeardown {
        reason: obzenflow_core::event::types::ViolationCause,
        upstream: Option<StageId>,
    },
    StartCompletionSubscription,
    ProcessCompletionEvents,
    HandleStageCompleted {
        envelope: obzenflow_core::EventEnvelope<SystemEvent>,
    },
}

/// Pipeline context - holds all mutable state
#[derive(Clone)]
pub struct PipelineContext {
    /// System ID for this pipeline component
    pub system_id: SystemId,

    /// Message bus for communication
    pub bus: Arc<FsmMessageBus>,

    /// Topology for structure queries
    pub topology: Arc<obzenflow_topology::Topology>,

    /// System journal for pipeline orchestration events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage supervisors by ID (non-sources only)
    pub stage_supervisors:
        Arc<RwLock<HashMap<StageId, crate::stages::common::stage_handle::BoxedStageHandle>>>,

    /// Source supervisors by ID (sources only)
    pub source_supervisors:
        Arc<RwLock<HashMap<StageId, crate::stages::common::stage_handle::BoxedStageHandle>>>,

    /// Completed stages tracking
    pub completed_stages: Arc<RwLock<Vec<StageId>>>,

    /// Running stages tracking (for startup coordination)
    pub running_stages: Arc<RwLock<std::collections::HashSet<StageId>>>,

    /// System subscription for stage completion events from system journal
    pub completion_subscription: Arc<RwLock<Option<SystemSubscription<SystemEvent>>>>,

    /// Metrics exporter for accessing aggregated metrics
    pub metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,

    /// Stage data journals (for metrics aggregator)
    pub stage_data_journals: Arc<RwLock<Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>>>,

    /// Stage error journals (for error sink) (FLOWIP-082e)
    pub stage_error_journals: Arc<RwLock<Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>>>,

    /// Per-source contract status (pass/fail) keyed by source StageId
    pub contract_status: Arc<RwLock<HashMap<StageId, bool>>>,

    /// Per-edge contract status (upstream, reader) keyed by topology edge
    pub contract_pairs:
        Arc<RwLock<HashMap<(StageId, StageId), crate::pipeline::supervisor::ContractEdgeStatus>>>,

    /// Expected contract edges derived from the topology (upstream -> reader)
    pub expected_contract_pairs: Arc<HashSet<(StageId, StageId)>>,

    /// Expected source stages (used to decide when to drain on success)
    pub expected_sources: Arc<Vec<StageId>>,
    // TODO: Add metrics handle once MetricsAggregatorBuilder is implemented
    // pub metrics_handle: Option<MetricsHandle>,
}

impl FsmContext for PipelineContext {}

// Implement FsmAction for PipelineAction
#[async_trait::async_trait]
impl FsmAction for PipelineAction {
    type Context = PipelineContext;

    async fn execute(&self, context: &Self::Context) -> Result<(), String> {
        match self {
            PipelineAction::CreateStages => {
                tracing::info!("PipelineAction::CreateStages starting");
                // Stages are already in the stage_supervisors map from the builder
                // We just need to initialize them
                let mut supervisors = context.stage_supervisors.write().await;

                tracing::info!("Supervisors count: {}", supervisors.len());

                // Collect stage IDs to avoid holding the lock while initializing
                let stage_ids: Vec<_> = supervisors.keys().cloned().collect();

                tracing::info!("Stage IDs count: {}", stage_ids.len());

                for stage_id in stage_ids {
                    if let Some(mut stage) = supervisors.remove(&stage_id) {
                        let stage_name = stage.stage_name().to_string();

                        tracing::info!("Initializing stage: {} (id: {:?})", stage_name, stage_id);

                        // Initialize the stage
                        stage.initialize().await.map_err(|e| {
                            format!("Failed to initialize stage {}: {}", stage_name, e)
                        })?;

                        // Put it back
                        supervisors.insert(stage_id, stage);

                        tracing::info!("Stage {} initialized", stage_name);
                    }
                }

                tracing::info!(
                    "All {} stages initialized successfully, CreateStages complete",
                    supervisors.len()
                );

                // Initialize all source supervisors (finite and infinite)
                let mut source_supers = context.source_supervisors.write().await;
                tracing::info!("Source supervisors count: {}", source_supers.len());
                let source_ids: Vec<_> = source_supers.keys().cloned().collect();
                for source_id in source_ids {
                    if let Some(mut source) = source_supers.remove(&source_id) {
                        let stage_name = source.stage_name().to_string();
                        tracing::info!("Initializing source: {} (id: {:?})", stage_name, source_id);
                        source.initialize().await.map_err(|e| {
                            format!("Failed to initialize source {}: {}", stage_name, e)
                        })?;
                        source_supers.insert(source_id, source);
                        tracing::info!("Source {} initialized", stage_name);
                    }
                }
                tracing::info!(
                    "All {} sources initialized successfully",
                    source_supers.len()
                );
            }

            PipelineAction::NotifyStagesStart => {
                // Start all non-source stages (transforms and sinks)
                let supervisors = context.stage_supervisors.read().await;
                let non_source_stages: Vec<_> = supervisors
                    .iter()
                    .filter(|(stage_id, stage)| {
                        !context
                            .topology
                            .upstream_stages(stage_id.to_topology_id())
                            .is_empty()
                            || !stage.stage_type().is_source()
                    })
                    .map(|(stage_id, _)| *stage_id)
                    .collect();
                drop(supervisors);

                // Start each non-source stage
                let mut supervisors = context.stage_supervisors.write().await;
                for stage_id in non_source_stages {
                    if let Some(stage) = supervisors.get_mut(&stage_id) {
                        tracing::info!(
                            "Starting non-source stage: {} (id: {:?})",
                            stage.stage_name(),
                            stage_id
                        );
                        stage.start().await.map_err(|e| {
                            format!("Failed to start stage {}: {}", stage.stage_name(), e)
                        })?;
                    }
                }

                tracing::debug!("NotifyStagesStart: All non-source stages started");
            }

            PipelineAction::NotifySourceReady => {
                let mut supervisors = context.source_supervisors.write().await;
                for (source_id, source) in supervisors.iter_mut() {
                    tracing::info!(
                        "Marking source ready (WaitingForGun): {:?} ({})",
                        source_id,
                        source.stage_name()
                    );
                    source.ready().await.map_err(|e| {
                        format!("Failed to ready source {}: {}", source.stage_name(), e)
                    })?;
                }
                tracing::info!("All sources moved to WaitingForGun");
            }

            PipelineAction::NotifySourceStart => {
                let mut supervisors = context.source_supervisors.write().await;
                tracing::info!("Starting {} source stages", supervisors.len());
                for (source_id, source) in supervisors.iter_mut() {
                    tracing::info!(
                        "Starting source stage: {:?} ({})",
                        source_id,
                        source.stage_name()
                    );
                    // Ensure source is in WaitingForGun before start
                    source.ready().await.map_err(|e| {
                        format!(
                            "Failed to ready source stage {}: {}",
                            source.stage_name(),
                            e
                        )
                    })?;
                    source.start().await.map_err(|e| {
                        format!(
                            "Failed to start source stage {}: {}",
                            source.stage_name(),
                            e
                        )
                    })?;
                }
                tracing::info!("All sources started");
            }

            PipelineAction::BeginDrain => {
                // Publish drain signal to system journal (lifecycle) and propagate FlowControl::Drain to all stage data journals
                let writer_id = WriterId::from(context.system_id);

                // System lifecycle event
                let system_event_factory = SystemEventFactory::new(context.system_id);
                let drain_system_event = system_event_factory.pipeline_draining();
                context
                    .system_journal
                    .append(drain_system_event, None)
                    .await
                    .map_err(|e| format!("Failed to publish system drain event: {}", e))?;

                // Flow-control drain into every stage data journal so downstream stages see it
                let drain_event = ChainEventFactory::drain_event(writer_id);
                let stage_journals = context.stage_data_journals.read().await.clone();
                for (stage_id, journal) in stage_journals {
                    journal
                        .append(drain_event.clone(), None)
                        .await
                        .map_err(|e| {
                            format!(
                                "Failed to publish drain event to stage {:?}: {}",
                                stage_id, e
                            )
                        })?;
                }

                tracing::info!(
                    "Published drain event to system journal and all stage data journals"
                );
            }

            PipelineAction::WritePipelineCompleted => {
                // Get flow name from context or use default
                let flow_name = "default"; // TODO: Get from first completed stage event

                // Create SystemEventFactory locally to create consistent pipeline lifecycle event
                let system_event_factory = SystemEventFactory::new(context.system_id);
                let pipeline_completed = system_event_factory.pipeline_completed();

                context
                    .system_journal
                    .append(pipeline_completed, None)
                    .await
                    .map_err(|e| format!("Failed to write pipeline completed event: {}", e))?;

                tracing::info!("Pipeline completion event written");
            }

            PipelineAction::Cleanup => {
                // Cleanup will be handled by supervisor drop
                tracing::info!("Pipeline cleanup action");
            }

            PipelineAction::StartMetricsAggregator => {
                tracing::info!("StartMetricsAggregator action triggered");
                // Start metrics aggregator if we have an exporter
                if let Some(exporter) = context.metrics_exporter.clone() {
                    tracing::info!("Found metrics exporter, starting metrics aggregator");

                    // Get stage journals from context
                    let stage_journals = context.stage_data_journals.read().await.clone();

                    if stage_journals.is_empty() {
                        tracing::warn!("No stage journals available for metrics aggregator");
                        return Ok(());
                    }

                    tracing::info!(
                        stage_journal_ids = ?stage_journals.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                        "Stage journals passed to metrics aggregator"
                    );

                    let system_journal = context.system_journal.clone();

                    // Build stage metadata from topology and stage supervisors
                    let supervisors = context.stage_supervisors.read().await;
                    let mut stage_metadata = std::collections::HashMap::new();

                    for (stage_id, stage_handle) in supervisors.iter() {
                        if let Some(stage_info) = context
                            .topology
                            .stages()
                            .find(|s| s.id == stage_id.to_topology_id())
                        {
                            let metadata = obzenflow_core::metrics::StageMetadata {
                                name: stage_info.name.clone(),
                                stage_type: stage_handle.stage_type(),
                                flow_name: context.topology.flow_name(),
                            };
                            stage_metadata.insert(*stage_id, metadata);
                        }
                    }
                    drop(supervisors);

                    // Include sources in metadata
                    let sources = context.source_supervisors.read().await;
                    for (stage_id, stage_handle) in sources.iter() {
                        if let Some(stage_info) = context
                            .topology
                            .stages()
                            .find(|s| s.id == stage_id.to_topology_id())
                        {
                            let metadata = obzenflow_core::metrics::StageMetadata {
                                name: stage_info.name.clone(),
                                stage_type: stage_handle.stage_type(),
                                flow_name: context.topology.flow_name(),
                            };
                            stage_metadata.insert(*stage_id, metadata);
                        }
                    }
                    drop(sources);

                    // Get error journals for metrics (FLOWIP-082g)
                    let error_journals = context.stage_error_journals.read().await.clone();
                    if !error_journals.is_empty() {
                        tracing::info!(
                            error_journal_ids = ?error_journals.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                            "Error journals passed to metrics aggregator"
                        );
                    } else {
                        tracing::info!("No error journals passed to metrics aggregator");
                    }
                    tracing::info!(
                        stage_metadata = ?stage_metadata
                            .iter()
                            .map(|(id, meta)| (*id, meta.name.clone(), meta.stage_type.clone()))
                            .collect::<Vec<_>>(),
                        "Stage metadata collected for metrics aggregator"
                    );

                    // Spawn metrics aggregator using the builder pattern
                    tokio::spawn(async move {
                        use crate::metrics::{MetricsAggregatorBuilder, MetricsInputs};
                        use crate::supervised_base::SupervisorBuilder;

                        // Create MetricsInputs with both data and error journals (FLOWIP-082g)
                        let inputs = MetricsInputs::new(stage_journals, error_journals);

                        match MetricsAggregatorBuilder::new(inputs, system_journal, exporter)
                            .with_stage_metadata(stage_metadata)
                            .with_export_interval(1) // 10 second interval
                            .build()
                            .await
                        {
                            Ok(handle) => {
                                // Store handle in context for future use
                                // TODO: Add metrics_handle field to PipelineContext

                                // For now, just wait for completion
                                if let Err(e) = handle.wait_for_completion().await {
                                    tracing::error!("Metrics aggregator failed: {}", e);
                                }
                            }
                            Err(e) => {
                                tracing::error!("Failed to build metrics aggregator: {}", e);
                            }
                        }
                    });

                    // Subscribe to system journal to wait for metrics ready event
                    let mut ready_reader =
                        context.system_journal.reader().await.map_err(|e| {
                            format!("Failed to create system journal reader: {}", e)
                        })?;

                    // Wait for metrics aggregator to be ready
                    let deadline =
                        tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
                    loop {
                        match tokio::time::timeout_at(deadline, ready_reader.next()).await {
                            Ok(Ok(Some(envelope))) => {
                                // Check if this is the metrics ready event
                                if let obzenflow_core::event::SystemEventType::MetricsCoordination(
                                    obzenflow_core::event::MetricsCoordinationEvent::Ready,
                                ) = &envelope.event.event
                                {
                                    tracing::info!("Metrics aggregator is ready");
                                    break;
                                }
                                // Otherwise continue waiting for the right event
                            }
                            Ok(Ok(None)) => {
                                // No more events, continue waiting
                            }
                            Ok(Err(e)) => {
                                return Err(
                                    format!("Failed to read metrics ready event: {}", e).into()
                                );
                            }
                            Err(_) => {
                                return Err(
                                    "Timeout waiting for metrics aggregator to be ready".into()
                                );
                            }
                        }
                    }
                } else {
                    tracing::info!("No metrics exporter configured, skipping metrics aggregator");
                }
            }

            PipelineAction::DrainMetrics => {
                tracing::info!("Requesting metrics drain via data journals");

                let writer_id = WriterId::from(context.system_id);

                // 1. Create the proper drain event (FlowSignalPayload::Drain)
                // Pipeline (system component) creates ChainEvent with system writer ID
                let drain_event = ChainEventFactory::drain_event(writer_id);

                // 2. Publish drain request to ALL stage data journals
                // (metrics aggregator reads from these journals)
                let stage_journals = context.stage_data_journals.read().await;
                for (stage_id, journal) in stage_journals.iter() {
                    journal
                        .append(drain_event.clone(), None)
                        .await
                        .map_err(|e| {
                            format!(
                                "Failed to publish drain event to stage {:?}: {}",
                                stage_id, e
                            )
                        })?;
                }
                drop(stage_journals);

                // 3. Wait for drain completion event from system journal
                // The metrics aggregator will publish MetricsCoordination::Drained event when done
                let mut reader =
                    context.system_journal.reader().await.map_err(|e| {
                        format!("Failed to create reader for drain completion: {}", e)
                    })?;

                // Wait for the specific drain completion event with a reasonable timeout
                // Keep this short to avoid long post-completion hangs if the metrics aggregator is gone.
                let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(1);
                loop {
                    match tokio::time::timeout_at(deadline, reader.next()).await {
                        Ok(Ok(Some(envelope))) => {
                            if matches!(
                                &envelope.event.event,
                                obzenflow_core::event::SystemEventType::MetricsCoordination(
                                    obzenflow_core::event::MetricsCoordinationEvent::Drained
                                )
                            ) {
                                tracing::info!("Metrics successfully drained");
                                break;
                            }
                            // Otherwise continue waiting for the right event
                        }
                        Ok(Ok(None)) => {
                            // No more events, continue waiting
                        }
                        Ok(Err(e)) => {
                            return Err(format!("Failed to receive drain completion: {}", e).into())
                        }
                        Err(_) => {
                            tracing::warn!(
                                "Timeout waiting for metrics drain completion, proceeding anyway"
                            );
                            break;
                        }
                    }
                }
            }

            PipelineAction::WritePipelineAbort { reason, upstream } => {
                let writer_id = WriterId::from(context.system_id);
                let abort_event =
                    ChainEventFactory::pipeline_abort_event(writer_id, reason.clone(), *upstream);
                // Publish abort to all stage data journals for visibility
                let stage_journals = context.stage_data_journals.read().await.clone();
                for (stage_id, journal) in stage_journals {
                    journal
                        .append(abort_event.clone(), None)
                        .await
                        .map_err(|e| {
                            format!(
                                "Failed to write pipeline abort event to {:?}: {}",
                                stage_id, e
                            )
                        })?;
                }

                tracing::error!(?reason, ?upstream, "Pipeline abort event written");
            }

            PipelineAction::AbortTeardown { reason, upstream } => {
                // Drop subscriptions/readers to stop further polling
                *context.completion_subscription.write().await = None;
                let _ = reason;
                let _ = upstream;
            }

            PipelineAction::StartCompletionSubscription => {
                // Create reader for system journal - will receive system events
                let reader = context
                    .system_journal
                    .reader()
                    .await
                    .map_err(|e| format!("Failed to create system journal reader: {:?}", e))?;

                // Wrap in SystemSubscription for consistent PollResult handling
                let subscription =
                    SystemSubscription::new(reader, "pipeline_supervisor".to_string());

                *context.completion_subscription.write().await = Some(subscription);

                tracing::info!("Started system subscription for journal events");
            }

            PipelineAction::ProcessCompletionEvents => {
                // This action processes events but doesn't directly trigger transitions
                // The supervisor's dispatch_state will check for events and return appropriate directives
                // For now, this is a no-op as the actual processing happens in dispatch_state
                // In a future refactor, we could move all the logic here and use a channel to communicate back
                tracing::debug!(
                    "ProcessCompletionEvents action - processing handled in dispatch_state"
                );
            }

            PipelineAction::HandleStageCompleted { envelope } => {
                let event = &envelope.event;

                // Extract stage_id from the SystemEvent structure
                if let obzenflow_core::event::SystemEventType::StageLifecycle {
                    stage_id,
                    event: obzenflow_core::event::StageLifecycleEvent::Completed,
                } = &event.event
                {
                    let stage_id = *stage_id;

                    // Get stage name from topology
                    let stage_name = context
                        .topology
                        .stages()
                        .find(|info| info.id == stage_id.to_topology_id())
                        .map(|info| info.name.clone())
                        .unwrap_or_else(|| "unknown".to_string());

                    tracing::info!("Stage completed: {} ({})", stage_name, stage_id);

                    // Add to completed stages
                    let mut completed = context.completed_stages.write().await;
                    if !completed.contains(&stage_id) {
                        completed.push(stage_id);
                    }

                    // Check if all expected stages have completed
                    let expected_stages: std::collections::HashSet<StageId> = context
                        .topology
                        .stages()
                        .map(|info| StageId::from_topology_id(info.id))
                        .collect();
                    let total_stages = expected_stages.len();

                    tracing::debug!(
                        "Stage completion: {} of {} stages completed",
                        completed.len(),
                        total_stages
                    );

                    if completed.len() >= total_stages {
                        tracing::info!("All {} stages have completed!", total_stages);

                        // Write a SystemEvent that the pipeline supervisor will pick up
                        let system_event_factory = SystemEventFactory::new(context.system_id);
                        let all_stages_completed_event =
                            system_event_factory.pipeline_all_stages_completed();

                        context
                            .system_journal
                            .append(all_stages_completed_event, None)
                            .await
                            .map_err(|e| {
                                format!("Failed to write all stages completed event: {}", e)
                            })?;
                    }
                } else {
                    tracing::warn!(
                        "HandleStageCompleted called with non-completed stage event: {:?}",
                        event.event
                    );
                }
            }
        }
        Ok(())
    }
}

/// Type alias for our pipeline FSM
pub type PipelineFsm = StateMachine<PipelineState, PipelineEvent, PipelineContext, PipelineAction>;

/// Build the pipeline FSM with all transitions
pub fn build_pipeline_fsm() -> PipelineFsm {
    FsmBuilder::new(PipelineState::Created)
        .when("Created")
        .on(
            "Materialize",
            |_state, _event: &PipelineEvent, _ctx| async move {
                tracing::info!("FSM: Transitioning from Created to Materializing");
                Ok(Transition {
                    next_state: PipelineState::Materializing,
                    actions: vec![PipelineAction::CreateStages],
                })
            },
        )
        .done()
        .when("Materializing")
        .on(
            "MaterializationComplete",
            |_state, _event: &PipelineEvent, _ctx| async move {
                tracing::info!("FSM: Transitioning from Materializing to Materialized");
                Ok(Transition {
                    next_state: PipelineState::Materialized,
                    actions: vec![
                        PipelineAction::StartCompletionSubscription,
                        PipelineAction::StartMetricsAggregator,
                        PipelineAction::NotifyStagesStart,
                        PipelineAction::NotifySourceReady,
                    ],
                })
            },
        )
        .on("Error", |_state, event, _ctx| {
            let event = event.clone();
            async move {
                if let PipelineEvent::Error { message } = event {
                    Ok(Transition {
                        next_state: PipelineState::Failed { reason: message },
                        actions: vec![PipelineAction::Cleanup],
                    })
                } else {
                    Err("Invalid event".to_string())
                }
            }
        })
        .done()
        .when("Materialized")
        .on("Run", |_state, _event: &PipelineEvent, _ctx| async move {
            tracing::info!(
                "FSM: Transitioning from Materialized to Running (triggered by Run event)"
            );
            Ok(Transition {
                next_state: PipelineState::Running,
                actions: vec![PipelineAction::NotifySourceStart],
            })
        })
        .done()
        .when("Running")
        .on("Abort", |_state, event, _ctx| {
            let event = event.clone();
            async move {
                if let PipelineEvent::Abort { reason, upstream } = event {
                    let reason_clone = reason.clone();
                    Ok(Transition {
                        next_state: PipelineState::AbortRequested {
                            reason: reason.clone(),
                            upstream,
                        },
                        actions: vec![
                            PipelineAction::WritePipelineAbort { reason, upstream },
                            PipelineAction::AbortTeardown {
                                reason: reason_clone,
                                upstream,
                            },
                        ],
                    })
                } else {
                    Err("Invalid event".to_string())
                }
            }
        })
        .on("StageCompleted", |_state, event, _ctx| {
            let event = event.clone();
            async move {
                if let PipelineEvent::StageCompleted { envelope } = event {
                    Ok(Transition {
                        next_state: PipelineState::Running,
                        actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                    })
                } else {
                    Err("Invalid event".to_string())
                }
            }
        })
        .on(
            "Shutdown",
            |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::SourceCompleted,
                    actions: vec![], // No actions yet - just track state
                })
            },
        )
        .on("Error", |_state, event, _ctx| {
            let event = event.clone();
            async move {
                if let PipelineEvent::Error { message } = event {
                    Ok(Transition {
                        next_state: PipelineState::Failed { reason: message },
                        actions: vec![PipelineAction::Cleanup],
                    })
                } else {
                    Err("Invalid event".to_string())
                }
            }
        })
        .done()
        .when("SourceCompleted")
        .on(
            "BeginDrain",
            |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::Draining,
                    actions: vec![PipelineAction::BeginDrain],
                })
            },
        )
        .done()
        .when("Draining")
        .on(
            "Shutdown",
            |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::Draining,
                    actions: vec![PipelineAction::BeginDrain],
                })
            },
        )
        .on("Abort", |_state, event, _ctx| {
            let event = event.clone();
            async move {
                if let PipelineEvent::Abort { reason, upstream } = event {
                    let reason_clone = reason.clone();
                    Ok(Transition {
                        next_state: PipelineState::AbortRequested {
                            reason: reason.clone(),
                            upstream,
                        },
                        actions: vec![
                            PipelineAction::WritePipelineAbort { reason, upstream },
                            PipelineAction::AbortTeardown {
                                reason: reason_clone,
                                upstream,
                            },
                        ],
                    })
                } else {
                    Err("Invalid event".to_string())
                }
            }
        })
        .on("StageCompleted", |_state, event, _ctx| {
            let event = event.clone();
            async move {
                if let PipelineEvent::StageCompleted { envelope } = event {
                    Ok(Transition {
                        next_state: PipelineState::Draining,
                        actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                    })
                } else {
                    Err("Invalid event".to_string())
                }
            }
        })
        .on(
            "AllStagesCompleted",
            |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::Drained,
                    actions: vec![
                        PipelineAction::DrainMetrics, // Drain metrics AFTER all stages complete
                        PipelineAction::WritePipelineCompleted,
                        PipelineAction::Cleanup,
                    ],
                })
            },
        )
        .on("Error", |_state, event, _ctx| {
            let event = event.clone();
            async move {
                if let PipelineEvent::Error { message } = event {
                    Ok(Transition {
                        next_state: PipelineState::Failed { reason: message },
                        actions: vec![PipelineAction::Cleanup],
                    })
                } else {
                    Err("Invalid event".to_string())
                }
            }
        })
        .done()
        .when("AbortRequested")
        .on("Error", |_state, event, _ctx| {
            let event = event.clone();
            async move {
                if let PipelineEvent::Error { message } = event {
                    Ok(Transition {
                        next_state: PipelineState::Failed { reason: message },
                        actions: vec![PipelineAction::Cleanup],
                    })
                } else {
                    Err("Invalid event".to_string())
                }
            }
        })
        .on(
            "Shutdown",
            |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::AbortRequested {
                        reason: obzenflow_core::event::types::ViolationCause::Other(
                            "shutdown_requested".into(),
                        ),
                        upstream: None,
                    },
                    actions: vec![PipelineAction::Cleanup],
                })
            },
        )
        .done()
        .build()
}

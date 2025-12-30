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
use obzenflow_core::id::{FlowId, SystemId};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::metrics::{FlowLifecycleMetricsSnapshot, StageMetricsSnapshot};
use obzenflow_core::StageId;
use obzenflow_fsm::{
    fsm, EventVariant, FsmAction, FsmContext, StateMachine, StateVariant, Transition,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

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
        failure_cause: Option<obzenflow_core::event::types::ViolationCause>,
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
    /// User-initiated stop request (distinct from natural source completion).
    StopRequested,
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
            PipelineEvent::StopRequested => "StopRequested",
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
    /// Request that all sources begin draining (stop producing and emit EOF).
    StopSources,
    BeginDrain,
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
pub struct PipelineContext {
    /// System ID for this pipeline component
    pub system_id: SystemId,

    /// Message bus for communication
    pub bus: Arc<FsmMessageBus>,

    /// Topology for structure queries
    pub topology: Arc<obzenflow_topology::Topology>,

    /// User-specified flow name (from `flow!`)
    pub flow_name: String,

    /// Flow execution ID (for metrics/observability joinability)
    pub flow_id: FlowId,

    /// System journal for pipeline orchestration events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage supervisors by ID (non-sources only)
    pub stage_supervisors: HashMap<StageId, crate::stages::common::stage_handle::BoxedStageHandle>,

    /// Source supervisors by ID (sources only)
    pub source_supervisors: HashMap<StageId, crate::stages::common::stage_handle::BoxedStageHandle>,

    /// Completed stages tracking
    pub completed_stages: Vec<StageId>,

    /// Running stages tracking (for startup coordination)
    pub running_stages: std::collections::HashSet<StageId>,

    /// System subscription for stage completion events from system journal
    pub completion_subscription: Option<SystemSubscription<SystemEvent>>,

    /// Metrics exporter for accessing aggregated metrics
    pub metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,

    /// Stage data journals (for metrics aggregator)
    pub stage_data_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// Stage error journals (for error sink) (FLOWIP-082e)
    pub stage_error_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// Per-source contract status (pass/fail) keyed by source StageId
    pub contract_status: HashMap<StageId, bool>,

    /// Per-edge contract status (upstream, reader) keyed by topology edge
    pub contract_pairs:
        HashMap<(StageId, StageId), crate::pipeline::supervisor::ContractEdgeStatus>,

    /// Expected contract edges derived from the topology (upstream -> reader)
    pub expected_contract_pairs: HashSet<(StageId, StageId)>,

    /// Expected source stages (used to decide when to drain on success)
    pub expected_sources: Vec<StageId>,
    // TODO: Add metrics handle once MetricsAggregatorBuilder is implemented
    // pub metrics_handle: Option<MetricsHandle>,
    /// Last known per-stage lifecycle metrics (for flow rollup)
    pub stage_lifecycle_metrics: HashMap<StageId, StageMetricsSnapshot>,

    /// Flow start time for duration calculation
    pub flow_start_time: Option<std::time::Instant>,

    /// Last system event ID observed via completion_subscription (for tail reconciliation)
    pub last_system_event_id_seen: Option<obzenflow_core::EventId>,

    /// Whether a user stop has been requested for this run.
    pub stop_requested: bool,

    /// Whether stop should be reported as a failure (e.g., finite-only flows).
    pub stop_should_fail: bool,

    /// Deadline for stop-triggered drain to complete.
    pub stop_deadline: Option<std::time::Instant>,
}

impl FsmContext for PipelineContext {}

/// Stop-triggered drain timeout.
///
/// Controlled via `OBZENFLOW_SHUTDOWN_TIMEOUT_SECS` with a sensible default:
/// - If the env var is unset or invalid, defaults to 30 seconds.
pub(crate) fn stop_drain_timeout() -> Duration {
    static TIMEOUT: OnceLock<Duration> = OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        std::env::var("OBZENFLOW_SHUTDOWN_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30))
    })
}

/// Compute flow-level lifecycle metrics from per-stage snapshots in the context.
pub(crate) fn compute_flow_lifecycle_metrics(
    context: &PipelineContext,
) -> FlowLifecycleMetricsSnapshot {
    use obzenflow_core::event::context::StageType as CoreStageType;

    let mut events_in_total: u64 = 0;
    let mut events_out_total: u64 = 0;
    let mut errors_total: u64 = 0;

    for (stage_id, snapshot) in &context.stage_lifecycle_metrics {
        // Map core StageId to topology StageId
        let topo_stage_id = stage_id.to_topology_id();

        // Look up stage info to determine semantic type
        if let Some(stage_info) = context.topology.stages().find(|s| s.id == topo_stage_id) {
            // Map topology StageType to core StageType (they share the same shape)
            let core_type = match stage_info.stage_type {
                obzenflow_topology::StageType::FiniteSource => CoreStageType::FiniteSource,
                obzenflow_topology::StageType::InfiniteSource => CoreStageType::InfiniteSource,
                obzenflow_topology::StageType::Transform => CoreStageType::Transform,
                obzenflow_topology::StageType::Sink => CoreStageType::Sink,
                obzenflow_topology::StageType::Stateful => CoreStageType::Stateful,
                obzenflow_topology::StageType::Join => CoreStageType::Join,
            };

            match core_type {
                CoreStageType::FiniteSource | CoreStageType::InfiniteSource => {
                    events_in_total =
                        events_in_total.saturating_add(snapshot.events_processed_total);
                }
                CoreStageType::Sink => {
                    events_out_total =
                        events_out_total.saturating_add(snapshot.events_processed_total);
                }
                _ => {}
            }
        }

        // Always include errors for all stages
        errors_total = errors_total.saturating_add(snapshot.errors_total);
    }

    FlowLifecycleMetricsSnapshot {
        events_in_total,
        events_out_total,
        errors_total,
    }
}

// Implement FsmAction for PipelineAction
#[async_trait::async_trait]
impl FsmAction for PipelineAction {
    type Context = PipelineContext;

    async fn execute(&self, context: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            PipelineAction::CreateStages => {
                tracing::info!("PipelineAction::CreateStages starting");
                // Stages are already in the stage_supervisors map from the builder
                // We just need to initialize them
                let supervisors = &mut context.stage_supervisors;

                tracing::info!("Supervisors count: {}", supervisors.len());

                // Collect stage IDs to avoid borrow issues while initializing
                let stage_ids: Vec<_> = supervisors.keys().cloned().collect();

                tracing::info!("Stage IDs count: {}", stage_ids.len());

                for stage_id in stage_ids {
                    if let Some(mut stage) = supervisors.remove(&stage_id) {
                        let stage_name = stage.stage_name().to_string();

                        tracing::info!("Initializing stage: {} (id: {:?})", stage_name, stage_id);

                        // Initialize the stage
                        stage.initialize().await.map_err(|e| {
                            obzenflow_fsm::FsmError::HandlerError(format!(
                                "Failed to initialize stage {}: {}",
                                stage_name, e
                            ))
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
                let source_supers = &mut context.source_supervisors;
                tracing::info!("Source supervisors count: {}", source_supers.len());
                let source_ids: Vec<_> = source_supers.keys().cloned().collect();
                for source_id in source_ids {
                    if let Some(mut source) = source_supers.remove(&source_id) {
                        let stage_name = source.stage_name().to_string();
                        tracing::info!("Initializing source: {} (id: {:?})", stage_name, source_id);
                        source.initialize().await.map_err(|e| {
                            obzenflow_fsm::FsmError::HandlerError(format!(
                                "Failed to initialize source {}: {}",
                                stage_name, e
                            ))
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
                let supervisors = &context.stage_supervisors;
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
                // Start each non-source stage
                let supervisors = &mut context.stage_supervisors;
                for stage_id in non_source_stages {
                    if let Some(stage) = supervisors.get_mut(&stage_id) {
                        tracing::info!(
                            "Starting non-source stage: {} (id: {:?})",
                            stage.stage_name(),
                            stage_id
                        );
                        stage.start().await.map_err(|e| {
                            obzenflow_fsm::FsmError::HandlerError(format!(
                                "Failed to start stage {}: {}",
                                stage.stage_name(),
                                e
                            ))
                        })?;
                    }
                }

                tracing::debug!("NotifyStagesStart: All non-source stages started");
            }

            PipelineAction::NotifySourceReady => {
                let supervisors = &mut context.source_supervisors;
                for (source_id, source) in supervisors.iter_mut() {
                    tracing::info!(
                        "Marking source ready (WaitingForGun): {:?} ({})",
                        source_id,
                        source.stage_name()
                    );
                    source.ready().await.map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to ready source {}: {}",
                            source.stage_name(),
                            e
                        ))
                    })?;
                }
                tracing::info!("All sources moved to WaitingForGun");
            }

            PipelineAction::NotifySourceStart => {
                let supervisors = &mut context.source_supervisors;
                tracing::info!("Starting {} source stages", supervisors.len());

                // Publish initial pipeline lifecycle events so that downstream
                // consumers (SSE, UI, metrics) can reliably observe that the
                // flow has started and is running.
                //
                // We emit:
                // - pipeline_starting
                // - pipeline_running (with optional stage_count via topology)
                let system_event_factory = SystemEventFactory::new(context.system_id);
                let starting_event = system_event_factory.pipeline_starting();
                context
                    .system_journal
                    .append(starting_event, None)
                    .await
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to publish pipeline starting event: {}",
                            e
                        ))
                    })?;

                // Use the topology to derive an optional stage_count for the running event.
                let stage_count = context.topology.stages().count();
                let running_event = obzenflow_core::event::SystemEvent::new(
                    obzenflow_core::event::WriterId::from(context.system_id),
                    obzenflow_core::event::SystemEventType::PipelineLifecycle(
                        obzenflow_core::event::PipelineLifecycleEvent::Running {
                            stage_count: Some(stage_count),
                        },
                    ),
                );
                context
                    .system_journal
                    .append(running_event, None)
                    .await
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to publish pipeline running event: {}",
                            e
                        ))
                    })?;

                // Record flow start time on first source start
                if context.flow_start_time.is_none() {
                    context.flow_start_time = Some(std::time::Instant::now());
                }

                for (source_id, source) in supervisors.iter_mut() {
                    tracing::info!(
                        "Starting source stage: {:?} ({})",
                        source_id,
                        source.stage_name()
                    );
                    // Ensure source is in WaitingForGun before start
                    source.ready().await.map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to ready source stage {}: {}",
                            source.stage_name(),
                            e
                        ))
                    })?;
                    source.start().await.map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to start source stage {}: {}",
                            source.stage_name(),
                            e
                        ))
                    })?;
                }
                tracing::info!("All sources started");
            }

            PipelineAction::StopSources => {
                // Best-effort: request that all sources begin draining so they stop
                // producing and emit authored EOF, allowing downstream stages to
                // drain deterministically.
                for (stage_id, source) in context.source_supervisors.iter() {
                    if source.is_drained() {
                        continue;
                    }

                    tracing::info!(
                        source_stage_id = %stage_id,
                        source_stage_name = %source.stage_name(),
                        source_stage_type = %source.stage_type(),
                        "Requesting source begin_drain for StopRequested"
                    );

                    if let Err(e) = source.begin_drain().await {
                        tracing::warn!(
                            source_stage_id = %stage_id,
                            source_stage_name = %source.stage_name(),
                            source_stage_type = %source.stage_type(),
                            error = ?e,
                            "Failed to request source begin_drain during stop; continuing"
                        );
                    }
                }
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
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to publish system drain event: {}",
                            e
                        ))
                    })?;

                // Flow-control drain into every stage data journal so downstream stages see it
                let drain_event = ChainEventFactory::drain_event(writer_id);
                let stage_journals = context.stage_data_journals.clone();
                for (stage_id, journal) in stage_journals {
                    journal
                        .append(drain_event.clone(), None)
                        .await
                        .map_err(|e| {
                            obzenflow_fsm::FsmError::HandlerError(format!(
                                "Failed to publish drain event to stage {:?}: {}",
                                stage_id, e
                            ))
                        })?;
                }

                tracing::info!(
                    "Published drain event to system journal and all stage data journals"
                );
            }

            PipelineAction::Cleanup => {
                tracing::info!("Pipeline cleanup: signaling stages to shut down");

                // Signal all non-source stages to shut down
                for (stage_id, handle) in context.stage_supervisors.iter() {
                    if let Err(e) = handle.force_shutdown().await {
                        tracing::warn!(
                            stage_id = %stage_id,
                            error = ?e,
                            "Failed to send force_shutdown to stage"
                        );
                    }
                }

                // Signal all source stages to shut down
                for (stage_id, handle) in context.source_supervisors.iter() {
                    if let Err(e) = handle.force_shutdown().await {
                        tracing::warn!(
                            stage_id = %stage_id,
                            error = ?e,
                            "Failed to send force_shutdown to source"
                        );
                    }
                }

                tracing::info!("Pipeline cleanup: waiting for stages to complete");

                // Wait for all stages to complete (with timeout)
                //
                // Timeout is configurable via OBZENFLOW_SHUTDOWN_TIMEOUT_SECS
                // (default: 30 seconds) so operators can tune shutdown behavior
                // without code changes.
                use std::time::{Duration, Instant};

                let timeout = std::env::var("OBZENFLOW_SHUTDOWN_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .map(Duration::from_secs)
                    .unwrap_or_else(|| Duration::from_secs(30));

                let start = Instant::now();

                // Helper closure to wait on a single handle with the remaining time budget
                async fn wait_handle_with_budget(
                    stage_id: StageId,
                    handle: &crate::stages::common::stage_handle::BoxedStageHandle,
                    timeout: Duration,
                    start: Instant,
                ) {
                    let elapsed = start.elapsed();
                    if elapsed >= timeout {
                        tracing::warn!(
                            stage_id = %stage_id,
                            "Pipeline cleanup: timeout budget exhausted before waiting on stage"
                        );
                        return;
                    }

                    let remaining = timeout.saturating_sub(elapsed);

                    match tokio::time::timeout(remaining, handle.wait_for_completion()).await {
                        Ok(Ok(())) => {
                            tracing::debug!(stage_id = %stage_id, "Stage completed during cleanup");
                        }
                        Ok(Err(e)) => {
                            tracing::warn!(
                                stage_id = %stage_id,
                                error = ?e,
                                "Stage failed during shutdown cleanup"
                            );
                        }
                        Err(_) => {
                            tracing::warn!(
                                stage_id = %stage_id,
                                "Timeout waiting for stage during cleanup"
                            );
                        }
                    }
                }

                // Wait for non-source stages
                for (stage_id, handle) in context.stage_supervisors.iter() {
                    wait_handle_with_budget(*stage_id, handle, timeout, start).await;
                }

                // Wait for source stages
                for (stage_id, handle) in context.source_supervisors.iter() {
                    wait_handle_with_budget(*stage_id, handle, timeout, start).await;
                }

                tracing::info!("Pipeline cleanup complete");
            }

            PipelineAction::StartMetricsAggregator => {
                tracing::info!("StartMetricsAggregator action triggered");
                // Start metrics aggregator if we have an exporter
                if let Some(exporter) = context.metrics_exporter.clone() {
                    tracing::info!("Found metrics exporter, starting metrics aggregator");

                    // Get stage journals from context
                    let stage_journals = context.stage_data_journals.clone();

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
                    let mut stage_metadata = std::collections::HashMap::new();

                    for (stage_id, stage_handle) in context.stage_supervisors.iter() {
                        if let Some(stage_info) = context
                            .topology
                            .stages()
                            .find(|s| s.id == stage_id.to_topology_id())
                        {
                            let metadata = obzenflow_core::metrics::StageMetadata {
                                name: stage_info.name.clone(),
                                stage_type: stage_handle.stage_type(),
                                flow_name: context.flow_name.clone(),
                                flow_id: Some(context.flow_id.clone()),
                            };
                            stage_metadata.insert(*stage_id, metadata);
                        }
                    }
                    // Include sources in metadata
                    for (stage_id, stage_handle) in context.source_supervisors.iter() {
                        if let Some(stage_info) = context
                            .topology
                            .stages()
                            .find(|s| s.id == stage_id.to_topology_id())
                        {
                            let metadata = obzenflow_core::metrics::StageMetadata {
                                name: stage_info.name.clone(),
                                stage_type: stage_handle.stage_type(),
                                flow_name: context.flow_name.clone(),
                                flow_id: Some(context.flow_id.clone()),
                            };
                            stage_metadata.insert(*stage_id, metadata);
                        }
                    }
                    // Get error journals for metrics (FLOWIP-082g)
                    let error_journals = context.stage_error_journals.clone();
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
                    let mut ready_reader = context.system_journal.reader().await.map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to create system journal reader: {}",
                            e
                        ))
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
                                return Err(obzenflow_fsm::FsmError::HandlerError(format!(
                                    "Failed to read metrics ready event: {}",
                                    e
                                )));
                            }
                            Err(_) => {
                                return Err(obzenflow_fsm::FsmError::HandlerError(
                                    "Timeout waiting for metrics aggregator to be ready".into(),
                                ));
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
                let stage_journals = &context.stage_data_journals;
                for (stage_id, journal) in stage_journals.iter() {
                    journal
                        .append(drain_event.clone(), None)
                        .await
                        .map_err(|e| {
                            obzenflow_fsm::FsmError::HandlerError(format!(
                                "Failed to publish drain event to stage {:?}: {}",
                                stage_id, e
                            ))
                        })?;
                }
                drop(stage_journals);

                // 3. Wait for drain completion event from system journal
                // The metrics aggregator will publish MetricsCoordination::Drained event when done
                let mut reader = context.system_journal.reader().await.map_err(|e| {
                    obzenflow_fsm::FsmError::HandlerError(format!(
                        "Failed to create reader for drain completion: {}",
                        e
                    ))
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
                            return Err(obzenflow_fsm::FsmError::HandlerError(format!(
                                "Failed to receive drain completion: {}",
                                e
                            )))
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
                let stage_journals = context.stage_data_journals.clone();
                for (stage_id, journal) in stage_journals {
                    journal
                        .append(abort_event.clone(), None)
                        .await
                        .map_err(|e| {
                            obzenflow_fsm::FsmError::HandlerError(format!(
                                "Failed to write pipeline abort event to {:?}: {}",
                                stage_id, e
                            ))
                        })?;
                }

                tracing::error!(?reason, ?upstream, "Pipeline abort event written");
            }

            PipelineAction::AbortTeardown { reason, upstream } => {
                // Drop subscriptions/readers to stop further polling
                context.completion_subscription = None;
                let _ = reason;
                let _ = upstream;
            }

            PipelineAction::StartCompletionSubscription => {
                // Create reader for system journal - will receive system events
                let reader = context.system_journal.reader().await.map_err(|e| {
                    obzenflow_fsm::FsmError::HandlerError(format!(
                        "Failed to create system journal reader: {:?}",
                        e
                    ))
                })?;

                // Wrap in SystemSubscription for consistent PollResult handling
                let subscription =
                    SystemSubscription::new(reader, "pipeline_supervisor".to_string());

                context.completion_subscription = Some(subscription);

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
                    event: obzenflow_core::event::StageLifecycleEvent::Completed { .. },
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
                    if !context.completed_stages.contains(&stage_id) {
                        context.completed_stages.push(stage_id);
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
                        context.completed_stages.len(),
                        total_stages
                    );

                    if context.completed_stages.len() >= total_stages {
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
                                obzenflow_fsm::FsmError::HandlerError(format!(
                                    "Failed to write all stages completed event: {}",
                                    e
                                ))
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
    fsm! {
        state:   PipelineState;
        event:   PipelineEvent;
        context: PipelineContext;
        action:  PipelineAction;
        initial: PipelineState::Created;

        state PipelineState::Created {
            on PipelineEvent::Materialize => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
                    tracing::info!("FSM: Transitioning from Created to Materializing");
                    Ok(Transition {
                        next_state: PipelineState::Materializing,
                        actions: vec![PipelineAction::CreateStages],
                    })
                })
            };

            on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: PipelineState::Created,
                        actions: vec![],
                    })
                })
            };
        }

        state PipelineState::Materializing {
            on PipelineEvent::MaterializationComplete => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
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
                })
            };

            on PipelineEvent::Error => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::Error { message } = event {
                        let failure_cause =
                            if message == "stop_drain_timeout" {
                                Some(obzenflow_core::event::types::ViolationCause::Other(
                                    "stop_drain_timeout".into(),
                                ))
                            } else {
                                None
                            };
                        Ok(Transition {
                            next_state: PipelineState::Failed {
                                reason: message,
                                failure_cause,
                            },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    } else {
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            };

            on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: PipelineState::Failed {
                            reason: "stop_requested_during_materialization".to_string(),
                            failure_cause: None,
                        },
                        actions: vec![PipelineAction::Cleanup],
                    })
                })
            };
        }

        state PipelineState::Materialized {
            on PipelineEvent::Run => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
                    tracing::info!(
                        "FSM: Transitioning from Materialized to Running (triggered by Run event)"
                    );
                    Ok(Transition {
                        next_state: PipelineState::Running,
                        actions: vec![PipelineAction::NotifySourceStart],
                    })
                })
            };

            on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: PipelineState::Failed {
                            reason: "stop_requested_before_run".to_string(),
                            failure_cause: None,
                        },
                        actions: vec![PipelineAction::Cleanup],
                    })
                })
            };
        }

        state PipelineState::Running {
            on PipelineEvent::Abort => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                let event = event.clone();
                Box::pin(async move {
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
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            };

            on PipelineEvent::StageCompleted => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::StageCompleted { envelope } = event {
                        Ok(Transition {
                            next_state: PipelineState::Running,
                            actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                        })
                    } else {
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            };

            on PipelineEvent::Shutdown => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: PipelineState::SourceCompleted,
                        actions: vec![], // No actions yet - just track state
                    })
                })
            };

            on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, ctx: &mut PipelineContext| {
                Box::pin(async move {
                    // A user stop is a cancellation signal, not a "drain to completion".
                    // Draining can take arbitrarily long under backpressure or large backlogs.
                    if !ctx.stop_requested {
                        let mut has_active_sources = false;
                        let mut has_infinite_active_source = false;
                        for source in ctx.source_supervisors.values() {
                            if source.is_drained() {
                                continue;
                            }
                            has_active_sources = true;
                            if source.stage_type().is_infinite_source() {
                                has_infinite_active_source = true;
                                break;
                            }
                        }
                        ctx.stop_requested = true;
                        ctx.stop_should_fail = has_active_sources && !has_infinite_active_source;
                        ctx.stop_deadline = Some(std::time::Instant::now() + stop_drain_timeout());
                    }

                    Ok(Transition {
                        next_state: PipelineState::Failed {
                            reason: "user_stop".to_string(),
                            failure_cause: Some(obzenflow_core::event::types::ViolationCause::Other(
                                "user_stop".into(),
                            )),
                        },
                        actions: vec![PipelineAction::Cleanup],
                    })
                })
            };

            on PipelineEvent::Error => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::Error { message } = event {
                        let failure_cause =
                            if message == "stop_drain_timeout" {
                                Some(obzenflow_core::event::types::ViolationCause::Other(
                                    "stop_drain_timeout".into(),
                                ))
                            } else {
                                None
                            };
                        Ok(Transition {
                            next_state: PipelineState::Failed {
                                reason: message,
                                failure_cause,
                            },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    } else {
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            };
        }

        state PipelineState::SourceCompleted {
            on PipelineEvent::BeginDrain => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: PipelineState::Draining,
                        actions: vec![PipelineAction::BeginDrain],
                    })
                })
            };

            // Stop while transitioning into drain is still a cancellation signal.
            on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, ctx: &mut PipelineContext| {
                Box::pin(async move {
                    if !ctx.stop_requested {
                        let mut has_active_sources = false;
                        let mut has_infinite_active_source = false;
                        for source in ctx.source_supervisors.values() {
                            if source.is_drained() {
                                continue;
                            }
                            has_active_sources = true;
                            if source.stage_type().is_infinite_source() {
                                has_infinite_active_source = true;
                                break;
                            }
                        }
                        ctx.stop_requested = true;
                        ctx.stop_should_fail = has_active_sources && !has_infinite_active_source;
                    }

                    Ok(Transition {
                        next_state: PipelineState::Failed {
                            reason: "user_stop".to_string(),
                            failure_cause: Some(obzenflow_core::event::types::ViolationCause::Other(
                                "user_stop".into(),
                            )),
                        },
                        actions: vec![PipelineAction::Cleanup],
                    })
                })
            };
        }

        state PipelineState::Draining {
            on PipelineEvent::Shutdown => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: PipelineState::Draining,
                        actions: vec![PipelineAction::BeginDrain],
                    })
                })
            };

            // Stop during draining is still a cancellation signal: fail fast.
            on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, ctx: &mut PipelineContext| {
                Box::pin(async move {
                    if !ctx.stop_requested {
                        let mut has_active_sources = false;
                        let mut has_infinite_active_source = false;
                        for source in ctx.source_supervisors.values() {
                            if source.is_drained() {
                                continue;
                            }
                            has_active_sources = true;
                            if source.stage_type().is_infinite_source() {
                                has_infinite_active_source = true;
                                break;
                            }
                        }
                        ctx.stop_requested = true;
                        ctx.stop_should_fail = has_active_sources && !has_infinite_active_source;
                    }

                    Ok(Transition {
                        next_state: PipelineState::Failed {
                            reason: "user_stop".to_string(),
                            failure_cause: Some(obzenflow_core::event::types::ViolationCause::Other(
                                "user_stop".into(),
                            )),
                        },
                        actions: vec![PipelineAction::Cleanup],
                    })
                })
            };

            on PipelineEvent::Abort => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                let event = event.clone();
                Box::pin(async move {
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
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            };

            on PipelineEvent::StageCompleted => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::StageCompleted { envelope } = event {
                        Ok(Transition {
                            next_state: PipelineState::Draining,
                            actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                        })
                    } else {
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            };

            on PipelineEvent::AllStagesCompleted => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: PipelineState::Drained,
                        actions: vec![
                            PipelineAction::DrainMetrics, // Drain metrics AFTER all stages complete
                            PipelineAction::Cleanup,
                        ],
                    })
                })
            };

            // Stop during draining should not be a no-op: arm the stop deadline so the
            // drain loop can time out (stop_drain_timeout) instead of waiting forever.
            on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, ctx: &mut PipelineContext| {
                Box::pin(async move {
                    if !ctx.stop_requested {
                        let mut has_active_sources = false;
                        let mut has_infinite_active_source = false;
                        for source in ctx.source_supervisors.values() {
                            if source.is_drained() {
                                continue;
                            }
                            has_active_sources = true;
                            if source.stage_type().is_infinite_source() {
                                has_infinite_active_source = true;
                                break;
                            }
                        }
                        ctx.stop_requested = true;
                        ctx.stop_should_fail = has_active_sources && !has_infinite_active_source;
                    }

                    if ctx.stop_deadline.is_none() {
                        ctx.stop_deadline = Some(std::time::Instant::now() + stop_drain_timeout());
                    }

                    Ok(Transition {
                        next_state: PipelineState::Draining,
                        actions: vec![PipelineAction::StopSources],
                    })
                })
            };

            on PipelineEvent::Error => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::Error { message } = event {
                        Ok(Transition {
                            next_state: PipelineState::Failed {
                                reason: message,
                                failure_cause: None,
                            },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    } else {
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            };

            on PipelineEvent::StopRequested => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: PipelineState::Draining,
                        actions: vec![],
                    })
                })
            };
        }

        state PipelineState::AbortRequested {
            on PipelineEvent::Error => |state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                let event = event.clone();
                let state = state.clone();
                Box::pin(async move {
                    match (state, event) {
                        (
                            PipelineState::AbortRequested { reason: abort_reason, .. },
                            PipelineEvent::Error { message },
                        ) => {
                            Ok(Transition {
                                next_state: PipelineState::Failed {
                                    reason: message,
                                    failure_cause: Some(abort_reason),
                                },
                                actions: vec![PipelineAction::Cleanup],
                            })
                        }
                        _ => Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        )),
                    }
                })
            };

            on PipelineEvent::Shutdown => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: PipelineState::AbortRequested {
                            reason: obzenflow_core::event::types::ViolationCause::Other(
                                "shutdown_requested".into(),
                            ),
                            upstream: None,
                        },
                        actions: vec![PipelineAction::Cleanup],
                    })
                })
            };

            on PipelineEvent::StopRequested => |state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                let state = state.clone();
                Box::pin(async move {
                    Ok(Transition {
                        next_state: state,
                        actions: vec![],
                    })
                })
            };
        }
    }
}

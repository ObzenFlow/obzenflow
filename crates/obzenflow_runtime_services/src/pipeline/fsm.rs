//! Pipeline FSM using obzenflow_fsm
//!
//! This defines the pipeline state machine without the supervision logic

use crate::message_bus::FsmMessageBus;
use obzenflow_core::StageId;
use obzenflow_fsm::{FsmBuilder, StateMachine, Transition, StateVariant, EventVariant, FsmAction};
use obzenflow_core::{Journal, ChainEvent};
use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use tokio::sync::RwLock;
use crate::supervised_base::SupervisorHandle;

/// Pipeline states
#[derive(Clone, Debug, PartialEq)]
pub enum PipelineState {
    Created,
    Materializing,
    Materialized,
    Running,
    SourceCompleted,  // Source has finished, initiating Jonestown protocol
    Draining,
    Drained,
    Failed { reason: String },
}

impl StateVariant for PipelineState {
    fn variant_name(&self) -> &str {
        match self {
            PipelineState::Created => "Created",
            PipelineState::Materializing => "Materializing",
            PipelineState::Materialized => "Materialized",
            PipelineState::Running => "Running",
            PipelineState::SourceCompleted => "SourceCompleted",
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
    Shutdown,  // Source has completed
    BeginDrain,  // Start draining all stages
    StageCompleted { envelope: obzenflow_core::EventEnvelope },
    AllStagesCompleted,
    Error { message: String },
}

impl EventVariant for PipelineEvent {
    fn variant_name(&self) -> &str {
        match self {
            PipelineEvent::Materialize => "Materialize",
            PipelineEvent::MaterializationComplete => "MaterializationComplete",
            PipelineEvent::Run => "Run",
            PipelineEvent::Shutdown => "Shutdown",
            PipelineEvent::BeginDrain => "BeginDrain",
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
    NotifySourceStart,
    BeginDrain,
    WritePipelineCompleted,
    Cleanup,
    StartMetricsAggregator,
    DrainMetrics,
    StartCompletionSubscription,
    ProcessCompletionEvents,
    HandleStageCompleted { envelope: obzenflow_core::EventEnvelope },
}

/// Pipeline context - holds all mutable state
#[derive(Clone)]
pub struct PipelineContext {
    /// Message bus for communication
    pub bus: Arc<FsmMessageBus>,
    
    /// Topology for structure queries
    pub topology: Arc<obzenflow_topology_services::topology::Topology>,
    
    /// Journal for event flow
    pub journal: Arc<crate::messaging::reactive_journal::ReactiveJournal>,
    
    /// Stage supervisors by ID
    pub stage_supervisors: Arc<RwLock<HashMap<StageId, crate::stages::common::stage_handle::BoxedStageHandle>>>,
    
    /// Completed stages tracking
    pub completed_stages: Arc<RwLock<Vec<StageId>>>,
    
    /// Running stages tracking (for startup coordination)
    pub running_stages: Arc<RwLock<std::collections::HashSet<StageId>>>,
    
    /// Subscription for stage completion events
    pub completion_subscription: Arc<RwLock<Option<crate::messaging::JournalSubscription>>>,
    
    /// Metrics exporter for accessing aggregated metrics
    pub metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
    
    /// Metrics journal that reads from all stage journals
    pub metrics_journal: Option<Arc<crate::messaging::reactive_journal::ReactiveJournal>>,
    
    /// TODO: Add metrics handle once MetricsAggregatorBuilder is implemented
    // pub metrics_handle: Option<MetricsHandle>,
    
    /// Writer ID for journal events (immutable after creation)
    pub writer_id: obzenflow_core::WriterId,
}

impl obzenflow_fsm::FsmContext for PipelineContext {}

// Implement FsmAction for PipelineAction
#[async_trait::async_trait]
impl FsmAction for PipelineAction {
    type Context = PipelineContext;

    async fn execute(
        &self,
        context: &Self::Context,
    ) -> Result<(), String> {
        match self {
            PipelineAction::CreateStages => {
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
                        stage.initialize().await
                            .map_err(|e| format!("Failed to initialize stage {}: {}", stage_name, e))?;
                        
                        // Put it back
                        supervisors.insert(stage_id, stage);
                        
                        tracing::info!("Stage {} initialized", stage_name);
                    }
                }
                
                tracing::info!("All {} stages initialized successfully", supervisors.len());
            }
            
            PipelineAction::NotifyStagesStart => {
                // Start all non-source stages (transforms and sinks)
                let supervisors = context.stage_supervisors.read().await;
                let non_source_stages: Vec<_> = supervisors.iter()
                    .filter(|(stage_id, stage)| !context.topology.upstream_stages(**stage_id).is_empty() || !stage.stage_type().is_source())
                    .map(|(stage_id, _)| *stage_id)
                    .collect();
                drop(supervisors);
                
                // Start each non-source stage
                let mut supervisors = context.stage_supervisors.write().await;
                for stage_id in non_source_stages {
                    if let Some(stage) = supervisors.get_mut(&stage_id) {
                        tracing::info!("Starting non-source stage: {} (id: {:?})", stage.stage_name(), stage_id);
                        stage.start().await
                            .map_err(|e| format!("Failed to start stage {}: {}", stage.stage_name(), e))?;
                    }
                }
                
                tracing::debug!("NotifyStagesStart: All non-source stages started");
            }
            
            PipelineAction::NotifySourceStart => {
                // Find the source stage (stage with no upstreams) and trigger FSM transition
                let supervisors = context.stage_supervisors.read().await;
                let source_id = supervisors.iter()
                    .find(|(stage_id, _)| context.topology.upstream_stages(**stage_id).is_empty())
                    .map(|(stage_id, _)| *stage_id);
                drop(supervisors);
                    
                if let Some(source_id) = source_id {
                    tracing::info!("Starting source stage: {:?}", source_id);
                    
                    // Get the source supervisor and send Start event
                    let mut supervisors = context.stage_supervisors.write().await;
                    if let Some(stage) = supervisors.get_mut(&source_id) {
                        stage.start().await
                            .map_err(|e| format!("Failed to start source stage: {}", e))?;
                    }
                }
            }
            
            PipelineAction::BeginDrain => {
                // Publish drain event to journal for stages to pick up
                let writer_id = context.writer_id.clone();
                
                let drain_event = obzenflow_core::ChainEvent::new(
                    obzenflow_core::EventId::new(),
                    writer_id.clone(),
                    ChainEvent::SYSTEM_PIPELINE_DRAIN,
                    serde_json::json!({
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    })
                );
                
                context.journal.append(&writer_id, drain_event, None).await
                    .map_err(|e| format!("Failed to publish drain event: {}", e))?;
                    
                tracing::info!("Published drain event to journal");
            }
            
            PipelineAction::WritePipelineCompleted => {
                // Get flow name from context or use default
                let flow_name = "default"; // TODO: Get from first completed stage event
                
                // Register writer for pipeline
                // In the new architecture, ReactiveJournal already has its writer_id
                let writer_id = context.journal.writer_id.clone();
                
                let supervisors = context.stage_supervisors.read().await;
                let mut pipeline_completed = obzenflow_core::ChainEvent::new(
                    obzenflow_core::EventId::new(),
                    writer_id.clone(),
                    ChainEvent::SYSTEM_PIPELINE_COMPLETED,
                    serde_json::json!({
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "completed_stages": supervisors.len(),
                    })
                );
                drop(supervisors);
                
                // Set flow context for pipeline completion event
                pipeline_completed.flow_context = obzenflow_core::event::flow_context::FlowContext {
                    flow_name: flow_name.to_string(),
                    flow_id: "default".to_string(),
                    stage_name: "pipeline".to_string(),
                    stage_type: obzenflow_core::event::flow_context::StageType::Transform,
                };
                
                context.journal.write_control_event(pipeline_completed).await
                    .map_err(|e| format!("Failed to write pipeline completion event: {}", e))?;
                    
                tracing::info!("Pipeline completion event written");
            }
            
            PipelineAction::Cleanup => {
                // Cleanup will be handled by supervisor drop
                tracing::info!("Pipeline cleanup action");
            }
            
            PipelineAction::StartMetricsAggregator => {
                tracing::info!("StartMetricsAggregator action triggered");
                // Start metrics aggregator if we have an exporter and metrics journal
                if let (Some(exporter), Some(metrics_journal)) = (context.metrics_exporter.clone(), context.metrics_journal.clone()) {
                    tracing::info!("Found metrics exporter and metrics journal, starting metrics aggregator");
                    let journal = metrics_journal;
                    
                    // Subscribe to control journal - will filter for metrics ready in recv loop
                    let mut ready_subscription = journal
                        .subscribe()
                        .await
                        .map_err(|e| format!("Failed to subscribe for metrics ready: {}", e))?;
                    
                    // Spawn metrics aggregator using the builder pattern
                    tokio::spawn(async move {
                        use crate::metrics::MetricsAggregatorBuilder;
                        use crate::supervised_base::SupervisorBuilder;
                        
                        match MetricsAggregatorBuilder::new(journal, exporter)
                            .with_export_interval(10) // 10 second interval
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
                    
                    // Wait for metrics aggregator to be ready
                    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(5);
                    loop {
                        match tokio::time::timeout_at(deadline, ready_subscription.recv()).await {
                            Ok(Ok(event)) => {
                                // Check if this is the metrics ready event
                                if event.event.event_type == ChainEvent::SYSTEM_METRICS_READY {
                                    tracing::info!("Metrics aggregator is ready");
                                    break;
                                }
                                // Otherwise continue waiting for the right event
                            }
                            Ok(Err(e)) => {
                                return Err(format!("Failed to receive metrics ready event: {}", e).into());
                            }
                            Err(_) => {
                                return Err("Timeout waiting for metrics aggregator to be ready".into());
                            }
                        }
                    }
                } else {
                    tracing::warn!("No metrics exporter or metrics journal configured in pipeline context");
                }
            }
            
            PipelineAction::DrainMetrics => {
                tracing::info!("Requesting metrics drain via journal");
                
                let writer_id = context.writer_id.clone();
                
                // 1. Publish drain request to journal
                let drain_event = obzenflow_core::ChainEvent::new(
                    obzenflow_core::EventId::new(),
                    writer_id.clone(),
                    ChainEvent::SYSTEM_METRICS_DRAIN,
                    serde_json::json!({}),
                );
                
                context.journal
                    .write_control_event(drain_event)
                    .await
                    .map_err(|e| format!("Failed to publish drain event: {}", e))?;
                
                // 2. Subscribe and wait for drain completion event
                let mut subscription = context.journal
                    .subscribe()
                    .await
                    .map_err(|e| format!("Failed to subscribe for drain completion: {}", e))?;
                
                // Wait for the specific drain completion event
                // No timeout - let metrics aggregator take as much time as it needs
                loop {
                    match subscription.recv().await {
                        Ok(event) => {
                            if event.event.event_type == ChainEvent::SYSTEM_METRICS_DRAINED {
                                tracing::info!("Metrics successfully drained");
                                break;
                            }
                            // Otherwise continue waiting for the right event
                        }
                        Err(e) => return Err(format!("Failed to receive drain completion: {}", e).into()),
                    }
                }
            }
            
            PipelineAction::StartCompletionSubscription => {
                // Create subscription to control journal - will receive ALL control events
                // The new architecture (FLOWIP-008) doesn't filter at subscription time
                let subscription = context.journal
                    .subscribe()
                    .await
                    .map_err(|e| format!("Failed to create control subscription: {:?}", e))?;
                
                *context.completion_subscription.write().await = Some(subscription);
                
                tracing::info!("Started subscription for control journal events");
            }
            
            PipelineAction::ProcessCompletionEvents => {
                // This action processes events but doesn't directly trigger transitions
                // The supervisor's dispatch_state will check for events and return appropriate directives
                // For now, this is a no-op as the actual processing happens in dispatch_state
                // In a future refactor, we could move all the logic here and use a channel to communicate back
                tracing::debug!("ProcessCompletionEvents action - processing handled in dispatch_state");
            }
            
            PipelineAction::HandleStageCompleted { envelope } => {
                let event = &envelope.event;
                
                // Get stage info from the event payload
                let stage_name = event
                    .payload
                    .get("stage_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                
                // Get stage ID from the event payload (not writer registry)
                let stage_id_str = event
                    .payload
                    .get("stage_id")
                    .and_then(|v| v.as_str());
                
                if let Some(stage_id_str) = stage_id_str {
                    // Parse the stage ID (handle "stage_" prefix if present)
                    let ulid_str = if stage_id_str.starts_with("stage_") {
                        &stage_id_str[6..]
                    } else {
                        stage_id_str
                    };
                    
                    let stage_id = match ulid_str.parse::<ulid::Ulid>() {
                        Ok(ulid) => StageId::from_ulid(ulid),
                        Err(_) => {
                            tracing::warn!("Failed to parse stage ID from event: {}", stage_id_str);
                            return Ok(());
                        }
                    };
                    
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
                        .map(|info| info.id)
                        .collect();
                    let total_stages = expected_stages.len();
                    
                    tracing::debug!(
                        "Stage completion: {} of {} stages completed",
                        completed.len(),
                        total_stages
                    );
                    
                    if completed.len() >= total_stages {
                        tracing::info!("All {} stages have completed!", total_stages);
                        
                        // Write an event that the pipeline supervisor itself will pick up
                        let writer_id = context.writer_id.clone();
                        let all_complete_event = obzenflow_core::ChainEvent::new(
                            obzenflow_core::EventId::new(),
                            writer_id.clone(),
                            ChainEvent::SYSTEM_PIPELINE_ALL_STAGES_COMPLETED,
                            serde_json::json!({
                                "total_stages": total_stages,
                                "completed_stages": completed.len()
                            }),
                        );
                        
                        context.journal
                            .append(&writer_id, all_complete_event, None)
                            .await
                            .map_err(|e| format!("Failed to publish all stages completed event: {}", e))?;
                    }
                } else {
                    tracing::warn!(
                        "Stage completed event missing stage_id in payload: {:?}",
                        event.payload
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
            .on("Materialize", |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::Materializing,
                    actions: vec![PipelineAction::CreateStages],
                })
            })
            .done()
        
        .when("Materializing")
            .on("MaterializationComplete", |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::Materialized,
                    actions: vec![
                        PipelineAction::StartCompletionSubscription,
                        PipelineAction::StartMetricsAggregator,
                        PipelineAction::NotifyStagesStart,
                    ],
                })
            })
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
                Ok(Transition {
                    next_state: PipelineState::Running,
                    actions: vec![PipelineAction::NotifySourceStart],
                })
            })
            .done()
        
        .when("Running")
            .on("Shutdown", |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::SourceCompleted,
                    actions: vec![],  // No actions yet - just track state
                })
            })
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
            .on("BeginDrain", |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::Draining,
                    actions: vec![PipelineAction::BeginDrain],
                })
            })
            .done()
        
        .when("Draining")
            .on("AllStagesCompleted", |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::Drained,
                    actions: vec![
                        PipelineAction::DrainMetrics,  // Drain metrics AFTER all stages complete
                        PipelineAction::WritePipelineCompleted,
                        PipelineAction::Cleanup
                    ],
                })
            })
            .done()
        
        .build()
}


//! Pipeline Supervisor - Manages the lifecycle of a pipeline and its stages
//!
//! The supervisor pattern provides:
//! - Hierarchical ownership of FSMs
//! - Message bus for inter-FSM communication
//! - Clean separation between supervision and business logic

use super::{
    pipeline::Pipeline,
    fsm::{PipelineState, PipelineEvent, PipelineAction, PipelineContext, PipelineFsm, build_pipeline_fsm}
};
use crate::stages::common::stage_handle::BoxedStageHandle;
use crate::event_flow::reactive_journal::ReactiveJournal;
use crate::message_bus::FsmMessageBus;
use crate::errors::{FlowError, PipelineSupervisorError};
use obzenflow_topology_services::stages::StageId;
use obzenflow_core::{ChainEvent, EventId};
use crate::metrics::MetricsAggregator;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Pipeline supervisor - manages the lifecycle of a pipeline
pub struct PipelineSupervisor {
    /// The pipeline FSM
    pipeline_fsm: PipelineFsm,
    
    /// Pipeline context
    pipeline_context: Arc<PipelineContext>,
    
    /// Message bus for inter-FSM communication
    message_bus: Arc<FsmMessageBus>,
    
    /// Stage supervisors by ID
    stage_supervisors: HashMap<StageId, BoxedStageHandle>,
    
    /// Pipeline configuration
    pipeline: Pipeline,
    
    /// Reactive journal wrapper
    reactive_journal: Arc<ReactiveJournal>,
    
    /// Completion tracker task handle
    completion_tracker_handle: Option<tokio::task::JoinHandle<()>>,
    
    /// App metrics collection task handle (FLOWIP-056-666)
    app_metrics_task_handle: Option<tokio::task::JoinHandle<()>>,
    
    /// Metrics exporter for accessing aggregated metrics (FLOWIP-056-666)
    /// Uses trait object to avoid runtime→adapters dependency
    metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
    
    /// Metrics aggregator instance (FLOWIP-056-666)
    /// Uses trait object to avoid runtime→adapters dependency
    metrics_aggregator: Option<Arc<std::sync::Mutex<Box<dyn MetricsAggregator>>>>,
}

impl PipelineSupervisor {
    /// Create a new pipeline supervisor
    pub fn new(
        topology: Arc<obzenflow_topology_services::topology::Topology>, 
        reactive_journal: Arc<ReactiveJournal>,
        stages: Vec<BoxedStageHandle>,
        metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
        metrics_aggregator: Option<Arc<std::sync::Mutex<Box<dyn MetricsAggregator>>>>,
    ) -> Result<Self, PipelineSupervisorError> {
        // Create message bus
        let message_bus = Arc::new(FsmMessageBus::new());
        
        // Create pipeline context - use reactive_journal so everyone uses the same instance
        let pipeline_context = Arc::new(PipelineContext {
            bus: message_bus.clone(),
            topology: topology.clone(),
            journal: reactive_journal.clone(),
            completed_stages: Arc::new(RwLock::new(Vec::new())),
        });
        
        // Build pipeline FSM
        let pipeline_fsm = build_pipeline_fsm();
        
        // Create pipeline with the provided stages
        let pipeline = Pipeline {
            journal: reactive_journal.clone(),
            topology: topology.clone(),
            stages,
            observers: Vec::new(),
        };
        
        Ok(Self {
            pipeline_fsm,
            pipeline_context,
            message_bus,
            stage_supervisors: HashMap::new(),
            pipeline,
            reactive_journal,
            completion_tracker_handle: None,
            app_metrics_task_handle: None,
            metrics_exporter,
            metrics_aggregator,
        })
    }
    
    /// Materialize the pipeline (creates stages)
    pub async fn materialize(&mut self) -> Result<(), String> {
        // Start app metrics collection if we have both exporter and factory
        tracing::info!("Checking metrics configuration: exporter={}, aggregator={}", 
            self.metrics_exporter.is_some(), 
            self.metrics_aggregator.is_some()
        );
        
        // Start metrics collection if we have both aggregator and exporter
        if let (Some(aggregator), Some(exporter)) = (&self.metrics_aggregator, &self.metrics_exporter) {
            tracing::info!("Starting metrics collection");
            
            // Create subscription for all events
            let filter = crate::event_flow::SubscriptionFilter::All;
            let mut subscription = match self.reactive_journal.subscribe(filter).await {
                Ok(sub) => sub,
                Err(e) => {
                    tracing::error!("Failed to create metrics subscription: {:?}", e);
                    return Err(format!("Failed to create metrics subscription: {:?}", e));
                }
            };
            
            let aggregator_clone = aggregator.clone();
            let exporter_clone = exporter.clone();
            
            // Spawn metrics event loop
            let handle = tokio::spawn(async move {
                tracing::info!("MetricsAggregator event loop started");
                
                // Log aggregator instance address
                let agg_ptr = Arc::as_ptr(&aggregator_clone) as usize;
                tracing::warn!("Event loop using aggregator at 0x{:x}", agg_ptr);
                
                // Create snapshot interval
                let mut snapshot_interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
                
                loop {
                    tokio::select! {
                        // Process events
                        result = subscription.recv_batch() => {
                            match result {
                                Ok(events) if !events.is_empty() => {
                                    tracing::info!("MetricsAggregator@0x{:x} received {} events", agg_ptr, events.len());
                                    
                                    // Process events
                                    for envelope in events {
                                        match aggregator_clone.lock() {
                                            Ok(mut boxed_agg) => {
                                                tracing::info!("Successfully locked aggregator, type is Box<dyn MetricsAggregator>");
                                                // Need to deref the Box to get to the trait object
                                                boxed_agg.as_mut().process_event(&envelope);
                                                tracing::info!("process_event returned");
                                            }
                                            Err(e) => {
                                                tracing::error!("Failed to lock aggregator: {}", e);
                                                break;
                                            }
                                        }
                                    }
                                }
                                Ok(_) => {
                                    // Empty batch
                                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                                }
                                Err(e) => {
                                    tracing::error!("Subscription error: {}", e);
                                    break;
                                }
                            }
                        }
                        
                        // Push snapshots periodically
                        _ = snapshot_interval.tick() => {
                            tracing::info!("MetricsAggregator@0x{:x} pushing snapshot to exporter", agg_ptr);
                            
                            // Create snapshot
                            let snapshot = {
                                if let Ok(agg) = aggregator_clone.lock() {
                                    agg.create_snapshot()
                                } else {
                                    tracing::error!("Failed to lock aggregator for snapshot");
                                    continue;
                                }
                            };
                            
                            // Push to exporter
                            if let Err(e) = exporter_clone.update_app_metrics(snapshot) {
                                tracing::warn!("Failed to push app metrics snapshot: {}", e);
                            } else {
                                tracing::debug!("Successfully pushed metrics snapshot");
                            }
                        }
                    }
                }
            });
            
            self.app_metrics_task_handle = Some(handle);
        }

        // Trigger FSM to materialize
        let actions = self.pipeline_fsm.handle(
            PipelineEvent::Materialize,
            self.pipeline_context.clone()
        ).await?;
        
        // Handle actions
        for action in actions {
            self.handle_action(action).await?;
        }
        
        // Complete materialization
        let actions = self.pipeline_fsm.handle(
            PipelineEvent::MaterializationComplete,
            self.pipeline_context.clone()
        ).await?;
        
        for action in actions {
            self.handle_action(action).await?;
        }
        
        Ok(())
    }
    
    /// Run the pipeline (starts sources)
    pub async fn run(&mut self) -> Result<(), String> {
        let actions = self.pipeline_fsm.handle(
            PipelineEvent::Run,
            self.pipeline_context.clone()
        ).await?;
        
        for action in actions {
            self.handle_action(action).await?;
        }
        
        // Wait for completion - the completion tracker writes to journal when all stages complete
        // For finite sources, we need to wait for all stages to complete naturally
        if let Some(handle) = self.completion_tracker_handle.take() {
            // The completion tracker exits when all stages complete
            let _ = handle.await;
        }
        
        Ok(())
    }
    
    /// Shutdown the pipeline gracefully
    pub async fn shutdown(&mut self) -> Result<(), String> {
        let actions = self.pipeline_fsm.handle(
            PipelineEvent::Shutdown,
            self.pipeline_context.clone()
        ).await?;
        
        for action in actions {
            self.handle_action(action).await?;
        }
        
        // Stop app metrics collection
        if let Some(handle) = self.app_metrics_task_handle.take() {
            handle.abort();
        }
        
        Ok(())
    }
    
    /// Force shutdown the pipeline
    pub async fn force_shutdown(&mut self, reason: &str) -> Result<(), String> {
        let actions = self.pipeline_fsm.handle(
            PipelineEvent::Error { message: reason.to_string() },
            self.pipeline_context.clone()
        ).await?;
        
        for action in actions {
            self.handle_action(action).await?;
        }
        
        // Stop completion tracker
        if let Some(handle) = self.completion_tracker_handle.take() {
            handle.abort();
        }
        
        // Stop app metrics collection
        if let Some(handle) = self.app_metrics_task_handle.take() {
            handle.abort();
        }
        
        Ok(())
    }
    
    /// Get current pipeline state
    pub fn pipeline_state(&self) -> &PipelineState {
        self.pipeline_fsm.state()
    }

    /// Handle pipeline actions
    async fn handle_action(&mut self, action: PipelineAction) -> Result<(), String> {
        match action {
            PipelineAction::CreateStages => {
                self.create_all_stages().await?;
            }
            PipelineAction::NotifyStagesStart => {
                // Non-source stages now start automatically when initialized and ready
                // This action is a no-op since stages handle their own lifecycle
                tracing::debug!("NotifyStagesStart: Non-source stages start automatically");
            }
            PipelineAction::NotifySourceStart => {
                // Find the source stage (stage with no upstreams) and trigger FSM transition
                let source_id = self.stage_supervisors.iter()
                    .find(|(stage_id, _)| self.pipeline_context.topology.upstream_stages(**stage_id).is_empty())
                    .map(|(stage_id, _)| *stage_id);
                    
                if let Some(source_id) = source_id {
                    tracing::info!("Starting source stage: {:?}", source_id);
                    
                    // Get the source supervisor and send Start event
                    if let Some(stage) = self.stage_supervisors.get_mut(&source_id) {
                        stage.start().await
                            .map_err(|e| format!("Failed to start source stage: {}", e))?;
                    }
                }
            }
            PipelineAction::BeginDrain => {
                self.message_bus.send_stage_command(
                    crate::message_bus::StageCommand::BeginDrain
                ).await.map_err(|e| format!("Failed to send drain command: {:?}", e))?;
            }
            PipelineAction::Cleanup => {
                // Cleanup will be handled by supervisor drop
            }
        }
        Ok(())
    }
    
    /// Initialize all stage supervisors
    async fn create_all_stages(&mut self) -> Result<(), String> {
        // Stages are already created as BoxedStageHandle instances
        // We just need to move them into our supervisor map and initialize them
        
        for mut stage in self.pipeline.stages.drain(..) {
            let stage_id = stage.stage_id();
            let stage_name = stage.stage_name().to_string();
            
            tracing::info!("Initializing stage: {} (id: {:?})", stage_name, stage_id);
            
            // Initialize the stage
            stage.initialize().await
                .map_err(|e| format!("Failed to initialize stage {}: {}", stage_name, e))?;
            
            // Store supervisor
            self.stage_supervisors.insert(stage_id, stage);
            
            tracing::info!("Stage {} initialized", stage_name);
        }
        
        tracing::info!("All {} stages initialized successfully", self.stage_supervisors.len());
        Ok(())
    }
    
    /// Start journal-based completion tracking
    async fn start_completion_tracker(&mut self) -> Result<(), String> {
        // Create subscription for system control events from all stages
        let filter = crate::event_flow::reactive_journal::SubscriptionFilter::EventTypes {
            event_types: vec![
                "system.stage.completed".to_string(),
                "system.stage.failed".to_string(),
            ],
        };
        
        let mut subscription = self.reactive_journal.subscribe(filter).await
            .map_err(|e| format!("Failed to create control subscription: {:?}", e))?;
            
        let pipeline_context = self.pipeline_context.clone();
        let reactive_journal = self.reactive_journal.clone();
        
        let handle = tokio::spawn(async move {
            // Get all stage IDs from topology
            let expected_stages: std::collections::HashSet<StageId> = pipeline_context.topology.stages()
                .map(|info| info.id)
                .collect();
            let total_stages = expected_stages.len();
            
            tracing::info!("Pipeline completion tracker started - expecting {} stages to complete: {:?}", 
                total_stages, expected_stages);
            
            // Track flow name from first completed stage
            let mut flow_name: Option<String> = None;
            
            loop {
                match subscription.recv_batch().await {
                    Ok(events) if !events.is_empty() => {
                        tracing::debug!("Completion tracker received {} events", events.len());
                        for envelope in events {
                            let event = envelope.event;
                            tracing::debug!("Completion tracker processing event type: {}", event.event_type);
                            match event.event_type.as_str() {
                                // TODO add proper event types, similar to EOF
                                "system.stage.completed" => {
                                    // Get stage info from the event
                                    let stage_name = event.payload.get("stage_name")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("unknown");
                                    
                                    let _stage_id_str = event.payload.get("stage_id")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("unknown");
                                    
                                    // Get stage ID from the writer registry
                                    if let Some(writer_info) = reactive_journal.get_writer_info(&envelope.writer_id).await {
                                        let stage_id = writer_info.stage_id;
                                        
                                        tracing::info!("Stage completed: {} ({})", stage_name, stage_id);
                                        
                                        // Capture flow name from first completed stage event
                                        if flow_name.is_none() && !event.flow_context.flow_name.is_empty() {
                                            flow_name = Some(event.flow_context.flow_name.clone());
                                        }
                                        
                                        // Add to completed stages
                                        let mut completed = pipeline_context.completed_stages.write().await;
                                        if !completed.contains(&stage_id) {
                                            completed.push(stage_id);
                                        }
                                        
                                        // Check if this stage was expected
                                        if !expected_stages.contains(&stage_id) {
                                            tracing::warn!("Received completion for unexpected stage: {} ({})", stage_name, stage_id);
                                        }
                                        
                                        tracing::debug!("Completion tracker: {} stages completed. Expected count: {}, Expected IDs: {:?}, Completed IDs: {:?}", 
                                            completed.len(), total_stages, expected_stages, completed);
                                            
                                        if completed.len() >= total_stages {
                                            tracing::info!("All {} stages have completed!", total_stages);
                                            
                                            // Write pipeline completion event to journal  
                                            let pipeline_stage_id = StageId::new(); // Pipeline gets its own unique ID
                                            let writer_id = reactive_journal.register_writer(pipeline_stage_id, None)
                                                .await
                                                .unwrap_or_else(|e| panic!("FATAL: Failed to register pipeline writer: {:?}", e));
                                            
                                            let mut pipeline_completed = ChainEvent::new(
                                                EventId::new(),
                                                writer_id.clone(),
                                                "system.pipeline.completed",
                                                serde_json::json!({
                                                    "timestamp": chrono::Utc::now().to_rfc3339(),
                                                    "completed_stages": completed.len(),
                                                })
                                            );
                                            
                                            // Set flow context for pipeline completion event
                                            pipeline_completed.flow_context = obzenflow_core::event::flow_context::FlowContext {
                                                flow_name: flow_name.clone().unwrap_or_else(|| "unknown".to_string()),
                                                flow_id: "default".to_string(),
                                                stage_name: "pipeline".to_string(),
                                                stage_type: obzenflow_core::event::flow_context::StageType::Transform,
                                            };
                                            
                                            if let Err(e) = reactive_journal.write(&writer_id, pipeline_completed, None).await {
                                                panic!("FATAL: Failed to write pipeline completion event: {}", e);
                                            }
                                            
                                            // Exit the completion tracker task
                                            return;
                                        }
                                    } else {
                                        tracing::warn!("Stage completed event from unknown writer: {:?}", envelope.writer_id);
                                    }
                                }
                                // TODO add proper event types, similar to EOF
                                "system.stage.failed" => {
                                    let stage_name = event.payload.get("stage_name")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("unknown");
                                    let error = event.payload.get("error")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("Unknown error");
                                        
                                    // No recovery from stage failure - panic immediately
                                    panic!("FATAL: Stage '{}' failed: {}", stage_name, error);
                                }
                                _ => {}
                            }
                        }
                    }
                    Ok(_) => {
                        // Empty batch
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
                    Err(e) => {
                        // Fatal error - can't monitor completion without subscription
                        panic!("FATAL: Control subscription error - pipeline cannot track completion: {}", e);
                    }
                }
            }
        });
        
        self.completion_tracker_handle = Some(handle);
        Ok(())
    }
}

/// Flow handle for external control
pub struct FlowHandle {
    supervisor: Arc<RwLock<PipelineSupervisor>>,
}

impl FlowHandle {
    pub fn new(supervisor: Arc<RwLock<PipelineSupervisor>>) -> Self {
        Self { supervisor }
    }
    
    /// Start the pipeline (triggers sources)
    pub async fn run(&self) -> Result<(), FlowError> {
        let mut supervisor = self.supervisor.write().await;
        supervisor.run().await
            .map_err(|e| FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                e
            ))))
    }
    
    /// Graceful shutdown
    pub async fn shutdown(&self) -> Result<(), FlowError> {
        let mut supervisor = self.supervisor.write().await;
        supervisor.shutdown().await
            .map_err(|e| FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                e
            ))))
    }
    
    /// Force shutdown
    pub async fn abort(&self, reason: &str) -> Result<(), FlowError> {
        let mut supervisor = self.supervisor.write().await;
        supervisor.force_shutdown(reason).await
            .map_err(|e| FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                e
            ))))
    }
    
    /// Query current state
    pub async fn state(&self) -> PipelineState {
        let supervisor = self.supervisor.read().await;
        supervisor.pipeline_state().clone()
    }
    
    /// Render metrics in Prometheus format
    pub async fn render_metrics(&self) -> Result<String, FlowError> {
        let supervisor = self.supervisor.read().await;
        
        if let Some(ref exporter) = supervisor.metrics_exporter {
            exporter.render_metrics()
                .map_err(|e| FlowError::ExecutionFailed(Box::new(
                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                )))
        } else {
            Err(FlowError::ExecutionFailed(Box::new(
                std::io::Error::new(std::io::ErrorKind::Other, "No metrics exporter configured")
            )))
        }
    }
}

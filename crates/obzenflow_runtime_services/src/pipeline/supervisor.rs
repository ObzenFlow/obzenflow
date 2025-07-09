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
use obzenflow_core::{ChainEvent, EventId, WriterId, Journal};
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
    
    /// Subscription for stage completion events
    completion_subscription: Option<crate::event_flow::JournalSubscription>,
    
    /// Metrics exporter for accessing aggregated metrics (FLOWIP-056-666)
    /// Uses trait object to avoid runtime→adapters dependency
    metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
    
    /// Writer ID for journal events
    writer_id: Option<WriterId>,
}

impl PipelineSupervisor {
    /// Create a new pipeline supervisor
    pub fn new(
        topology: Arc<obzenflow_topology_services::topology::Topology>, 
        reactive_journal: Arc<ReactiveJournal>,
        stages: Vec<BoxedStageHandle>,
        metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
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
            completion_subscription: None,
            metrics_exporter,
            writer_id: None,
        })
    }
    
    /// Materialize the pipeline (creates stages)
    pub async fn materialize(&mut self) -> Result<(), String> {
        // Register as a writer first
        let stage_id = StageId::new();
        self.writer_id = Some(
            self.reactive_journal
                .register_writer(stage_id, None)
                .await
                .map_err(|e| format!("Failed to register pipeline writer: {}", e))?
        );

        // Start metrics aggregator if we have an exporter
        if let Some(exporter) = self.metrics_exporter.clone() {
            let journal = self.reactive_journal.clone();
            
            // Subscribe for metrics ready event before spawning
            let mut ready_subscription = self.reactive_journal
                .subscribe(crate::event_flow::SubscriptionFilter::EventTypes { 
                    event_types: vec!["system.metrics.ready".to_string()] 
                })
                .await
                .map_err(|e| format!("Failed to subscribe for metrics ready: {}", e))?;
            
            // Spawn metrics aggregator
            tokio::spawn(async move {
                let mut metrics = crate::metrics::MetricsAggregatorSupervisor::new(
                    journal,
                    Some(exporter),
                    10, // 10 second export interval
                );
                if let Err(e) = metrics.start().await {
                    tracing::error!("Metrics aggregator failed: {}", e);
                }
            });
            
            // Wait for metrics aggregator to be ready
            match tokio::time::timeout(
                tokio::time::Duration::from_secs(5),
                ready_subscription.recv_batch()
            ).await {
                Ok(Ok(batch)) if !batch.is_empty() => {
                    tracing::info!("Metrics aggregator is ready");
                }
                Ok(Ok(_)) => {
                    return Err("No metrics ready event received".to_string());
                }
                Ok(Err(e)) => {
                    return Err(format!("Failed to receive metrics ready event: {}", e));
                }
                Err(_) => {
                    return Err("Timeout waiting for metrics aggregator to be ready".to_string());
                }
            }
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
        
        // Start processing stage completion events
        self.start_completion_subscription().await?;
        
        // Process stage completion events until pipeline is drained
        // This is event-driven, not polling
        loop {
            // Process completion events (this blocks until events arrive)
            self.process_completion_events().await?;
            
            // Check if we've transitioned to a terminal state
            match self.pipeline_fsm.state() {
                PipelineState::Drained => {
                    tracing::info!("Pipeline has reached Drained state");
                    break;
                }
                PipelineState::Failed { reason } => {
                    return Err(format!("Pipeline failed: {}", reason));
                }
                _ => {
                    // Continue processing events
                }
            }
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
        
        // Request metrics drain if we have metrics running
        if self.metrics_exporter.is_some() {
            self.drain_metrics().await?;
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
        
        // Stop completion subscription
        self.completion_subscription = None;
        
        Ok(())
    }
    
    /// Get current pipeline state
    pub fn pipeline_state(&self) -> &PipelineState {
        self.pipeline_fsm.state()
    }
    
    /// Drain metrics aggregator and wait for completion
    async fn drain_metrics(&mut self) -> Result<(), String> {
        tracing::info!("Requesting metrics drain via journal");
        
        let writer_id = self.writer_id.clone()
            .ok_or_else(|| "No writer ID available".to_string())?;
        
        // 1. Publish drain request to journal
        let drain_event = ChainEvent::new(
            EventId::new(),
            writer_id,
            "system.metrics.drain",
            serde_json::json!({})
        );
        
        self.reactive_journal.append(&writer_id, drain_event, None).await
            .map_err(|e| format!("Failed to publish drain event: {}", e))?;
        
        // 2. Subscribe and wait for drain completion event
        let mut subscription = self.reactive_journal
            .subscribe(crate::event_flow::SubscriptionFilter::EventTypes { 
                event_types: vec!["system.metrics.drained".to_string()] 
            })
            .await
            .map_err(|e| format!("Failed to subscribe for drain completion: {}", e))?;
        
        match tokio::time::timeout(
            tokio::time::Duration::from_secs(30),
            subscription.recv_batch()
        ).await {
            Ok(Ok(batch)) if !batch.is_empty() => {
                tracing::info!("Metrics successfully drained");
                Ok(())
            }
            Ok(Ok(_)) => Err("No drain completion event received".to_string()),
            Ok(Err(e)) => Err(format!("Failed to receive drain completion: {}", e)),
            Err(_) => Err("Metrics drain timeout".to_string())
        }
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
                // Publish drain event to journal for stages to pick up
                let writer_id = self.writer_id.clone()
                    .ok_or_else(|| "No writer ID available".to_string())?;
                
                let drain_event = ChainEvent::new(
                    EventId::new(),
                    writer_id.clone(),
                    "system.pipeline.drain",
                    serde_json::json!({
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    })
                );
                
                self.reactive_journal.append(&writer_id, drain_event, None).await
                    .map_err(|e| format!("Failed to publish drain event: {}", e))?;
                    
                tracing::info!("Published drain event to journal");
            }
            PipelineAction::WritePipelineCompleted => {
                self.write_pipeline_completed_event().await?;
            }
            PipelineAction::Cleanup => {
                // Cleanup will be handled by supervisor drop
            }
        }
        Ok(())
    }
    
    /// Write pipeline completed event to journal
    async fn write_pipeline_completed_event(&self) -> Result<(), String> {
        // Get flow name from context or use default
        let flow_name = "default"; // TODO: Get from first completed stage event
        
        // Register writer for pipeline
        let pipeline_stage_id = StageId::new();
        let writer_id = self.reactive_journal.register_writer(pipeline_stage_id, None)
            .await
            .map_err(|e| format!("Failed to register pipeline writer: {:?}", e))?;
        
        let mut pipeline_completed = ChainEvent::new(
            EventId::new(),
            writer_id.clone(),
            "system.pipeline.completed",
            serde_json::json!({
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "completed_stages": self.stage_supervisors.len(),
            })
        );
        
        // Set flow context for pipeline completion event
        pipeline_completed.flow_context = obzenflow_core::event::flow_context::FlowContext {
            flow_name: flow_name.to_string(),
            flow_id: "default".to_string(),
            stage_name: "pipeline".to_string(),
            stage_type: obzenflow_core::event::flow_context::StageType::Transform,
        };
        
        self.reactive_journal.write(&writer_id, pipeline_completed, None).await
            .map_err(|e| format!("Failed to write pipeline completion event: {}", e))?;
            
        tracing::info!("Pipeline completion event written");
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
    
    /// Start subscription for stage completion events
    async fn start_completion_subscription(&mut self) -> Result<(), String> {
        // Create subscription for system control events from all stages
        let filter = crate::event_flow::reactive_journal::SubscriptionFilter::EventTypes {
            event_types: vec![
                "system.stage.completed".to_string(),
                "system.stage.failed".to_string(),
            ],
        };
        
        let subscription = self.reactive_journal.subscribe(filter).await
            .map_err(|e| format!("Failed to create control subscription: {:?}", e))?;
            
        self.completion_subscription = Some(subscription);
        
        tracing::info!("Started subscription for stage completion events");
        Ok(())
    }
    
    /// Process completion events from the subscription
    async fn process_completion_events(&mut self) -> Result<(), String> {
        let Some(ref mut subscription) = self.completion_subscription else {
            return Ok(());
        };
        
        // Receive events (blocks until events are available)
        match subscription.recv_batch().await {
            Ok(events) if !events.is_empty() => {
                for envelope in events {
                    let event = &envelope.event;
                    match event.event_type.as_str() {
                        "system.stage.completed" => {
                            self.handle_stage_completed(&envelope).await?;
                        }
                        "system.stage.failed" => {
                            let stage_name = event.payload.get("stage_name")
                                .and_then(|v| v.as_str())
                                .unwrap_or("unknown");
                            let error = event.payload.get("error")
                                .and_then(|v| v.as_str())
                                .unwrap_or("Unknown error");
                            
                            // Trigger error event on FSM
                            let _ = self.pipeline_fsm.handle(
                                PipelineEvent::Error { message: format!("Stage '{}' failed: {}", stage_name, error) },
                                self.pipeline_context.clone()
                            ).await?;
                        }
                        _ => {}
                    }
                }
            }
            Ok(_) => {
                // No events available
            }
            Err(e) => {
                tracing::error!("Failed to receive completion events: {:?}", e);
                return Err(format!("Subscription error: {:?}", e));
            }
        }
        
        Ok(())
    }
    
    /// Handle stage completed event
    async fn handle_stage_completed(&mut self, envelope: &obzenflow_core::EventEnvelope) -> Result<(), String> {
        let event = &envelope.event;
        
        // Get stage info from the event
        let stage_name = event.payload.get("stage_name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        
        // Get stage ID from the writer registry
        if let Some(writer_info) = self.reactive_journal.get_writer_info(&envelope.writer_id).await {
            let stage_id = writer_info.stage_id;
            
            tracing::info!("Stage completed: {} ({})", stage_name, stage_id);
            
            // Check if this is the source stage completing (triggers Jonestown protocol)
            if self.pipeline_context.topology.upstream_stages(stage_id).is_empty() {
                tracing::info!("Source stage completed - initiating Jonestown protocol");
                
                // Transition to draining state when source completes
                let actions = self.pipeline_fsm.handle(
                    PipelineEvent::Shutdown,
                    self.pipeline_context.clone()
                ).await?;
                
                for action in actions {
                    self.handle_action(action).await?;
                }
            }
            
            // Add to completed stages
            let mut completed = self.pipeline_context.completed_stages.write().await;
            if !completed.contains(&stage_id) {
                completed.push(stage_id);
            }
            
            // Check if all expected stages have completed
            let expected_stages: std::collections::HashSet<StageId> = self.pipeline_context.topology.stages()
                .map(|info| info.id)
                .collect();
            let total_stages = expected_stages.len();
            
            tracing::debug!("Stage completion: {} of {} stages completed", completed.len(), total_stages);
            
            if completed.len() >= total_stages {
                tracing::info!("All {} stages have completed!", total_stages);
                drop(completed); // Release the lock
                
                // Drain metrics
                if self.metrics_exporter.is_some() {
                    self.drain_metrics().await?;
                }
                
                // Trigger FSM transition
                let actions = self.pipeline_fsm.handle(
                    PipelineEvent::AllStagesCompleted,
                    self.pipeline_context.clone()
                ).await?;
                
                // Process the actions
                for action in actions {
                    self.handle_action(action).await?;
                }
            }
        } else {
            tracing::warn!("Stage completed event from unknown writer: {:?}", envelope.writer_id);
        }
        
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

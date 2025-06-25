//! Pipeline Supervisor - Manages the lifecycle of a pipeline and its stages
//!
//! The supervisor pattern provides:
//! - Hierarchical ownership of FSMs
//! - Message bus for inter-FSM communication
//! - Clean separation between supervision and business logic

use crate::control_plane::pipeline_supervisor::pipeline::pipeline::Pipeline;
use crate::control_plane::pipeline_supervisor::pipeline_fsm::{
    PipelineState, PipelineEvent, PipelineAction, PipelineContext, PipelineFsm, build_pipeline_fsm
};
use crate::control_plane::stage_supervisor::stage_supervisor::{StageSupervisor, StageConfig};
use crate::control_plane::stage_supervisor::stage_fsm::StageEvent;
use crate::data_plane::journal_subscription::ReactiveJournal;
use crate::message_bus::{FsmMessageBus, PipelineInternalEvent, StageNotification};
use crate::errors::{FlowError, PipelineSupervisorError};
use obzenflow_topology_services::stages::StageId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, mpsc};

/// Pipeline supervisor - manages the lifecycle of a pipeline
pub struct PipelineSupervisor {
    /// The pipeline FSM
    pipeline_fsm: PipelineFsm,
    
    /// Pipeline context
    pipeline_context: Arc<PipelineContext>,
    
    /// Message bus for inter-FSM communication
    message_bus: Arc<FsmMessageBus>,
    
    /// Stage supervisors by ID
    stage_supervisors: HashMap<StageId, Arc<RwLock<StageSupervisor>>>,
    
    /// Pipeline configuration
    pipeline: Pipeline,
    
    /// Reactive journal wrapper
    reactive_journal: Arc<ReactiveJournal>,
    
    /// Message routing task handle
    router_handle: Option<tokio::task::JoinHandle<()>>,
    
    /// Receivers from message bus (taken during construction)
    pipeline_inbox_rx: Option<mpsc::Receiver<PipelineInternalEvent>>,
    stage_events_rx: Option<mpsc::Receiver<(StageId, StageNotification)>>,
}

impl PipelineSupervisor {
    /// Create a new pipeline supervisor
    pub fn new(pipeline: Pipeline) -> Result<Self, PipelineSupervisorError> {
        // Create message bus and take receivers before Arc
        let mut message_bus = FsmMessageBus::new();
        let pipeline_inbox_rx = message_bus.take_pipeline_inbox_receiver()
            .map_err(|e| PipelineSupervisorError::MessageBus {
                operation: "take pipeline inbox receiver".to_string(),
                source: e,
            })?;
        let stage_events_rx = message_bus.take_stage_events_receiver()
            .map_err(|e| PipelineSupervisorError::MessageBus {
                operation: "take stage events receiver".to_string(),
                source: e,
            })?;
        
        // Now wrap in Arc
        let message_bus = Arc::new(message_bus);
        
        // Create reactive journal wrapper
        let reactive_journal = Arc::new(ReactiveJournal::new(pipeline.journal.clone()));
        
        // Create pipeline context
        let pipeline_context = Arc::new(PipelineContext {
            bus: message_bus.clone(),
            topology: pipeline.topology.clone(),
            journal: pipeline.journal.clone(),
            completed_stages: Arc::new(RwLock::new(Vec::new())),
        });
        
        // Build pipeline FSM
        let pipeline_fsm = build_pipeline_fsm();
        
        Ok(Self {
            pipeline_fsm,
            pipeline_context,
            message_bus,
            stage_supervisors: HashMap::new(),
            pipeline,
            reactive_journal,
            router_handle: None,
            pipeline_inbox_rx: Some(pipeline_inbox_rx),
            stage_events_rx: Some(stage_events_rx),
        })
    }
    
    /// Materialize the pipeline (creates stages)
    pub async fn materialize(&mut self) -> Result<(), String> {
        // Start message router
        self.start_message_router().await;
        
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
        
        // Stop message router
        if let Some(handle) = self.router_handle.take() {
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
                // Start all non-source stages
                // For now, send to all stages but sources will ignore if in WaitingForGun state
                self.message_bus.send_stage_command(
                    crate::message_bus::StageCommand::Start
                ).await.map_err(|e| format!("Failed to send start command: {:?}", e))?;
            }
            PipelineAction::NotifySourceStart => {
                // Find the source stage (stage with no upstreams) and trigger FSM transition
                let source_id = self.stage_supervisors.iter()
                    .find(|(stage_id, _)| self.pipeline_context.topology.upstream_stages(**stage_id).is_empty())
                    .map(|(stage_id, _)| *stage_id);
                    
                if let Some(source_id) = source_id {
                    tracing::info!("Starting source stage: {:?}", source_id);
                    
                    // Get the source supervisor and send Start event through FSM
                    if let Some(supervisor_arc) = self.stage_supervisors.get(&source_id) {
                        let mut supervisor = supervisor_arc.write().await;
                        supervisor.process_event(StageEvent::Start).await
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
    
    /// Create all stage supervisors
    async fn create_all_stages(&mut self) -> Result<(), String> {
        use crate::control_plane::stage_supervisor::stage_supervisor::{StageSupervisor, StageConfig};
        use crate::control_plane::stage_supervisor::stage_fsm::StageEvent;
        use crate::control_plane::pipeline_supervisor::in_flight_tracker::InFlightTracker;
        
        // Create in-flight tracker for all stages
        let stage_ids: Vec<StageId> = self.pipeline.stages.iter()
            .map(|s| s.stage_id)
            .collect();
        let in_flight_tracker = Arc::new(InFlightTracker::new(stage_ids));
        
        // Create start barrier for source synchronization
        let source_count = self.pipeline.stages.iter()
            .filter(|s| s.is_source)
            .count();
        let start_barrier = if source_count > 0 {
            Some(Arc::new(tokio::sync::Barrier::new(source_count)))
        } else {
            None
        };
        
        // Create each stage supervisor
        for stage_config in &self.pipeline.stages {
            tracing::info!("Creating stage supervisor for: {} (id: {:?})", 
                stage_config.name, stage_config.stage_id);
            
            // Create stage configuration
            let config = StageConfig {
                stage_id: stage_config.stage_id,
                stage_name: stage_config.name.clone(),
                handler: stage_config.handler.clone(),
                topology: self.pipeline.topology.clone(),
                journal: self.reactive_journal.clone(),
                in_flight_tracker: in_flight_tracker.clone(),
                message_bus: self.message_bus.clone(),
                start_barrier: if stage_config.is_source { 
                    start_barrier.clone() 
                } else { 
                    None 
                },
            };
            
            // Create supervisor
            let mut supervisor = StageSupervisor::new(config);
            
            // Initialize the stage
            supervisor.process_event(StageEvent::Initialize).await
                .map_err(|e| format!("Failed to initialize stage {}: {}", stage_config.name, e))?;
            
            // Mark as ready
            supervisor.process_event(StageEvent::Ready).await
                .map_err(|e| format!("Failed to mark stage {} as ready: {}", stage_config.name, e))?;
            
            // Store supervisor
            self.stage_supervisors.insert(
                stage_config.stage_id, 
                Arc::new(RwLock::new(supervisor))
            );
            
            tracing::info!("Stage {} created and initialized", stage_config.name);
        }
        
        tracing::info!("All {} stages created successfully", self.stage_supervisors.len());
        Ok(())
    }
    
    /// Start the message router task
    async fn start_message_router(&mut self) {
        // Take the receiver from self
        let stage_events_rx = self.stage_events_rx.take()
            .expect("Stage events receiver already taken");
            
        let pipeline_context = self.pipeline_context.clone();
        let message_bus = self.message_bus.clone();
        
        let handle = tokio::spawn(async move {
            let mut stage_events_rx = stage_events_rx;
            while let Some((stage_id, notification)) = stage_events_rx.recv().await {
                // Route stage notifications to pipeline FSM
                match notification {
                    StageNotification::Completed { .. } => {
                        // Check if all stages completed
                        let mut completed = pipeline_context.completed_stages.write().await;
                        completed.push(stage_id);
                        
                        let total_stages = pipeline_context.topology.stages().count();
                        if completed.len() >= total_stages {
                            // All stages completed - send event via message bus
                            let _ = message_bus.send_pipeline_event(
                                PipelineInternalEvent::AllStagesReady
                            ).await;
                        }
                    }
                    StageNotification::Failed { error: _ } => {
                        // Send error event via message bus
                        let _ = message_bus.send_pipeline_event(
                            PipelineInternalEvent::StageReady { stage_id }
                        ).await;
                    }
                    _ => {
                        // Other notifications can be logged or handled as needed
                    }
                }
            }
        });
        
        self.router_handle = Some(handle);
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
}
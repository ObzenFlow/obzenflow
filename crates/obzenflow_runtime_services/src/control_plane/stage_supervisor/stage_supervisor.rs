//! Stage Supervisor - Manages a single stage using our FSM
//!
//! The supervisor is responsible for:
//! - Managing the stage FSM lifecycle
//! - Handling actions produced by FSM transitions
//! - Coordinating with the pipeline supervisor

use crate::control_plane::stage_supervisor::stage_fsm::{
    StageState, StageEvent, StageAction, StageContext, StageFsm, build_stage_fsm
};
use crate::control_plane::stage_supervisor::event_handler::{EventHandler, ProcessingMode};
use crate::data_plane::journal_subscription::{ReactiveJournal, JournalSubscription, SubscriptionFilter, SubscriptionEvent};
use crate::control_plane::pipeline_supervisor::in_flight_tracker::InFlightTracker;
use crate::message_bus::{FsmMessageBus, StageNotification, StageCommand};
use obzenflow_topology_services::topology::Topology;
use obzenflow_topology_services::stages::StageId;
use std::sync::Arc;
use tokio::sync::{RwLock, Barrier};
use tokio::task::JoinHandle;
use std::time::Duration;
use serde_json::json;

/// Configuration for creating a stage supervisor
pub struct StageConfig {
    pub stage_id: StageId,
    pub stage_name: String,
    pub handler: Arc<dyn EventHandler>,
    pub topology: Arc<Topology>,
    pub journal: Arc<ReactiveJournal>,
    pub in_flight_tracker: Arc<InFlightTracker>,
    pub message_bus: Arc<FsmMessageBus>,
    pub start_barrier: Option<Arc<Barrier>>,
}

/// Stage supervisor - manages a single stage with our FSM
pub struct StageSupervisor {
    /// The FSM managing state
    fsm: StageFsm,
    
    /// Stage context
    context: Arc<StageContext>,
    
    /// Stage identity
    stage_id: StageId,
    stage_name: String,
    
    /// Subscription to upstream events
    subscription: Arc<RwLock<Option<JournalSubscription>>>,
    
    /// Processing task handle
    task: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl StageSupervisor {
    /// Create a new stage supervisor
    pub fn new(config: StageConfig) -> Self {
        let is_source = config.topology.upstream_stages(config.stage_id).is_empty();
        
        // Create context
        let context = Arc::new(StageContext {
            bus: config.message_bus.clone(),
            stage_id: config.stage_id,
            stage_name: config.stage_name.clone(),
            journal: config.journal,
            handler: config.handler,
            topology: config.topology,
            in_flight: config.in_flight_tracker,
            start_barrier: config.start_barrier,
            completed_upstreams: Arc::new(RwLock::new(Vec::new())),
            writer_id: Arc::new(RwLock::new(None)),
        });
        
        // Build FSM
        let fsm = build_stage_fsm(is_source);
        
        Self {
            fsm,
            context,
            stage_id: config.stage_id,
            stage_name: config.stage_name,
            subscription: Arc::new(RwLock::new(None)),
            task: Arc::new(RwLock::new(None)),
        }
    }
    
    /// Get current state
    pub fn state(&self) -> &StageState {
        self.fsm.state()
    }
    
    
    /// Process an event
    pub async fn process_event(&mut self, event: StageEvent) -> Result<(), String> {
        let actions = self.fsm.handle(event, self.context.clone()).await?;
        
        // Handle actions
        for action in actions {
            self.handle_action(action).await?;
        }
        
        Ok(())
    }
    
    /// Handle actions from FSM transitions
    async fn handle_action(&mut self, action: StageAction) -> Result<(), String> {
        match action {
            StageAction::CreateSubscriptions => {
                tracing::info!("[{}] Creating subscriptions", self.stage_name);
                self.create_subscriptions().await?;
            }
            
            StageAction::StartProcessing => {
                tracing::info!("[{}] Starting event processing", self.stage_name);
                self.start_processing().await?;
            }
            
            StageAction::StopAcceptingEvents => {
                tracing::info!("[{}] Stopping event acceptance", self.stage_name);
                // Subscription will handle this via EOF signals
            }
            
            StageAction::SignalEOF { natural } => {
                tracing::info!("[{}] Signaling EOF (natural: {})", self.stage_name, natural);
                self.context.journal.signal_stage_complete(self.stage_id, natural).await
                    .map_err(|e| e.to_string())?;
            }
            
            StageAction::WaitForInFlight => {
                tracing::info!("[{}] Waiting for in-flight events to drain", self.stage_name);
                self.monitor_in_flight_drain().await;
            }
            
            StageAction::CleanupResources => {
                tracing::info!("[{}] Cleaning up stage resources", self.stage_name);
                self.cleanup().await;
            }
            
            StageAction::NotifyPipeline(notification) => {
                self.context.bus.send_stage_notification(
                    self.stage_id, 
                    notification
                ).await.map_err(|e| format!("Failed to send notification: {:?}", e))?;
            }
        }
        
        Ok(())
    }
    
    /// Create subscriptions to upstream stages
    async fn create_subscriptions(&self) -> Result<(), String> {
        // Register writer for this stage
        let writer_id = self.context.journal.register_writer(self.stage_id, None).await
            .map_err(|e| format!("Failed to register writer: {:?}", e))?;
        
        *self.context.writer_id.write().await = Some(writer_id);
        
        let upstreams = self.context.topology.upstream_stages(self.stage_id);
        
        if !upstreams.is_empty() {
            let filter = SubscriptionFilter {
                upstream_stages: upstreams.to_vec(),
                event_types: None,
            };
            
            let subscription = self.context.journal.subscribe(filter).await
                .map_err(|e| format!("Failed to create subscription: {:?}", e))?;
                
            *self.subscription.write().await = Some(subscription);
        }
        
        Ok(())
    }
    
    /// Start the event processing loop
    async fn start_processing(&self) -> Result<(), String> {
        let stage_id = self.stage_id;
        let stage_name = self.stage_name.clone();
        let _handler = self.context.handler.clone();
        let subscription = self.subscription.clone();
        let _in_flight_handle = self.context.in_flight.stage_handle(stage_id);
        let mut commands_rx = self.context.bus.subscribe_to_stage_commands();
        let is_source = self.context.topology.upstream_stages(stage_id).is_empty();
        let journal = self.context.journal.clone();
        let writer_id_ref = self.context.writer_id.clone();
        
        let task = tokio::spawn(async move {
            // Process events
            loop {
                tokio::select! {
                    // Check for stage commands
                    Ok(command) = commands_rx.recv() => {
                        match command {
                            StageCommand::Start => {
                                tracing::info!("[{}] Received start command", stage_name);
                                // Sources need to be transitioned to Running state via FSM
                                // This will be handled by the supervisor, not here
                            }
                            StageCommand::BeginDrain => {
                                tracing::info!("[{}] Received drain command", stage_name);
                                break;
                            }
                            StageCommand::ForceShutdown { reason } => {
                                tracing::warn!("[{}] Forced shutdown: {}", stage_name, reason);
                                break;
                            }
                            _ => {}
                        }
                    }
                    
                    // Process events from subscription (or generate for sources)
                    _ = async {
                        if is_source {
                            // Sources generate events
                            let writer_id_opt = writer_id_ref.read().await.clone();
                            if let Some(writer_id) = writer_id_opt {
                                // Generate a dummy event to trigger the source
                                let trigger_event = obzenflow_core::ChainEvent::new(
                                    obzenflow_core::EventId::new(),
                                    writer_id.clone(),
                                    "trigger", 
                                    json!({})
                                );
                                let outputs = _handler.transform(trigger_event);
                                
                                // Write generated events
                                for output in outputs {
                                    if let Err(e) = journal.write(&writer_id, output, None).await {
                                        tracing::error!("[{}] Failed to write source event: {}", stage_name, e);
                                    }
                                }
                            }
                            // Small delay to avoid tight loop
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        } else if let Some(sub) = &mut *subscription.write().await {
                            match sub.recv_batch().await {
                                Ok(SubscriptionEvent::Events(events)) => {
                                    tracing::debug!("[{}] Processing batch of {} events", stage_name, events.len());
                                    
                                    // Track in-flight events
                                    for _ in &events {
                                        let _ = _in_flight_handle.event_entered().await;
                                    }
                                    
                                    // Process based on handler mode
                                    let processing_mode = _handler.processing_mode();
                                    match processing_mode {
                                        ProcessingMode::Transform => {
                                            // Get writer ID
                                            let writer_id_opt = writer_id_ref.read().await.clone();
                                            if let Some(writer_id) = writer_id_opt {
                                                for envelope in events {
                                                    let outputs = _handler.transform(envelope.event.clone());
                                                    
                                                    // Write outputs to journal
                                                    for output in outputs {
                                                        if let Err(e) = journal.write(&writer_id, output, Some(&envelope)).await {
                                                            tracing::error!("[{}] Failed to write transformed event: {}", stage_name, e);
                                                        }
                                                    }
                                                    
                                                    // Mark event as processed
                                                    let _ = _in_flight_handle.event_completed().await;
                                                }
                                            } else {
                                                tracing::error!("[{}] No writer ID available for transforms", stage_name);
                                                for _ in events {
                                                    let _ = _in_flight_handle.event_completed().await;
                                                }
                                            }
                                        }
                                        ProcessingMode::Observe => {
                                            for envelope in events {
                                                if let Err(e) = _handler.observe(&envelope.event) {
                                                    tracing::error!("[{}] Observer error: {}", stage_name, e);
                                                }
                                                
                                                // Mark event as processed
                                                let _ = _in_flight_handle.event_completed().await;
                                            }
                                        }
                                        ProcessingMode::Aggregate => {
                                            // Aggregate mode requires mutable access
                                            // For now, we log a warning
                                            tracing::warn!("[{}] Aggregate mode not yet supported with Arc<dyn EventHandler>", stage_name);
                                            for _ in events {
                                                let _ = _in_flight_handle.event_completed().await;
                                            }
                                        }
                                    }
                                }
                                Ok(SubscriptionEvent::EndOfStream { stage_id: upstream_id, natural_completion, .. }) => {
                                    tracing::info!("[{}] Upstream {} completed (natural: {})", 
                                        stage_name, upstream_id, natural_completion);
                                    // Upstream completion will be handled by the FSM
                                }
                                Ok(SubscriptionEvent::AllUpstreamsComplete) => {
                                    tracing::info!("[{}] All upstreams complete", stage_name);
                                    return;
                                }
                                Err(e) => {
                                    tracing::error!("[{}] Error receiving events: {}", stage_name, e);
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                            }
                        }
                    } => {}
                    
                    // Periodically check if we should stop
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        // Check for natural completion for sources
                        if is_source {
                            // TODO: Check if source has more data to emit
                        }
                    }
                }
            }
            
            tracing::info!("[{}] Event processing loop ended", stage_name);
        });
        
        *self.task.write().await = Some(task);
        Ok(())
    }
    
    /// Monitor in-flight drain
    async fn monitor_in_flight_drain(&self) {
        let stage_handle = self.context.in_flight.stage_handle(self.stage_id);
        let stage_name = self.stage_name.clone();
        let stage_id = self.stage_id;
        let bus = self.context.bus.clone();
        
        tokio::spawn(async move {
            loop {
                if stage_handle.is_drained().await {
                    tracing::info!("[{}] In-flight events drained", stage_name);
                    // Send notification via message bus
                    let _ = bus.send_stage_notification(
                        stage_id,
                        StageNotification::DrainProgress { in_flight: 0 }
                    ).await;
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
    }
    
    /// Track upstream completion
    pub async fn upstream_completed(&mut self, upstream_id: StageId) {
        let should_send_event = {
            let mut completed = self.context.completed_upstreams.write().await;
            if !completed.contains(&upstream_id) {
                completed.push(upstream_id);
                
                let total_upstreams = self.context.topology.upstream_stages(self.stage_id).len();
                completed.len() >= total_upstreams
            } else {
                false
            }
        };
        
        if should_send_event {
            // All upstreams complete
            let _ = self.process_event(StageEvent::AllUpstreamsComplete).await;
        }
    }
    
    /// Cleanup stage resources
    async fn cleanup(&self) {
        // Cancel processing task
        if let Some(task) = self.task.write().await.take() {
            task.abort();
        }
        
        // Clear subscription
        *self.subscription.write().await = None;
    }
}
//! Supervisor for transform stages

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use obzenflow_core::{ChainEvent, EventId};

use crate::control_plane::stages::{
    handler_traits::TransformHandler,
    StageHandler,
    TransformWrapper,
    states::TransformState,
    events::TransformEvent,
    actions::TransformAction,
    handler_contexts::TransformContext,
};
use super::{
    config::StageConfig, 
    traits::StageSupervisor,
    stage_handle::{StageHandle, StageType, StageEvent}
};
use obzenflow_topology_services::stages::StageId;
use obzenflow_fsm::StateMachine;

pub struct TransformSupervisor<H: TransformHandler + 'static> {
    fsm: StateMachine<TransformState, TransformEvent, TransformContext<H>, TransformAction>,
    context: Arc<TransformContext<H>>,
    stage_id: StageId,
    stage_name: String,
    upstream_stages: Vec<StageId>,
    processing_task: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl<H: TransformHandler + 'static> TransformSupervisor<H> {
    pub fn new(handler: H, config: StageConfig) -> Self {
        let wrapper = TransformWrapper(handler);
        let context = Arc::new(TransformContext::new(
            wrapper.0,
            config.stage_id,
            config.stage_name.clone(),
            config.journal,
            config.message_bus,
        ));
        
        let fsm = <TransformWrapper<H> as StageHandler>::build_fsm();
        
        Self {
            fsm,
            context,
            stage_id: config.stage_id,
            stage_name: config.stage_name.clone(),
            upstream_stages: config.upstream_stages,
            processing_task: Arc::new(RwLock::new(None)),
        }
    }
    
    async fn handle_transform_action(&mut self, action: TransformAction) -> Result<(), String> {
        use TransformAction::*;
        
        match action {
            AllocateResources => {
                tracing::info!("[{}] Allocating resources", self.stage_name);
                // Register writer (None = single worker for this stage)
                let writer_id = self.context.journal
                    .register_writer(self.stage_id, None)
                    .await
                    .map_err(|e| format!("Failed to register writer: {:?}", e))?;
                
                *self.context.writer_id.write().await = Some(writer_id);
                
                // Create subscription to upstreams (provided by pipeline)
                if !self.upstream_stages.is_empty() {
                    let filter = crate::data_plane::journal_subscription::SubscriptionFilter::UpstreamStages {
                        stages: self.upstream_stages.clone(),
                    };
                    
                    let subscription = self.context.journal.subscribe(filter).await
                        .map_err(|e| format!("Failed to create subscription: {:?}", e))?;
                    
                    *self.context.subscription.write().await = Some(subscription);
                }
            }
            
            StartProcessing => {
                tracing::info!("[{}] Starting processing", self.stage_name);
                self.start_transform_loop().await?;
            }
            
            ForwardEOF => {
                tracing::info!("[{}] Forwarding EOF", self.stage_name);
                // Write EOF event to journal
                if let Some(writer_id) = &*self.context.writer_id.read().await {
                    let eof_event = ChainEvent::eof(
                        EventId::new(),
                        writer_id.clone(),
                        false // Not natural - this is triggered by drain
                    );
                    self.context.journal
                        .write(writer_id, eof_event, None)
                        .await
                        .map_err(|e| format!("Failed to write EOF: {:?}", e))?;
                }
            }
            
            Cleanup => {
                tracing::info!("[{}] Cleaning up", self.stage_name);
                if let Some(task) = self.processing_task.write().await.take() {
                    task.abort();
                }
            }
            
            LogTransition { from, to } => {
                tracing::debug!("[{}] State transition: {} -> {}", self.stage_name, from, to);
            }
        }
        
        Ok(())
    }
    
    async fn start_transform_loop(&self) -> Result<(), String> {
        let context = self.context.clone();
        let stage_name = self.stage_name.clone();
        
        let task = tokio::spawn(async move {
            tracing::info!("[{}] Transform loop started", stage_name);
            
            // Get subscription
            let subscription = context.subscription.clone();
            
            let mut eof_received = false;
            loop {
                if let Some(sub) = &mut *subscription.write().await {
                    match sub.recv_batch().await {
                        Ok(events) if !events.is_empty() => {
                            tracing::debug!("[{}] Processing {} events", stage_name, events.len());
                            
                            // Process each event
                            for envelope in events {
                                // Check if this is an EOF event
                                match envelope.event.as_control_type() {
                                    Some(ChainEvent::EOF_EVENT_TYPE) => {
                                        tracing::info!("[{}] Transform received EOF event from upstream", stage_name);
                                        
                                        // Forward EOF downstream with our writer_id
                                        if let Some(writer_id) = &*context.writer_id.read().await {
                                            let eof_event = ChainEvent::eof(
                                                EventId::new(),
                                                writer_id.clone(),
                                                envelope.event.payload["natural"].as_bool().unwrap_or(false)
                                            );
                                            if let Err(e) = context.journal.write(writer_id, eof_event, Some(&envelope)).await {
                                                tracing::error!("[{}] Failed to write EOF: {}", stage_name, e);
                                            }
                                        }
                                        
                                        // Call the handler's drain method if it has custom logic
                                        // Transform handlers are stateless, but may have cleanup logic
                                        // We need to get a mutable reference through Arc::get_mut or similar
                                        // For now, we'll skip drain since transforms are stateless
                                        tracing::debug!("[{}] Transform has no special drain logic", stage_name);
                                        
                                        // Exit the processing loop
                                        tracing::info!("[{}] Transform completed processing after EOF", stage_name);
                                        eof_received = true;
                                        break;
                                    }
                                    Some(other) => {
                                        tracing::debug!("[{}] Ignoring control event: {}", stage_name, other);
                                    }
                                    None => {
                                        // Regular data event
                                        let outputs = context.handler.process(envelope.event);
                                        
                                        // Write outputs
                                        if let Some(writer_id) = &*context.writer_id.read().await {
                                            for output in outputs {
                                                if let Err(e) = context.journal.write(writer_id, output, None).await {
                                                    tracing::error!("[{}] Failed to write output: {}", stage_name, e);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            
                            // Check if we received EOF and should exit
                            if eof_received {
                                break;
                            }
                        }
                        Ok(_) => {
                            // Empty batch, continue
                            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                        }
                        Err(e) => {
                            tracing::error!("[{}] Subscription error: {}", stage_name, e);
                            break;
                        }
                    }
                } else {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
            
            // Write completion event after exiting the loop
            tracing::info!("[{}] Writing completion event to journal", stage_name);
            if let Some(writer_id) = &*context.writer_id.read().await {
                let completion_event = ChainEvent::new(
                    EventId::new(),
                    writer_id.clone(),
                    "system.stage.completed",
                    serde_json::json!({
                        "stage_id": format!("{}", context.stage_id),
                        "stage_name": stage_name,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    })
                );
                if let Err(e) = context.journal.write(writer_id, completion_event, None).await {
                    // CRITICAL: If we can't write completion, pipeline will hang forever!
                    panic!("[{}] FATAL: Failed to write completion event after processing all data: {}", stage_name, e);
                }
            } else {
                panic!("[{}] FATAL: No writer_id available to write completion event", stage_name);
            }
            
            tracing::info!("[{}] Transform task completed successfully", stage_name);
        });
        
        *self.processing_task.write().await = Some(task);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<H: TransformHandler + 'static> StageSupervisor for TransformSupervisor<H> {
    fn stage_id(&self) -> StageId {
        self.stage_id
    }
    
    fn stage_name(&self) -> &str {
        &self.stage_name
    }
    
    async fn initialize(&mut self) -> Result<(), String> {
        let actions = self.fsm.handle(TransformEvent::Initialize, self.context.clone()).await?;
        for action in actions {
            self.handle_transform_action(action).await?;
        }
        
        // Transforms go straight to ready
        let actions = self.fsm.handle(TransformEvent::Ready, self.context.clone()).await?;
        for action in actions {
            self.handle_transform_action(action).await?;
        }
        
        Ok(())
    }
    
    async fn start(&mut self) -> Result<(), String> {
        // Transforms don't need explicit start
        Ok(())
    }
    
    async fn begin_drain(&mut self) -> Result<(), String> {
        let actions = self.fsm.handle(TransformEvent::BeginDrain, self.context.clone()).await?;
        for action in actions {
            self.handle_transform_action(action).await?;
        }
        Ok(())
    }
    
    async fn is_drained(&self) -> bool {
        matches!(self.fsm.state(), TransformState::Drained)
    }
    
    async fn force_shutdown(&mut self) -> Result<(), String> {
        let actions = self.fsm.handle(
            TransformEvent::Error("Force shutdown".to_string()), 
            self.context.clone()
        ).await?;
        for action in actions {
            self.handle_transform_action(action).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<H: TransformHandler + 'static> StageHandle for TransformSupervisor<H> {
    fn stage_id(&self) -> StageId {
        self.stage_id
    }
    
    fn stage_name(&self) -> &str {
        &self.stage_name
    }
    
    fn stage_type(&self) -> StageType {
        StageType::Transform
    }
    
    async fn initialize(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::initialize(self).await
    }
    
    async fn start(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::start(self).await
    }
    
    async fn send_event(&mut self, event: StageEvent) -> Result<(), String> {
        // Map StageEvent to TransformEvent
        let transform_event = match event {
            StageEvent::Initialize => TransformEvent::Initialize,
            StageEvent::Start => TransformEvent::Ready, // Transforms auto-start
            StageEvent::Shutdown => TransformEvent::BeginDrain,
            _ => return Err(format!("Unsupported event for transform: {:?}", event)),
        };
        
        let actions = self.fsm.handle(transform_event, self.context.clone()).await?;
        for action in actions {
            self.handle_transform_action(action).await?;
        }
        Ok(())
    }
    
    async fn begin_drain(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::begin_drain(self).await
    }
    
    fn is_ready(&self) -> bool {
        matches!(self.fsm.state(), TransformState::Running)
    }
    
    fn is_drained(&self) -> bool {
        matches!(self.fsm.state(), TransformState::Drained)
    }
    
    async fn force_shutdown(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::force_shutdown(self).await
    }
}
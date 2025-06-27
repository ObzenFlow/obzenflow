//! Supervisor for infinite source stages

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use obzenflow_core::{ChainEvent, EventId};

use crate::control_plane::stages::{
    handler_traits::InfiniteSourceHandler,
    StageHandler,
    InfiniteSourceWrapper,
    states::SourceState,
    events::SourceEvent,
    actions::SourceAction,
    handler_contexts::InfiniteSourceContext,
};
use super::{
    config::StageConfig, 
    traits::StageSupervisor,
    stage_handle::{StageHandle, StageType, StageEvent}
};
use obzenflow_topology_services::stages::StageId;
use obzenflow_fsm::StateMachine;

pub struct InfiniteSourceSupervisor<H: InfiniteSourceHandler + 'static> {
    fsm: StateMachine<SourceState, SourceEvent, InfiniteSourceContext<H>, SourceAction>,
    context: Arc<InfiniteSourceContext<H>>,
    stage_id: StageId,
    stage_name: String,
    processing_task: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl<H: InfiniteSourceHandler + 'static> InfiniteSourceSupervisor<H> {
    pub fn new(handler: H, config: StageConfig) -> Self {
        let wrapper = InfiniteSourceWrapper(handler);
        let context = Arc::new(InfiniteSourceContext::new(
            wrapper.0,
            config.stage_id,
            config.stage_name.clone(),
            config.journal,
            config.message_bus,
        ));
        
        let fsm = <InfiniteSourceWrapper<H> as StageHandler>::build_fsm();
        
        Self {
            fsm,
            context,
            stage_id: config.stage_id,
            stage_name: config.stage_name,
            processing_task: Arc::new(RwLock::new(None)),
        }
    }
    
    async fn handle_source_action(&mut self, action: SourceAction) -> Result<(), String> {
        use SourceAction::*;
        
        match action {
            AllocateResources => {
                tracing::info!("[{}] Allocating resources", self.stage_name);
                // Register writer ID
                let writer_id = self.context.journal
                    .register_writer(self.stage_id, None)
                    .await
                    .map_err(|e| format!("Failed to register writer: {:?}", e))?;
                
                *self.context.writer_id.write().await = Some(writer_id);
            }
            
            StartEmitting => {
                tracing::info!("[{}] Starting event emission", self.stage_name);
                self.start_source_loop().await?;
            }
            
            SendEOF => {
                tracing::info!("[{}] Sending EOF", self.stage_name);
                // Write EOF event to journal
                if let Some(writer_id) = &*self.context.writer_id.read().await {
                    let eof_event = ChainEvent::eof(
                        EventId::new(),
                        writer_id.clone(),
                        false // Not natural - infinite sources only EOF on drain/error
                    );
                    self.context.journal
                        .write(writer_id, eof_event, None)
                        .await
                        .map_err(|e| format!("Failed to write EOF: {:?}", e))?;
                }
            }
            
            Cleanup => {
                tracing::info!("[{}] Cleaning up", self.stage_name);
                // Cancel processing task
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
    
    async fn start_source_loop(&self) -> Result<(), String> {
        let context = self.context.clone();
        let stage_name = self.stage_name.clone();
        
        let task = tokio::spawn(async move {
            tracing::info!("[{}] Infinite source loop started", stage_name);
            
            loop {
                // Check if we can emit
                if !*context.can_emit.read().await {
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    continue;
                }
                
                // Check if shutdown was requested
                if *context.shutdown_requested.read().await {
                    tracing::info!("[{}] Infinite source shutting down", stage_name);
                    break;
                }
                
                // Get next event from handler
                let mut handler = context.handler.write().await;
                match handler.next() {
                    Some(event) => {
                        // Write event to journal
                        if let Some(writer_id) = &*context.writer_id.read().await {
                            if let Err(e) = context.journal.write(writer_id, event, None).await {
                                tracing::error!("[{}] Failed to write event: {}", stage_name, e);
                            }
                        }
                    }
                    None => {
                        // No event available right now, sleep briefly
                        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    }
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
                        "stage_id": context.stage_id.to_string(),
                        "stage_name": stage_name,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    })
                );
                if let Err(e) = context.journal.write(writer_id, completion_event, None).await {
                    // CRITICAL: If we can't write completion, pipeline will hang forever!
                    panic!("[{}] FATAL: Failed to write completion event after shutdown: {}", stage_name, e);
                }
            } else {
                panic!("[{}] FATAL: No writer_id available to write completion event", stage_name);
            }
            
            tracing::info!("[{}] Infinite source task completed successfully", stage_name);
        });
        
        *self.processing_task.write().await = Some(task);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<H: InfiniteSourceHandler + 'static> StageSupervisor for InfiniteSourceSupervisor<H> {
    fn stage_id(&self) -> StageId {
        self.stage_id
    }
    
    fn stage_name(&self) -> &str {
        &self.stage_name
    }
    
    async fn initialize(&mut self) -> Result<(), String> {
        let actions = self.fsm.handle(SourceEvent::Initialize, self.context.clone()).await?;
        for action in actions {
            self.handle_source_action(action).await?;
        }
        
        // Transition to ready
        let actions = self.fsm.handle(SourceEvent::Ready, self.context.clone()).await?;
        for action in actions {
            self.handle_source_action(action).await?;
        }
        
        Ok(())
    }
    
    async fn start(&mut self) -> Result<(), String> {
        // Only sources handle start
        let actions = self.fsm.handle(SourceEvent::Start, self.context.clone()).await?;
        for action in actions {
            self.handle_source_action(action).await?;
        }
        Ok(())
    }
    
    async fn begin_drain(&mut self) -> Result<(), String> {
        // Signal the processing loop to stop
        *self.context.shutdown_requested.write().await = true;
        
        let actions = self.fsm.handle(SourceEvent::BeginDrain, self.context.clone()).await?;
        for action in actions {
            self.handle_source_action(action).await?;
        }
        Ok(())
    }
    
    async fn is_drained(&self) -> bool {
        matches!(self.fsm.state(), SourceState::Drained)
    }
    
    async fn force_shutdown(&mut self) -> Result<(), String> {
        let actions = self.fsm.handle(
            SourceEvent::Error("Force shutdown".to_string()), 
            self.context.clone()
        ).await?;
        for action in actions {
            self.handle_source_action(action).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<H: InfiniteSourceHandler + 'static> StageHandle for InfiniteSourceSupervisor<H> {
    fn stage_id(&self) -> StageId {
        self.stage_id
    }
    
    fn stage_name(&self) -> &str {
        &self.stage_name
    }
    
    fn stage_type(&self) -> StageType {
        StageType::InfiniteSource
    }
    
    async fn initialize(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::initialize(self).await
    }
    
    async fn start(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::start(self).await
    }
    
    async fn send_event(&mut self, event: StageEvent) -> Result<(), String> {
        // Map StageEvent to SourceEvent
        let source_event = match event {
            StageEvent::Initialize => SourceEvent::Initialize,
            StageEvent::Start => SourceEvent::Start,
            StageEvent::Shutdown => SourceEvent::BeginDrain,
            _ => return Err(format!("Unsupported event for source: {:?}", event)),
        };
        
        let actions = self.fsm.handle(source_event, self.context.clone()).await?;
        for action in actions {
            self.handle_source_action(action).await?;
        }
        Ok(())
    }
    
    async fn begin_drain(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::begin_drain(self).await
    }
    
    fn is_ready(&self) -> bool {
        matches!(
            self.fsm.state(), 
            SourceState::WaitingForGun | SourceState::Running
        )
    }
    
    fn is_drained(&self) -> bool {
        matches!(self.fsm.state(), SourceState::Drained)
    }
    
    async fn force_shutdown(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::force_shutdown(self).await
    }
}
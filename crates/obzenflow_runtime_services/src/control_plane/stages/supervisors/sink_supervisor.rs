//! Supervisor for sink stages

use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use obzenflow_core::{ChainEvent, EventId};

use crate::control_plane::stages::{
    handler_traits::SinkHandler,
    StageHandler,
    SinkWrapper,
    states::SinkState,
    events::SinkEvent,
    actions::SinkAction,
    handler_contexts::SinkContext,
};
use super::{
    config::StageConfig, 
    traits::StageSupervisor,
    stage_handle::{StageHandle, StageType, StageEvent}
};
use obzenflow_topology_services::stages::StageId;
use obzenflow_fsm::StateMachine;

pub struct SinkSupervisor<H: SinkHandler + 'static> {
    fsm: StateMachine<SinkState, SinkEvent, SinkContext<H>, SinkAction>,
    context: Arc<SinkContext<H>>,
    stage_id: StageId,
    stage_name: String,
    upstream_stages: Vec<StageId>,
    processing_task: Arc<RwLock<Option<JoinHandle<()>>>>,
}

impl<H: SinkHandler + 'static> SinkSupervisor<H> {
    pub fn new(handler: H, config: StageConfig) -> Self {
        let wrapper = SinkWrapper(handler);
        let context = Arc::new(SinkContext::new(
            wrapper.0,
            config.stage_id,
            config.stage_name.clone(),
            config.journal,
            config.message_bus,
        ));
        
        let fsm = <SinkWrapper<H> as StageHandler>::build_fsm();
        
        Self {
            fsm,
            context,
            stage_id: config.stage_id,
            stage_name: config.stage_name.clone(),
            upstream_stages: config.upstream_stages,
            processing_task: Arc::new(RwLock::new(None)),
        }
    }
    
    async fn handle_sink_action(&mut self, action: SinkAction) -> Result<(), String> {
        match action {
            SinkAction::AllocateResources => {
                tracing::info!("[{}] Allocating resources", self.stage_name);
                
                // Create subscription to upstreams (provided by pipeline)
                if !self.upstream_stages.is_empty() {
                    let filter = crate::data_plane::journal_subscription::SubscriptionFilter {
                        upstream_stages: self.upstream_stages.clone(),
                        event_types: None,
                    };
                    
                    let subscription = self.context.journal.subscribe(filter).await
                        .map_err(|e| format!("Failed to create subscription: {:?}", e))?;
                    
                    *self.context.subscription.write().await = Some(subscription);
                }
            }
            
            SinkAction::StartConsuming => {
                tracing::info!("[{}] Starting consumption", self.stage_name);
                self.start_sink_loop().await?;
            }
            
            SinkAction::FlushBuffers => {
                tracing::info!("[{}] Flushing sink", self.stage_name);
                // Call handler's flush method
                let mut handler = self.context.handler.write().await;
                handler.flush()
                    .map_err(|e| format!("Failed to flush: {:?}", e))?;
            }
            
            SinkAction::Cleanup => {
                tracing::info!("[{}] Cleaning up", self.stage_name);
                if let Some(task) = self.processing_task.write().await.take() {
                    task.abort();
                }
            }
            
            SinkAction::LogTransition { from, to } => {
                tracing::debug!("[{}] State transition: {} -> {}", self.stage_name, from, to);
            }
        }
        
        Ok(())
    }
    
    async fn start_sink_loop(&self) -> Result<(), String> {
        let context = self.context.clone();
        let stage_name = self.stage_name.clone();
        
        let task = tokio::spawn(async move {
            tracing::info!("[{}] Sink loop started", stage_name);
            
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
                                let event = envelope.event;
                                
                                // Check if this is an EOF event
                                match event.as_control_type() {
                                    Some(ChainEvent::EOF_EVENT_TYPE) => {
                                        tracing::info!("[{}] Sink received EOF event from upstream", stage_name);
                                        
                                        // Call the handler's drain method
                                        let mut handler = context.handler.write().await;
                                        if let Err(e) = handler.drain().await {
                                            tracing::error!("[{}] Failed to drain handler: {}", stage_name, e);
                                        }
                                        
                                        // Exit the processing loop
                                        tracing::info!("[{}] Sink completed processing after EOF", stage_name);
                                        // TODO: Send ReceivedEOF event to FSM to trigger flush
                                        eof_received = true;
                                        break;
                                    }
                                    Some(other) => {
                                        tracing::debug!("[{}] Ignoring control event: {}", stage_name, other);
                                    }
                                    None => {
                                        // Regular data event
                                        // Consume the event
                                        let mut handler = context.handler.write().await;
                                        if let Err(e) = handler.consume(event) {
                                            tracing::error!("[{}] Failed to consume event: {:?}", stage_name, e);
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
            // Sinks need to register a writer just for the completion event
            let writer_id = context.journal.register_writer(context.stage_id, None)
                .await
                .unwrap_or_else(|e| panic!("[{}] FATAL: Failed to register writer for completion event: {:?}", stage_name, e));
            
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
            
            if let Err(e) = context.journal.write(&writer_id, completion_event, None).await {
                // CRITICAL: If we can't write completion, pipeline will hang forever!
                panic!("[{}] FATAL: Failed to write completion event after processing all data: {}", stage_name, e);
            }
            
            tracing::info!("[{}] Sink task completed successfully", stage_name);
        });
        
        *self.processing_task.write().await = Some(task);
        Ok(())
    }
}

#[async_trait::async_trait]
impl<H: SinkHandler + 'static> StageSupervisor for SinkSupervisor<H> {
    fn stage_id(&self) -> StageId {
        self.stage_id
    }
    
    fn stage_name(&self) -> &str {
        &self.stage_name
    }
    
    async fn initialize(&mut self) -> Result<(), String> {
        let actions = self.fsm.handle(SinkEvent::Initialize, self.context.clone()).await?;
        for action in actions {
            self.handle_sink_action(action).await?;
        }
        
        // Sinks go straight to ready
        let actions = self.fsm.handle(SinkEvent::Ready, self.context.clone()).await?;
        for action in actions {
            self.handle_sink_action(action).await?;
        }
        
        Ok(())
    }
    
    async fn start(&mut self) -> Result<(), String> {
        // Sinks don't need explicit start
        Ok(())
    }
    
    async fn begin_drain(&mut self) -> Result<(), String> {
        let actions = self.fsm.handle(SinkEvent::BeginDrain, self.context.clone()).await?;
        for action in actions {
            self.handle_sink_action(action).await?;
        }
        Ok(())
    }
    
    async fn is_drained(&self) -> bool {
        matches!(self.fsm.state(), SinkState::Drained)
    }
    
    async fn force_shutdown(&mut self) -> Result<(), String> {
        let actions = self.fsm.handle(
            SinkEvent::Error("Force shutdown".to_string()), 
            self.context.clone()
        ).await?;
        for action in actions {
            self.handle_sink_action(action).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<H: SinkHandler + 'static> StageHandle for SinkSupervisor<H> {
    fn stage_id(&self) -> StageId {
        self.stage_id
    }
    
    fn stage_name(&self) -> &str {
        &self.stage_name
    }
    
    fn stage_type(&self) -> StageType {
        StageType::Sink
    }
    
    async fn initialize(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::initialize(self).await
    }
    
    async fn start(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::start(self).await
    }
    
    async fn send_event(&mut self, event: StageEvent) -> Result<(), String> {
        // Map StageEvent to SinkEvent
        let sink_event = match event {
            StageEvent::Initialize => SinkEvent::Initialize,
            StageEvent::Start => SinkEvent::Ready, // Sinks auto-start
            StageEvent::Shutdown => SinkEvent::BeginDrain,
            _ => return Err(format!("Unsupported event for sink: {:?}", event)),
        };
        
        let actions = self.fsm.handle(sink_event, self.context.clone()).await?;
        for action in actions {
            self.handle_sink_action(action).await?;
        }
        Ok(())
    }
    
    async fn begin_drain(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::begin_drain(self).await
    }
    
    fn is_ready(&self) -> bool {
        matches!(self.fsm.state(), SinkState::Running)
    }
    
    fn is_drained(&self) -> bool {
        matches!(self.fsm.state(), SinkState::Drained)
    }
    
    async fn force_shutdown(&mut self) -> Result<(), String> {
        <Self as StageSupervisor>::force_shutdown(self).await
    }
}
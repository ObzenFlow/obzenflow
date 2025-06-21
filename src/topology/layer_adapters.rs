//! Adapters to make existing components work with LayerComponent trait

use crate::topology::{LayerComponent, LayerType, StageId, Drainable};
use crate::lifecycle::GenericEventProcessor;
use crate::lifecycle::EventHandler;
use crate::step::Result;
use async_trait::async_trait;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::task::JoinHandle;

/// Adapter for EventSourcedStage to work with LayerComponent
pub struct StageLayerAdapter {
    pub stage_id: StageId,
    pub stage_name: String,
    pub is_ready: Arc<AtomicBool>,
    pub is_drained: Arc<AtomicBool>,
}

impl StageLayerAdapter {
    pub fn new(stage_id: StageId, stage_name: String) -> Self {
        Self {
            stage_id,
            stage_name,
            is_ready: Arc::new(AtomicBool::new(false)),
            is_drained: Arc::new(AtomicBool::new(false)),
        }
    }
    
    /// Mark stage as ready (called by EventSourcedStage)
    pub fn mark_ready(&self) {
        self.is_ready.store(true, Ordering::SeqCst);
    }
    
    /// Mark stage as drained (called by EventSourcedStage)
    pub fn mark_drained(&self) {
        self.is_drained.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl LayerComponent for StageLayerAdapter {
    fn layer_type(&self) -> LayerType {
        LayerType::Stage
    }
    
    fn id(&self) -> &str {
        &self.stage_name
    }
    
    async fn initialize(&self) -> Result<()> {
        // Stage initialization is handled by EventSourcedStage itself
        // This adapter just participates in the layer coordination
        tracing::debug!("Stage adapter '{}' initialized", self.stage_name);
        Ok(())
    }
    
    async fn start(&self) -> Result<()> {
        // Stage is already started via tokio::spawn in the DSL
        tracing::debug!("Stage adapter '{}' started", self.stage_name);
        Ok(())
    }
    
    async fn begin_drain(&mut self) -> Result<()> {
        // Drain signal is sent via shutdown channel to the stage
        tracing::debug!("Stage adapter '{}' drain requested", self.stage_name);
        Ok(())
    }
    
    fn is_drained(&self) -> bool {
        self.is_drained.load(Ordering::SeqCst)
    }
    
    async fn force_shutdown(&mut self) -> Result<()> {
        // Force shutdown is handled via shutdown channel
        self.is_drained.store(true, Ordering::SeqCst);
        Ok(())
    }
    
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Adapter for GenericEventProcessor (observers) to work with LayerComponent
pub struct ObserverLayerAdapter<H: EventHandler> {
    pub name: String,
    pub processor: Arc<GenericEventProcessor<H>>,
    pub handle: Arc<tokio::sync::Mutex<Option<JoinHandle<Result<()>>>>>,
    pub barrier: Option<Arc<tokio::sync::Barrier>>,
}

impl<H: EventHandler + 'static> ObserverLayerAdapter<H> {
    pub fn new(name: String, processor: GenericEventProcessor<H>) -> Self {
        Self {
            name,
            processor: Arc::new(processor),
            handle: Arc::new(tokio::sync::Mutex::new(None)),
            barrier: None,
        }
    }
    
}

#[async_trait]
impl<H: EventHandler + 'static> LayerComponent for ObserverLayerAdapter<H> {
    fn layer_type(&self) -> LayerType {
        LayerType::Observer
    }
    
    fn id(&self) -> &str {
        &self.name
    }
    
    async fn initialize(&self) -> Result<()> {
        // Observer initialization happens when start() is called
        // This ensures we don't subscribe until stages are running
        tracing::debug!("Observer adapter '{}' initialized", self.name);
        Ok(())
    }
    
    async fn start(&self) -> Result<()> {
        // Participate in barrier coordination like Stage components
        if let Some(ref barrier) = self.barrier {
            tracing::info!("Observer '{}' waiting at barrier", self.name);
            barrier.wait().await;
            tracing::info!("Observer '{}' passed barrier", self.name);
        }
        
        // NOW we start the processor, after stages are running
        tracing::info!("Starting observer '{}' (stages are running)", self.name);
        
        let handle = self.processor.start().await?;
        let mut handle_lock = self.handle.lock().await;
        *handle_lock = Some(handle);
        
        Ok(())
    }
    
    async fn begin_drain(&mut self) -> Result<()> {
        // GenericEventProcessor handles drain via its Drainable implementation
        // Since we can't get mutable access to Arc, we need to use the drainable interface
        // through a different approach - the processor will receive drain signal via event
        tracing::debug!("Observer adapter '{}' drain requested", self.name);
        Ok(())
    }
    
    fn is_drained(&self) -> bool {
        self.processor.is_drained()
    }
    
    async fn force_shutdown(&mut self) -> Result<()> {
        // Cancel the task if running - this will cause the processor to stop
        let mut handle_lock = self.handle.lock().await;
        if let Some(handle) = handle_lock.take() {
            handle.abort();
        }
        
        Ok(())
    }
    
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    
    fn set_barrier(&mut self, barrier: Arc<tokio::sync::Barrier>) {
        self.barrier = Some(barrier);
    }
}
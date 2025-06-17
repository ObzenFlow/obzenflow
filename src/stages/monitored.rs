use crate::chain_event::ChainEvent;
use crate::step::{Step, StepType, Result};
use crate::monitoring::Taxonomy;
use async_trait::async_trait;
use std::marker::PhantomData;

/// Wrapper that adds monitoring to any Step
/// This works with EventStore - no channels needed!
pub struct MonitoredStep<S, T> 
where 
    S: Step,
    T: Taxonomy,
{
    inner: S,
    metrics: T::Metrics,
    taxonomy: T,
    _phantom: PhantomData<T>,
}

impl<S, T> MonitoredStep<S, T>
where
    S: Step,
    T: Taxonomy,
{
    pub fn new(step: S, taxonomy: T) -> Self {
        let name = std::any::type_name::<S>()
            .split("::")
            .last()
            .unwrap_or("UnknownStep");
            
        let metrics = T::create_metrics(name);
        
        Self {
            inner: step,
            metrics,
            taxonomy,
            _phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<S, T> Step for MonitoredStep<S, T>
where
    S: Step + 'static,
    T: Taxonomy + 'static,
    T::Metrics: Send + Sync,
{
    type Taxonomy = T;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &self.taxonomy
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        self.inner.step_type()
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // For now, just pass through - monitoring integration will be updated
        // when we implement the new flow! macro
        self.inner.handle(event)
    }
    
    async fn initialize(&mut self) -> Result<()> {
        self.inner.initialize().await
    }
    
    async fn shutdown(&mut self) -> Result<()> {
        self.inner.shutdown().await
    }
}

/// Helper to create monitored steps
pub struct Monitor;

impl Monitor {
    /// Monitor any step with a taxonomy
    pub fn step<S: Step, T: Taxonomy>(step: S, taxonomy: T) -> MonitoredStep<S, T> {
        MonitoredStep::new(step, taxonomy)
    }
}

/// Legacy aliases for compatibility during migration
pub type MonitoredSource<S, T> = MonitoredStep<S, T>;
pub type MonitoredStage<S, T> = MonitoredStep<S, T>;
pub type MonitoredSink<S, T> = MonitoredStep<S, T>;

pub use Monitor as MonitorSource;
pub use Monitor as MonitorStage;
pub use Monitor as MonitorSink;
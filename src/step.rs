// src/step.rs
use async_trait::async_trait;
use std::error::Error;
pub use crate::chain_event::ChainEvent;
use crate::monitoring::Taxonomy;

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// Step type in the pipeline topology
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepType {
    /// Source: () → Events (generates/reads data, pushes downstream)
    Source,
    /// Stage: Events → Events (receives input, processes, pushes output)
    Stage,
    /// Sink: Events → () (receives input, persists/outputs, no downstream)
    Sink,
}

/// Core trait for all pipeline steps (Sources, Stages, Sinks)
#[async_trait]
pub trait Step: Send + Sync {
    /// The taxonomy this step uses for monitoring
    /// Must be explicitly specified - no defaults!
    type Taxonomy: Taxonomy;
    
    /// Get the taxonomy instance for this step
    fn taxonomy(&self) -> &Self::Taxonomy;
    
    /// Get the metrics instance for this step
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics;
    
    /// Indicate what type of step this is for proper runtime topology
    fn step_type(&self) -> StepType {
        StepType::Stage // Default to stage for backward compatibility
    }
    /// Process an event and produce zero or more output events
    /// This is the ONLY processing method!
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event] // Default passthrough
    }

    /// Called before processing starts
    async fn initialize(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called after processing completes
    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }

}

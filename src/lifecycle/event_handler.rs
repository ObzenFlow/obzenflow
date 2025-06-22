//! Core event processing trait defining how components handle events

use crate::chain_event::ChainEvent;
use crate::step::Result;
use crate::lifecycle::behavior::BehaviorCondition;

/// Core trait defining HOW a component processes events
pub trait EventHandler: Send + Sync {
    /// Transform events (stages) - 1:N mapping
    /// Used by stages that modify, enrich, or filter events
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event] // Default passthrough
    }

    /// Observe events without transformation (monitors, loggers)
    /// Used by components that need to see events but not modify flow
    fn observe(&self, event: &ChainEvent) -> Result<()> {
        Ok(()) // Default no-op
    }

    /// Aggregate events (batchers, windows) - N:M mapping
    /// Used by components that collect events and emit batches
    fn aggregate(&mut self, event: ChainEvent) -> Option<Vec<ChainEvent>> {
        Some(vec![event]) // Default immediate emission
    }
    
    /// Declare processing mode for optimization
    /// The GenericEventProcessor uses this to call the right method
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
    
    /// Check if this handler has any behavior conditions to report
    /// Called periodically by EventSourcedStage to detect completion, state changes, etc.
    fn check_behaviors(&mut self) -> Option<BehaviorCondition> {
        None // Default: no behaviors
    }
}

/// How this handler processes events
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingMode {
    /// Stateless 1:N transformation (most stages)
    Transform,
    /// Side-effect only, no output (monitors, loggers)
    Observe,
    /// Stateful N:M transformation (batchers, windows)
    Aggregate,
}

// Blanket implementation for Box<dyn EventHandler>
impl EventHandler for Box<dyn EventHandler> {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        (**self).transform(event)
    }
    
    fn observe(&self, event: &ChainEvent) -> Result<()> {
        (**self).observe(event)
    }
    
    fn aggregate(&mut self, event: ChainEvent) -> Option<Vec<ChainEvent>> {
        (**self).aggregate(event)
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        (**self).processing_mode()
    }
    
    fn check_behaviors(&mut self) -> Option<BehaviorCondition> {
        (**self).check_behaviors()
    }
}
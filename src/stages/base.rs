use crate::chain_event::ChainEvent;
use crate::step::StepType;

/// Base trait for stages
/// With EventStore, all stages just implement Step trait
/// This trait exists only for backward compatibility with monitoring
pub trait Stage: Send + Sync {
    /// What type of step is this?
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    /// Handle a single event
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event] // Default passthrough
    }
}

/// Source and Sink traits are deprecated
/// With EventStore, sources and sinks are just Steps:
/// - Sources: Steps that generate events (no input dependency)
/// - Sinks: Steps that produce side effects (may return empty vec)
pub use Stage as Source;
pub use Stage as Sink;
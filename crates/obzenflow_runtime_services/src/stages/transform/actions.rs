//! Actions that transform FSM transitions can emit

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransformAction {
    /// Allocate resources needed by the transform
    /// - Create subscriptions to upstream stages
    /// - Initialize any caches or state
    AllocateResources,
    
    /// Start processing events
    /// - Begin consuming from journal subscriptions
    /// - Process events as they arrive
    StartProcessing,
    
    /// Forward EOF downstream when all upstreams complete
    /// - Ensures EOF propagates through the pipeline
    ForwardEOF,
    
    /// Clean up all resources
    /// - Cancel subscriptions
    /// - Clear caches
    /// - Release memory
    Cleanup,
    
    /// Log state transition for debugging
    LogTransition { from: String, to: String },
}
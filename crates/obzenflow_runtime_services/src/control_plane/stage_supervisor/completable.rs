//! Trait for components that can naturally complete their work
//! 
//! Part of FLOWIP-058: Deterministic Pipeline Shutdown with Source Completion

/// Trait for components that can signal natural completion
/// 
/// This is primarily used by source stages to indicate when they have
/// finished producing events (e.g., end of file, all records processed).
/// When a source signals completion, it triggers EOF propagation through
/// the pipeline enabling deterministic shutdown.
pub trait Completable: Send + Sync {
    /// Check if this component has naturally completed its work
    /// 
    /// Returns true when the component has no more events to produce
    /// and has reached a natural completion state (not due to shutdown).
    /// 
    /// # Examples
    /// 
    /// - File reader: Returns true when EOF is reached
    /// - Database query: Returns true when all rows are processed
    /// - Bounded generator: Returns true when limit is reached
    /// - Network stream: Typically returns false (endless source)
    fn is_complete(&self) -> bool;
}
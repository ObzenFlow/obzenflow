//! Actions that sink FSM transitions can emit

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SinkAction {
    /// Allocate resources needed by the sink
    /// - Open database connections
    /// - Create output files
    /// - Initialize client connections
    AllocateResources,
    
    /// Start consuming events
    /// - Begin reading from journal subscriptions
    /// - Write events to destination
    StartConsuming,
    
    /// Flush any buffered data to ensure durability
    /// - Database: commit transactions
    /// - Files: fsync to disk
    /// - Network: flush send buffers
    FlushBuffers,
    
    /// Clean up all resources
    /// - Close connections
    /// - Close file handles
    /// - Release memory
    Cleanup,
    
    /// Log state transition for debugging
    LogTransition { from: String, to: String },
}
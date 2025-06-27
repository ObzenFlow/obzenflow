//! Actions that source FSM transitions can emit
//! 
//! Actions represent side effects that need to be performed as a result
//! of state transitions. The stage supervisor executes these actions.

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceAction {
    /// Allocate resources needed by the source
    /// - Register writer ID with journal
    /// - Open file handles, network connections, etc.
    AllocateResources,
    
    /// Start the event production loop
    /// - Begin reading from file/network/queue
    /// - Start generating events
    StartEmitting,
    
    /// Send EOF event downstream to signal completion
    /// - For finite sources: when is_complete() returns true
    /// - For all sources: during shutdown
    SendEOF,
    
    /// Clean up all resources
    /// - Close file handles
    /// - Disconnect from services
    /// - Release memory
    Cleanup,
    
    /// Log state transition for debugging
    LogTransition { from: String, to: String },
}
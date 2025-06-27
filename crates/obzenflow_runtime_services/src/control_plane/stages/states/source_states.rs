//! State definitions for source stages (both finite and infinite)
//! 
//! Sources have a unique "WaitingForGun" state that other stages don't have.
//! This ensures sources don't start emitting until the pipeline is ready.

use serde::{Deserialize, Serialize};
use obzenflow_fsm::StateVariant;

/// FSM states for source stages (both finite and infinite)
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SourceState {
    /// Initial state - source has been created but not initialized
    Created,
    
    /// Resources allocated, subscriptions created, ready to wait for start signal
    Initialized,
    
    /// UNIQUE TO SOURCES: Waiting for explicit start command from pipeline
    /// This prevents sources from emitting events before the pipeline is ready
    WaitingForGun,
    
    /// Actively producing events - the source is now allowed to emit
    Running,
    
    /// Shutting down gracefully, finishing any pending work
    Draining,
    
    /// All work complete, EOF sent downstream
    Drained,
    
    /// Unrecoverable error occurred
    Failed(String),
}

impl SourceState {
    /// Check if this is a terminal state (no more transitions possible)
    pub fn is_terminal(&self) -> bool {
        matches!(self, SourceState::Drained | SourceState::Failed(_))
    }
    
    /// Check if the source can emit events in this state
    pub fn can_emit(&self) -> bool {
        matches!(self, SourceState::Running)
    }
    
    /// Check if the source is waiting for the start signal
    pub fn is_waiting_for_start(&self) -> bool {
        matches!(self, SourceState::WaitingForGun)
    }
}

// ============================================================================
// FSM Framework Integration
// ============================================================================
// 
// StateVariant is required by obzenflow_fsm to get string names from states.
// This is used for:
// - Logging: "Transitioning from WaitingForGun to Running"
// - FSM Builder: .when("WaitingForGun") matches these strings
// - Debugging and visualization tools
// ============================================================================

impl StateVariant for SourceState {
    fn variant_name(&self) -> &str {
        match self {
            SourceState::Created => "Created",
            SourceState::Initialized => "Initialized",
            SourceState::WaitingForGun => "WaitingForGun",
            SourceState::Running => "Running",
            SourceState::Draining => "Draining",
            SourceState::Drained => "Drained",
            SourceState::Failed(_) => "Failed",  // Just "Failed", not the error message
        }
    }
}
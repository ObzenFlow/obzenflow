//! State definitions for sink stages
//! 
//! Sinks have a unique "Flushing" state to ensure all buffered data is
//! written before shutdown.

use serde::{Deserialize, Serialize};
use obzenflow_fsm::StateVariant;

/// FSM states for sink stages
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SinkState {
    /// Initial state - sink has been created but not initialized
    Created,
    
    /// Resources allocated (DB connections, file handles, etc.)
    Initialized,
    
    /// Actively consuming events and writing to destination
    Running,
    
    /// UNIQUE TO SINKS: Flushing any buffered data before drain
    /// This ensures no data loss during shutdown
    Flushing,
    
    /// Flushing complete, waiting for remaining events
    Draining,
    
    /// All events consumed, resources cleaned up
    Drained,
    
    /// Unrecoverable error occurred
    Failed(String),
}

impl SinkState {
    /// Check if this is a terminal state (no more transitions possible)
    pub fn is_terminal(&self) -> bool {
        matches!(self, SinkState::Drained | SinkState::Failed(_))
    }
    
    /// Check if the sink can consume events in this state
    pub fn can_consume(&self) -> bool {
        matches!(self, SinkState::Running)
    }
    
    /// Check if the sink is currently flushing buffers
    pub fn is_flushing(&self) -> bool {
        matches!(self, SinkState::Flushing)
    }
}

// ============================================================================
// FSM Framework Integration - see source_states.rs for detailed explanation
// ============================================================================

impl StateVariant for SinkState {
    fn variant_name(&self) -> &str {
        match self {
            SinkState::Created => "Created",
            SinkState::Initialized => "Initialized",
            SinkState::Running => "Running",
            SinkState::Flushing => "Flushing",  // Unique to sinks!
            SinkState::Draining => "Draining",
            SinkState::Drained => "Drained",
            SinkState::Failed(_) => "Failed",
        }
    }
}
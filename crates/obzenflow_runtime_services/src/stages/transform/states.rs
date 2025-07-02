//! State definitions for transform stages
//! 
//! Transforms start processing immediately after initialization - they don't
//! wait for a start signal like sources do.

use serde::{Deserialize, Serialize};
use obzenflow_fsm::StateVariant;

/// FSM states for transform stages
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransformState {
    /// Initial state - transform has been created but not initialized
    Created,
    
    /// Resources allocated, subscriptions to upstream stages created
    /// Ready to start processing immediately
    Initialized,
    
    /// Actively processing events from upstream stages
    /// NOTE: No WaitingForGun state - transforms start immediately!
    Running,
    
    /// Received EOF from upstream, processing remaining events
    Draining,
    
    /// All events processed, EOF forwarded downstream
    Drained,
    
    /// Unrecoverable error occurred
    Failed(String),
}

impl TransformState {
    /// Check if this is a terminal state (no more transitions possible)
    pub fn is_terminal(&self) -> bool {
        matches!(self, TransformState::Drained | TransformState::Failed(_))
    }
    
    /// Check if the transform can process events in this state
    pub fn can_process(&self) -> bool {
        matches!(self, TransformState::Running)
    }
}

// ============================================================================
// FSM Framework Integration - see source_states.rs for detailed explanation
// ============================================================================

impl StateVariant for TransformState {
    fn variant_name(&self) -> &str {
        match self {
            TransformState::Created => "Created",
            TransformState::Initialized => "Initialized",
            TransformState::Running => "Running",
            TransformState::Draining => "Draining",
            TransformState::Drained => "Drained",
            TransformState::Failed(_) => "Failed",
        }
    }
}
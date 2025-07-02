//! Events that trigger state transitions for transform stages

use serde::{Deserialize, Serialize};
use obzenflow_fsm::EventVariant;

/// Events that can trigger transform state transitions
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransformEvent {
    /// Initialize the transform - allocate resources, create subscriptions
    Initialize,
    
    /// Ready to start processing
    /// NOTE: No "Start" event - transforms begin immediately!
    Ready,
    
    /// Received EOF from all upstream stages
    ReceivedEOF,
    
    /// Begin graceful shutdown
    BeginDrain,
    
    /// Unrecoverable error occurred
    Error(String),
}

// ============================================================================
// FSM Framework Integration - see source_events.rs for explanation
// ============================================================================

impl EventVariant for TransformEvent {
    fn variant_name(&self) -> &str {
        match self {
            TransformEvent::Initialize => "Initialize",
            TransformEvent::Ready => "Ready",  // Not "Start"!
            TransformEvent::ReceivedEOF => "ReceivedEOF",
            TransformEvent::BeginDrain => "BeginDrain",
            TransformEvent::Error(_) => "Error",
        }
    }
}
//! Events that trigger state transitions for source stages

use serde::{Deserialize, Serialize};
use obzenflow_fsm::EventVariant;

/// Events that can trigger source state transitions
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceEvent {
    /// Initialize the source - allocate resources, create writer ID
    Initialize,
    
    /// Source is ready - transition to WaitingForGun state
    Ready,
    
    /// Start event production - the "gun" has been fired!
    /// Only sources receive this event
    Start,
    
    /// Begin graceful shutdown - stop producing new events
    BeginDrain,
    
    /// Source completed naturally (finite sources only)
    /// This is triggered when is_complete() returns true
    Completed,
    
    /// Unrecoverable error occurred
    Error(String),
}

// ============================================================================
// FSM Framework Integration
// 
// EventVariant is required by obzenflow_fsm to get string names from events.
// This enables the FSM builder to match events by name.
// ============================================================================

impl EventVariant for SourceEvent {
    fn variant_name(&self) -> &str {
        match self {
            SourceEvent::Initialize => "Initialize",
            SourceEvent::Ready => "Ready",
            SourceEvent::Start => "Start",  // Source-specific!
            SourceEvent::BeginDrain => "BeginDrain",
            SourceEvent::Completed => "Completed",  // For finite sources
            SourceEvent::Error(_) => "Error",
        }
    }
}
//! Error types for FSM operations

use thiserror::Error;

/// Error types for FSM operations
#[derive(Error, Debug)]
pub enum FsmError {
    #[error("Invalid transition from {from:?} on event {event:?}")]
    InvalidTransition { from: String, event: String },
    
    #[error("Handler error: {0}")]
    HandlerError(String),
    
    #[error("Timeout in state {state:?}")]
    Timeout { state: String },
}
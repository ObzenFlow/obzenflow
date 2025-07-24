//! Processing outcome types
//!
//! Defines the possible outcomes of processing an event.

use serde::{Deserialize, Serialize};

/// The outcome of processing an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessingStatus {
    /// Event was processed successfully
    Success,
    
    /// Event was filtered out (intentionally not processed)
    Filtered,
    
    /// Processing failed with an error
    Error(String),
    
    /// Event should be retried
    Retry { attempt: u32 },
}

impl ProcessingStatus {
    /// Create a success outcome
    pub fn success() -> Self {
        ProcessingStatus::Success
    }
    
    /// Create an error outcome
    pub fn error(msg: impl Into<String>) -> Self {
        ProcessingStatus::Error(msg.into())
    }
    
    /// Check if this outcome is terminal (no more processing needed)
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Success | Self::Filtered | Self::Error(_))
    }
    
    /// Check if the event should be retried
    pub fn should_retry(&self) -> bool {
        matches!(self, Self::Retry { .. })
    }
    
    /// Check if processing was successful
    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success)
    }
}

impl Default for ProcessingStatus {
    fn default() -> Self {
        Self::Success
    }
}
//! Processing information for events
//!
//! Tracks how an event was processed, by whom, and the outcome.

use serde::{Deserialize, Serialize};
use super::processing_outcome::ProcessingOutcome;

/// Information about how an event was processed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingInfo {
    /// Which component processed this event
    pub processed_by: String,
    
    /// How long processing took in milliseconds
    pub processing_time_ms: u64,
    
    /// When the event occurred (milliseconds since Unix epoch)
    pub event_time: u64,
    
    /// The outcome of processing
    pub outcome: ProcessingOutcome,
}

impl Default for ProcessingInfo {
    fn default() -> Self {
        Self {
            processed_by: "unknown".to_string(),
            processing_time_ms: 0,
            event_time: 0,
            outcome: ProcessingOutcome::Success,
        }
    }
}
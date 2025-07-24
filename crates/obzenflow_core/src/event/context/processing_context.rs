//! Processing information for events
//!
//! Tracks how an event was processed, by whom, and the outcome.

use serde::{Deserialize, Serialize};
use crate::event::status::processing_status::ProcessingStatus;

/// Information about how an event was processed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingContext {
    /// Which component processed this event
    pub processed_by: String,
    
    /// How long processing took in milliseconds
    pub processing_time_ms: u64,
    
    /// When the event occurred (milliseconds since Unix epoch)
    pub event_time: u64,
    
    /// The outcome of processing
    pub status: ProcessingStatus,
}

impl Default for ProcessingContext {
    fn default() -> Self {
        Self {
            processed_by: "unknown".to_string(),
            processing_time_ms: 0,
            event_time: 0,
            status: ProcessingStatus::Success,
        }
    }
}
//! State management trait for components that need persistence and deduplication

use crate::step::Result;
use crate::event_types::EventId;
use std::time::Duration;
use serde_json::Value as JsonValue;

/// Components that maintain state across events
pub trait Stateful: Send + Sync {
    /// Checkpoint progress for resumption after restart
    fn checkpoint(&self) -> Option<CheckpointData> {
        None // Default: no checkpointing
    }
    
    /// Resume from a previous checkpoint
    fn restore(&mut self, _checkpoint: CheckpointData) -> Result<()> {
        Ok(()) // Default: ignore checkpoint
    }
    
    /// Track that an event has been processed (for deduplication)
    fn mark_processed(&mut self, _event_id: EventId) {
        // Default: no tracking
    }
    
    /// Check if an event has already been processed
    fn is_processed(&self, _event_id: &EventId) -> bool {
        false // Default: always process
    }
    
    /// Get state summary for monitoring
    fn state_summary(&self) -> StateInfo {
        StateInfo::default()
    }
}

/// Data format for checkpoints
#[derive(Debug, Clone)]
pub enum CheckpointData {
    /// No checkpoint data
    None,
    /// JSON-serialized state
    Json(JsonValue),
    /// Binary checkpoint data
    Binary(Vec<u8>),
}

/// Summary of component state for monitoring
#[derive(Debug, Default, Clone)]
pub struct StateInfo {
    /// Number of items currently in memory
    pub items_in_memory: usize,
    /// Age of the last checkpoint
    pub checkpoint_age: Option<Duration>,
    /// ID of the last processed event
    pub last_event_id: Option<EventId>,
    /// Custom state information
    pub custom: Option<JsonValue>,
}
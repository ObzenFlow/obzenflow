//! Causality tracking for events
//!
//! Tracks parent-child relationships between events to maintain
//! causal ordering in distributed systems.

use serde::{Deserialize, Serialize};
use crate::event::event_id::EventId;

/// Causality information for an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalityInfo {
    /// Parent event IDs that caused this event
    pub parent_ids: Vec<EventId>,
}

impl CausalityInfo {
    /// Create new empty causality info (root event)
    pub fn new() -> Self {
        Self { parent_ids: Vec::new() }
    }
    
    /// Create with a single parent
    pub fn with_parent(parent: EventId) -> Self {
        Self { parent_ids: vec![parent] }
    }
    
    /// Add a parent to the causality chain
    pub fn add_parent(mut self, parent: EventId) -> Self {
        self.parent_ids.push(parent);
        self
    }
    
    /// Check if this is a root event (no parents)
    pub fn is_root(&self) -> bool {
        self.parent_ids.is_empty()
    }
    
    /// Get the number of direct parents
    pub fn parent_count(&self) -> usize {
        self.parent_ids.len()
    }
}

impl Default for CausalityInfo {
    fn default() -> Self {
        Self::new()
    }
}
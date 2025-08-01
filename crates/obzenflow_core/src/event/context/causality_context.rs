//! Causality tracking for events
//!
//! Tracks parent-child relationships between events to maintain
//! causal ordering in distributed systems.

use serde::{Deserialize, Serialize};
use crate::event::types::EventId;

/// Causality information for an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalityContext {
    /// Parent event IDs that caused this event
    pub parent_ids: Vec<EventId>,
}

impl CausalityContext {
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
    
    /// Check if this event is in a cycle (event ID appears in its own lineage)
    pub fn contains_cycle(&self, event_id: &EventId) -> bool {
        self.parent_ids.contains(event_id)
    }
    
    /// Get the full lineage including the given event
    pub fn full_lineage(&self, event_id: EventId) -> Vec<EventId> {
        let mut lineage = vec![event_id];
        lineage.extend(self.parent_ids.clone());
        lineage
    }
    
    /// Find the cycle if one exists
    pub fn find_cycle(&self, event_id: &EventId) -> Option<Vec<EventId>> {
        if let Some(pos) = self.parent_ids.iter().position(|id| id == event_id) {
            // Return the cycle: from the repeated event to the end
            let mut cycle = self.parent_ids[pos..].to_vec();
            cycle.push(*event_id); // Complete the cycle
            Some(cycle)
        } else {
            None
        }
    }
    
    /// Get the depth of the lineage (number of ancestors)
    pub fn depth(&self) -> usize {
        self.parent_ids.len()
    }
}

impl Default for CausalityContext {
    fn default() -> Self {
        Self::new()
    }
}
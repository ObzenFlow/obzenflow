// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Causality tracking for events
//!
//! Tracks parent-child relationships between events to maintain
//! causal ordering in distributed systems.

use super::ChainEventProvenance;
use crate::event::types::{EventId, EventType};
use serde::{Deserialize, Serialize};

/// Causality information for an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalityContext {
    /// Parent event IDs that caused this event
    pub parent_ids: Vec<EventId>,
    /// The events this event was made from, each with its event type
    #[serde(default)]
    pub inputs: Vec<CausalInput>,
}

/// An event that another event was made from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CausalInput {
    pub id: EventId,
    pub event_type: EventType,
}

impl CausalInput {
    /// The identity and event type `event` records.
    pub fn of(event: &ChainEventProvenance) -> Self {
        Self {
            id: event.id,
            event_type: EventType(event.event_type.clone()),
        }
    }
}

impl CausalityContext {
    /// Create new empty causality info (root event)
    pub fn new() -> Self {
        Self {
            parent_ids: Vec::new(),
            inputs: Vec::new(),
        }
    }

    /// Create with a single input
    pub fn with_input(input: CausalInput) -> Self {
        Self::from_inputs(vec![input])
    }

    /// Create from the inputs an event was made from, in order
    pub fn from_inputs(inputs: Vec<CausalInput>) -> Self {
        Self {
            parent_ids: inputs.iter().map(|input| input.id).collect(),
            inputs,
        }
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

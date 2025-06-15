//! Vector Clock implementation for causal consistency
//! 
//! Provides happens-before relationships without global coordination.
//! Each writer maintains its own vector clock component.

use crate::event_store::WriterId;
use std::collections::BTreeMap;
use std::cmp::Ordering;
use serde::{Serialize, Deserialize};

/// Vector clock for tracking causal relationships between events
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorClock {
    /// Map of writer_id -> logical timestamp
    /// BTreeMap for deterministic serialization
    clocks: BTreeMap<WriterId, u64>,
}

impl VectorClock {
    /// Create a new empty vector clock
    pub fn new() -> Self {
        Self { 
            clocks: BTreeMap::new() 
        }
    }
    
    /// Increment this writer's component
    pub fn tick(&mut self, writer_id: &WriterId) {
        *self.clocks.entry(writer_id.clone()).or_insert(0) += 1;
    }
    
    /// Update clock based on received event (happens-before)
    /// This is the key operation for maintaining causality
    pub fn update(&mut self, writer_id: &WriterId, other: &VectorClock) {
        // Take max of all components from other clock
        for (id, &timestamp) in &other.clocks {
            let current = self.clocks.entry(id.clone()).or_insert(0);
            *current = (*current).max(timestamp);
        }
        // Then increment our component
        self.tick(writer_id);
    }
    
    /// Check if self happened-before other
    pub fn happened_before(&self, other: &VectorClock) -> bool {
        // All our components <= other's AND at least one is <
        let mut all_leq = true;
        let mut exists_less = false;
        
        for (id, &our_time) in &self.clocks {
            let their_time = other.clocks.get(id).copied().unwrap_or(0);
            if our_time > their_time {
                all_leq = false;
                break;
            }
            if our_time < their_time {
                exists_less = true;
            }
        }
        
        // Check other's components we don't have
        for (id, &their_time) in &other.clocks {
            if !self.clocks.contains_key(id) && their_time > 0 {
                exists_less = true;
            }
        }
        
        all_leq && exists_less
    }
    
    /// Check if events are concurrent (no causal relationship)
    pub fn concurrent_with(&self, other: &VectorClock) -> bool {
        !self.happened_before(other) && !other.happened_before(self)
    }
    
    /// Partial ordering based on happened-before
    pub fn partial_cmp(&self, other: &VectorClock) -> Option<Ordering> {
        if self.happened_before(other) {
            Some(Ordering::Less)
        } else if other.happened_before(self) {
            Some(Ordering::Greater)
        } else if self == other {
            Some(Ordering::Equal)
        } else {
            None // Concurrent events
        }
    }
    
    /// Get the timestamp for a specific writer
    pub fn get(&self, writer_id: &WriterId) -> u64 {
        self.clocks.get(writer_id).copied().unwrap_or(0)
    }
    
    /// Get all writer timestamps
    pub fn all_clocks(&self) -> &BTreeMap<WriterId, u64> {
        &self.clocks
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::StageId;
    
    #[test]
    fn test_vector_clock_basics() {
        let mut clock = VectorClock::new();
        let writer1 = WriterId::new(StageId::next());
        
        // Initial state
        assert_eq!(clock.get(&writer1), 0);
        
        // Tick increments
        clock.tick(&writer1);
        assert_eq!(clock.get(&writer1), 1);
        
        clock.tick(&writer1);
        assert_eq!(clock.get(&writer1), 2);
    }
    
    #[test]
    fn test_happened_before() {
        let mut clock1 = VectorClock::new();
        let mut clock2 = VectorClock::new();
        let writer_a = WriterId::new(StageId::next());
        let writer_b = WriterId::new(StageId::next());
        
        // clock1: {A:1}
        clock1.tick(&writer_a);
        
        // clock2: {A:1, B:1} - happens after clock1
        clock2.update(&writer_b, &clock1);
        
        assert!(clock1.happened_before(&clock2));
        assert!(!clock2.happened_before(&clock1));
        assert!(!clock1.concurrent_with(&clock2));
    }
    
    #[test]
    fn test_concurrent_events() {
        let mut clock1 = VectorClock::new();
        let mut clock2 = VectorClock::new();
        let writer_a = WriterId::new(StageId::next());
        let writer_b = WriterId::new(StageId::next());
        
        // Independent events
        clock1.tick(&writer_a); // {A:1}
        clock2.tick(&writer_b); // {B:1}
        
        assert!(clock1.concurrent_with(&clock2));
        assert!(clock2.concurrent_with(&clock1));
        assert!(!clock1.happened_before(&clock2));
        assert!(!clock2.happened_before(&clock1));
    }
    
    #[test]
    fn test_causal_chain() {
        let mut clock1 = VectorClock::new();
        let writer_a = WriterId::new(StageId::next());
        let writer_b = WriterId::new(StageId::next());
        let writer_c = WriterId::new(StageId::next());
        
        clock1.tick(&writer_a); // {A:1}
        
        let mut clock2 = VectorClock::new();
        clock2.update(&writer_b, &clock1); // {A:1, B:1}
        
        let mut clock3 = VectorClock::new();
        clock3.update(&writer_c, &clock2); // {A:1, B:1, C:1}
        
        // Transitive causality
        assert!(clock1.happened_before(&clock3));
        assert!(clock2.happened_before(&clock3));
        assert!(clock1.happened_before(&clock2));
    }
    
    #[test]
    fn test_partial_ordering() {
        let mut clock1 = VectorClock::new();
        let mut clock2 = VectorClock::new();
        let mut clock3 = VectorClock::new();
        let writer_a = WriterId::new(StageId::next());
        let writer_b = WriterId::new(StageId::next());
        let writer_c = WriterId::new(StageId::next());
        
        clock1.tick(&writer_a); // {A:1}
        clock2.update(&writer_b, &clock1); // {A:1, B:1}
        clock3.tick(&writer_c); // {C:1} - concurrent with both
        
        assert_eq!(clock1.partial_cmp(&clock2), Some(Ordering::Less));
        assert_eq!(clock2.partial_cmp(&clock1), Some(Ordering::Greater));
        assert_eq!(clock1.partial_cmp(&clock3), None); // Concurrent
        assert_eq!(clock2.partial_cmp(&clock3), None); // Concurrent
    }
}
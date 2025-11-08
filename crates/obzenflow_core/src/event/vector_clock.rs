//! Vector clock for causal ordering
//!
//! This is a simple data structure that holds component->sequence mappings.
//! The causal ordering logic is implemented separately in domain services.

use serde::{Serialize, Deserialize};
use std::collections::HashMap;

/// Vector clock data structure for causal ordering
///
/// This is a pure data structure representing the causal history of an event.
/// Use CausalOrderingService for vector clock operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClock {
    /// Map from writer ID to sequence number
    /// Using String for writer ID to keep it simple and serializable
    pub clocks: HashMap<String, u64>,
}

impl VectorClock {
    /// Create an empty vector clock
    pub fn new() -> Self {
        Self {
            clocks: HashMap::new(),
        }
    }

    /// Get the sequence number for a writer
    pub fn get(&self, writer_id: &str) -> u64 {
        self.clocks.get(writer_id).copied().unwrap_or(0)
    }

    /// Check if this clock has any entries
    pub fn is_empty(&self) -> bool {
        self.clocks.is_empty()
    }
}

impl Default for VectorClock {
    fn default() -> Self {
        Self::new()
    }
}

/// Domain service for causal ordering operations
pub struct CausalOrderingService;

impl CausalOrderingService {
    /// Increment the vector clock for a writer
    pub fn increment(clock: &mut VectorClock, writer_id: &str) {
        let current = clock.get(writer_id);
        clock.clocks.insert(writer_id.to_string(), current + 1);
    }

    /// Update clock with causal dependency
    pub fn update_with_parent(clock: &mut VectorClock, parent: &VectorClock) {
        for (writer_id, &parent_seq) in &parent.clocks {
            let current = clock.get(writer_id);
            if parent_seq > current {
                clock.clocks.insert(writer_id.clone(), parent_seq);
            }
        }
    }

    /// Check if a happened before b
    pub fn happened_before(a: &VectorClock, b: &VectorClock) -> bool {
        // a happened-before b if:
        // 1. For all writers in a: a[w] <= b[w]
        // 2. There exists at least one writer where a[w] < b[w]

        let all_leq = true;
        let mut exists_less = false;

        for (writer, &seq_a) in &a.clocks {
            let seq_b = b.get(writer);
            if seq_a > seq_b {
                return false;
            }
            if seq_a < seq_b {
                exists_less = true;
            }
        }

        // Also check writers that exist in b but not in a
        for writer in b.clocks.keys() {
            if !a.clocks.contains_key(writer) && b.get(writer) > 0 {
                exists_less = true;
            }
        }

        all_leq && exists_less
    }

    /// Check if two events are concurrent
    pub fn are_concurrent(a: &VectorClock, b: &VectorClock) -> bool {
        !Self::happened_before(a, b) && !Self::happened_before(b, a)
    }

    /// Compare for causal ordering (for sorting)
    pub fn causal_compare(a: &VectorClock, b: &VectorClock) -> Option<std::cmp::Ordering> {
        if Self::happened_before(a, b) {
            Some(std::cmp::Ordering::Less)
        } else if Self::happened_before(b, a) {
            Some(std::cmp::Ordering::Greater)
        } else {
            None // Concurrent
        }
    }

    /// Calculate L1 (Manhattan) distance between two vector clocks.
    ///
    /// This represents the total number of events that happened
    /// between the two clock states across all writers.
    ///
    /// # Arguments
    ///
    /// * `a` - First vector clock
    /// * `b` - Second vector clock
    ///
    /// # Returns
    ///
    /// The total causal distance as the sum of absolute differences
    pub fn causal_distance(a: &VectorClock, b: &VectorClock) -> usize {
        let mut distance = 0;

        // Check all writers in both clocks
        let mut all_writers: Vec<String> = a.clocks.keys().cloned().collect();
        for writer in b.clocks.keys() {
            if !all_writers.contains(writer) {
                all_writers.push(writer.clone());
            }
        }

        for writer in all_writers {
            let seq_a = a.get(&writer);
            let seq_b = b.get(&writer);
            distance += seq_a.abs_diff(seq_b) as usize;
        }

        distance
    }
}

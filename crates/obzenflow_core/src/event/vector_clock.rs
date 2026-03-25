// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Vector clock for causal ordering
//!
//! This is a simple data structure that holds component->sequence mappings.
//! The causal ordering logic is implemented separately in domain services.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::journal::JournalError;

use crate::event::types::EventId;

/// Vector clock data structure for causal ordering
///
/// This is a pure data structure representing the causal history of an event.
/// Use CausalOrderingService for vector clock operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorClock {
    /// Map from writer ID to sequence number
    /// Using String for writer ID to keep it simple and serializable
    ///
    /// `BTreeMap` provides deterministic iteration order, which helps keep JSON
    /// encodings stable for hashing/replay tooling.
    pub clocks: BTreeMap<String, u64>,
}

impl VectorClock {
    /// Create an empty vector clock
    pub fn new() -> Self {
        Self {
            clocks: BTreeMap::new(),
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

    /// A deterministic scalar derived from a vector clock.
    ///
    /// This scalar is strictly monotonic under happened-before: if `a` happened-before `b`, then
    /// `causal_rank(a) < causal_rank(b)`.
    pub fn causal_rank(clock: &VectorClock) -> u128 {
        clock
            .clocks
            .values()
            .fold(0u128, |acc, &seq| acc.saturating_add(seq as u128))
    }

    /// Deterministically compare two vector clocks using a monotonic scalar plus `EventId`.
    ///
    /// Note: a comparator defined as "happened-before first, otherwise `EventId`" is not a strict
    /// total order and must not be used with `slice::sort_by`, as it can violate transitivity and
    /// trigger Rust's sort-time total-order checks.
    pub fn total_compare_by_event_id(
        a_clock: &VectorClock,
        a_event_id: &EventId,
        b_clock: &VectorClock,
        b_event_id: &EventId,
    ) -> std::cmp::Ordering {
        let a_rank = Self::causal_rank(a_clock);
        let b_rank = Self::causal_rank(b_clock);

        a_rank.cmp(&b_rank).then_with(|| a_event_id.cmp(b_event_id))
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

    /// Produce a deterministic causal readback order using a monotonic scalar plus `EventId`.
    ///
    /// This is intended for *iteration* APIs like `Journal::read_causally_ordered()` that must be
    /// deterministic and must not use wall-clock timestamps. It guarantees that if `a`
    /// happened-before `b`, then `a` appears before `b` in the output.
    pub fn order_envelopes_by_event_id<T>(
        mut events: Vec<super::EventEnvelope<T>>,
    ) -> Result<Vec<super::EventEnvelope<T>>, JournalError>
    where
        T: super::JournalEvent,
    {
        // Fast path.
        if events.len() <= 1 {
            return Ok(events);
        }

        events.sort_by_cached_key(|e| (Self::causal_rank(&e.vector_clock), *e.event.id()));

        Ok(events)
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::chain_event::ChainEventFactory;
    use crate::event::{ChainEvent, EventEnvelope};
    use crate::{StageId, WriterId};
    use chrono::Utc;
    use serde_json::json;

    fn envelope_with_event_id_and_clock(
        event_id: EventId,
        vector_clock: VectorClock,
    ) -> EventEnvelope<ChainEvent> {
        let writer_id = WriterId::from(StageId::new());
        let mut event =
            ChainEventFactory::data_event(writer_id, "test.vector_clock", json!({ "ok": true }));
        event.id = event_id;

        EventEnvelope {
            journal_writer_id: crate::event::JournalWriterId::new(),
            vector_clock,
            timestamp: Utc::now(),
            event,
        }
    }

    #[test]
    fn transitivity_violation_regression_orders_deterministically() {
        let w1 = WriterId::from(StageId::new()).to_string();
        let w2 = WriterId::from(StageId::new()).to_string();
        let w3 = WriterId::from(StageId::new()).to_string();

        let mut clock_a = VectorClock::new();
        clock_a.clocks.insert(w1.clone(), 1);

        let mut clock_b = VectorClock::new();
        clock_b.clocks.insert(w1.clone(), 1);
        clock_b.clocks.insert(w2.clone(), 1);

        let mut clock_c = VectorClock::new();
        clock_c.clocks.insert(w2.clone(), 1);
        clock_c.clocks.insert(w3.clone(), 1);

        assert!(CausalOrderingService::happened_before(&clock_a, &clock_b));
        assert!(CausalOrderingService::are_concurrent(&clock_b, &clock_c));
        assert!(CausalOrderingService::are_concurrent(&clock_a, &clock_c));

        let a_id = EventId::from_string("ZZZZZZZZZZZZZZZZZZZZZZZZZZ").unwrap();
        let b_id = EventId::from_string("00000000000000000000000000").unwrap();
        let c_id = EventId::from_string("MMMMMMMMMMMMMMMMMMMMMMMMMM").unwrap();

        let a = envelope_with_event_id_and_clock(a_id, clock_a);
        let b = envelope_with_event_id_and_clock(b_id, clock_b);
        let c = envelope_with_event_id_and_clock(c_id, clock_c);

        let input = vec![c.clone(), a.clone(), b.clone()];
        let output1 = CausalOrderingService::order_envelopes_by_event_id(input.clone()).unwrap();
        let output2 = CausalOrderingService::order_envelopes_by_event_id(input).unwrap();

        let ids1: Vec<_> = output1.iter().map(|e| e.event.id).collect();
        let ids2: Vec<_> = output2.iter().map(|e| e.event.id).collect();

        assert_eq!(ids1, vec![a_id, b_id, c_id]);
        assert_eq!(ids1, ids2);

        let idx_a = ids1.iter().position(|id| *id == a_id).unwrap();
        let idx_b = ids1.iter().position(|id| *id == b_id).unwrap();
        assert!(idx_a < idx_b);
    }

    #[test]
    fn order_is_stable_under_permutation() {
        let w1 = WriterId::from(StageId::new()).to_string();
        let w2 = WriterId::from(StageId::new()).to_string();
        let w3 = WriterId::from(StageId::new()).to_string();

        let mut clock_a = VectorClock::new();
        clock_a.clocks.insert(w1.clone(), 1);

        let mut clock_b = VectorClock::new();
        clock_b.clocks.insert(w1.clone(), 1);
        clock_b.clocks.insert(w2.clone(), 1);

        let mut clock_c = VectorClock::new();
        clock_c.clocks.insert(w2.clone(), 1);
        clock_c.clocks.insert(w3.clone(), 1);

        let a_id = EventId::from_string("ZZZZZZZZZZZZZZZZZZZZZZZZZZ").unwrap();
        let b_id = EventId::from_string("00000000000000000000000000").unwrap();
        let c_id = EventId::from_string("MMMMMMMMMMMMMMMMMMMMMMMMMM").unwrap();

        let a = envelope_with_event_id_and_clock(a_id, clock_a);
        let b = envelope_with_event_id_and_clock(b_id, clock_b);
        let c = envelope_with_event_id_and_clock(c_id, clock_c);

        let expected = vec![a_id, b_id, c_id];
        let permutations = [
            vec![a.clone(), b.clone(), c.clone()],
            vec![a.clone(), c.clone(), b.clone()],
            vec![b.clone(), a.clone(), c.clone()],
            vec![b.clone(), c.clone(), a.clone()],
            vec![c.clone(), a.clone(), b.clone()],
            vec![c.clone(), b.clone(), a.clone()],
        ];

        for permutation in permutations {
            let ordered = CausalOrderingService::order_envelopes_by_event_id(permutation).unwrap();
            let ordered_ids: Vec<_> = ordered.iter().map(|e| e.event.id).collect();
            assert_eq!(ordered_ids, expected);
        }
    }

    #[test]
    fn causal_rank_sums_components_and_respects_happened_before() {
        let empty = VectorClock::new();
        assert_eq!(CausalOrderingService::causal_rank(&empty), 0);

        let mut single = VectorClock::new();
        single.clocks.insert("writer_1".to_string(), 3);
        assert_eq!(CausalOrderingService::causal_rank(&single), 3);

        let mut multi = VectorClock::new();
        multi.clocks.insert("writer_1".to_string(), 2);
        multi.clocks.insert("writer_2".to_string(), 3);
        assert_eq!(CausalOrderingService::causal_rank(&multi), 5);

        let mut a = VectorClock::new();
        a.clocks.insert("writer_1".to_string(), 1);

        let mut b = VectorClock::new();
        b.clocks.insert("writer_1".to_string(), 1);
        b.clocks.insert("writer_2".to_string(), 1);

        assert!(CausalOrderingService::happened_before(&a, &b));
        assert!(CausalOrderingService::causal_rank(&a) < CausalOrderingService::causal_rank(&b));
    }
}

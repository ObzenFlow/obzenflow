// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Vector clock for causal ordering
//!
//! This is a simple data structure that holds component->sequence mappings.
//! The causal ordering logic is implemented separately in domain services.

use super::{CausalCoordinate, CausalError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;

use crate::journal::JournalError;

use crate::event::types::EventId;

/// Finite admission limit, rejected rather than silently truncating evidence.
/// The physical codec also enforces its frame and collection byte budgets.
pub const MAX_CAUSAL_COORDINATES: usize = 65_536;

pub(super) fn bounded_entries<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Entries<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Entries<T> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a bounded list of causal entries")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Vec<T>, A::Error> {
            let mut values = Vec::new();
            while let Some(value) = sequence.next_element()? {
                if values.len() == MAX_CAUSAL_COORDINATES {
                    return Err(serde::de::Error::custom(
                        "causal coordinate budget exceeded",
                    ));
                }
                values.push(value);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_seq(Entries(std::marker::PhantomData))
}

/// Vector clock data structure for causal ordering
///
/// This is a pure data structure representing the causal history of an event.
/// Use CausalOrderingService for vector clock operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorClock {
    /// Sequence numbers keyed by physical journal incarnation.
    ///
    /// `BTreeMap` provides deterministic iteration order, which helps keep JSON
    /// encodings stable for hashing/replay tooling.
    pub clocks: BTreeMap<CausalCoordinate, u64>,
}

/// The wire format is a sorted set of typed entries, never string-parsed writer keys.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClockEntry {
    journal_writer_id: super::JournalWriterId,
    sequence: u64,
}

impl Serialize for VectorClock {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        if self.clocks.len() > MAX_CAUSAL_COORDINATES
            || self.clocks.values().any(|sequence| *sequence == 0)
        {
            return Err(serde::ser::Error::custom(
                "invalid or oversized causal clock",
            ));
        }
        let entries: Vec<_> = self
            .clocks
            .iter()
            .map(|(coordinate, sequence)| ClockEntry {
                journal_writer_id: coordinate.journal_writer_id,
                sequence: *sequence,
            })
            .collect();
        let mut wire = serializer.serialize_struct("VectorClock", 1)?;
        wire.serialize_field("entries", &entries)?;
        wire.end()
    }
}

impl<'de> Deserialize<'de> for VectorClock {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            #[serde(deserialize_with = "bounded_entries")]
            entries: Vec<ClockEntry>,
        }
        let wire = Wire::deserialize(deserializer)?;
        let mut clocks = BTreeMap::new();
        for entry in wire.entries {
            if entry.sequence == 0
                || clocks
                    .insert(
                        CausalCoordinate::new(entry.journal_writer_id),
                        entry.sequence,
                    )
                    .is_some()
            {
                return Err(serde::de::Error::custom(
                    "zero or duplicate causal coordinate",
                ));
            }
        }
        Ok(Self { clocks })
    }
}

impl VectorClock {
    /// Create an empty vector clock
    pub fn new() -> Self {
        Self {
            clocks: BTreeMap::new(),
        }
    }

    /// Get the sequence number for a writer
    pub fn get(&self, writer_id: &CausalCoordinate) -> u64 {
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
    pub fn increment(
        clock: &mut VectorClock,
        writer_id: &CausalCoordinate,
    ) -> Result<(), CausalError> {
        let next = clock
            .get(writer_id)
            .checked_add(1)
            .ok_or(CausalError::SequenceExhausted)?;
        clock.clocks.insert(*writer_id, next);
        Ok(())
    }

    /// Update clock with causal dependency
    pub fn update_with_parent(clock: &mut VectorClock, parent: &VectorClock) {
        for (writer_id, &parent_seq) in &parent.clocks {
            let current = clock.get(writer_id);
            if parent_seq > current {
                clock.clocks.insert(*writer_id, parent_seq);
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
        let mut all_writers: Vec<CausalCoordinate> = a.clocks.keys().cloned().collect();
        for writer in b.clocks.keys() {
            if !all_writers.contains(writer) {
                all_writers.push(*writer);
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
    pub fn order_envelopes_by_event_id<P>(
        mut events: Vec<super::JournalRecord<P>>,
    ) -> Result<Vec<super::JournalRecord<P>>, JournalError>
    where
        P: super::payloads::JournalPayload,
    {
        // Fast path.
        if events.len() <= 1 {
            return Ok(events);
        }

        events.sort_by_cached_key(|e| {
            (
                Self::causal_rank(&e.envelope.provenance.journal.vector_clock),
                *e.id(),
            )
        });

        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::chain_event::ChainEventFactory;
    use crate::event::provenance::JournalProvenance;
    use crate::event::{ChainPayload, JournalRecord, JournalWriterId};
    use crate::{StageId, WriterId};
    use chrono::Utc;
    use serde_json::json;

    fn envelope_with_event_id_and_clock(
        event_id: EventId,
        vector_clock: VectorClock,
    ) -> JournalRecord<ChainPayload> {
        let coordinate = *vector_clock.clocks.keys().next().unwrap();
        let writer_id = WriterId::from(StageId::new());
        let mut event =
            ChainEventFactory::data_event(writer_id, "test.vector_clock", json!({ "ok": true }));
        event.id = event_id;

        JournalRecord::commit_event(
            event,
            JournalProvenance {
                run_id: crate::FlowId::new(),
                causal: Default::default(),
                journal_writer_id: coordinate.journal_writer_id,
                vector_clock,
                timestamp: Utc::now(),
                journal_group_id: None,
                journal_group_member: None,
            },
        )
        .unwrap()
    }

    #[test]
    fn transitivity_violation_regression_orders_deterministically() {
        let w1 = CausalCoordinate::new(JournalWriterId::new());
        let w2 = CausalCoordinate::new(JournalWriterId::new());
        let w3 = CausalCoordinate::new(JournalWriterId::new());

        let mut clock_a = VectorClock::new();
        clock_a.clocks.insert(w1, 1);

        let mut clock_b = VectorClock::new();
        clock_b.clocks.insert(w1, 1);
        clock_b.clocks.insert(w2, 1);

        let mut clock_c = VectorClock::new();
        clock_c.clocks.insert(w2, 1);
        clock_c.clocks.insert(w3, 1);

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

        let ids1: Vec<_> = output1
            .iter()
            .map(|e| e.envelope.provenance.event.id)
            .collect();
        let ids2: Vec<_> = output2
            .iter()
            .map(|e| e.envelope.provenance.event.id)
            .collect();

        assert_eq!(ids1, vec![a_id, b_id, c_id]);
        assert_eq!(ids1, ids2);

        let idx_a = ids1.iter().position(|id| *id == a_id).unwrap();
        let idx_b = ids1.iter().position(|id| *id == b_id).unwrap();
        assert!(idx_a < idx_b);
    }

    #[test]
    fn order_is_stable_under_permutation() {
        let w1 = CausalCoordinate::new(JournalWriterId::new());
        let w2 = CausalCoordinate::new(JournalWriterId::new());
        let w3 = CausalCoordinate::new(JournalWriterId::new());

        let mut clock_a = VectorClock::new();
        clock_a.clocks.insert(w1, 1);

        let mut clock_b = VectorClock::new();
        clock_b.clocks.insert(w1, 1);
        clock_b.clocks.insert(w2, 1);

        let mut clock_c = VectorClock::new();
        clock_c.clocks.insert(w2, 1);
        clock_c.clocks.insert(w3, 1);

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
            let ordered_ids: Vec<_> = ordered
                .iter()
                .map(|e| e.envelope.provenance.event.id)
                .collect();
            assert_eq!(ordered_ids, expected);
        }
    }

    #[test]
    fn causal_rank_sums_components_and_respects_happened_before() {
        let w1 = CausalCoordinate::new(JournalWriterId::new());
        let w2 = CausalCoordinate::new(JournalWriterId::new());
        let empty = VectorClock::new();
        assert_eq!(CausalOrderingService::causal_rank(&empty), 0);

        let mut single = VectorClock::new();
        single.clocks.insert(w1, 3);
        assert_eq!(CausalOrderingService::causal_rank(&single), 3);

        let mut multi = VectorClock::new();
        multi.clocks.insert(w1, 2);
        multi.clocks.insert(w2, 3);
        assert_eq!(CausalOrderingService::causal_rank(&multi), 5);

        let mut a = VectorClock::new();
        a.clocks.insert(w1, 1);

        let mut b = VectorClock::new();
        b.clocks.insert(w1, 1);
        b.clocks.insert(w2, 1);

        assert!(CausalOrderingService::happened_before(&a, &b));
        assert!(CausalOrderingService::causal_rank(&a) < CausalOrderingService::causal_rank(&b));
    }
}

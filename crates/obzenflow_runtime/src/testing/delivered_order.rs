// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Delivered-order projections for FLOWIP-095d determinism assertions.
//!
//! A fan-in stage's delivered input order is not directly recorded (the
//! canonical merge records nothing); it is witnessed by the stage's output
//! journal, where outputs appear in delivery order and each output's causality
//! names the consumed input. Projecting outputs onto
//! `(upstream stage key, per-input ordinal, event type, payload)` rows gives a
//! comparison that is stable across runs, while raw journal bytes legitimately
//! differ until FLOWIP-120o because event ids, envelope timestamps, and clock
//! values regenerate per run.
//!
//! Constraint: the projection only witnesses inputs that produced at least one
//! output, so determinism fixtures should emit one output per consumed input.

use obzenflow_core::event::{ChainEvent, EventEnvelope};
use obzenflow_core::EventId;
use std::collections::HashMap;

/// One delivered-order row: an output of the fan-in stage attributed to the
/// consumed input that produced it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveredOrderRow {
    /// Stable key of the upstream stage the consumed input came from.
    pub upstream_stage_key: String,
    /// 1-based ordinal of the consumed input within its upstream, counted in
    /// first-consumption order.
    pub per_input_ordinal: u64,
    /// The output's event type.
    pub event_type: String,
    /// The output's payload serialized as JSON (deterministic across runs for
    /// deterministic handlers).
    pub payload: String,
    /// The consumed input's event id in THIS run (per-run; never compare
    /// across runs, use it only for intra-run causality assertions).
    pub parent_event_id: EventId,
}

/// The delivered-order projection of a fan-in stage: its data outputs in
/// journal append order, attributed to consumed inputs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveredOrderProjection {
    pub rows: Vec<DeliveredOrderRow>,
}

impl DeliveredOrderProjection {
    /// Project a fan-in stage's delivered order from its output envelopes and
    /// the envelopes of its upstream journals.
    ///
    /// `stage_outputs` must be in journal append order. Each upstream is
    /// `(stable stage key, envelopes)`. Outputs whose parent is not found in
    /// any upstream (for example framework rows) are skipped.
    pub fn from_envelopes(
        stage_outputs: &[EventEnvelope<ChainEvent>],
        upstreams: &[(String, Vec<EventEnvelope<ChainEvent>>)],
    ) -> Self {
        let mut parent_index: HashMap<EventId, &str> = HashMap::new();
        for (stage_key, envelopes) in upstreams {
            for envelope in envelopes {
                if envelope.event.is_data() {
                    parent_index.insert(envelope.event.id, stage_key.as_str());
                }
            }
        }

        let mut ordinals: HashMap<&str, u64> = HashMap::new();
        let mut assigned: HashMap<EventId, (String, u64)> = HashMap::new();
        let mut rows = Vec::new();

        for envelope in stage_outputs {
            let event = &envelope.event;
            if !event.is_data() {
                continue;
            }
            let Some(parent_id) = event.causality.parent_ids.first().copied() else {
                continue;
            };
            let Some(stage_key) = parent_index.get(&parent_id).copied() else {
                continue;
            };
            let (upstream_stage_key, per_input_ordinal) = assigned
                .entry(parent_id)
                .or_insert_with(|| {
                    let next = ordinals.entry(stage_key).or_insert(0);
                    *next += 1;
                    (stage_key.to_string(), *next)
                })
                .clone();

            let payload = match &event.content {
                obzenflow_core::event::ChainEventContent::Data { payload, .. } => {
                    serde_json::to_string(payload).unwrap_or_default()
                }
                _ => String::new(),
            };

            rows.push(DeliveredOrderRow {
                upstream_stage_key,
                per_input_ordinal,
                event_type: event.event_type(),
                payload,
                parent_event_id: parent_id,
            });
        }

        Self { rows }
    }

    /// The sequence of consumed inputs as (upstream stage key, ordinal) pairs,
    /// in first-consumption order (one entry per input, however many outputs
    /// it produced).
    pub fn consumption_sequence(&self) -> Vec<(String, u64)> {
        let mut sequence = Vec::new();
        let mut last: Option<EventId> = None;
        for row in &self.rows {
            if last != Some(row.parent_event_id) {
                sequence.push((row.upstream_stage_key.clone(), row.per_input_ordinal));
                last = Some(row.parent_event_id);
            }
        }
        sequence
    }

    /// Assert equality against another run's projection with a useful diff on
    /// the first divergence. Parent event ids are deliberately excluded from
    /// the comparison (they are per-run until FLOWIP-120o).
    pub fn assert_equal(&self, other: &Self) {
        let comparable = |projection: &Self| {
            projection
                .rows
                .iter()
                .map(|row| {
                    (
                        row.upstream_stage_key.clone(),
                        row.per_input_ordinal,
                        row.event_type.clone(),
                        row.payload.clone(),
                    )
                })
                .collect::<Vec<_>>()
        };
        let left = comparable(self);
        let right = comparable(other);

        for (index, (a, b)) in left.iter().zip(right.iter()).enumerate() {
            assert_eq!(
                a, b,
                "delivered-order projections diverge at row {index}: {a:?} vs {b:?}"
            );
        }
        assert_eq!(
            left.len(),
            right.len(),
            "delivered-order projections differ in length: {} vs {}",
            left.len(),
            right.len()
        );
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::context::{CompositeActivationContext, ReplayContext};
use obzenflow_core::event::payloads::correlation_payload::CorrelationPayload;
use obzenflow_core::event::types::{CorrelationId, EventId};
use obzenflow_core::ChainEvent;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};

// FLOWIP-010 §7: the lineage policy is a PARAMETER to `record_event`, never a
// `TraceState` field. TraceState is serialized accumulator state; build
// configuration must not leak into persisted state snapshots.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct TraceState {
    parent_ids: VecDeque<EventId>,

    correlation_seen: bool,
    correlation_mixed: bool,
    correlation_ids: BTreeSet<CorrelationId>,
    correlation_ids_truncated: bool,
    correlation_id: Option<CorrelationId>,
    correlation_payload: Option<CorrelationPayload>,
    replay_context: Option<ReplayContext>,
    /// Exact, untruncated composite inputs that contributed to this state.
    #[serde(default)]
    composite_activations: Vec<CompositeActivationContext>,
}

impl TraceState {
    fn record_correlation_id(&mut self, correlation_id: CorrelationId, max_depth: usize) {
        if self.correlation_ids.len() < max_depth || self.correlation_ids.contains(&correlation_id)
        {
            self.correlation_ids.insert(correlation_id);
        } else {
            self.correlation_ids_truncated = true;
        }
    }

    pub(crate) fn record_event(
        &mut self,
        event: &ChainEvent,
        lineage: obzenflow_core::config::LineagePolicy,
    ) {
        if event.is_lifecycle() || event.is_control() {
            return;
        }

        let max_depth = lineage.max_lineage_depth.max(1);

        self.parent_ids.push_back(event.id);
        while self.parent_ids.len() > max_depth {
            self.parent_ids.pop_front();
        }

        for activation in event.composite_activations() {
            if !self.composite_activations.iter().any(|existing| {
                existing.composite_id == activation.composite_id
                    && existing.activation == activation.activation
                    && existing.entry_port == activation.entry_port
            }) {
                self.composite_activations.push(activation.clone());
            }
        }
        self.composite_activations.sort_by(|left, right| {
            (
                left.composite_id.as_ref(),
                left.activation,
                left.entry_port.as_str(),
            )
                .cmp(&(
                    right.composite_id.as_ref(),
                    right.activation,
                    right.entry_port.as_str(),
                ))
        });

        if let Some(correlation_ids) = event.correlation_ids() {
            for correlation_id in correlation_ids {
                self.record_correlation_id(*correlation_id, max_depth);
            }
        }
        if event.correlation_ids_truncated() {
            self.correlation_ids_truncated = true;
        }

        let has_composite_correlation = event
            .correlation
            .as_ref()
            .is_some_and(|correlation| correlation.single_id().is_none())
            || event.correlation_ids_truncated();

        if has_composite_correlation {
            self.correlation_seen = true;
            self.correlation_mixed = true;
            self.correlation_id = None;
            self.correlation_payload = None;
            self.replay_context = None;
            return;
        }

        if !self.correlation_seen {
            self.correlation_seen = true;
            self.correlation_id = event.correlation_id();
            self.correlation_payload = event.correlation_payload().cloned();
            self.replay_context = event.replay_context.clone();
            return;
        }

        if self.correlation_mixed {
            return;
        }

        if self.correlation_id != event.correlation_id() {
            self.correlation_mixed = true;
            self.correlation_id = None;
            self.correlation_payload = None;
            self.replay_context = None;
            return;
        }

        if self.correlation_payload.as_ref() != event.correlation_payload() {
            self.correlation_payload = None;
        }
        if self.replay_context != event.replay_context {
            self.replay_context = None;
        }
    }

    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }

    pub(crate) fn parent_ids(&self) -> Vec<EventId> {
        self.parent_ids.iter().copied().collect()
    }

    pub(crate) fn correlation_id(&self) -> Option<CorrelationId> {
        self.correlation_id
    }

    pub(crate) fn correlation_payload(&self) -> Option<CorrelationPayload> {
        self.correlation_payload.clone()
    }

    pub(crate) fn replay_context(&self) -> Option<ReplayContext> {
        self.replay_context.clone()
    }

    pub(crate) fn mixed_correlation_ids(&self) -> Option<Vec<CorrelationId>> {
        if self.correlation_mixed && !self.correlation_ids.is_empty() {
            Some(self.correlation_ids.iter().copied().collect())
        } else {
            None
        }
    }

    pub(crate) fn correlation_ids_truncated(&self) -> bool {
        self.correlation_ids_truncated
    }

    pub(crate) fn apply_correlation_to_event(&self, event: &mut ChainEvent) {
        for activation in &self.composite_activations {
            event.add_composite_activation(activation.clone());
        }
        if let Some(ids) = self.mixed_correlation_ids() {
            event.clear_correlation();
            event.replay_context = None;
            event.set_correlation_sample(ids, self.correlation_ids_truncated());
        } else {
            if let Some(correlation_id) = self.correlation_id() {
                event.set_single_correlation(correlation_id, self.correlation_payload());
            } else {
                event.clear_correlation();
            }
            event.replay_context = self.replay_context();
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.parent_ids.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::event::types::WriterId;
    use obzenflow_core::id::CompositeId;
    use obzenflow_core::StageId;
    use serde_json::json;
    use ulid::Ulid;

    fn event_with_correlation(correlation_id: CorrelationId) -> ChainEvent {
        let mut event =
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "in", json!({}));
        event.set_single_correlation(correlation_id, None);
        event
    }

    fn deterministic_correlation_id(value: u128) -> CorrelationId {
        CorrelationId::from_ulid(Ulid::from(value))
    }

    #[test]
    fn records_all_distinct_correlation_ids_under_cap() {
        let mut trace = TraceState::default();
        let ids = vec![
            deterministic_correlation_id(3),
            deterministic_correlation_id(1),
            deterministic_correlation_id(2),
        ];

        for id in &ids {
            trace.record_event(
                &event_with_correlation(*id),
                obzenflow_core::config::LineagePolicy::default(),
            );
        }

        assert_eq!(
            trace.mixed_correlation_ids(),
            Some(vec![
                deterministic_correlation_id(1),
                deterministic_correlation_id(2),
                deterministic_correlation_id(3),
            ])
        );
        assert!(!trace.correlation_ids_truncated());
    }

    #[test]
    fn records_bounded_replay_stable_sample_when_correlation_ids_exceed_cap() {
        let mut trace = TraceState::default();
        let ids: Vec<_> = (100..=200)
            .rev()
            .map(deterministic_correlation_id)
            .collect();

        for id in &ids {
            trace.record_event(
                &event_with_correlation(*id),
                obzenflow_core::config::LineagePolicy::default(),
            );
        }

        let recorded = trace
            .mixed_correlation_ids()
            .expect("expected mixed correlation IDs");
        assert_eq!(recorded.len(), 100);
        assert!(trace.correlation_ids_truncated());
        assert!(!recorded.contains(&deterministic_correlation_id(100)));

        let mut expected: Vec<_> = ids.iter().copied().take(100).collect();
        expected.sort();
        assert_eq!(recorded, expected);
    }

    #[test]
    fn carries_truncation_from_upstream_mixed_correlation_event() {
        let mut event =
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "aggregate", json!({}));
        event.set_correlation_sample(vec![deterministic_correlation_id(1)], true);

        let mut trace = TraceState::default();
        trace.record_event(&event, obzenflow_core::config::LineagePolicy::default());

        assert_eq!(
            trace.mixed_correlation_ids(),
            Some(vec![deterministic_correlation_id(1)])
        );
        assert!(trace.correlation_ids_truncated());
    }

    #[test]
    fn composite_activation_union_is_untruncated_and_survives_state_replay() {
        let writer = WriterId::from(StageId::new());
        let mut trace = TraceState::default();
        for index in 0..3 {
            let mut input = ChainEventFactory::data_event(writer, "input", json!({}));
            input.add_composite_activation(CompositeActivationContext::new(
                CompositeId::new("saga:checkout"),
                input.id,
                format!("input_{index}"),
                100 + index,
            ));
            trace.record_event(
                &input,
                obzenflow_core::config::LineagePolicy {
                    max_lineage_depth: 1,
                },
            );
        }

        let encoded = serde_json::to_vec(&trace).unwrap();
        let replayed: TraceState = serde_json::from_slice(&encoded).unwrap();
        let mut output = ChainEventFactory::data_event(writer, "output", json!({}));
        replayed.apply_correlation_to_event(&mut output);
        assert_eq!(output.composite_activations().len(), 3);
        let mut ports: Vec<_> = output
            .composite_activations()
            .iter()
            .map(|activation| activation.entry_port.as_str())
            .collect();
        ports.sort_unstable();
        assert_eq!(ports, vec!["input_0", "input_1", "input_2"]);
    }
}

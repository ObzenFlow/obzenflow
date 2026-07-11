// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{ChainEvent, CorrelationContext};
use crate::event::context::observability_context::ObservabilityContext;
use crate::event::context::CompositeActivationContext;
use crate::event::payloads::correlation_payload::CorrelationPayload;
use crate::event::types::CorrelationId;

impl ChainEvent {
    /// Create correlation for a source event (flow entry)
    pub fn with_new_correlation(mut self, stage_name: impl Into<String>) -> Self {
        let correlation_id = CorrelationId::new();
        self.set_single_correlation(
            correlation_id,
            Some(CorrelationPayload::new(stage_name, self.id)),
        );
        self
    }

    /// Propagate correlation and cycle state from parent event to derived event
    pub fn with_correlation_from(mut self, parent: &ChainEvent) -> Self {
        self.correlation = parent.correlation.clone();
        self.cycle_depth = parent.cycle_depth;
        self.cycle_scc_id = parent.cycle_scc_id;
        self.merge_composite_activations_from(parent);
        self
    }

    /// Composite activations that causally contribute to this event.
    pub fn composite_activations(&self) -> &[CompositeActivationContext] {
        self.observability
            .as_ref()
            .map(|observability| observability.composite_activations.as_slice())
            .unwrap_or_default()
    }

    /// Stamp one reconstructable input-boundary activation, idempotently.
    pub fn add_composite_activation(&mut self, activation: CompositeActivationContext) {
        let activations = &mut self
            .observability
            .get_or_insert_with(ObservabilityContext::default)
            .composite_activations;
        if !activations.iter().any(|existing| {
            existing.composite_id == activation.composite_id
                && existing.activation == activation.activation
                && existing.entry_port == activation.entry_port
        }) {
            activations.push(activation);
            activations.sort_by(|left, right| {
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
        }
    }

    /// Union exact activation provenance from one causal parent.
    pub fn merge_composite_activations_from(&mut self, parent: &ChainEvent) {
        for activation in parent.composite_activations() {
            self.add_composite_activation(activation.clone());
        }
    }

    /// Check if this event has correlation info
    pub fn has_correlation(&self) -> bool {
        self.correlation.is_some()
    }

    /// Calculate latency if this event has correlation payload
    pub fn correlation_latency(&self) -> Option<std::time::Duration> {
        self.correlation_payload().map(|p| p.calculate_latency())
    }

    pub fn correlation_id(&self) -> Option<CorrelationId> {
        self.correlation.as_ref().and_then(|c| c.single_id())
    }

    pub fn correlation_ids(&self) -> Option<&[CorrelationId]> {
        self.correlation.as_ref().map(|c| c.ids.as_slice())
    }

    pub fn correlation_payload(&self) -> Option<&CorrelationPayload> {
        self.correlation.as_ref().and_then(|c| c.payload.as_ref())
    }

    pub fn correlation_ids_truncated(&self) -> bool {
        self.correlation
            .as_ref()
            .map(|c| c.truncated)
            .unwrap_or(false)
    }

    pub fn set_single_correlation(
        &mut self,
        id: CorrelationId,
        payload: Option<CorrelationPayload>,
    ) {
        self.correlation = Some(CorrelationContext::single(id, payload));
    }

    pub fn set_correlation_sample(&mut self, ids: Vec<CorrelationId>, truncated: bool) {
        self.correlation = Some(CorrelationContext::sample(ids, truncated));
    }

    pub fn clear_correlation(&mut self) {
        self.correlation = None;
    }
}

#[cfg(test)]
mod composite_activation_tests {
    use crate::config::LineagePolicy;
    use crate::event::chain_event::ChainEventFactory;
    use crate::event::context::CompositeActivationContext;
    use crate::event::types::WriterId;
    use crate::id::{CompositeId, StageId};
    use serde_json::json;

    #[test]
    fn derived_events_preserve_exact_activation_identity() {
        let writer = WriterId::from(StageId::new());
        let mut entry = ChainEventFactory::data_event(writer, "test.input.v1", json!({}));
        entry.processing_info.event_time = 100;
        entry.add_composite_activation(CompositeActivationContext::new(
            CompositeId::new("test:composite"),
            entry.id,
            "in",
            entry.processing_info.event_time,
        ));

        let child = ChainEventFactory::derived_data_event(
            writer,
            &entry,
            "test.output.v1",
            json!({}),
            LineagePolicy::default(),
        );

        assert_eq!(child.composite_activations(), entry.composite_activations());
        let json = serde_json::to_value(&child).expect("event serializes");
        assert_eq!(
            json["observability"]["composite_activations"][0]["entry_port"],
            "in"
        );
        assert_eq!(
            json["observability"]["composite_activations"][0],
            serde_json::json!({
                "composite_id": "test:composite",
                "activation": entry.id,
                "entry_port": "in",
                "entered_at_ms": 100,
            })
        );

        let mut legacy = json;
        legacy["observability"]
            .as_object_mut()
            .expect("observability object")
            .remove("composite_activations");
        let legacy: crate::event::ChainEvent =
            serde_json::from_value(legacy).expect("pre-activation ChainEvent remains decodable");
        assert!(legacy.composite_activations().is_empty());
    }

    #[test]
    fn activation_merge_is_idempotent_and_deterministic() {
        let writer = WriterId::from(StageId::new());
        let mut event = ChainEventFactory::data_event(writer, "test.v1", json!({}));
        let activation =
            CompositeActivationContext::new(CompositeId::new("test:composite"), event.id, "in", 10);
        event.add_composite_activation(activation.clone());
        event.add_composite_activation(activation);
        assert_eq!(event.composite_activations().len(), 1);
    }
}

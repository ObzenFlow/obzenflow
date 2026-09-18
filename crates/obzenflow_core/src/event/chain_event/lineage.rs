// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{ChainEvent, CorrelationContext};
use crate::event::payloads::correlation_payload::CorrelationPayload;
use crate::event::provenance::composite_activation_context::{
    union_composite_activations, CompositeActivationConflict,
};
use crate::event::provenance::CompositeActivationContext;
use crate::event::types::CorrelationId;

impl ChainEvent {
    /// Create correlation for a source event (flow entry)
    pub fn with_new_correlation(mut self) -> Self {
        let correlation_id = CorrelationId::new();
        self.set_single_correlation(correlation_id, Some(CorrelationPayload::new(self.id)));
        self
    }

    /// Copy correlation from a parent event.
    pub fn with_correlation_from(mut self, parent: &ChainEvent) -> Self {
        self.correlation = parent.correlation.clone();
        self
    }

    /// Copy cycle state from a parent event.
    pub fn with_cycle_state_from(mut self, parent: &ChainEvent) -> Self {
        self.cycle_depth = parent.cycle_depth;
        self.cycle_scc_id = parent.cycle_scc_id;
        self
    }

    /// Return this event with the supplied activation provenance merged in.
    ///
    /// This value-oriented form is for event construction and one-parent
    /// propagation. Fan-in code should use [`Self::try_extend_composite_activations`].
    #[doc(hidden)]
    pub fn try_with_composite_activations(
        mut self,
        activations: Vec<CompositeActivationContext>,
    ) -> Result<Self, CompositeActivationConflict> {
        let merged = union_composite_activations(self.composite_activations(), &activations)?;
        self.replace_composite_activations(merged);
        Ok(self)
    }

    /// Composite activations that causally contribute to this event.
    pub fn composite_activations(&self) -> &[CompositeActivationContext] {
        &self.envelope.provenance.event.composite_activations
    }

    /// Insert one reconstructable input-boundary activation idempotently.
    ///
    /// Returns whether the canonical activation set changed. A duplicate
    /// identity carrying a different timestamp is conflicting durable
    /// evidence and returns an error.
    #[doc(hidden)]
    pub fn try_insert_composite_activation(
        &mut self,
        activation: CompositeActivationContext,
    ) -> Result<bool, CompositeActivationConflict> {
        Ok(self.try_extend_composite_activations(std::slice::from_ref(&activation))? > 0)
    }

    /// Extend this existing event with exact fan-in activation provenance.
    ///
    /// Returns the number of newly inserted activation identities. Mutation is
    /// reserved for runtime accumulation seams; construction uses
    /// [`Self::try_with_composite_activations`].
    #[doc(hidden)]
    pub fn try_extend_composite_activations(
        &mut self,
        incoming: &[CompositeActivationContext],
    ) -> Result<usize, CompositeActivationConflict> {
        let current = union_composite_activations(self.composite_activations(), &[])?;
        let merged = union_composite_activations(&current, incoming)?;
        let inserted = merged.len().saturating_sub(current.len());
        self.replace_composite_activations(merged);
        Ok(inserted)
    }

    fn replace_composite_activations(&mut self, activations: Vec<CompositeActivationContext>) {
        self.envelope.provenance.event.composite_activations = activations;
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
    use crate::event::provenance::CompositeActivationContext;
    use crate::event::types::WriterId;
    use crate::event::ChainEvent;
    use crate::id::{CompositeId, StageId};
    use serde_json::json;

    #[test]
    fn derived_events_preserve_exact_activation_identity() {
        let writer = WriterId::from(StageId::new());
        let mut entry = ChainEventFactory::data_event(writer, "test.input.v1", json!({}));
        entry.processing.event_time = 100;
        let entry_id = entry.id;
        let entered_at_ms = entry.processing.event_time;
        entry = entry
            .try_with_composite_activations(vec![CompositeActivationContext::new(
                CompositeId::new("test:composite"),
                entry_id,
                "in",
                entered_at_ms,
            )])
            .unwrap();

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
            json["envelope"]["provenance"]["event"]["composite_activations"][0]["entry_port"],
            "in"
        );
        assert_eq!(
            json["envelope"]["provenance"]["event"]["composite_activations"][0],
            serde_json::json!({
                "composite_id": "test:composite",
                "activation": entry.id,
                "entry_port": "in",
                "entered_at_ms": 100,
            })
        );

        let mut without_measurements = json;
        without_measurements["envelope"]
            .as_object_mut()
            .unwrap()
            .remove("observability");
        let decoded: ChainEvent = serde_json::from_value(without_measurements.clone()).unwrap();
        assert_eq!(
            decoded.composite_activations(),
            child.composite_activations()
        );
        without_measurements["envelope"]["provenance"]["event"]
            .as_object_mut()
            .unwrap()
            .remove("composite_activations");
        assert!(
            serde_json::from_value::<ChainEvent>(without_measurements).is_err(),
            "protected activation membership is required by this schema"
        );
    }

    #[test]
    fn activation_merge_is_idempotent_and_deterministic() {
        let writer = WriterId::from(StageId::new());
        let mut event = ChainEventFactory::data_event(writer, "test.v1", json!({}));
        let activation =
            CompositeActivationContext::new(CompositeId::new("test:composite"), event.id, "in", 10);
        assert!(event
            .try_insert_composite_activation(activation.clone())
            .unwrap());
        assert!(!event.try_insert_composite_activation(activation).unwrap());
        assert_eq!(event.composite_activations().len(), 1);
    }

    #[test]
    fn activation_merge_rejects_conflicting_entry_timestamps() {
        let writer = WriterId::from(StageId::new());
        let mut event = ChainEventFactory::data_event(writer, "test.v1", json!({}));
        let activation =
            CompositeActivationContext::new(CompositeId::new("test:composite"), event.id, "in", 10);
        event
            .try_insert_composite_activation(activation.clone())
            .unwrap();

        let conflicting = CompositeActivationContext::new(
            activation.composite_id.clone(),
            activation.activation,
            activation.entry_port.clone(),
            11,
        );
        let error = event
            .try_insert_composite_activation(conflicting)
            .unwrap_err();

        assert!(error
            .to_string()
            .contains("conflicting entry timestamps 10 and 11"));
        assert_eq!(event.composite_activations(), &[activation]);
    }
}

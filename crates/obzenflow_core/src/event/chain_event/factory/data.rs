// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ChainEventFactory;
use crate::config::LineagePolicy;
use crate::event::context::causality_context::CausalityContext;
use crate::event::types::WriterId;
use crate::event::{ChainEvent, ChainEventContent};
use serde_json::Value;

impl ChainEventFactory {
    /// Create a data event
    pub fn data_event(
        writer_id: WriterId,
        event_type: impl Into<String>,
        payload: Value,
    ) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::Data {
                event_type: event_type.into(),
                payload,
            },
        )
    }

    /// Create a data event from a serializable struct
    pub fn data_event_from<T: serde::Serialize>(
        writer_id: WriterId,
        event_type: impl Into<String>,
        data: &T,
    ) -> Result<ChainEvent, serde_json::Error> {
        let payload = serde_json::to_value(data)?;
        Ok(Self::data_event(writer_id, event_type, payload))
    }

    /// Create a derived event from a parent event (propagates correlation).
    ///
    /// FLOWIP-010 §7: the lineage policy is build-resolved data threaded by
    /// the caller; the data path performs no global config read.
    pub fn derived_event(
        writer_id: WriterId,
        parent: &ChainEvent,
        content: ChainEventContent,
        lineage: LineagePolicy,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);

        event.correlation = parent.correlation.clone();
        event.merge_composite_activations_from(parent);
        event.replay_context = parent.replay_context.clone();
        event.ingress_context = parent.ingress_context.clone();
        event.cycle_depth = parent.cycle_depth;
        event.cycle_scc_id = parent.cycle_scc_id;

        event.causality = CausalityContext::with_parent(parent.id);

        let ancestors_to_add = parent
            .causality
            .parent_ids
            .iter()
            .take(lineage.max_lineage_depth.saturating_sub(1));

        for ancestor in ancestors_to_add {
            event.causality = event.causality.add_parent(*ancestor);
        }

        event
    }

    /// Create a derived data event from a parent
    pub fn derived_data_event(
        writer_id: WriterId,
        parent: &ChainEvent,
        event_type: impl Into<String>,
        payload: Value,
        lineage: LineagePolicy,
    ) -> ChainEvent {
        Self::derived_event(
            writer_id,
            parent,
            ChainEventContent::Data {
                event_type: event_type.into(),
                payload,
            },
            lineage,
        )
    }

    /// Create an event for a source (flow entry point) with new correlation
    pub fn source_event(
        writer_id: WriterId,
        stage_name: impl Into<String>,
        content: ChainEventContent,
    ) -> ChainEvent {
        Self::create_event(writer_id, content).with_new_correlation(stage_name)
    }

    /// Create a data event from system component
    pub fn system_data_event(
        writer_id: WriterId,
        event_type: impl Into<String>,
        payload: Value,
    ) -> ChainEvent {
        Self::create_event(
            writer_id,
            ChainEventContent::Data {
                event_type: event_type.into(),
                payload,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::CorrelationId;
    use crate::id::StageId;
    use crate::ingress::IngressContext;
    use crate::WriterId;
    use serde_json::json;

    #[test]
    fn derived_event_propagates_ingress_context() {
        let writer_id = WriterId::from(StageId::new());
        let parent = ChainEventFactory::data_event(writer_id, "parent.event", json!({"id": 1}))
            .with_ingress_context(IngressContext {
                accepted_at_ns: 42,
                ingress_key: "orders".into(),
                batch_index: Some(3),
                attempt_seq: crate::ingress::IngressAttemptSeq(0),
            });

        let child = ChainEventFactory::derived_data_event(
            writer_id,
            &parent,
            "child.event",
            json!({}),
            LineagePolicy::default(),
        );

        assert_eq!(child.ingress_context, parent.ingress_context);
    }

    #[test]
    fn derived_event_propagates_mixed_correlation_sample_metadata() {
        let writer_id = WriterId::from(StageId::new());
        let mut parent = ChainEventFactory::data_event(writer_id, "parent.event", json!({"id": 1}));
        parent.set_correlation_sample(vec![CorrelationId::new()], true);

        let child = ChainEventFactory::derived_data_event(
            writer_id,
            &parent,
            "child.event",
            json!({}),
            LineagePolicy::default(),
        );

        assert_eq!(child.correlation, parent.correlation);
        assert!(child.correlation_ids_truncated());
    }
}

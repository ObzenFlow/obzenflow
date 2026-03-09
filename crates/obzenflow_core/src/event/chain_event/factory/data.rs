// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ChainEventFactory;
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

    /// Create a derived event from a parent event (propagates correlation)
    pub fn derived_event(
        writer_id: WriterId,
        parent: &ChainEvent,
        content: ChainEventContent,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);

        event.correlation_id = parent.correlation_id;
        event.correlation_payload = parent.correlation_payload.clone();
        event.replay_context = parent.replay_context.clone();
        event.cycle_depth = parent.cycle_depth;
        event.cycle_scc_id = parent.cycle_scc_id;

        const DEFAULT_MAX_LINEAGE_DEPTH: usize = 100;

        event.causality = CausalityContext::with_parent(parent.id);

        let max_depth = std::env::var("OBZENFLOW_MAX_LINEAGE_DEPTH")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_MAX_LINEAGE_DEPTH);

        let ancestors_to_add = parent
            .causality
            .parent_ids
            .iter()
            .take(max_depth.saturating_sub(1));

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
    ) -> ChainEvent {
        Self::derived_event(
            writer_id,
            parent,
            ChainEventContent::Data {
                event_type: event_type.into(),
                payload,
            },
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

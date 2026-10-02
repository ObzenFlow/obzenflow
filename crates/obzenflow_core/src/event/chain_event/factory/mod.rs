// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::envelope::AuthoredEnvelope;
use crate::event::provenance::{AuthoredProvenance, ChainEventProvenance, ProcessingProvenance};
use crate::event::status::processing_status::ProcessingStatus;
mod control;
mod data;
mod lifecycle;
mod middleware;

use super::{ChainEvent, ChainPayload};
use crate::event::payloads::delivery_payload::DeliveryPayload;
use crate::event::provenance::causality_context::CausalityContext;
use crate::event::provenance::FlowContext;
use crate::event::types::{EventId, WriterId};
use std::num::NonZeroU32;

/// Stateless factory for creating ChainEvents with consistent patterns.
/// Raw authoring requires an explicit positive version. Generic construction
/// stays inside Core so external callers choose a typed or family constructor.
///
/// ```compile_fail
/// use obzenflow_core::{StageId, WriterId};
/// use obzenflow_core::event::{ChainEventFactory, ChainPayload};
/// let _ = ChainEventFactory::create_event(
///     WriterId::from(StageId::new()), ChainPayload::Fact(serde_json::json!({})),
///     "example", std::num::NonZeroU32::MIN,
/// );
/// ```
///
/// ```compile_fail
/// use obzenflow_core::{StageId, WriterId};
/// use obzenflow_core::event::ChainEventFactory;
/// let _ = ChainEventFactory::data_event(
///     WriterId::from(StageId::new()), "example", 0, serde_json::json!({}),
/// );
/// ```
pub struct ChainEventFactory;

impl ChainEventFactory {
    pub fn composite_event(
        writer_id: WriterId,
        payload: crate::event::payloads::composite_data_payload::CompositeDataPayload,
    ) -> ChainEvent {
        Self::framework_event(writer_id, ChainPayload::CompositeData(payload))
    }

    pub fn derived_composite_event(
        writer_id: WriterId,
        parent: &ChainEvent,
        payload: crate::event::payloads::composite_data_payload::CompositeDataPayload,
        lineage: crate::config::LineagePolicy,
    ) -> ChainEvent {
        let name = payload.event_type();
        let version = payload.payload_schema_version();
        Self::derived_event(
            writer_id,
            parent,
            ChainPayload::CompositeData(payload),
            name,
            version,
            lineage,
        )
    }

    pub fn flow_signal_event(
        writer_id: WriterId,
        payload: crate::event::payloads::flow_control_payload::FlowControlPayload,
    ) -> ChainEvent {
        Self::framework_event(writer_id, ChainPayload::FlowControl(payload))
    }

    /// Create a delivery event
    pub fn delivery_event(writer_id: WriterId, payload: DeliveryPayload) -> ChainEvent {
        let parent = payload.subject.input.event_id;
        Self::framework_event(writer_id, ChainPayload::Delivery(payload))
            .with_causality(CausalityContext::with_parent(parent))
    }

    pub(crate) fn framework_event(writer_id: WriterId, content: ChainPayload) -> ChainEvent {
        let event_type = content
            .framework_event_type(&FlowContext::default().stage_name)
            .expect("closed framework payload")
            .into_owned();
        let version = content
            .framework_schema_version()
            .expect("closed framework payload");
        Self::create_event(writer_id, content, event_type, version)
    }

    pub(crate) fn create_event(
        writer_id: WriterId,
        content: ChainPayload,
        event_type: impl Into<String>,
        payload_schema_version: NonZeroU32,
    ) -> ChainEvent {
        let provenance = ChainEventProvenance {
            id: EventId::new(),
            writer_id,
            event_kind: content.kind(),
            event_type: event_type.into(),
            payload_schema_version,
            causality: CausalityContext::new(),
            flow_context: FlowContext::default(),
            processing: ProcessingProvenance {
                event_time: current_timestamp(),
                status: ProcessingStatus::Success,
            },
            correlation: None,
            replay_context: None,
            ingress_context: None,
            cycle_depth: None,
            cycle_scc_id: None,
            runtime: None,
            effect_provenance: None,
            admission_seq: None,
            composite_activations: Vec::new(),
        };
        ChainEvent {
            envelope: AuthoredEnvelope {
                provenance: AuthoredProvenance { event: provenance },
                observability: None,
            },
            payload: content,
        }
    }
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

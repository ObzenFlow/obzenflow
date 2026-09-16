// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::provenance::{
    AuthoredEnvelope, AuthoredProvenance, ChainEventProvenance, ProcessingProvenance,
};
use crate::event::status::processing_status::ProcessingStatus;
mod control;
mod data;
mod lifecycle;
mod middleware;

use super::{ChainEvent, ChainPayload};
use crate::event::context::causality_context::CausalityContext;
use crate::event::context::FlowContext;
use crate::event::observation::ObservabilityContext;
use crate::event::payloads::delivery_payload::DeliveryPayload;
use crate::event::types::{EventId, WriterId};

/// Stateless factory for creating ChainEvents with consistent patterns.
pub struct ChainEventFactory;

impl ChainEventFactory {
    /// Create a delivery event
    pub fn delivery_event(writer_id: WriterId, payload: DeliveryPayload) -> ChainEvent {
        Self::create_event(writer_id, ChainPayload::Delivery(payload))
    }

    /// Create an event with flow context
    pub fn create_with_context(
        writer_id: WriterId,
        content: ChainPayload,
        flow_context: FlowContext,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);
        event.flow_context = flow_context;
        event
    }

    /// Create an event with observability context
    pub fn create_with_observability(
        writer_id: WriterId,
        content: ChainPayload,
        observability: ObservabilityContext,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);
        event.envelope.observability = Some(observability);
        event
    }

    pub fn create_event(writer_id: WriterId, content: ChainPayload) -> ChainEvent {
        let provenance = ChainEventProvenance {
            id: EventId::new(),
            writer_id,
            event_kind: content.kind(),
            event_type: content
                .framework_event_type()
                .unwrap_or("application.fact")
                .to_string(),
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

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod control;
mod data;
mod lifecycle;
mod middleware;

use super::{ChainEvent, ChainEventContent};
use crate::event::context::causality_context::CausalityContext;
use crate::event::context::observability_context::ObservabilityContext;
use crate::event::context::{FlowContext, IntentContext, ProcessingContext};
use crate::event::payloads::delivery_payload::DeliveryPayload;
use crate::event::types::{EventId, WriterId};

/// Stateless factory for creating ChainEvents with consistent patterns.
pub struct ChainEventFactory;

impl ChainEventFactory {
    /// Create a delivery event
    pub fn delivery_event(writer_id: WriterId, payload: DeliveryPayload) -> ChainEvent {
        Self::create_event(writer_id, ChainEventContent::Delivery(payload))
    }

    /// Create an event with flow context
    pub fn create_with_context(
        writer_id: WriterId,
        content: ChainEventContent,
        flow_context: FlowContext,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);
        event.flow_context = flow_context;
        event
    }

    /// Create an event with observability context
    pub fn create_with_observability(
        writer_id: WriterId,
        content: ChainEventContent,
        observability: ObservabilityContext,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);
        event.observability = Some(observability);
        event
    }

    /// Create an event with intent
    pub fn create_with_intent(
        writer_id: WriterId,
        content: ChainEventContent,
        intent: IntentContext,
    ) -> ChainEvent {
        let mut event = Self::create_event(writer_id, content);
        event.intent = Some(intent);
        event
    }

    fn create_event(writer_id: WriterId, content: ChainEventContent) -> ChainEvent {
        let mut event = ChainEvent {
            id: EventId::new(),
            writer_id,
            content,
            causality: CausalityContext::new(),
            flow_context: FlowContext::default(),
            processing_info: ProcessingContext::default(),
            intent: None,
            correlation_id: None,
            correlation_payload: None,
            replay_context: None,
            cycle_depth: None,
            cycle_scc_id: None,
            runtime_context: None,
            observability: None,
        };

        event.processing_info.event_time = current_timestamp();
        event
    }
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

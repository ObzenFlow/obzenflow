// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, MiddlewareLifecycle, ObservabilityPayload, RateLimiterEvent,
};
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::{ChainEventContent, SystemEvent, SystemEventType, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, EventEnvelope};
use std::sync::Arc;

pub async fn mirror_middleware_event_to_system_journal(
    envelope: &EventEnvelope<ChainEvent>,
    system_journal: &Arc<dyn Journal<SystemEvent>>,
) {
    let middleware = match &envelope.event.content {
        ChainEventContent::Observability(ObservabilityPayload::Middleware(mw)) => mw,
        _ => return,
    };

    // Guardrail: only mirror middleware events that are authored by the local stage.
    // This prevents accidentally re-attributing forwarded control events.
    let stage_id = envelope.event.flow_context.stage_id;
    let Some(writer_stage_id) = envelope.event.writer_id.as_stage() else {
        return;
    };
    if *writer_stage_id != stage_id {
        return;
    }

    let should_mirror = match middleware {
        MiddlewareLifecycle::CircuitBreaker(cb) => matches!(
            cb,
            CircuitBreakerEvent::Opened { .. }
                | CircuitBreakerEvent::Closed { .. }
                | CircuitBreakerEvent::HalfOpen { .. }
                | CircuitBreakerEvent::Summary { .. }
        ),
        MiddlewareLifecycle::RateLimiter(rl) => {
            matches!(
                rl,
                RateLimiterEvent::ActivityPulse { .. }
                    | RateLimiterEvent::ModeChange { .. }
                    | RateLimiterEvent::WindowUtilization { .. }
            )
        }
    };
    if !should_mirror {
        return;
    }

    let writer_key = envelope.event.writer_id.to_string();
    let origin = obzenflow_core::event::system_event::MiddlewareEventOrigin {
        event_id: envelope.event.id,
        writer_key: writer_key.clone(),
        seq: SeqNo(envelope.vector_clock.get(&writer_key)),
    };

    let event = SystemEvent::new(
        WriterId::from(stage_id),
        SystemEventType::MiddlewareLifecycle {
            stage_id,
            stage_name: Some(envelope.event.flow_context.stage_name.clone()),
            flow_id: Some(envelope.event.flow_context.flow_id.clone()),
            flow_name: Some(envelope.event.flow_context.flow_name.clone()),
            origin,
            middleware: middleware.clone(),
        },
    );

    let system_journal = system_journal.clone();
    let mirror = crate::supervised_base::publication::commit(async move {
        if let Err(error) = system_journal.append(event, None).await {
            if crate::supervised_base::publication::is_indeterminate(&error) {
                return Err(Box::new(error) as crate::supervised_base::publication::BoxError);
            }
            // A confirmed rejection of optional telemetry does not undo the
            // source row or its mandatory accounting. Uncertainty still poisons
            // the owning publication scope and reaches its join.
            tracing::warn!(stage_id = %stage_id, journal_error = %error,
                "Failed to mirror middleware event into system journal");
        }
        Ok(())
    });
    if let Err(e) = mirror.await {
        tracing::warn!(
            stage_id = %stage_id,
            journal_error = %e,
            "Failed to mirror middleware event into system journal"
        );
    }
}

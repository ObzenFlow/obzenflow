use obzenflow_core::event::payloads::observability_payload::{
    BackpressureEvent, CircuitBreakerEvent, MiddlewareLifecycle, ObservabilityPayload,
    RateLimiterEvent,
};
use obzenflow_core::event::{ChainEventContent, SystemEvent, SystemEventType, WriterId};
use obzenflow_core::journal::journal::Journal;
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
        MiddlewareLifecycle::Backpressure(bp) => matches!(bp, BackpressureEvent::ActivityPulse { .. }),
        _ => false,
    };
    if !should_mirror {
        return;
    }

    let writer_key = envelope.event.writer_id.to_string();
    let origin = obzenflow_core::event::system_event::MiddlewareEventOrigin {
        event_id: envelope.event.id.clone(),
        writer_key: writer_key.clone(),
        seq: envelope.vector_clock.get(&writer_key),
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

    if let Err(e) = system_journal.append(event, None).await {
        tracing::warn!(
            stage_id = %stage_id,
            journal_error = %e,
            "Failed to mirror middleware event into system journal"
        );
    }
}

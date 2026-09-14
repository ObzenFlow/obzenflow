// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::execution_payload::{
    CircuitBreakerFact, ExecutionPayload, MiddlewareFact, RateLimiterFact,
};
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::{ChainPayload, SystemEvent, SystemPayload, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_core::JournalRecord;
use std::sync::Arc;

pub async fn mirror_middleware_event_to_system_journal(
    envelope: &JournalRecord<obzenflow_core::event::ChainPayload>,
    system_journal: &Arc<dyn Journal<SystemEvent>>,
) {
    let middleware = match &envelope.payload {
        ChainPayload::Execution(ExecutionPayload::CircuitBreaker(
            fact @ (CircuitBreakerFact::Opened { .. }
            | CircuitBreakerFact::Closed { .. }
            | CircuitBreakerFact::HalfOpen { .. }
            | CircuitBreakerFact::StateChanged { .. }),
        )) => MiddlewareFact::CircuitBreaker(fact.clone()),
        ChainPayload::Execution(ExecutionPayload::RateLimiter(
            fact @ (RateLimiterFact::ModeChange { .. } | RateLimiterFact::ConfigChanged { .. }),
        )) => MiddlewareFact::RateLimiter(fact.clone()),
        _ => return,
    };

    // Guardrail: only mirror middleware events that are authored by the local stage.
    // This prevents accidentally re-attributing forwarded control events.
    let stage_id = envelope.envelope.provenance.event.flow_context.stage_id;
    let Some(writer_stage_id) = envelope.envelope.provenance.event.writer_id.as_stage() else {
        return;
    };
    if *writer_stage_id != stage_id {
        return;
    }

    let writer_key = envelope.envelope.provenance.event.writer_id.to_string();
    let origin = obzenflow_core::event::system_event::MiddlewareEventOrigin {
        event_id: envelope.envelope.provenance.event.id,
        writer_key: writer_key.clone(),
        seq: SeqNo(
            envelope
                .envelope
                .provenance
                .journal
                .vector_clock
                .get(&writer_key),
        ),
    };

    let event = SystemEvent::new(
        WriterId::from(stage_id),
        SystemPayload::MiddlewareLifecycle {
            stage_id,
            stage_name: Some(
                envelope
                    .envelope
                    .provenance
                    .event
                    .flow_context
                    .stage_name
                    .clone(),
            ),
            flow_id: Some(
                envelope
                    .envelope
                    .provenance
                    .event
                    .flow_context
                    .flow_id
                    .clone(),
            ),
            flow_name: Some(
                envelope
                    .envelope
                    .provenance
                    .event
                    .flow_context
                    .flow_name
                    .clone(),
            ),
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
            // A confirmed rejection of the factual mirror does not undo the
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

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::observability_payload::CircuitBreakerRejectionReason;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, TypedPayload};
use serde::de::DeserializeOwned;
use serde::Serialize;

pub(super) fn build_typed_fallback_event<In, Out, F>(f: &F, event: &ChainEvent) -> Vec<ChainEvent>
where
    In: TypedPayload + DeserializeOwned,
    Out: TypedPayload + Serialize,
    F: Fn(&In) -> Out + Send + Sync + 'static,
{
    use obzenflow_core::event::status::processing_status::ProcessingStatus;

    let payload = event.payload();
    let input: In = match serde_json::from_value(payload.clone()) {
        Ok(v) => v,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_fallback_deserialize_failed: {err}"));
            return vec![clone];
        }
    };

    let out = f(&input);

    let out_value = match serde_json::to_value(&out) {
        Ok(v) => v,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_fallback_serialize_failed: {err}"));
            return vec![clone];
        }
    };

    let event_type = Out::versioned_event_type();
    let ev = ChainEventFactory::derived_data_event(event.writer_id, event, &event_type, out_value);

    vec![ev]
}

/// Build the outcome-shaped fallback events (FLOWIP-120m): the closure's
/// `E::Outcome` carrier lowers through `into_facts`, one derived data event
/// per fact, each parented on the protected input.
pub(super) fn build_outcome_fallback_events<E, In, F>(f: &F, event: &ChainEvent) -> Vec<ChainEvent>
where
    E: obzenflow_runtime::effects::Effect,
    In: TypedPayload + DeserializeOwned,
    F: Fn(&In) -> E::Outcome + Send + Sync + 'static,
{
    use obzenflow_core::event::schema::TypedFactSet;
    use obzenflow_core::event::status::processing_status::ProcessingStatus;

    let payload = event.payload();
    let input: In = match serde_json::from_value(payload.clone()) {
        Ok(value) => value,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_outcome_fallback_deserialize_failed: {err}"));
            return vec![clone];
        }
    };

    let outcome = f(&input);
    let facts = match outcome.into_facts() {
        Ok(facts) => facts,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_outcome_fallback_serialize_failed: {err}"));
            return vec![clone];
        }
    };

    facts
        .into_iter()
        .map(|fact| {
            ChainEventFactory::derived_data_event(
                event.writer_id,
                event,
                fact.event_type.as_str(),
                fact.payload,
            )
        })
        .collect()
}

/// Build the typed rejection fact for typed-outcome mode (FLOWIP-120h),
/// derived from the guarded input so causality records it as the parent.
pub(super) fn build_typed_rejection_event<In, R, F>(
    f: &F,
    event: &ChainEvent,
    reason: CircuitBreakerRejectionReason,
) -> Vec<ChainEvent>
where
    In: TypedPayload + DeserializeOwned,
    R: TypedPayload + Serialize,
    F: Fn(&In, CircuitBreakerRejectionReason) -> R + Send + Sync + 'static,
{
    use obzenflow_core::event::status::processing_status::ProcessingStatus;

    let payload = event.payload();
    let input: In = match serde_json::from_value(payload.clone()) {
        Ok(v) => v,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_rejection_deserialize_failed: {err}"));
            return vec![clone];
        }
    };

    let rejection = f(&input, reason);

    let rejection_value = match serde_json::to_value(&rejection) {
        Ok(v) => v,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_rejection_serialize_failed: {err}"));
            return vec![clone];
        }
    };

    let event_type = R::versioned_event_type();
    let ev =
        ChainEventFactory::derived_data_event(event.writer_id, event, &event_type, rejection_value);

    vec![ev]
}

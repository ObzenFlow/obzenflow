// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::chain_event::{
    ChainEvent, ChainEventContent, ChainEventFactory, RetryAttemptFailedEventParams,
    RetryExhaustedEventParams, RetrySucceededAfterRetryEventParams,
};
use obzenflow_core::event::payloads::effect_payload::{EffectCursor, EffectType};
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload, RetryEvent, RetryExhaustionCause, RetryInvocation,
    RetryLifecycleContext, RetryProtectedUnit,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::{EventId, StageId, Ulid, WriterId};
use serde_json::{json, Value};

fn event_id(value: u128) -> EventId {
    EventId::from(Ulid::from(value))
}

fn stage_id(value: u128) -> StageId {
    StageId::from_ulid(Ulid::from(value))
}

fn effect_context() -> RetryLifecycleContext {
    RetryLifecycleContext {
        stage_id: stage_id(1),
        attachment_id: Ulid::from(2_u128),
        protected_unit: RetryProtectedUnit::Effect {
            effect_type: EffectType::from("authorize_payment"),
        },
        invocation: RetryInvocation::Effect {
            cursor: EffectCursor::new("flow-01", "authorize", 42_u64, 0_u32),
        },
    }
}

fn retry_payload(event: &ChainEvent) -> &RetryEvent {
    match &event.content {
        ChainEventContent::Observability(ObservabilityPayload::Middleware(
            MiddlewareLifecycle::Retry(retry),
        )) => retry,
        other => panic!("expected retry lifecycle payload, got {other:?}"),
    }
}

#[test]
fn retry_context_variants_use_the_locked_tagged_wire_shape() {
    let effect = serde_json::to_value(effect_context()).expect("effect context serializes");
    assert_eq!(effect["protected_unit"]["kind"], "effect");
    assert_eq!(effect["protected_unit"]["effect_type"], "authorize_payment");
    assert_eq!(effect["invocation"]["kind"], "effect");
    assert_eq!(
        effect["invocation"]["cursor"]["recorded_flow_id"],
        "flow-01"
    );
    assert_eq!(effect["invocation"]["cursor"]["stage_key"], "authorize");
    assert_eq!(effect["invocation"]["cursor"]["input_seq"], 42);
    assert_eq!(effect["invocation"]["cursor"]["effect_ordinal"], 0);

    let source = RetryLifecycleContext {
        stage_id: stage_id(3),
        attachment_id: Ulid::from(4_u128),
        protected_unit: RetryProtectedUnit::SourcePoll,
        invocation: RetryInvocation::SourcePoll {
            poll_id: event_id(5),
        },
    };
    let source = serde_json::to_value(source).expect("source context serializes");
    assert_eq!(source["protected_unit"], json!({ "kind": "source_poll" }));
    assert_eq!(source["invocation"]["kind"], "source_poll");
    assert!(source["invocation"].get("poll_id").is_some());

    let stage_sink = RetryLifecycleContext {
        stage_id: stage_id(6),
        attachment_id: Ulid::from(7_u128),
        protected_unit: RetryProtectedUnit::SinkDelivery {
            configured_target_id: None,
        },
        invocation: RetryInvocation::SinkDelivery {
            parent_event_id: event_id(8),
        },
    };
    let stage_sink = serde_json::to_value(stage_sink).expect("sink context serializes");
    assert_eq!(
        stage_sink["protected_unit"],
        json!({ "kind": "sink_delivery", "configured_target_id": null })
    );
    assert_eq!(stage_sink["invocation"]["kind"], "sink_delivery");
    assert!(stage_sink["invocation"].get("parent_event_id").is_some());

    let configured_sink = RetryProtectedUnit::SinkDelivery {
        configured_target_id: Some(Ulid::from(9_u128)),
    };
    let configured_sink =
        serde_json::to_value(configured_sink).expect("configured sink target serializes");
    assert_eq!(configured_sink["kind"], "sink_delivery");
    assert!(configured_sink.get("configured_target_id").is_some());
}

#[test]
fn legacy_retry_rows_decode_with_absent_new_fields() {
    let attempt_failed: RetryEvent = serde_json::from_value(json!({
        "action": "attempt_failed",
        "attempt_number": 1,
        "max_attempts": 3,
        "error_kind": "Timeout",
        "delay_ms": 250
    }))
    .expect("legacy attempt_failed row decodes");

    match &attempt_failed {
        RetryEvent::AttemptFailed {
            context,
            attempt_number,
            max_attempts,
            error_kind,
            delay_ms,
            elapsed_ms,
            remaining_wall_ms,
        } => {
            assert!(context.is_none());
            assert_eq!(*attempt_number, 1);
            assert_eq!(*max_attempts, 3);
            assert_eq!(*error_kind, Some(ErrorKind::Timeout));
            assert_eq!(*delay_ms, Some(250));
            assert!(elapsed_ms.is_none());
            assert!(remaining_wall_ms.is_none());
        }
        other => panic!("expected AttemptFailed, got {other:?}"),
    }

    let reencoded = serde_json::to_value(attempt_failed).expect("legacy row reserializes");
    assert!(reencoded.get("context").is_none());
    assert!(reencoded.get("elapsed_ms").is_none());
    assert!(reencoded.get("remaining_wall_ms").is_none());

    let succeeded: RetryEvent = serde_json::from_value(json!({
        "action": "succeeded_after_retry",
        "total_attempts": 2,
        "total_duration_ms": 17
    }))
    .expect("legacy succeeded row decodes");
    match succeeded {
        RetryEvent::SucceededAfterRetry {
            context,
            total_attempts,
            total_duration_ms,
        } => {
            assert!(context.is_none());
            assert_eq!(total_attempts, 2);
            assert_eq!(total_duration_ms, 17);
        }
        other => panic!("expected SucceededAfterRetry, got {other:?}"),
    }

    let exhausted: RetryEvent = serde_json::from_value(json!({
        "action": "exhausted",
        "total_attempts": 3,
        "last_error": "legacy raw remote error",
        "total_duration_ms": 41
    }))
    .expect("legacy exhausted row decodes");
    match exhausted {
        RetryEvent::Exhausted {
            context,
            total_attempts,
            exhaustion_cause,
            last_error_kind,
            last_error,
            total_duration_ms,
        } => {
            assert!(context.is_none());
            assert_eq!(total_attempts, 3);
            assert!(exhaustion_cause.is_none());
            assert!(last_error_kind.is_none());
            assert_eq!(last_error.as_deref(), Some("legacy raw remote error"));
            assert_eq!(total_duration_ms, 41);
        }
        other => panic!("expected Exhausted, got {other:?}"),
    }
}

#[test]
fn legacy_attempt_started_remains_decode_only_in_the_factory_surface() {
    let payload: ObservabilityPayload = serde_json::from_value(json!({
        "observability_type": "middleware",
        "middleware_event": "retry",
        "details": {
            "action": "attempt_started",
            "attempt_number": 1,
            "max_attempts": 3,
            "backoff_ms": 250
        }
    }))
    .expect("legacy attempt_started row decodes through its archive envelope");

    match payload {
        ObservabilityPayload::Middleware(MiddlewareLifecycle::Retry(
            RetryEvent::AttemptStarted {
                attempt_number,
                max_attempts,
                backoff_ms,
            },
        )) => {
            assert_eq!(attempt_number, 1);
            assert_eq!(max_attempts, 3);
            assert_eq!(backoff_ms, Some(250));
        }
        other => panic!("expected legacy AttemptStarted, got {other:?}"),
    }

    // Deliberately no ChainEventFactory constructor exists for AttemptStarted.
}

#[test]
fn attempt_failed_constructor_populates_context_bounded_fields_and_timing() {
    let writer_id = WriterId::from(stage_id(10));
    let parent = event_id(11);
    let event = ChainEventFactory::retry_attempt_failed(
        writer_id,
        RetryAttemptFailedEventParams {
            context: effect_context(),
            attempt_number: 1,
            max_attempts: 3,
            error_kind: ErrorKind::RateLimited,
            delay_ms: 250,
            elapsed_ms: 18,
            remaining_wall_ms: 19_982,
            cause: Some(parent),
        },
    );

    assert_eq!(event.event_type(), "lifecycle.middleware.retry");
    assert_eq!(event.causality.parent_ids, vec![parent]);
    assert!(event.processing_info.event_time > 0);

    let retry = retry_payload(&event);
    let encoded = serde_json::to_value(retry).expect("attempt_failed serializes");
    assert_eq!(encoded["action"], "attempt_failed");
    assert_eq!(encoded["context"]["protected_unit"]["kind"], "effect");
    assert_eq!(encoded["attempt_number"], 1);
    assert_eq!(encoded["max_attempts"], 3);
    assert_eq!(encoded["error_kind"], "RateLimited");
    assert_eq!(encoded["delay_ms"], 250);
    assert_eq!(encoded["elapsed_ms"], 18);
    assert_eq!(encoded["remaining_wall_ms"], 19_982);
    assert!(encoded.get("last_error").is_none());
}

#[test]
fn terminal_constructors_populate_context_and_never_write_legacy_error_text() {
    let writer_id = WriterId::from(stage_id(12));
    let parent = event_id(13);

    let succeeded = ChainEventFactory::retry_succeeded_after_retry(
        writer_id,
        RetrySucceededAfterRetryEventParams {
            context: effect_context(),
            total_attempts: 2,
            total_duration_ms: 31,
            cause: Some(parent),
        },
    );
    let succeeded =
        serde_json::to_value(retry_payload(&succeeded)).expect("succeeded_after_retry serializes");
    assert_eq!(succeeded["action"], "succeeded_after_retry");
    assert!(succeeded.get("context").is_some());
    assert_eq!(succeeded["total_attempts"], 2);
    assert_eq!(succeeded["total_duration_ms"], 31);

    let exhausted = ChainEventFactory::retry_exhausted(
        writer_id,
        RetryExhaustedEventParams {
            context: effect_context(),
            total_attempts: 2,
            exhaustion_cause: RetryExhaustionCause::TerminalFailure,
            last_error_kind: Some(ErrorKind::PermanentFailure),
            total_duration_ms: 44,
            cause: Some(parent),
        },
    );
    assert_eq!(exhausted.causality.parent_ids, vec![parent]);
    let exhausted = serde_json::to_value(retry_payload(&exhausted)).expect("exhausted serializes");
    assert_eq!(exhausted["action"], "exhausted");
    assert!(exhausted.get("context").is_some());
    assert_eq!(exhausted["total_attempts"], 2);
    assert_eq!(exhausted["exhaustion_cause"], "terminal_failure");
    assert_eq!(exhausted["last_error_kind"], "PermanentFailure");
    assert_eq!(exhausted["total_duration_ms"], 44);
    assert!(exhausted.get("last_error").is_none());

    let zero_execution = ChainEventFactory::retry_exhausted(
        writer_id,
        RetryExhaustedEventParams {
            context: effect_context(),
            total_attempts: 0,
            exhaustion_cause: RetryExhaustionCause::TotalWallTime,
            last_error_kind: None,
            total_duration_ms: 0,
            cause: None,
        },
    );
    let zero_execution = serde_json::to_value(retry_payload(&zero_execution))
        .expect("zero-execution exhausted serializes");
    assert_eq!(zero_execution["total_attempts"], 0);
    assert_eq!(zero_execution["exhaustion_cause"], "total_wall_time");
    assert!(zero_execution.get("last_error_kind").is_none());
    assert!(zero_execution.get("last_error").is_none());
}

#[test]
fn exhaustion_causes_have_bounded_locked_wire_spellings() {
    let cases = [
        (RetryExhaustionCause::MaxAttempts, "max_attempts"),
        (RetryExhaustionCause::TotalWallTime, "total_wall_time"),
        (RetryExhaustionCause::DrainRequested, "drain_requested"),
        (
            RetryExhaustionCause::RetryHintExceedsLimit,
            "retry_hint_exceeds_limit",
        ),
        (RetryExhaustionCause::PolicyRejected, "policy_rejected"),
        (RetryExhaustionCause::TerminalFailure, "terminal_failure"),
    ];

    for (cause, expected) in cases {
        let encoded = serde_json::to_value(cause).expect("cause serializes");
        assert_eq!(encoded, Value::String(expected.to_string()));
        let decoded: RetryExhaustionCause =
            serde_json::from_value(encoded).expect("cause deserializes");
        assert_eq!(decoded, cause);
    }
}

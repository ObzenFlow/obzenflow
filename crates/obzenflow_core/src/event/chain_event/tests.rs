// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::event::payloads::effect_payload::{
    EffectCursor, EffectDescriptor, EFFECT_EXECUTION_SUCCEEDED_EVENT_TYPE,
};
use crate::event::payloads::execution_payload::ExecutionPayload;
use crate::event::types::CorrelationId;
use crate::id::StageId;
use crate::ingress::{IngressAttemptSeq, IngressContext};
use crate::WriterId;
use serde_json::json;

#[test]
fn stage_occurrence_descriptor_is_bound_to_the_canonical_context_name() {
    let stage = StageId::new();
    let mut event = ChainEventFactory::stage_running(stage.into(), stage);
    let mut context = event.flow_context.clone();
    context.stage_name = "orders.v2/é".into();
    event = event.with_flow_context(context);
    assert_eq!(
        event.event_type(),
        "supervisor.stage.orders%2Ev2%2F%C3%A9.milestone.ready"
    );
    let value = serde_json::to_value(event).unwrap();
    assert!(serde_json::from_value::<ChainEvent>(value.clone()).is_ok());
    for wrong in [
        "supervisor.stage.orders.v2/é.milestone.ready",
        "lifecycle.stage.running",
        "supervisor.stage.other.milestone.ready",
    ] {
        let mut invalid = value.clone();
        invalid["envelope"]["provenance"]["event"]["event_type"] = json!(wrong);
        assert!(serde_json::from_value::<ChainEvent>(invalid)
            .unwrap_err()
            .to_string()
            .contains("event descriptor does not match payload"));
    }
}

#[test]
fn test_factory_creation() {
    let writer_id = WriterId::from(StageId::new());
    let event = ChainEventFactory::data_event(
        writer_id,
        "test.event",
        std::num::NonZeroU32::MIN,
        json!({"key": "value"}),
    );

    assert_eq!(event.writer_id, writer_id);
    assert!(event.consumes_data_credit());
    assert_eq!(event.event_type(), "test.event");
}

#[test]
fn test_derived_event() {
    let writer_id = WriterId::from(StageId::new());
    let parent = ChainEventFactory::data_event(
        writer_id,
        "parent.event",
        std::num::NonZeroU32::MIN,
        json!({"data": "parent"}),
    )
    .with_new_correlation()
    .with_ingress_context(IngressContext {
        accepted_at_ns: 42,
        ingress_key: "test".into(),
        batch_index: Some(1),
        attempt_seq: IngressAttemptSeq(0),
    });

    let child = ChainEventFactory::derived_data_event(
        writer_id,
        &parent,
        "child.event",
        std::num::NonZeroU32::MIN,
        json!({"data": "child"}),
        crate::config::LineagePolicy::default(),
    );

    assert_eq!(child.correlation, parent.correlation);
    assert_eq!(child.causality.parent_ids, vec![parent.id]);
    assert_eq!(child.ingress_context, parent.ingress_context);
}

#[test]
fn framework_effect_data_is_not_source_replayable() {
    let writer_id = WriterId::from(StageId::new());
    use crate::event::payloads::effect_payload::{EffectOutcomePayload, EffectRecord};
    let record = EffectRecord {
        observation: crate::event::payloads::effect_payload::EffectObservation::returned_success(),
        cursor: EffectCursor::new("flow", "stage", 1, 0),
        descriptor_hash: "hash".into(),
        descriptor: EffectDescriptor::new("test.effect", "test", 1, "v1", "input"),
        outcome: EffectOutcomePayload::Succeeded { output: json!({}) },
        origin: None,
    };
    let event =
        ChainEventFactory::execution_event(writer_id, ExecutionPayload::EffectRecord(record));
    // Application descriptors cannot impersonate execution records.
    let fact = ChainEventFactory::data_event(
        writer_id,
        EFFECT_EXECUTION_SUCCEEDED_EVENT_TYPE,
        std::num::NonZeroU32::MIN,
        json!({}),
    );
    assert!(fact.is_source_replayable());
    assert!(!event.is_source_replayable());
}

#[test]
fn correlation_serializes_as_single_context_field() {
    let writer_id = WriterId::from(StageId::new());
    let correlation_id = CorrelationId::new();
    let mut event = ChainEventFactory::data_event(
        writer_id,
        "test.event",
        std::num::NonZeroU32::MIN,
        json!({"key": "value"}),
    );

    event.set_single_correlation(correlation_id, None);

    let serialized = serde_json::to_value(event).expect("event should serialize");
    let protected = &serialized["envelope"]["provenance"]["event"];
    let object = protected
        .as_object()
        .expect("event should serialize as object");

    assert!(object.contains_key("correlation"));
    assert!(!object.contains_key("correlation_id"));
    assert!(!object.contains_key("correlation_ids"));
    assert!(!object.contains_key("correlation_payload"));
    assert_eq!(protected["correlation"]["ids"].as_array().unwrap().len(), 1);
    assert!(protected["correlation"].get("truncated").is_none());
}

#[test]
fn catch_up_complete_round_trips_and_classifies_re_admit() {
    use crate::event::payloads::flow_control_payload::FlowControlPayload;
    use crate::event::types::ReaderGeneration;
    use crate::id::StageKey;

    let payload = FlowControlPayload::CatchUpComplete {
        generation: ReaderGeneration(1),
        stage_key: StageKey("tx_source".into()),
    };
    assert!(!payload.is_reporting_telemetry());

    let json = serde_json::to_value(&payload).expect("payload should serialize");
    assert_eq!(json["flow_control_type"], "catch_up_complete");
    let back: FlowControlPayload =
        serde_json::from_value(json).expect("payload should deserialize");
    match back {
        FlowControlPayload::CatchUpComplete {
            generation,
            stage_key,
        } => {
            assert_eq!(generation, ReaderGeneration(1));
            assert_eq!(stage_key.as_str(), "tx_source");
        }
        other => panic!("expected CatchUpComplete, got {other:?}"),
    }

    let writer_id = WriterId::from(StageId::new());
    let event = ChainEventFactory::flow_signal_event(
        writer_id,
        FlowControlPayload::CatchUpComplete {
            generation: ReaderGeneration(1),
            stage_key: StageKey("tx_source".into()),
        },
    )
    .with_new_correlation();
    assert_eq!(event.event_type(), "runtime.stream.catch_up_completed");
    assert_eq!(event.replay_disposition(), ReplayDisposition::ReAdmit);
    assert!(event.is_source_replayable());
}

#[test]
fn test_flow_signals() {
    let writer_id = WriterId::from(StageId::new());

    let eof = ChainEventFactory::eof_event(writer_id, true);
    assert!(eof.is_eof());
    assert!(eof.is_control());

    let drain = ChainEventFactory::drain_event(writer_id);
    assert!(drain.is_control());
    assert_eq!(drain.event_type(), "runtime.stream.drain_requested");
}

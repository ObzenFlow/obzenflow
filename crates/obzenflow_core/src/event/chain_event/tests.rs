// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::event::payloads::effect_payload::{
    EffectCursor, EffectDescriptor, EffectFactOwner, EffectProvenance, EFFECT_RECORD_EVENT_TYPE,
};
use crate::event::types::CorrelationId;
use crate::id::StageId;
use crate::ingress::IngressContext;
use crate::WriterId;
use serde_json::json;

#[test]
fn test_factory_creation() {
    let writer_id = WriterId::from(StageId::new());
    let event = ChainEventFactory::data_event(writer_id, "test.event", json!({"key": "value"}));

    assert_eq!(event.writer_id, writer_id);
    assert!(event.is_data());
    assert_eq!(event.event_type(), "test.event");
}

#[test]
fn test_derived_event() {
    let writer_id = WriterId::from(StageId::new());
    let parent = ChainEventFactory::source_event(
        writer_id,
        "test_stage",
        ChainEventContent::Data {
            event_type: "parent.event".to_string(),
            payload: json!({"data": "parent"}),
        },
    )
    .with_ingress_context(IngressContext {
        accepted_at_ns: 42,
        ingress_key: "test".into(),
        batch_index: Some(1),
        attempt_seq: crate::ingress::IngressAttemptSeq(0),
    });

    let child = ChainEventFactory::derived_data_event(
        writer_id,
        &parent,
        "child.event",
        json!({"data": "child"}),
    );

    assert_eq!(child.correlation, parent.correlation);
    assert_eq!(child.causality.parent_ids, vec![parent.id]);
    assert_eq!(child.ingress_context, parent.ingress_context);
}

#[test]
fn framework_effect_data_is_not_source_replayable() {
    let writer_id = WriterId::from(StageId::new());
    let mut event = ChainEventFactory::data_event(writer_id, EFFECT_RECORD_EVENT_TYPE, json!({}));
    event.effect_provenance = Some(EffectProvenance {
        cursor: EffectCursor::new("flow", "stage", 1, 0),
        descriptor_hash: "hash".into(),
        descriptor: EffectDescriptor::new("test.effect", "test", 1, "v1", "input"),
        outcome_fact_ordinal: None,
        group_id: None,
        fact_owner: EffectFactOwner::Framework,
        origin: None,
    });

    assert!(!event.is_source_replayable());
}

#[test]
fn correlation_serializes_as_single_context_field() {
    let writer_id = WriterId::from(StageId::new());
    let correlation_id = CorrelationId::new();
    let mut event = ChainEventFactory::data_event(writer_id, "test.event", json!({"key": "value"}));

    event.set_single_correlation(correlation_id, None);

    let serialized = serde_json::to_value(event).expect("event should serialize");
    let object = serialized
        .as_object()
        .expect("event should serialize as object");

    assert!(object.contains_key("correlation"));
    assert!(!object.contains_key("correlation_id"));
    assert!(!object.contains_key("correlation_ids"));
    assert!(!object.contains_key("correlation_payload"));
    assert_eq!(
        serialized["correlation"]["ids"].as_array().unwrap().len(),
        1
    );
    assert!(serialized["correlation"].get("truncated").is_none());
}

#[test]
fn test_flow_signals() {
    let writer_id = WriterId::from(StageId::new());

    let eof = ChainEventFactory::eof_event(writer_id, true);
    assert!(eof.is_eof());
    assert!(eof.is_control());

    let drain = ChainEventFactory::drain_event(writer_id);
    assert!(drain.is_control());
    assert_eq!(drain.event_type(), "control.drain");
}

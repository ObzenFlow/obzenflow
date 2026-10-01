// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{ChainEventFactory, ChainPayload, WriterId};
use obzenflow_core::id::StageId;
use serde_json::json;

#[test]
fn receipt_and_lifecycle_names_validate_subjects_and_partial_counts() {
    use obzenflow_core::event::payloads::delivery_payload::{
        DeliveryMethod, DeliveryOutcome, DeliveryPayload, DeliveryResult, DeliverySubject,
        SinkAuditPayload, SinkLifecycleOperation,
    };
    use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
    use obzenflow_core::{ChainEvent, JournalRecord, JournalWriterId};

    let writer = WriterId::from(StageId::new());
    let input = JournalRecord::new(
        JournalWriterId::new(),
        ChainEventFactory::data_event(
            writer,
            "order.ready",
            std::num::NonZeroU32::new(7).unwrap(),
            json!({"id": 1}),
        ),
    );
    let subject = DeliverySubject::from_record(&input);
    let mut partial = DeliveryOutcome::success(DeliveryMethod::Noop, None).with_items(2);
    partial.result = DeliveryResult::Partial {
        successful_count: 2,
        failed_count: 1,
        error_summary: "one item failed".into(),
        failed_items: None,
    };
    for (name, outcome) in [
        (
            "delivery.buffered",
            DeliveryOutcome::buffered(DeliveryMethod::Noop, None),
        ),
        (
            "delivery.succeeded",
            DeliveryOutcome::success(DeliveryMethod::Noop, None),
        ),
        ("delivery.partially_succeeded", partial.clone()),
        (
            "delivery.failed",
            DeliveryOutcome::failed(DeliveryMethod::Noop, "timeout", "unknown completion"),
        ),
        (
            "delivery.rejected",
            DeliveryOutcome::rejected(DeliveryMethod::Noop, "quota", "full"),
        ),
    ] {
        let event = ChainEventFactory::delivery_event(
            writer,
            DeliveryPayload {
                subject: subject.clone(),
                outcome,
            },
        );
        assert_eq!(event.event_type(), name);
        let exported = serde_json::to_value(&event).unwrap();
        let decoded: ChainEvent = serde_json::from_value(exported.clone()).unwrap();
        let ChainPayload::Delivery(receipt) = decoded.payload else {
            panic!("receipt family")
        };
        assert!(receipt.subject.matches_record(&input));
        assert_eq!(exported["payload"]["subject"]["payload_schema_version"], 7);

        let mut wrong_name = exported.clone();
        wrong_name["envelope"]["provenance"]["event"]["event_type"] = json!("sink.delivery");
        assert!(serde_json::from_value::<ChainEvent>(wrong_name).is_err());
        let mut wrong_parent = exported.clone();
        wrong_parent["envelope"]["provenance"]["event"]["causality"]["parent_ids"] = json!([]);
        assert!(serde_json::from_value::<ChainEvent>(wrong_parent).is_err());
        let mut uncommitted_subject = exported;
        uncommitted_subject["payload"]["subject"]["input"]["sequence"] = json!(0);
        assert!(serde_json::from_value::<ChainEvent>(uncommitted_subject).is_err());
    }

    for (operation, success_name, partial_name) in [
        (
            SinkLifecycleOperation::Flush,
            "sink.flush_succeeded",
            "sink.flush_partially_succeeded",
        ),
        (
            SinkLifecycleOperation::Drain,
            "sink.drain_succeeded",
            "sink.drain_partially_succeeded",
        ),
    ] {
        for (name, outcome) in [
            (
                success_name,
                DeliveryOutcome::success(DeliveryMethod::Noop, None),
            ),
            (partial_name, partial.clone()),
        ] {
            let audit = ChainEventFactory::execution_event(
                writer,
                ExecutionPayload::SinkAudit(SinkAuditPayload { operation, outcome }),
            );
            assert_eq!(audit.event_type(), name);
            let mut exported = serde_json::to_value(&audit).unwrap();
            assert!(exported["payload"].get("subject").is_none());
            serde_json::from_value::<ChainEvent>(exported.clone()).unwrap();
            exported["payload"]["operation"] =
                json!(if operation == SinkLifecycleOperation::Flush {
                    "drain"
                } else {
                    "flush"
                });
            assert!(serde_json::from_value::<ChainEvent>(exported).is_err());
        }
    }

    for (successful, failed, items) in [(0, 1, None), (1, 0, None), (2, 1, Some(3))] {
        let mut outcome = partial.clone();
        outcome.result = DeliveryResult::Partial {
            successful_count: successful,
            failed_count: failed,
            error_summary: String::new(),
            failed_items: None,
        };
        outcome.items_delivered = items;
        let event = ChainEventFactory::delivery_event(
            writer,
            DeliveryPayload {
                subject: subject.clone(),
                outcome,
            },
        );
        assert!(serde_json::to_value(event)
            .and_then(serde_json::from_value::<ChainEvent>)
            .is_err());
    }
}

#[test]
fn test_control_event_type_strings() {
    let writer_id = WriterId::from(StageId::new());

    // Test EOF event type string
    let eof_event = ChainEventFactory::eof_event(writer_id, true);
    assert_eq!(eof_event.event_type(), "control.eof");
    assert!(eof_event.is_control());
    assert!(eof_event.is_eof());

    // Test drain event type string
    let drain_event = ChainEventFactory::drain_event(writer_id);
    assert_eq!(drain_event.event_type(), "control.drain");
    assert!(drain_event.is_control());

    // Test watermark event type string
    let watermark_event =
        ChainEventFactory::watermark_event(writer_id, 12345, Some("stage1".to_string()));
    assert_eq!(watermark_event.event_type(), "control.watermark");
    assert!(watermark_event.is_control());

    // Test checkpoint event type string
    let checkpoint_event = ChainEventFactory::checkpoint_event(
        writer_id,
        "checkpoint-1".to_string(),
        Some(json!({"offset": 100})),
    );
    assert_eq!(checkpoint_event.event_type(), "control.checkpoint");
    assert!(checkpoint_event.is_control());
}

#[test]
fn test_is_control_detection() {
    let writer_id = WriterId::from(StageId::new());

    // Test EOF event
    let eof_event = ChainEventFactory::eof_event(writer_id, true);
    assert!(eof_event.is_control());
    assert!(eof_event.is_eof());

    // Test data event
    let data_event = ChainEventFactory::data_event(
        writer_id,
        "user.data.processed",
        std::num::NonZeroU32::MIN,
        json!({"value": 42}),
    );
    assert!(!data_event.is_control());
    assert!(!data_event.is_eof());
    assert!(data_event.consumes_data_credit());
}

#[test]
fn test_flow_signal_payloads() {
    let writer_id = WriterId::from(StageId::new());

    // Test natural EOF kind
    let natural_eof = ChainEventFactory::eof_event(writer_id, true);
    match &natural_eof.payload {
        ChainPayload::FlowControl(FlowControlPayload::Eof { kind, .. }) => {
            assert!(kind.is_natural());
        }
        _ => panic!("Expected EOF signal"),
    }

    // Test poison EOF kind
    let forced_eof = ChainEventFactory::eof_event(writer_id, false);
    match &forced_eof.payload {
        ChainPayload::FlowControl(FlowControlPayload::Eof { kind, .. }) => {
            assert!(kind.is_poison());
        }
        _ => panic!("Expected EOF signal"),
    }
}

#[test]
fn test_legacy_eof_natural_bool_deserializes_to_kind() {
    let payload: FlowControlPayload = serde_json::from_value(json!({
        "flow_control_type": "eof",
        "natural": false,
        "timestamp": 12345
    }))
    .expect("legacy EOF natural bool should deserialize");

    match &payload {
        FlowControlPayload::Eof { kind, .. } => assert_eq!(*kind, EofKind::Poison),
        _ => panic!("Expected EOF signal"),
    }

    let serialized = serde_json::to_value(payload).expect("EOF payload should serialize");
    assert_eq!(serialized["kind"], "poison");
    assert!(serialized.get("natural").is_none());
}

#[test]
fn truncated_eof_kind_round_trips_through_serde() {
    let payload: FlowControlPayload = serde_json::from_value(json!({
        "flow_control_type": "eof",
        "kind": "truncated",
        "timestamp": 12345
    }))
    .expect("truncated EOF kind should deserialize");

    match &payload {
        FlowControlPayload::Eof { kind, .. } => assert_eq!(*kind, EofKind::Truncated),
        _ => panic!("Expected EOF signal"),
    }

    let serialized = serde_json::to_value(payload).expect("EOF payload should serialize");
    assert_eq!(serialized["kind"], "truncated");
}

#[test]
fn legacy_eof_natural_bool_never_produces_truncated() {
    // The bool path predates Truncated and can only express Natural/Poison.
    for (natural, expected) in [(true, EofKind::Natural), (false, EofKind::Poison)] {
        let payload: FlowControlPayload = serde_json::from_value(json!({
            "flow_control_type": "eof",
            "natural": natural,
            "timestamp": 1
        }))
        .expect("legacy bool should deserialize");
        match &payload {
            FlowControlPayload::Eof { kind, .. } => assert_eq!(*kind, expected),
            _ => panic!("Expected EOF signal"),
        }
    }
}

#[test]
fn eof_kind_worst_is_a_join_over_the_severity_order() {
    use EofKind::{Natural, Poison, Truncated};

    // Exhaustive nine-pair table: worst wins, Natural < Truncated < Poison.
    let table = [
        (Natural, Natural, Natural),
        (Natural, Truncated, Truncated),
        (Natural, Poison, Poison),
        (Truncated, Natural, Truncated),
        (Truncated, Truncated, Truncated),
        (Truncated, Poison, Poison),
        (Poison, Natural, Poison),
        (Poison, Truncated, Poison),
        (Poison, Poison, Poison),
    ];
    for (a, b, expected) in table {
        assert_eq!(a.worst(b), expected, "worst({a:?}, {b:?})");
    }

    let all = [Natural, Truncated, Poison];
    for a in all {
        // Idempotent, and Natural is the identity.
        assert_eq!(a.worst(a), a);
        assert_eq!(a.worst(Natural), a);
        assert_eq!(Natural.worst(a), a);
        for b in all {
            // Commutative.
            assert_eq!(a.worst(b), b.worst(a));
            for c in all {
                // Associative.
                assert_eq!(a.worst(b).worst(c), a.worst(b.worst(c)));
            }
        }
    }
}

#[test]
fn test_control_event_backward_compatibility() {
    let writer_id = WriterId::from(StageId::new());

    // Create various control events and verify their event_type() method
    let events = vec![
        (ChainEventFactory::eof_event(writer_id, true), "control.eof"),
        (ChainEventFactory::drain_event(writer_id), "control.drain"),
        (
            ChainEventFactory::watermark_event(writer_id, 1000, None),
            "control.watermark",
        ),
        (
            ChainEventFactory::checkpoint_event(writer_id, "cp1".to_string(), None),
            "control.checkpoint",
        ),
    ];

    for (event, expected_type) in events {
        assert_eq!(event.event_type(), expected_type);
        assert!(event.is_control());

        // Also check payload() backward compatibility
        let payload = event.payload();
        assert!(!payload.is_null());
    }
}

#[test]
fn test_data_vs_control_events() {
    let writer_id = WriterId::from(StageId::new());

    // Data events
    let data_types = vec![
        "user.created",
        "order.processed",
        "payment.completed",
        "notification.sent",
    ];

    for event_type in data_types {
        let event = ChainEventFactory::data_event(
            writer_id,
            event_type,
            std::num::NonZeroU32::MIN,
            json!({"test": true}),
        );
        assert!(event.consumes_data_credit());
        assert!(!event.is_control());
        assert!(!event.is_eof());
        assert_eq!(event.event_type(), event_type);
    }

    // Control events
    let control_events = vec![
        ChainEventFactory::eof_event(writer_id, true),
        ChainEventFactory::drain_event(writer_id),
        ChainEventFactory::watermark_event(writer_id, 1000, None),
        ChainEventFactory::checkpoint_event(writer_id, "test".to_string(), None),
    ];

    for event in control_events {
        assert!(event.is_control());
        assert!(!event.consumes_data_credit());
        assert!(event.event_type().starts_with("control."));
    }
}

#[test]
fn test_direct_chain_event_construction() {
    // Test that we can still create events directly using the new structure
    let event = ChainEventFactory::flow_signal_event(
        WriterId::from(StageId::new()),
        FlowControlPayload::Eof {
            kind: EofKind::Natural,
            timestamp: 12345,
            writer_id: Some(WriterId::from(StageId::new())),
            writer_seq: None,
            writer_seq_by_event_type: Default::default(),
            vector_clock: None,
            last_event_id: None,
        },
    );

    assert!(event.is_control());
    assert!(event.is_eof());
    assert_eq!(event.event_type(), "control.eof");
}

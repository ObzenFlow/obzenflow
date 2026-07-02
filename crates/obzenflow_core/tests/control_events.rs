// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{ChainEvent, ChainEventContent, ChainEventFactory, EventId, WriterId};
use obzenflow_core::id::StageId;
use serde_json::json;

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
    let data_event =
        ChainEventFactory::data_event(writer_id, "user.data.processed", json!({"value": 42}));
    assert!(!data_event.is_control());
    assert!(!data_event.is_eof());
    assert!(data_event.is_data());
}

#[test]
fn test_flow_signal_payloads() {
    let writer_id = WriterId::from(StageId::new());

    // Test natural EOF kind
    let natural_eof = ChainEventFactory::eof_event(writer_id, true);
    match &natural_eof.content {
        ChainEventContent::FlowControl(FlowControlPayload::Eof { kind, .. }) => {
            assert!(kind.is_natural());
        }
        _ => panic!("Expected EOF signal"),
    }

    // Test poison EOF kind
    let forced_eof = ChainEventFactory::eof_event(writer_id, false);
    match &forced_eof.content {
        ChainEventContent::FlowControl(FlowControlPayload::Eof { kind, .. }) => {
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
        let event = ChainEventFactory::data_event(writer_id, event_type, json!({"test": true}));
        assert!(event.is_data());
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
        assert!(!event.is_data());
        assert!(event.event_type().starts_with("control."));
    }
}

#[test]
fn test_direct_chain_event_construction() {
    // Test that we can still create events directly using the new structure
    let event = ChainEvent {
        id: EventId::new(),
        writer_id: WriterId::from(StageId::new()),
        content: ChainEventContent::FlowControl(FlowControlPayload::Eof {
            kind: EofKind::Natural,
            timestamp: 12345,
            writer_id: Some(WriterId::from(StageId::new())),
            writer_seq: None,
            writer_seq_by_event_type: Default::default(),
            vector_clock: None,
            last_event_id: None,
        }),
        causality: Default::default(),
        flow_context: Default::default(),
        processing_info: Default::default(),
        intent: None,
        correlation: None,
        replay_context: None,
        ingress_context: None,
        cycle_depth: None,
        cycle_scc_id: None,
        runtime_context: None,
        observability: None,
        effect_provenance: None,
        admission_seq: None,
    };

    assert!(event.is_control());
    assert!(event.is_eof());
    assert_eq!(event.event_type(), "control.eof");
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::event::ingestion::IngressContext;
use crate::id::StageId;
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
        base_path: "/api/test".to_string(),
        batch_index: Some(1),
    });

    let child = ChainEventFactory::derived_data_event(
        writer_id,
        &parent,
        "child.event",
        json!({"data": "child"}),
    );

    assert_eq!(child.correlation_id, parent.correlation_id);
    assert_eq!(child.causality.parent_ids, vec![parent.id]);
    assert_eq!(child.ingress_context, parent.ingress_context);
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

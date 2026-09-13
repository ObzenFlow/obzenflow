// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::topology::contract_boundary_aliases;
use super::super::*;
use super::stream::{collect_closing, frame_payload, frames};
use crate::journal::MemoryJournal;
use obzenflow_adapters::studio::ContractBoundaryAliases;
use obzenflow_core::event::system_event::{
    ContractName, ContractResultStatusLabel, MetricsCoordinationEvent, PipelineLifecycleEvent,
    SystemFeedRole,
};
use obzenflow_core::event::types::{EventType, SeqNo, WriterId};
use obzenflow_core::event::SystemEventType;
use obzenflow_core::id::SystemId;
use obzenflow_core::journal::Journal;
use obzenflow_core::JournalOwner;
use obzenflow_core::{event::event_envelope::SystemEventEnvelope, StageId};
use obzenflow_topology::{
    BoundaryPortSpec, CompositePortRef, DirectedEdge, EdgeKind, PortDirection, StageInfo,
    StageType, Topology, TopologySubgraphInfo,
};

fn dual_composite_edge() -> (
    Topology,
    obzenflow_topology::StageId,
    obzenflow_topology::StageId,
) {
    let checkout = obzenflow_topology::StageId::from_bytes(1_u128.to_be_bytes());
    let audit = obzenflow_topology::StageId::from_bytes(2_u128.to_be_bytes());
    let checkout_subgraph = TopologySubgraphInfo::new(
        "saga:checkout",
        "saga",
        "checkout",
        "Checkout",
        vec![checkout],
        vec![],
        vec![checkout],
        vec![checkout],
        true,
    )
    .with_boundary_ports(vec![BoundaryPortSpec::new(
        "completed",
        PortDirection::Output,
        checkout,
        vec!["checkout.completed.v1".to_string()],
        true,
    )]);
    let audit_subgraph = TopologySubgraphInfo::new(
        "audit:orders",
        "audit",
        "orders",
        "Orders audit",
        vec![audit],
        vec![],
        vec![audit],
        vec![audit],
        true,
    )
    .with_boundary_ports(vec![BoundaryPortSpec::new(
        "in",
        PortDirection::Input,
        audit,
        vec!["checkout.completed.v1".to_string()],
        true,
    )]);
    let topology = Topology::new_unvalidated(
        vec![
            StageInfo::new(checkout, "checkout", StageType::Transform),
            StageInfo::new(audit, "audit", StageType::Transform),
        ],
        vec![
            DirectedEdge::new(checkout, audit, EdgeKind::Forward).with_composite_ports(vec![
                CompositePortRef::new("saga:checkout", "completed"),
                CompositePortRef::new("audit:orders", "in"),
            ]),
        ],
    )
    .unwrap()
    .with_subgraphs(vec![checkout_subgraph, audit_subgraph]);
    (topology, checkout, audit)
}

async fn contract_result_envelope(upstream: StageId, reader: StageId) -> SystemEventEnvelope {
    let system_id = SystemId::new();
    let journal = MemoryJournal::<SystemEvent>::with_owner(JournalOwner::system(system_id));
    journal
        .append(
            SystemEvent::new(
                WriterId::from(system_id),
                SystemEventType::ContractResult {
                    upstream,
                    reader,
                    selected_event_type: Some(EventType::from("checkout.completed.v1")),
                    feed_role: Some(SystemFeedRole::Input),
                    contract_name: ContractName::from("DeliveryContract"),
                    status: ContractResultStatusLabel::Pending,
                    cause: None,
                    reader_seq: Some(SeqNo(7)),
                    advertised_writer_seq: Some(SeqNo(9)),
                },
            ),
            None,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn contract_frame_keeps_one_physical_cursor_and_both_composite_aliases() {
    let (topology, upstream, reader) = dual_composite_edge();
    topology.validate_composite_boundaries().unwrap();
    let aliases = contract_boundary_aliases(&topology).unwrap();
    let upstream = StageId::from_ulid(upstream.ulid());
    let reader = StageId::from_ulid(reader.ulid());
    let envelope = contract_result_envelope(upstream, reader).await;

    let mut projection = StudioProjection::new(vec![], aliases).unwrap();
    let events = projection.project(&envelope, 0);
    assert_eq!(events.len(), 1);
    let frame = &events[0];
    assert_eq!(frame.event.as_deref(), Some("contract_result"));
    assert_eq!(
        frame.id.as_deref(),
        Some(envelope.event.id.to_string().as_str())
    );
    let payload = frame_payload(frame);

    assert_eq!(payload["upstream_stage_id"], upstream.to_string());
    assert_eq!(payload["reader_stage_id"], reader.to_string());
    assert_eq!(payload["selected_event_type"], "checkout.completed.v1");
    assert_eq!(payload["feed_role"], "input");
    assert_eq!(
        payload["composite_boundaries"],
        serde_json::json!([
            {
                "composite_id": "audit:orders",
                "port": "in",
                "direction": "inbound"
            },
            {
                "composite_id": "saga:checkout",
                "port": "completed",
                "direction": "outbound"
            }
        ])
    );
}

#[tokio::test]
async fn valid_resume_streams_the_enriched_contract_frame_after_its_cursor() {
    let (topology, upstream, reader) = dual_composite_edge();
    let upstream = StageId::from_ulid(upstream.ulid());
    let reader = StageId::from_ulid(reader.ulid());
    let system_id = SystemId::new();
    let writer = WriterId::from(system_id);
    let journal = Arc::new(MemoryJournal::<SystemEvent>::with_owner(
        JournalOwner::system(system_id),
    ));
    let cursor = journal
        .append(
            SystemEvent::new(
                writer,
                SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Ready),
            ),
            None,
        )
        .await
        .unwrap();
    let contract = journal
        .append(
            SystemEvent::new(
                writer,
                SystemEventType::ContractResult {
                    upstream,
                    reader,
                    selected_event_type: Some(EventType::from("checkout.completed.v1")),
                    feed_role: Some(SystemFeedRole::Input),
                    contract_name: ContractName::from("DeliveryContract"),
                    status: ContractResultStatusLabel::Pending,
                    cause: None,
                    reader_seq: Some(SeqNo(7)),
                    advertised_writer_seq: Some(SeqNo(9)),
                },
            ),
            None,
        )
        .await
        .unwrap();
    journal
        .append(
            SystemEvent::new(
                writer,
                SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Drained),
            ),
            None,
        )
        .await
        .unwrap();

    let (closing, receiver) = watch::channel(false);
    let endpoint = StudioUpdatesEndpoint::new(
        journal,
        StudioProjection::new(vec![], contract_boundary_aliases(&topology).unwrap()).unwrap(),
        None,
        receiver,
    );
    let body = collect_closing(&endpoint, closing, Some(&cursor.event.id.to_string())).await;
    let contract_frames = frames(&body, "contract_result");
    assert_eq!(contract_frames.len(), 1);
    let frame = contract_frames[0];
    assert_eq!(
        frame.id.as_deref(),
        Some(contract.event.id.to_string().as_str())
    );
    assert_ne!(
        frame.id.as_deref(),
        Some(cursor.event.id.to_string().as_str())
    );
    let payload = frame_payload(frame);
    assert_eq!(payload["composite_boundaries"].as_array().unwrap().len(), 2);
    assert_eq!(
        payload["composite_boundaries"][0]["composite_id"],
        "audit:orders"
    );
    assert_eq!(
        payload["composite_boundaries"][1]["composite_id"],
        "saga:checkout"
    );
}

#[tokio::test]
async fn ordinary_physical_edge_omits_unavailable_aliases() {
    let envelope = contract_result_envelope(StageId::new(), StageId::new()).await;
    let mut projection = StudioProjection::new(vec![], ContractBoundaryAliases::default()).unwrap();
    let frames = projection.project(&envelope, 0);
    assert_eq!(frames.len(), 1);
    assert!(frame_payload(&frames[0])
        .get("composite_boundaries")
        .is_none());
}

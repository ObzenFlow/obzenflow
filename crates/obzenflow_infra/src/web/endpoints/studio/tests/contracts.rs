// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::topology::contract_boundary_aliases;
use super::super::*;
use super::stream::{collect_closing, frame_payload, frames};
use crate::journal::MemoryJournal;
use obzenflow_adapters::studio::ContractBoundaryAliases;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::payloads::system_payload::{
    ContractName, ContractResultStatusLabel, MetricsCoordinationEvent, PipelineLifecycleEvent,
    SystemFeedRole,
};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::types::{EventType, SeqNo, WriterId};
use obzenflow_core::event::{ChainEvent, ChainEventFactory, SystemPayload};
use obzenflow_core::id::SystemId;
use obzenflow_core::journal::Journal;
use obzenflow_core::JournalOwner;
use obzenflow_core::{event::journal_record::ChainJournalRecord, StageId};
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

async fn contract_result_envelope(upstream: StageId, reader: StageId) -> ChainJournalRecord {
    let journal = MemoryJournal::<ChainEvent>::with_owner(JournalOwner::stage(reader));
    journal
        .append(
            ChainEventFactory::execution_event(
                WriterId::from(reader),
                ExecutionPayload::ContractResult {
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
            )
            .with_flow_context(FlowContext::new("reader", reader)),
            Default::default(),
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
    let events = projection.project(&envelope.clone().into(), 0);
    assert_eq!(events.len(), 1);
    let frame = &events[0];
    assert_eq!(frame.event.as_deref(), Some("contract_result"));
    assert_eq!(
        frame.id.as_deref(),
        Some(envelope.envelope.provenance.event.id.to_string().as_str())
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
                SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Ready),
            ),
            Default::default(),
        )
        .await
        .unwrap();
    let stage_journal = Arc::new(MemoryJournal::<ChainEvent>::with_owner(
        JournalOwner::stage(reader),
    ));
    let contract = stage_journal
        .append(
            ChainEventFactory::execution_event(
                WriterId::from(reader),
                ExecutionPayload::ContractResult {
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
            )
            .with_flow_context(FlowContext::new("reader", reader)),
            Default::default(),
        )
        .await
        .unwrap();
    journal
        .append(
            SystemEvent::new(
                writer,
                SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
            ),
            Default::default(),
        )
        .await
        .unwrap();

    let (closing, receiver) = watch::channel(false);
    let endpoint = StudioUpdatesEndpoint::new(
        journal.clone(),
        StudioProjection::new(vec![], contract_boundary_aliases(&topology).unwrap()).unwrap(),
        None,
        receiver,
    )
    .with_live_journals(vec![(reader, stage_journal.clone())], vec![journal.clone()]);
    let body = collect_closing(&endpoint, closing, Some(&super::stream::cursor(&cursor))).await;
    let contract_frames = frames(&body, "contract_result");
    assert_eq!(contract_frames.len(), 1);
    let frame = contract_frames[0];
    let checkpoint: std::collections::BTreeMap<String, u64> =
        serde_json::from_str(frame.id.as_ref().unwrap().strip_prefix("jr1:").unwrap()).unwrap();
    assert_eq!(
        checkpoint[&stage_journal.id().to_string()],
        contract.local_sequence()
    );
    assert!(checkpoint[&journal.id().to_string()] >= cursor.local_sequence());
    assert_ne!(
        frame.id.as_deref(),
        Some(cursor.envelope.provenance.event.id.to_string().as_str())
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
    let frames = projection.project(&envelope.clone().into(), 0);
    assert_eq!(frames.len(), 1);
    assert!(frame_payload(&frames[0])
        .get("composite_boundaries")
        .is_none());
}

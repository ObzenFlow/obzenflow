// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::config::LineagePolicy;
use obzenflow_core::event::payloads::observability_payload::{
    LoggingEventName, LoggingEvidence, LoggingInputReference, LoggingLevel, LoggingOccurrence,
    MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::{ChainEventContent, ChainEventFactory};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, EventEnvelope, EventId, StageId, WriterId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use uuid::Uuid;

fn root_logging_event(writer: WriterId) -> ChainEvent {
    let evidence = LoggingEvidence::new(
        LoggingEventName::new("test.observer.evidence").unwrap(),
        LoggingLevel::Info,
        LoggingOccurrence::HandlerInputObserved {
            input: LoggingInputReference {
                event_id: EventId::new(),
                event_type: "test.input.v1".to_string(),
                stage_input_position: Some(1),
            },
        },
        Vec::new(),
    )
    .unwrap();
    ChainEventFactory::observability_event(
        writer,
        ObservabilityPayload::Middleware(MiddlewareLifecycle::Logging(evidence)),
    )
}

fn derived_logging_event(writer: WriterId, parent: &ChainEvent) -> ChainEvent {
    let evidence = LoggingEvidence::new(
        LoggingEventName::new("test.observer.evidence").unwrap(),
        LoggingLevel::Info,
        LoggingOccurrence::HandlerInputObserved {
            input: LoggingInputReference {
                event_id: parent.id,
                event_type: parent.event_type(),
                stage_input_position: Some(1),
            },
        },
        Vec::new(),
    )
    .unwrap();
    ChainEventFactory::derived_event(
        writer,
        parent,
        ChainEventContent::Observability(ObservabilityPayload::Middleware(
            MiddlewareLifecycle::Logging(evidence),
        )),
        LineagePolicy::default(),
    )
}

async fn assert_single_append_non_interference(
    treatment: &dyn Journal<ChainEvent>,
    control: &dyn Journal<ChainEvent>,
    writer: WriterId,
    first_seq: u64,
) {
    let treatment_first = treatment
        .append(
            ChainEventFactory::data_event(writer, "test.data.v1", json!({ "n": 1 })),
            None,
        )
        .await
        .unwrap();
    let evidence = treatment
        .append(
            derived_logging_event(writer, &treatment_first.event),
            Some(&treatment_first),
        )
        .await
        .unwrap();
    let treatment_second = treatment
        .append(
            ChainEventFactory::derived_data_event(
                writer,
                &treatment_first.event,
                "test.data.v1",
                json!({ "n": 2 }),
                LineagePolicy::default(),
            ),
            Some(&treatment_first),
        )
        .await
        .unwrap();

    let control_first = control
        .append(
            ChainEventFactory::data_event(writer, "test.data.v1", json!({ "n": 1 })),
            None,
        )
        .await
        .unwrap();
    let control_second = control
        .append(
            ChainEventFactory::derived_data_event(
                writer,
                &control_first.event,
                "test.data.v1",
                json!({ "n": 2 }),
                LineagePolicy::default(),
            ),
            Some(&control_first),
        )
        .await
        .unwrap();

    assert_eq!(treatment_first.event.admission_seq.unwrap().0, first_seq);
    assert_eq!(evidence.event.admission_seq, None);
    assert_eq!(
        treatment_second.event.admission_seq.unwrap().0,
        first_seq + 1
    );
    assert_eq!(
        treatment_second.vector_clock, control_second.vector_clock,
        "projecting observer evidence must leave the flow writer clock unchanged"
    );

    let flow_key = writer.to_string();
    let observer_key = format!("observer:{writer}");
    assert_eq!(evidence.vector_clock.get(&flow_key), 1);
    assert_eq!(evidence.vector_clock.get(&observer_key), 1);
    assert_eq!(treatment_second.vector_clock.get(&flow_key), 2);
    assert_eq!(treatment_second.vector_clock.get(&observer_key), 0);
}

async fn assert_group_classification(
    journal: &dyn Journal<ChainEvent>,
    writer: WriterId,
    first_seq: u64,
) -> Vec<EventEnvelope<ChainEvent>> {
    let written = journal
        .append_group(
            "mixed-flow-and-observer-evidence",
            vec![
                ChainEventFactory::data_event(writer, "test.data.v1", json!({ "n": 1 })),
                root_logging_event(writer),
                ChainEventFactory::data_event(writer, "test.data.v1", json!({ "n": 2 })),
            ],
            None,
        )
        .await
        .unwrap();

    assert_eq!(written[0].event.admission_seq.unwrap().0, first_seq);
    assert_eq!(written[1].event.admission_seq, None);
    assert_eq!(written[2].event.admission_seq.unwrap().0, first_seq + 1);
    let flow_key = writer.to_string();
    let observer_key = format!("observer:{writer}");
    assert_eq!(written[0].vector_clock.get(&flow_key), 1);
    assert_eq!(written[1].vector_clock.get(&observer_key), 1);
    assert_eq!(written[2].vector_clock.get(&flow_key), 2);
    assert_eq!(written[2].vector_clock.get(&observer_key), 0);
    written
}

#[tokio::test]
async fn memory_single_and_group_append_keep_evidence_out_of_flow_order() {
    let writer = WriterId::from(StageId::new());
    let treatment = MemoryJournal::with_owner(JournalOwner::stage(StageId::new()))
        .with_admission_sequencer(Arc::new(AtomicU64::new(40)));
    let control = MemoryJournal::with_owner(JournalOwner::stage(StageId::new()))
        .with_admission_sequencer(Arc::new(AtomicU64::new(40)));
    assert_single_append_non_interference(&treatment, &control, writer, 40).await;

    let grouped = MemoryJournal::with_owner(JournalOwner::stage(StageId::new()))
        .with_admission_sequencer(Arc::new(AtomicU64::new(70)));
    assert_group_classification(&grouped, writer, 70).await;
}

#[tokio::test]
async fn disk_single_and_group_append_keep_evidence_out_of_flow_order() {
    let base = PathBuf::from(format!(
        "target/observer-causal-lane-disk-{}",
        Uuid::new_v4()
    ));
    let writer = WriterId::from(StageId::new());
    let treatment = DiskJournal::with_owner(
        base.join("treatment.log"),
        JournalOwner::stage(StageId::new()),
    )
    .unwrap()
    .with_admission_sequencer(Arc::new(AtomicU64::new(40)));
    let control = DiskJournal::with_owner(
        base.join("control.log"),
        JournalOwner::stage(StageId::new()),
    )
    .unwrap()
    .with_admission_sequencer(Arc::new(AtomicU64::new(40)));
    assert_single_append_non_interference(&treatment, &control, writer, 40).await;

    let grouped =
        DiskJournal::with_owner(base.join("group.log"), JournalOwner::stage(StageId::new()))
            .unwrap()
            .with_admission_sequencer(Arc::new(AtomicU64::new(70)));
    assert_group_classification(&grouped, writer, 70).await;
    std::fs::remove_dir_all(base).ok();
}

#[tokio::test]
async fn disk_reopen_reconstructs_both_causal_lanes_without_advancing_flow() {
    let base = PathBuf::from(format!(
        "target/observer-causal-lane-reopen-{}",
        Uuid::new_v4()
    ));
    let path = base.join("reopen.log");
    let owner = JournalOwner::stage(StageId::new());
    let writer = WriterId::from(StageId::new());
    let sequencer = Arc::new(AtomicU64::new(100));

    let first;
    {
        let journal = DiskJournal::with_owner(path.clone(), owner.clone())
            .unwrap()
            .with_admission_sequencer(sequencer.clone());
        first = journal
            .append(
                ChainEventFactory::data_event(writer, "test.data.v1", json!({ "n": 1 })),
                None,
            )
            .await
            .unwrap();
        journal
            .append(derived_logging_event(writer, &first.event), Some(&first))
            .await
            .unwrap();
    }

    let reopened = DiskJournal::with_owner(path, owner)
        .unwrap()
        .with_admission_sequencer(sequencer);
    let second = reopened
        .append(
            ChainEventFactory::derived_data_event(
                writer,
                &first.event,
                "test.data.v1",
                json!({ "n": 2 }),
                LineagePolicy::default(),
            ),
            Some(&first),
        )
        .await
        .unwrap();
    let second_evidence = reopened
        .append(derived_logging_event(writer, &second.event), Some(&second))
        .await
        .unwrap();

    let flow_key = writer.to_string();
    let observer_key = format!("observer:{writer}");
    assert_eq!(second.event.admission_seq.unwrap().0, 101);
    assert_eq!(second.vector_clock.get(&flow_key), 2);
    assert_eq!(second.vector_clock.get(&observer_key), 0);
    assert_eq!(second_evidence.event.admission_seq, None);
    assert_eq!(second_evidence.vector_clock.get(&flow_key), 2);
    assert_eq!(second_evidence.vector_clock.get(&observer_key), 2);
    std::fs::remove_dir_all(base).ok();
}

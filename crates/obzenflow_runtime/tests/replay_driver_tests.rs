// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourceContinuationRefusal, SourceInputPinnedFact,
};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::reader::JournalReader;
use obzenflow_core::source::{
    EncodedSourcePosition, SourceInputIdentity, SourcePosition, SourceProgress,
};
use obzenflow_core::{ChainEvent, JournalWriterId, StageId, WriterId};
use obzenflow_runtime::replay::{ReplayContextTemplate, ReplayDriver, SourceHandoff};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

struct TestReader {
    envelopes: Vec<JournalRecord<ChainPayload>>,
    pos: usize,
    at_end_hint: bool,
    /// When set, `next` returns an error, simulating the reader's own
    /// corruption detection (FLOWIP-120q: the reader owns finality).
    fail: bool,
}

#[async_trait]
impl obzenflow_core::journal::JournalStorageReader<ChainEvent> for TestReader {
    async fn storage_next(&mut self) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
        if self.fail {
            return Err(JournalError::Implementation {
                message: "simulated corrupt archive record".to_string(),
                source: "simulated corrupt archive record".into(),
            });
        }
        if self.pos < self.envelopes.len() {
            let env = self.envelopes[self.pos].clone();
            self.pos += 1;
            return Ok(Some(env));
        }
        Ok(None)
    }

    fn storage_position(&self) -> u64 {
        self.pos as u64
    }

    fn storage_is_at_end(&self) -> bool {
        self.at_end_hint && self.pos >= self.envelopes.len()
    }
}

// Model an archived record crossing the reader port before using it as evidence.
async fn admitted_fixture(record: JournalRecord<ChainPayload>) -> JournalRecord<ChainPayload> {
    TestReader {
        envelopes: vec![record],
        pos: 0,
        at_end_hint: true,
        fail: false,
    }
    .next()
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn replay_origins_advance_only_with_the_corresponding_admitted_record() {
    use obzenflow_core::event::{CausalFrontier, JournalClock};

    let writer = WriterId::from(StageId::new());
    let event = || {
        ChainEventFactory::data_event(
            writer,
            "test.event",
            std::num::NonZeroU32::MIN,
            serde_json::json!({}),
        )
    };
    let first = admitted_fixture(JournalRecord::new(JournalWriterId::new(), event())).await;
    let later_input = admitted_fixture(JournalRecord::new(JournalWriterId::new(), event())).await;
    let next_event = event();
    let (next, previous) = JournalClock::prepare(
        first.envelope.provenance.journal.run_id,
        first.causal_coordinate(),
        next_event.id,
        Some(&JournalClock::from_record(&first).unwrap()),
        &CausalFrontier::from_record(&later_input).unwrap(),
    )
    .unwrap();
    let mut provenance = first.envelope.provenance.journal.clone();
    provenance.vector_clock = next.clock;
    provenance.previous = previous;
    let second =
        admitted_fixture(JournalRecord::commit_event(next_event, provenance).unwrap()).await;
    let reader = Box::new(TestReader {
        envelopes: vec![first.clone(), second.clone()],
        pos: 0,
        at_end_hint: true,
        fail: false,
    });
    let mut driver = ReplayDriver::new(reader, PathBuf::from("archive.log"), template());
    for original in [first, second] {
        let replayed = driver
            .next_replayed_event(writer, "source", flow_context())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replayed.event.id, *original.id());
        assert_eq!(
            replayed.origin,
            CausalFrontier::from_record(&original).unwrap()
        );
        assert_eq!(
            replayed.origin.clock(),
            &original.envelope.provenance.journal.vector_clock
        );
    }
}

#[tokio::test]
async fn replay_driver_preserves_recorded_ids_and_sets_replay_context() {
    let archived_writer = WriterId::from(StageId::new());
    let eof = ChainEventFactory::eof_event(archived_writer, true);

    let mut data = ChainEventFactory::data_event(
        archived_writer,
        "test.event",
        std::num::NonZeroU32::MIN,
        serde_json::json!({"k": "v"}),
    );
    data = data.with_new_correlation();
    data.correlation
        .as_mut()
        .unwrap()
        .payload
        .as_mut()
        .unwrap()
        .metadata = Some(serde_json::json!({"application": "keep_me", "empty": null}));

    let envelopes = vec![
        JournalRecord::new(JournalWriterId::new(), eof.clone()),
        JournalRecord::new(JournalWriterId::new(), data.clone()),
    ];

    let reader = Box::new(TestReader {
        envelopes,
        pos: 0,
        at_end_hint: true,
        fail: false,
    });

    let journal_path = PathBuf::from("/tmp/archive.log");
    let replay_context = ReplayContextTemplate {
        original_flow_id: "flow_01HARCHIVE".to_string(),
        original_stage_id: StageId::new(),
    };

    let mut driver = ReplayDriver::new(reader, journal_path.clone(), replay_context.clone());

    let new_writer = WriterId::from(StageId::new());
    let flow_context = FlowContext {
        flow_name: "new_flow".to_string(),
        flow_id: "flow_new".to_string(),
        stage_name: "new_source".to_string(),
        stage_id: StageId::new(),
        stage_type: StageType::FiniteSource,
    };

    let replayed = driver
        .next_replayed_event(new_writer, "new_source", flow_context.clone())
        .await
        .unwrap()
        .expect("should replay data after skipping eof");

    assert!(!replayed.origin.clock().is_empty());
    let replayed = replayed.event;
    assert_eq!(replayed.id, data.id);
    assert_eq!(replayed.writer_id, data.writer_id);
    assert_eq!(replayed.flow_context.flow_name, flow_context.flow_name);
    assert_eq!(replayed.flow_context.stage_name, flow_context.stage_name);

    let replay_ctx = replayed
        .replay_context
        .as_ref()
        .expect("replay_context set");
    assert_eq!(replay_ctx.original_event_id, data.id);
    assert_eq!(replay_ctx.original_flow_id, replay_context.original_flow_id);
    assert_eq!(
        replay_ctx.original_stage_id,
        replay_context.original_stage_id
    );
    assert_eq!(replayed.correlation, data.correlation);
    assert_eq!(replayed.processing.event_time, data.processing.event_time);
    let replay_wire = serde_json::to_value(replay_ctx).unwrap();
    assert_eq!(replay_wire.as_object().unwrap().len(), 3);

    assert!(replayed.is_fact());
    assert_eq!(replayed.event_type(), data.event_type());
    assert_eq!(
        replayed.payload.contract_body().unwrap(),
        data.payload.contract_body().unwrap()
    );
}

fn template() -> ReplayContextTemplate {
    ReplayContextTemplate {
        original_flow_id: "flow_01HARCHIVE".to_string(),
        original_stage_id: StageId::new(),
    }
}

fn flow_context() -> FlowContext {
    FlowContext {
        flow_name: "new_flow".to_string(),
        flow_id: "flow_new".to_string(),
        stage_name: "new_source".to_string(),
        stage_id: StageId::new(),
        stage_type: StageType::FiniteSource,
    }
}

#[tokio::test]
async fn replay_driver_treats_reader_none_as_clean_end() {
    // FLOWIP-120q: the reader owns the torn-tail policy, so a `None` from the
    // reader is always a clean end. The driver no longer re-derives finality
    // from `is_at_end`, so even an `is_at_end()==false` reader that returns
    // `None` ends the replay cleanly rather than erroring.
    let reader = Box::new(TestReader {
        envelopes: Vec::new(),
        pos: 0,
        at_end_hint: false,
        fail: false,
    });

    let mut driver = ReplayDriver::new(reader, PathBuf::from("/tmp/archive.log"), template());

    let result = driver
        .next_replayed_event(WriterId::from(StageId::new()), "new_source", flow_context())
        .await
        .expect("None from the reader is a clean end, not an error");

    assert!(result.is_none());
}

async fn drain_driver(driver: &mut ReplayDriver) {
    // Pull until exhaustion so every skipped row is observed.
    while driver
        .next_replayed_event(WriterId::from(StageId::new()), "new_source", flow_context())
        .await
        .expect("clean archive")
        .is_some()
    {}
}

#[tokio::test]
async fn replay_driver_captures_the_archived_eof_kind() {
    // FLOWIP-095k: the recorded completion kind is captured while the archived
    // EOF is skipped, so exhaustion can reproduce it.
    use obzenflow_core::event::payloads::flow_control_payload::EofKind;

    for (natural, expected) in [(true, EofKind::Natural), (false, EofKind::Poison)] {
        let archived_writer = WriterId::from(StageId::new());
        let data = ChainEventFactory::data_event(
            archived_writer,
            "test.event",
            std::num::NonZeroU32::MIN,
            serde_json::json!({}),
        );
        let eof = ChainEventFactory::eof_event(archived_writer, natural);
        let reader = Box::new(TestReader {
            envelopes: vec![
                JournalRecord::new(JournalWriterId::new(), data),
                JournalRecord::new(JournalWriterId::new(), eof),
            ],
            pos: 0,
            at_end_hint: true,
            fail: false,
        });

        let mut driver = ReplayDriver::new(reader, PathBuf::from("/tmp/archive.log"), template());
        assert_eq!(
            driver.archived_eof_kind(),
            None,
            "nothing captured before reading"
        );
        drain_driver(&mut driver).await;
        assert_eq!(driver.archived_eof_kind(), Some(expected));
    }
}

#[tokio::test]
async fn replay_driver_captures_no_kind_from_an_archive_with_no_committed_eof() {
    // A killed run's journal ends at the last committed record; a torn final
    // EOF is never parsed (FLOWIP-120q), so both shapes present as no-EOF here.
    let archived_writer = WriterId::from(StageId::new());
    let data = ChainEventFactory::data_event(
        archived_writer,
        "test.event",
        std::num::NonZeroU32::MIN,
        serde_json::json!({}),
    );
    let reader = Box::new(TestReader {
        envelopes: vec![JournalRecord::new(JournalWriterId::new(), data)],
        pos: 0,
        at_end_hint: false,
        fail: false,
    });

    let mut driver = ReplayDriver::new(reader, PathBuf::from("/tmp/archive.log"), template());
    drain_driver(&mut driver).await;
    assert_eq!(driver.archived_eof_kind(), None);
}

#[tokio::test]
async fn replay_driver_maps_reader_error_to_corrupted_archive() {
    // Corruption now surfaces as an `Err` from the reader (it owns finality and
    // classification); the driver maps that to `CorruptedArchive`.
    let reader = Box::new(TestReader {
        envelopes: Vec::new(),
        pos: 0,
        at_end_hint: false,
        fail: true,
    });

    let mut driver = ReplayDriver::new(reader, PathBuf::from("/tmp/archive.log"), template());

    let err = driver
        .next_replayed_event(WriterId::from(StageId::new()), "new_source", flow_context())
        .await
        .err()
        .unwrap();

    assert!(matches!(err, ReplayError::CorruptedArchive { .. }));
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Line(u64);

impl SourcePosition for Line {}

fn at(line: u64) -> EncodedSourcePosition {
    EncodedSourcePosition::encode(&Line(line)).unwrap()
}

fn input_pin(file: &str) -> SourceInputPinnedFact {
    SourceInputPinnedFact {
        identity: SourceInputIdentity::new("test", &serde_json::json!({ "file": file })).unwrap(),
        beginning: at(0),
    }
}

fn pinned(writer: WriterId, pin: &SourceInputPinnedFact) -> ChainEvent {
    ChainEventFactory::execution_event(writer, ExecutionPayload::SourceInputPinned(pin.clone()))
}

fn row(writer: WriterId, after: Option<u64>) -> ChainEvent {
    let mut event = ChainEventFactory::data_event(
        writer,
        "test.row",
        std::num::NonZeroU32::MIN,
        serde_json::json!({}),
    );
    event.source_progress = after.map(|line| SourceProgress { after: at(line) });
    event
}

/// Replays `events` to exhaustion: the handoff they support and the input count.
async fn reconstructed(
    events: Vec<ChainEvent>,
) -> (Result<SourceHandoff, SourceContinuationRefusal>, u64) {
    let reader = Box::new(TestReader {
        envelopes: events
            .into_iter()
            .map(|event| JournalRecord::new(JournalWriterId::new(), event))
            .collect(),
        pos: 0,
        at_end_hint: true,
        fail: false,
    });
    let mut driver = ReplayDriver::new(reader, PathBuf::from("archive.log"), template());
    drain_driver(&mut driver).await;
    (driver.frontier().handoff(), driver.replayed_events())
}

#[tokio::test]
async fn the_frontier_is_the_last_position_bound_to_the_latest_pin() {
    let writer = WriterId::from(StageId::new());
    let first = input_pin("first");
    let second = input_pin("second");

    // FLOWIP-134i B15: never pinned, nothing admitted.
    assert_eq!(
        reconstructed(Vec::new()).await,
        (Ok(SourceHandoff::Start), 0)
    );
    // A pinned empty prefix starts at the recorded beginning; the preserved
    // pin is not delivered input.
    assert_eq!(
        reconstructed(vec![pinned(writer, &first)]).await,
        (
            Ok(SourceHandoff::Seek {
                pin: first.clone(),
                after: None,
            }),
            0
        )
    );
    assert_eq!(
        reconstructed(vec![
            pinned(writer, &first),
            row(writer, Some(1)),
            row(writer, Some(2)),
        ])
        .await,
        (
            Ok(SourceHandoff::Seek {
                pin: first.clone(),
                after: Some(at(2)),
            }),
            2
        )
    );
    // A re-pin binds only the records after it.
    assert_eq!(
        reconstructed(vec![
            pinned(writer, &first),
            row(writer, Some(1)),
            pinned(writer, &second),
        ])
        .await
        .0,
        Ok(SourceHandoff::Seek {
            pin: second.clone(),
            after: None,
        })
    );
    assert_eq!(
        reconstructed(vec![
            pinned(writer, &first),
            row(writer, Some(1)),
            pinned(writer, &second),
            row(writer, Some(5)),
        ])
        .await
        .0,
        Ok(SourceHandoff::Seek {
            pin: second,
            after: Some(at(5)),
        })
    );
}

#[tokio::test]
async fn admitted_input_without_its_pin_or_position_refuses() {
    let writer = WriterId::from(StageId::new());
    // FLOWIP-134i B11: a positioned record with no preceding pin is corrupt.
    assert_eq!(
        reconstructed(vec![row(writer, Some(1))]).await.0,
        Err(SourceContinuationRefusal::MissingEvidence)
    );
    assert_eq!(
        reconstructed(vec![pinned(writer, &input_pin("file")), row(writer, None)])
            .await
            .0,
        Err(SourceContinuationRefusal::MissingEvidence)
    );
}

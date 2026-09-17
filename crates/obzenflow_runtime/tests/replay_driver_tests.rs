// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::reader::JournalReader;
use obzenflow_core::{ChainEvent, JournalWriterId, StageId, WriterId};
use obzenflow_runtime::replay::{ReplayContextTemplate, ReplayDriver};
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
impl JournalReader<ChainEvent> for TestReader {
    async fn next(&mut self) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
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

    fn position(&self) -> u64 {
        self.pos as u64
    }

    fn is_at_end(&self) -> bool {
        self.at_end_hint && self.pos >= self.envelopes.len()
    }
}

#[tokio::test]
async fn replay_driver_preserves_recorded_ids_and_sets_replay_context() {
    let archived_writer = WriterId::from(StageId::new());
    let eof = ChainEventFactory::eof_event(archived_writer, true);

    let mut data =
        ChainEventFactory::data_event(archived_writer, "test.event", serde_json::json!({"k": "v"}));
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
        let data =
            ChainEventFactory::data_event(archived_writer, "test.event", serde_json::json!({}));
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
    let data = ChainEventFactory::data_event(archived_writer, "test.event", serde_json::json!({}));
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

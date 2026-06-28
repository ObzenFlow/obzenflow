// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::context::{FlowContext, IntentContext, StageType};
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::{ChainEvent, JournalWriterId, StageId, WriterId};
use obzenflow_runtime::replay::{ReplayContextTemplate, ReplayDriver, ReplayError};
use std::path::PathBuf;

struct TestReader {
    envelopes: Vec<EventEnvelope<ChainEvent>>,
    pos: usize,
    at_end_hint: bool,
    /// When set, `next` returns an error, simulating the reader's own
    /// corruption detection (FLOWIP-120q: the reader owns finality).
    fail: bool,
}

#[async_trait]
impl JournalReader<ChainEvent> for TestReader {
    async fn next(&mut self) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
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
    let mut eof = ChainEventFactory::eof_event(archived_writer, true);
    eof.intent = Some(IntentContext::Event {
        fact: "should_be_skipped".to_string(),
    });

    let mut data =
        ChainEventFactory::data_event(archived_writer, "test.event", serde_json::json!({"k": "v"}));
    data = data.with_new_correlation("archived_stage");
    data.intent = Some(IntentContext::Event {
        fact: "keep_me".to_string(),
    });

    let envelopes = vec![
        EventEnvelope::new(JournalWriterId::new(), eof.clone()),
        EventEnvelope::new(JournalWriterId::new(), data.clone()),
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
        archive_path: PathBuf::from("/tmp/archive_run"),
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

    let replay_ctx = replayed.replay_context.expect("replay_context set");
    assert_eq!(replay_ctx.original_event_id, data.id);
    assert_eq!(replay_ctx.original_flow_id, replay_context.original_flow_id);
    assert_eq!(
        replay_ctx.original_stage_id,
        replay_context.original_stage_id
    );
    assert_eq!(replay_ctx.archive_path, replay_context.archive_path);

    match (&replayed.intent, &data.intent) {
        (Some(IntentContext::Event { fact: a }), Some(IntentContext::Event { fact: b })) => {
            assert_eq!(a, b)
        }
        _ => panic!("expected preserved Event intent"),
    }

    match (&replayed.content, &data.content) {
        (
            obzenflow_core::event::ChainEventContent::Data {
                event_type: a,
                payload: pa,
            },
            obzenflow_core::event::ChainEventContent::Data {
                event_type: b,
                payload: pb,
            },
        ) => {
            assert_eq!(a, b);
            assert_eq!(pa, pb);
        }
        _ => panic!("expected preserved Data content"),
    }
}

fn template() -> ReplayContextTemplate {
    ReplayContextTemplate {
        original_flow_id: "flow_01HARCHIVE".to_string(),
        original_stage_id: StageId::new(),
        archive_path: PathBuf::from("/tmp/archive_run"),
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

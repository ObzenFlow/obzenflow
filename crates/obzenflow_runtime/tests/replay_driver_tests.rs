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
}

#[async_trait]
impl JournalReader<ChainEvent> for TestReader {
    async fn next(&mut self) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
        if self.pos < self.envelopes.len() {
            let env = self.envelopes[self.pos].clone();
            self.pos += 1;
            return Ok(Some(env));
        }
        Ok(None)
    }

    async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
        let remaining = (self.envelopes.len().saturating_sub(self.pos)) as u64;
        let to_skip = remaining.min(n) as usize;
        self.pos += to_skip;
        Ok(to_skip as u64)
    }

    fn position(&self) -> u64 {
        self.pos as u64
    }

    fn is_at_end(&self) -> bool {
        self.at_end_hint && self.pos >= self.envelopes.len()
    }
}

#[tokio::test]
async fn replay_driver_rewrites_ids_and_sets_replay_context() {
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

    assert_ne!(replayed.id, data.id);
    assert_eq!(replayed.writer_id, new_writer);
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

#[tokio::test]
async fn replay_driver_treats_non_eof_none_with_is_at_end_false_as_corruption() {
    let reader = Box::new(TestReader {
        envelopes: Vec::new(),
        pos: 0,
        at_end_hint: false,
    });

    let journal_path = PathBuf::from("/tmp/archive.log");
    let replay_context = ReplayContextTemplate {
        original_flow_id: "flow_01HARCHIVE".to_string(),
        original_stage_id: StageId::new(),
        archive_path: PathBuf::from("/tmp/archive_run"),
    };

    let mut driver = ReplayDriver::new(reader, journal_path.clone(), replay_context);
    let new_writer = WriterId::from(StageId::new());
    let flow_context = FlowContext {
        flow_name: "new_flow".to_string(),
        flow_id: "flow_new".to_string(),
        stage_name: "new_source".to_string(),
        stage_id: StageId::new(),
        stage_type: StageType::FiniteSource,
    };

    let err = driver
        .next_replayed_event(new_writer, "new_source", flow_context)
        .await
        .err()
        .unwrap();

    assert!(matches!(err, ReplayError::CorruptedArchive { .. }));
}

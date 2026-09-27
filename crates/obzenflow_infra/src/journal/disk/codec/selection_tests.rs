// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::MemoryJournal;
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{ChainEvent, ChainEventFactory, ChainPayload};
use obzenflow_core::{Journal, StageId};

async fn mixed() -> (tempfile::TempDir, PathBuf, Vec<u8>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data.log");
    let stage = StageId::new();
    let report = || {
        ChainEventFactory::create_with_context(
            stage.into(),
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                StageLifecycleFact::Running { stage_id: stage },
            )),
            FlowContext::new("child", stage),
        )
    };
    let journal =
        MemoryJournal::<ChainEvent>::with_owner(obzenflow_core::JournalOwner::stage(stage));
    let rows = journal
        .append_group(
            "mixed",
            vec![
                report(),
                ChainEventFactory::data_event(
                    stage.into(),
                    "business",
                    serde_json::json!({"value":123}),
                ),
                report(),
            ],
            Default::default(),
        )
        .await
        .unwrap();
    let bytes = prepare(&rows, Some("mixed"), &path, DefinitionStore::default())
        .unwrap()
        .bytes;
    (dir, path, bytes)
}

fn payload_offset(body: &[u8], member: usize) -> usize {
    let envelope = routing::Envelope::parse(body).unwrap();
    let mut input = Cursor::new(envelope.members[member].body);
    input.bytes().unwrap();
    if input.byte().unwrap() == 2 {
        input.bytes().unwrap();
    }
    input.bytes().unwrap().as_ptr() as usize - body.as_ptr() as usize
}

fn class_offset(body: &[u8], member: usize) -> usize {
    let mut input = Cursor::new(body);
    let route = input.bytes().unwrap();
    let mut fields = Cursor::new(route);
    if fields.byte().unwrap() == 1 {
        fields.text().unwrap();
    }
    fields.length().unwrap();
    fields.take(32).unwrap();
    if fields.unsigned().unwrap() > 1 {
        fields.take(16).unwrap();
    }
    for _ in 0..member {
        fields.take(17).unwrap();
        fields.length().unwrap();
    }
    fields.take(16).unwrap();
    route.as_ptr() as usize - body.as_ptr() as usize + fields.position()
}

#[tokio::test]
async fn selective_group_validates_all_candidates_before_returning_any() {
    let (_dir, path, bytes) = mixed().await;
    let body = frame::validate(&bytes).unwrap();
    let full = Decoder::cold(&path)
        .decode::<ChainEvent>(body, 0)
        .unwrap()
        .into_records();
    let selected = Decoder::cold(&path)
        .decode_selected::<ChainEvent>(body, 0)
        .unwrap();
    assert_eq!(selected.records.len(), 2);
    assert_eq!(
        serde_json::to_value(&selected.records).unwrap(),
        serde_json::to_value(vec![&full[0], &full[2]]).unwrap()
    );
    let mut invalid = body.to_vec();
    invalid[payload_offset(body, 2)] = 0xff;
    let bytes = frame::encode(&invalid);
    let body = frame::validate(&bytes).unwrap();
    assert!(Decoder::cold(&path)
        .decode_selected::<ChainEvent>(body, 0)
        .is_err());
}

#[tokio::test]
async fn skipped_payload_semantics_are_distinct_from_integrity_and_routing() {
    let (_dir, path, bytes) = mixed().await;
    let body = frame::validate(&bytes).unwrap();
    let index = payload_offset(body, 1);
    let mut corrupt = bytes.clone();
    corrupt[frame::HEADER_LEN + index] = 0xff;
    assert!(
        frame::validate(&corrupt).is_err(),
        "skipped bytes remain checksum protected"
    );
    let mut malformed = body.to_vec();
    malformed[index] = 0xff;
    let repaired_crc = frame::encode(&malformed);
    let body = frame::validate(&repaired_crc).unwrap();
    assert_eq!(
        Decoder::cold(&path)
            .decode_selected::<ChainEvent>(body, 0)
            .unwrap()
            .records
            .len(),
        2
    );
    assert!(Decoder::cold(&path).decode::<ChainEvent>(body, 0).is_err());
}

#[tokio::test]
async fn routing_classifications_extents_and_payload_agreement_fail_explicitly() {
    let (_dir, path, bytes) = mixed().await;
    let body = frame::validate(&bytes).unwrap();
    let class = class_offset(body, 0);
    for (offset, value) in [(class, 2), (class + 1, 0)] {
        let mut malformed = body.to_vec();
        malformed[offset] = value;
        assert!(Decoder::cold(&path)
            .decode_selected::<ChainEvent>(&malformed, 0)
            .is_err());
        assert!(Decoder::cold(&path)
            .decode::<ChainEvent>(&malformed, 0)
            .is_err());
    }
    let mut wrong_class = body.to_vec();
    wrong_class[class] = 0;
    assert!(
        Decoder::cold(&path)
            .decode::<ChainEvent>(&wrong_class, 0)
            .is_err(),
        "full decoding checks writer classification parity"
    );
}

#[tokio::test]
async fn no_member_of_a_truncated_group_can_reach_the_decoder() {
    let (_dir, path, bytes) = mixed().await;
    for end in 0..bytes.len() {
        assert!(
            matches!(
                frame::validate(&bytes[..end]),
                Err(frame::FrameProblem::Incomplete(_))
            ),
            "truncation {end}"
        );
    }
    assert_eq!(
        Decoder::cold(&path)
            .decode_selected::<ChainEvent>(frame::validate(&bytes).unwrap(), 0)
            .unwrap()
            .records
            .len(),
        2
    );
    let mut old = bytes;
    old[3] = b'9';
    assert!(matches!(
        frame::validate(&old),
        Err(frame::FrameProblem::SchemaMismatch)
    ));
}

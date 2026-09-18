// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::observability::tests::event;
use crate::journal::{DiskJournal, MemoryJournal};
use obzenflow_core::event::observability::families::observation_families;
use obzenflow_core::event::observability::{CaptureSeq, ObservationRecord};
use obzenflow_core::event::provenance::JournalGroupMember;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, Journal, JournalOwner, StageId};

const FORMER_KEY_LIMIT: usize = 4096;

fn subject_event(template: &ChainEvent, subject: String, seq: u64) -> ChainEvent {
    let mut next = template.clone();
    let packet = next.envelope.observability.as_mut().unwrap();
    packet.capture.capture_seq = CaptureSeq(seq);
    packet.runtime = None;
    packet.records = vec![ObservationRecord::RateLimiterActivity {
        effect_type: Some(subject),
        window_ms: 1000,
        delayed_events: seq,
        delay_ms_total: seq,
        delay_ms_max: 1,
        limit_rate: 100.0,
    }];
    next
}

fn subject(template: &ChainEvent, index: usize) -> ChainEvent {
    subject_event(template, format!("effect-{index}"), index as u64 + 1)
}

fn key(event: &ChainEvent) -> ObservationKey {
    let packet = event.envelope.observability.as_ref().unwrap();
    let mut families = observation_families(packet.clone()).unwrap();
    assert_eq!(families.len(), 1);
    ObservationKey {
        capture_scope: packet.capture.capture_scope,
        observer: packet.capture.observer,
        kind: families.pop().unwrap().0,
    }
}

async fn append_subjects(journal: &dyn Journal<ChainEvent>, template: &ChainEvent, count: usize) {
    for index in 0..count {
        journal
            .append(subject(template, index), Default::default())
            .await
            .unwrap();
    }
}

fn assert_observation(
    lookup: ObservationLookup,
    expected: &ChainEvent,
    committed_len: u64,
    position: u64,
) {
    let ObservationLookup::Ready {
        committed_len: actual_len,
        observation: Some(located),
    } = lookup
    else {
        panic!("expected complete coverage and an observation, got {lookup:?}");
    };
    assert_eq!(actual_len, committed_len);
    assert_eq!(located.position, position);
    assert_eq!(
        serde_json::to_value(located.observation).unwrap(),
        serde_json::to_value(expected.envelope.observability.as_ref().unwrap()).unwrap()
    );
}

async fn assert_ready(
    reader: &dyn JournalObservationReader,
    expected: &ChainEvent,
    committed_len: u64,
    position: u64,
) {
    assert_observation(
        reader.latest_observation(&key(expected)).await.unwrap(),
        expected,
        committed_len,
        position,
    );
}

async fn rebuild_to_ready(
    reader: &DiskObservationReader<ChainEvent>,
    expected: &ChainEvent,
    committed_len: u64,
    position: u64,
) {
    let mut previous = 0;
    // All these archives have at most one frame per record. Bound the calls so
    // a rebuild stuck on the former key limit fails instead of looping forever.
    for _ in 0..=committed_len / REBUILD_FRAMES as u64 + 1 {
        let lookup = reader.latest_observation(&key(expected)).await.unwrap();
        match lookup {
            ObservationLookup::Rebuilding {
                examined_through,
                committed_len: None,
            } => {
                assert!(examined_through > previous);
                assert!(examined_through <= committed_len);
                previous = examined_through;
            }
            ready @ ObservationLookup::Ready { .. } => {
                assert!(previous > 0, "the archive must need incremental rebuilding");
                assert_observation(ready, expected, committed_len, position);
                return;
            }
            other => panic!("unexpected coverage during rebuild: {other:?}"),
        }
    }
    panic!("rebuild failed to finish");
}

#[tokio::test]
async fn disk_and_memory_retain_quiet_subjects_and_bound_history_past_the_key_limit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("subjects.log");
    let stage = StageId::new();
    let foreign = StageId::new();
    let mut template = event(stage, 1);
    // A local carrier forwards a foreign capture without changing its identity.
    template
        .envelope
        .observability
        .as_mut()
        .unwrap()
        .capture
        .observer = foreign.into();
    let quiet = subject(&template, 0);
    let new = subject(&template, FORMER_KEY_LIMIT);
    let disk = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
    let memory = MemoryJournal::with_owner(JournalOwner::stage(stage));
    for journal in [&disk as &dyn Journal<ChainEvent>, &memory] {
        append_subjects(journal, &template, FORMER_KEY_LIMIT).await;
        let reader = journal.observation_reader().unwrap();
        assert_ready(reader, &quiet, FORMER_KEY_LIMIT as u64, 0).await;
        journal
            .append(new.clone(), Default::default())
            .await
            .unwrap();
        let mut committed_len = FORMER_KEY_LIMIT as u64 + 1;
        assert_ready(reader, &quiet, committed_len, 0).await;
        assert_ready(reader, &new, committed_len, FORMER_KEY_LIMIT as u64).await;

        for seq in 5000..5010 {
            for index in [1, FORMER_KEY_LIMIT] {
                let update = subject_event(&template, format!("effect-{index}"), seq);
                journal
                    .append(update.clone(), Default::default())
                    .await
                    .unwrap();
                committed_len += 1;
                assert_ready(reader, &update, committed_len, committed_len - 1).await;
            }
        }
        // A later carrier with an older capture must not replace the latest one.
        journal
            .append(new.clone(), Default::default())
            .await
            .unwrap();
        committed_len += 1;
        let latest = subject_event(&template, format!("effect-{FORMER_KEY_LIMIT}"), 5009);
        assert_ready(reader, &latest, committed_len, committed_len - 2).await;
        assert_ready(reader, &quiet, committed_len, 0).await;
        let ObservationLookup::Ready {
            observation,
            committed_len: count,
        } = reader.latest_observations(foreign.into()).await.unwrap()
        else {
            panic!("every forwarded subject must remain represented");
        };
        assert_eq!(count, committed_len);
        assert_eq!(observation.len(), FORMER_KEY_LIMIT + 1);
        let mut absent = key(&quiet);
        absent.observer = stage.into();
        assert!(matches!(reader.latest_observation(&absent).await.unwrap(),
            ObservationLookup::Ready { committed_len: count, observation: None }
                if count == committed_len));
    }
    let reader = DiskObservationReader::<ChainEvent>::new(path, false);
    let state = reader.shared.state.lock().unwrap();
    assert_eq!(state.index.entries.len(), FORMER_KEY_LIMIT + 1);
    assert_eq!(state.index.entries[&key(&quiet)].len(), 1);
    for index in [1, FORMER_KEY_LIMIT] {
        let history = &state.index.entries[&key(&subject(&template, index))];
        assert_eq!(history.len(), HISTORY_PER_KEY);
        assert_eq!(history.front().unwrap().capture_seq, CaptureSeq(5006));
        assert_eq!(history.back().unwrap().capture_seq, CaptureSeq(5009));
    }
    assert!(state
        .index
        .entries
        .values()
        .all(|history| history.len() <= HISTORY_PER_KEY));
}

#[tokio::test]
async fn atomic_group_crosses_the_key_limit_with_forwarded_captures_and_terminal_controls() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("group.log");
    let prefix = FORMER_KEY_LIMIT - 1;
    let stage = StageId::new();
    let template = event(stage, 1);
    let quiet = subject(&template, 0);
    let first = subject(&template, prefix);
    let forwarded = subject_event(&event(StageId::new(), 1), "forwarded".into(), 9000);
    let eof = ChainEventFactory::eof_event(stage.into(), true)
        .with_observability_context(forwarded.envelope.observability.clone().unwrap());
    let disk = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
    let memory = MemoryJournal::with_owner(JournalOwner::stage(stage));
    let committed_len = prefix as u64 + 3;
    for journal in [&disk as &dyn Journal<ChainEvent>, &memory] {
        append_subjects(journal, &template, prefix).await;
        assert_ready(
            journal.observation_reader().unwrap(),
            &quiet,
            prefix as u64,
            0,
        )
        .await;
        // The first member reaches 4,096 keys; the terminal member exceeds it.
        let expected = journal
            .append_group(
                "over-capacity",
                vec![
                    first.clone(),
                    ChainEventFactory::drain_event(stage.into()),
                    eof.clone(),
                ],
                Default::default(),
            )
            .await
            .unwrap();
        let records = journal.read_all_unordered().await.unwrap();
        assert_eq!(records.len() as u64, committed_len);
        assert_eq!(
            serde_json::to_value(&records[prefix..]).unwrap(),
            serde_json::to_value(&expected).unwrap()
        );
        for (index, record) in expected.iter().enumerate() {
            let provenance = &record.envelope.provenance.journal;
            assert_eq!(
                provenance.journal_group_id.as_deref(),
                Some("over-capacity")
            );
            assert_eq!(
                provenance.journal_group_member,
                Some(JournalGroupMember {
                    index: index as u32,
                    size: 3
                })
            );
        }
        let reader = journal.observation_reader().unwrap();
        assert_ready(reader, &quiet, committed_len, 0).await;
        assert_ready(reader, &first, committed_len, prefix as u64).await;
        assert_ready(reader, &eof, committed_len, prefix as u64 + 2).await;
    }
    drop(disk);
    std::fs::remove_file(checkpoint_path(&path)).unwrap();
    let reader = DiskObservationReader::<ChainEvent>::open(path).unwrap();
    rebuild_to_ready(&reader, &eof, committed_len, prefix as u64 + 2).await;
    assert_ready(&reader, &quiet, committed_len, 0).await;
    assert_ready(&reader, &first, committed_len, prefix as u64).await;
    assert_eq!(
        reader.shared.state.lock().unwrap().rebuilt_frames,
        prefix + 1
    );
}

#[tokio::test]
async fn checkpoints_above_the_key_limit_load_completely_or_rebuild_when_missing_or_corrupt() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("checkpoint.log");
    let stage = StageId::new();
    let template = event(stage, 1);
    let quiet = subject(&template, 0);
    let last = subject(&template, FORMER_KEY_LIMIT);
    let count = FORMER_KEY_LIMIT as u64 + 1;
    let journal = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
    append_subjects(&journal, &template, FORMER_KEY_LIMIT + 1).await;
    assert_ready(
        journal.observation_reader().unwrap(),
        &last,
        count,
        count - 1,
    )
    .await;
    drop(journal);
    let valid = std::fs::read(checkpoint_path(&path)).unwrap();
    let checked: CheckedCheckpoint = serde_json::from_slice(&valid).unwrap();
    let checkpoint: Checkpoint = serde_json::from_str(&checked.body).unwrap();
    assert_eq!(checkpoint.entries.len(), FORMER_KEY_LIMIT + 1);
    assert_eq!(checkpoint.committed_len, count);
    assert!(valid.len() as u64 <= MAX_CHECKPOINT_BYTES);
    let reader = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
    assert_ready(&reader, &quiet, count, 0).await;
    assert_ready(&reader, &last, count, count - 1).await;
    assert_eq!(reader.shared.state.lock().unwrap().rebuilt_frames, 0);
    drop(reader);

    // Corrupt a checksummed body while keeping the outer JSON well formed.
    let corrupt = serde_json::to_vec(&CheckedCheckpoint {
        crc: checked.crc ^ 1,
        body: checked.body,
    })
    .unwrap();
    for bytes in [None, Some(corrupt)] {
        match bytes {
            None => std::fs::remove_file(checkpoint_path(&path)).unwrap(),
            Some(bytes) => std::fs::write(checkpoint_path(&path), bytes).unwrap(),
        }
        let reader = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
        rebuild_to_ready(&reader, &last, count, count - 1).await;
        assert_ready(&reader, &quiet, count, 0).await;
        let state = reader.shared.state.lock().unwrap();
        assert_eq!(state.rebuilt_frames as u64, count);
        assert_eq!(state.index.entries.len() as u64, count);
    }
}

#[tokio::test]
async fn oversized_checkpoint_preserves_live_coverage_and_the_last_complete_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("oversized.log");
    let stage = StageId::new();
    let template = event(stage, 1);
    let quiet = subject(&template, 0);
    let journal = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
    journal
        .append(quiet.clone(), Default::default())
        .await
        .unwrap();
    assert_ready(journal.observation_reader().unwrap(), &quiet, 1, 0).await;
    let previous = std::fs::read(checkpoint_path(&path)).unwrap();
    let suffix = "x".repeat(2048);
    let mut last = quiet.clone();
    for index in 1..=FORMER_KEY_LIMIT {
        last = subject_event(
            &template,
            format!("effect-{index}-{suffix}"),
            index as u64 + 1,
        );
        journal
            .append(last.clone(), Default::default())
            .await
            .unwrap();
    }
    let count = FORMER_KEY_LIMIT as u64 + 1;
    let reader = DiskObservationReader::<ChainEvent>::new(path.clone(), true);
    assert_ready(&reader, &last, count, count - 1).await;
    assert_ready(&reader, &quiet, count, 0).await;
    {
        let state = reader.shared.state.lock().unwrap();
        assert_eq!(state.index.entries.len() as u64, count);
        assert_eq!(state.index.examined_through, count);
        assert!(state.checkpointed < state.offset);
        assert!(write_checkpoint(&path, &state)
            .unwrap_err()
            .to_string()
            .contains("checkpoint capacity reached"));
    }
    assert_eq!(std::fs::read(checkpoint_path(&path)).unwrap(), previous);
    let saved = load_checkpoint(&path, std::fs::metadata(&path).unwrap().len()).unwrap();
    assert_eq!(saved.index.examined_through, 1);
    assert_eq!(saved.index.entries.len(), 1);
    drop(reader);
    drop(journal);

    // The old complete prefix and a missing checkpoint both recover every key.
    for keep_checkpoint in [true, false] {
        if !keep_checkpoint {
            std::fs::remove_file(checkpoint_path(&path)).unwrap();
        }
        let reader = DiskObservationReader::<ChainEvent>::open(path.clone()).unwrap();
        rebuild_to_ready(&reader, &last, count, count - 1).await;
        assert_ready(&reader, &quiet, count, 0).await;
        let state = reader.shared.state.lock().unwrap();
        assert_eq!(state.index.entries.len() as u64, count);
        assert_eq!(
            state.rebuilt_frames as u64,
            count - u64::from(keep_checkpoint)
        );
    }
}

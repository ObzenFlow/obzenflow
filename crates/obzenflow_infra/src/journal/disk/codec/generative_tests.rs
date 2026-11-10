// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::identity::{
    fixture_record, write_fixture_identity, CommitmentAdmission, JournalIdentity,
};
use super::super::log_record::{serialize_atomic_group, serialize_record};
use super::super::replay_archive::{verify_source_prefix, DiskReplayArchive};
use super::super::scanner::{
    classify_frame, damage_path, dispose, read_frame_sync, tail_after, Disposition,
    InterruptedAppend, ReadPolicy,
};
use super::{frame, Decoder};
use crate::journal::disk::DiskJournal;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::provenance::JournalGroupMember;
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::journal::TailKind;
use obzenflow_core::{FlowId, JournalId, JournalOwner, StageId, WriterId};
use obzenflow_runtime::runtime_config::JournalSync;
use proptest::prelude::*;
use std::path::Path;

const CASES: u32 = 192;
const BLOCK: usize = 4096;

#[derive(Debug, Clone)]
struct FrameShape {
    members: usize,
    payload_bytes: usize,
}

#[derive(Debug, Clone, Copy)]
enum Position {
    Boundary(usize),
    Fraction(u32),
}

#[derive(Debug, Clone)]
enum Mutation {
    Flip { at: Position, mask: u8 },
    Truncate { at: Position },
    ZeroTail { at: Position, extra: usize },
    RandomTail { at: Position, bytes: Vec<u8> },
    SwapBlocks { first: usize, second: usize },
}

fn frame_shape() -> impl Strategy<Value = FrameShape> {
    (
        prop_oneof![3 => Just(1usize), 1 => 2usize..=4],
        prop_oneof![3 => 0usize..400, 1 => 400usize..6000],
    )
        .prop_map(|(members, payload_bytes)| FrameShape {
            members,
            payload_bytes,
        })
}

fn position() -> impl Strategy<Value = Position> {
    prop_oneof![
        (0usize..16).prop_map(Position::Boundary),
        (0u32..=1_000_000).prop_map(Position::Fraction),
    ]
}

fn mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        (position(), 1u8..=255).prop_map(|(at, mask)| Mutation::Flip { at, mask }),
        position().prop_map(|at| Mutation::Truncate { at }),
        (position(), 0usize..=BLOCK).prop_map(|(at, extra)| Mutation::ZeroTail { at, extra }),
        (
            position(),
            proptest::collection::vec(any::<u8>(), 1..=2 * BLOCK)
        )
            .prop_map(|(at, bytes)| Mutation::RandomTail { at, bytes }),
        (0usize..8, 0usize..8).prop_map(|(first, second)| Mutation::SwapBlocks { first, second }),
    ]
}

fn encode(identity: JournalIdentity, shapes: &[FrameShape]) -> Vec<Vec<u8>> {
    let writer = WriterId::from(StageId::new());
    let mut previous = None;
    shapes
        .iter()
        .enumerate()
        .map(|(index, shape)| {
            let group = format!("generated-{index}");
            let records: Vec<_> = (0..shape.members)
                .map(|member| {
                    let event = ChainEventFactory::data_event(
                        writer,
                        "generated",
                        std::num::NonZeroU32::MIN,
                        serde_json::json!({
                            "frame": index,
                            "member": member,
                            "body": "x".repeat(shape.payload_bytes),
                        }),
                    );
                    let mut record = fixture_record(identity, event, &mut previous);
                    if shape.members > 1 {
                        record.envelope.provenance.journal.journal_group_id = Some(group.clone());
                        record.envelope.provenance.journal.journal_group_member =
                            Some(JournalGroupMember {
                                index: member as u32,
                                size: shape.members as u32,
                            });
                    }
                    record
                })
                .collect();
            if shape.members > 1 {
                serialize_atomic_group(&group, &records).unwrap()
            } else {
                serialize_record(&records[0]).unwrap()
            }
        })
        .collect()
}

fn mutate(original: &[u8], boundaries: &[usize], mutation: &Mutation) -> Vec<u8> {
    let resolve = |at: Position, bound: usize| match at {
        Position::Boundary(index) => boundaries[index % boundaries.len()].min(bound),
        Position::Fraction(parts) => (bound as u64 * u64::from(parts) / 1_000_000) as usize,
    };
    let len = original.len();
    match mutation {
        Mutation::Flip { at, mask } => {
            let mut bytes = original.to_vec();
            bytes[resolve(*at, len - 1)] ^= mask;
            bytes
        }
        Mutation::Truncate { at } => original[..resolve(*at, len)].to_vec(),
        Mutation::ZeroTail { at, extra } => {
            let mut bytes = original[..resolve(*at, len)].to_vec();
            bytes.resize(len + extra, 0);
            bytes
        }
        Mutation::RandomTail { at, bytes } => [&original[..resolve(*at, len)], bytes].concat(),
        Mutation::SwapBlocks { first, second } => {
            let mut bytes = original.to_vec();
            let blocks = len / BLOCK;
            if blocks >= 2 {
                let (low, high) = {
                    let (a, b) = (first % blocks, second % blocks);
                    (a.min(b), a.max(b))
                };
                if low != high {
                    let (head, tail) = bytes.split_at_mut(high * BLOCK);
                    head[low * BLOCK..(low + 1) * BLOCK].swap_with_slice(&mut tail[..BLOCK]);
                }
            }
            bytes
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expected {
    Clean { frames: usize },
    Interrupted { committed: usize, start: usize },
    Refused { committed: usize },
}

fn validates(bytes: &[u8]) -> bool {
    frame::frame_length(bytes)
        .ok()
        .and_then(|length| bytes.get(..length))
        .is_some_and(|candidate| frame::validate(candidate).is_ok())
}

fn expected(frames: &[Vec<u8>], mutated: &[u8]) -> Expected {
    let mut start = 0;
    let mut committed = 0;
    for written in frames {
        if mutated.get(start..start + written.len()) != Some(written.as_slice()) {
            break;
        }
        start += written.len();
        committed += 1;
    }
    if mutated.len() == start {
        return Expected::Clean { frames: committed };
    }
    let rest = &mutated[start..];
    if validates(rest) || frame::frame_length(rest) == Err(frame::FrameProblem::SchemaMismatch) {
        return Expected::Refused { committed };
    }
    let search_from = match frame::frame_length(rest) {
        Ok(length) => start.saturating_add(length),
        Err(_) => start + 1,
    };
    if (search_from..mutated.len()).any(|at| validates(&mutated[at..])) {
        Expected::Refused { committed }
    } else {
        Expected::Interrupted { committed, start }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum End {
    Clean,
    Skipped,
    Interrupted(InterruptedAppend),
    Refused,
}

fn read(path: &Path, policy: ReadPolicy) -> (Vec<Vec<u8>>, End) {
    let mut reader = std::io::BufReader::new(std::fs::File::open(path).unwrap());
    let mut admission = CommitmentAdmission::open(path).unwrap();
    let mut decoder = Decoder::new(path);
    let mut buf = Vec::new();
    let mut offset = 0u64;
    let mut frames = Vec::new();
    while let Some((consumed, termination)) = read_frame_sync(&mut reader, &mut buf).unwrap() {
        let at = offset;
        offset += consumed as u64;
        let end = match dispose(
            classify_frame::<ChainEvent>(&buf, &mut decoder, at),
            termination,
            policy,
            || tail_after(path, &buf, at, &mut reader),
        ) {
            Disposition::Yield(frame) => {
                let admitted = frame
                    .into_records()
                    .iter()
                    .all(|record| admission.admit(record).is_ok());
                if !admitted {
                    End::Refused
                } else {
                    frames.push(buf.clone());
                    continue;
                }
            }
            Disposition::Interrupted(append) => End::Interrupted(append),
            Disposition::Skip => End::Skipped,
            Disposition::EndOfCommittedRecords => End::Clean,
            Disposition::Corrupt(_) => End::Refused,
        };
        return (frames, end);
    }
    (frames, End::Clean)
}

const TOLERANT: ReadPolicy = ReadPolicy::SealedScan {
    tolerate_torn_tail: true,
};
const STRICT: ReadPolicy = ReadPolicy::SealedScan {
    tolerate_torn_tail: false,
};

proptest! {
    #![proptest_config(ProptestConfig {
        cases: CASES,
        ..ProptestConfig::default()
    })]

    #[test]
    fn mutated_journal_reads_written_prefix(
        shapes in proptest::collection::vec(frame_shape(), 1..=8),
        mutation in mutation(),
    ) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("generated.log");
        let identity = JournalIdentity {
            run_id: FlowId::new(),
            journal_id: JournalId::new(),
        };
        let frames = encode(identity, &shapes);
        let original = frames.concat();
        let boundaries: Vec<usize> = std::iter::once(0)
            .chain(frames.iter().scan(0, |end, written| {
                *end += written.len();
                Some(*end)
            }))
            .collect();
        let mutated = mutate(&original, &boundaries, &mutation);
        write_fixture_identity(&path, identity);
        std::fs::write(&path, &mutated).unwrap();
        let expected = expected(&frames, &mutated);

        for policy in [ReadPolicy::LiveTail, STRICT, TOLERANT] {
            let (read_frames, end) = read(&path, policy);
            for (index, read_frame) in read_frames.iter().enumerate() {
                prop_assert_eq!(read_frame, &frames[index], "frame {} under {:?}", index, policy);
            }
            let count = match expected {
                Expected::Clean { frames } => frames,
                Expected::Interrupted { committed, .. } | Expected::Refused { committed } => {
                    committed
                }
            };
            prop_assert_eq!(read_frames.len(), count, "{:?} under {:?}", expected, policy);
            match (expected, policy) {
                (Expected::Clean { .. }, _) => prop_assert_eq!(end, End::Clean),
                (Expected::Refused { .. }, _) => prop_assert_eq!(end, End::Refused),
                (Expected::Interrupted { start, .. }, TOLERANT) => prop_assert_eq!(
                    end,
                    End::Interrupted(InterruptedAppend {
                        committed_end: start as u64,
                        left_bytes: (mutated.len() - start) as u64,
                        kind: if mutated[start..].iter().all(|byte| *byte == 0) {
                            TailKind::ZeroFilled
                        } else {
                            TailKind::TornFrame
                        },
                    })
                ),
                (Expected::Interrupted { .. }, STRICT) => prop_assert_eq!(end, End::Refused),
                (Expected::Interrupted { .. }, _) => {
                    prop_assert!(matches!(end, End::Skipped | End::Refused), "{:?}", end)
                }
            }
        }

        let opened = DiskJournal::<ChainEvent>::with_owner(
            path.clone(),
            JournalOwner::stage(StageId::new()),
            JournalSync::Off,
        );
        let damage_files = || {
            std::fs::read_dir(directory.path())
                .unwrap()
                .filter(|entry| {
                    entry
                        .as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .ends_with(".damage")
                })
                .count()
        };
        match expected {
            Expected::Clean { .. } => {
                prop_assert!(opened.is_ok(), "{:?}", opened.err());
                prop_assert_eq!(std::fs::read(&path).unwrap(), mutated);
                prop_assert_eq!(damage_files(), 0);
            }
            Expected::Interrupted { start, .. } => {
                prop_assert!(opened.is_ok(), "{:?}", opened.err());
                prop_assert_eq!(std::fs::read(&path).unwrap(), &mutated[..start]);
                prop_assert_eq!(
                    std::fs::read(damage_path(&path, start as u64)).unwrap(),
                    &mutated[start..]
                );
                prop_assert_eq!(damage_files(), 1);
            }
            Expected::Refused { .. } => {
                prop_assert!(opened.is_err());
                prop_assert_eq!(std::fs::read(&path).unwrap(), mutated);
                prop_assert_eq!(damage_files(), 0);
            }
        }
    }
}

#[derive(Debug, Clone)]
enum History {
    Same,
    Prefix(usize),
    Extension(usize),
    Changed(usize),
    Removed(usize),
    Swapped(usize),
}

fn history() -> impl Strategy<Value = History> {
    prop_oneof![
        Just(History::Same),
        (0usize..8).prop_map(History::Prefix),
        (1usize..4).prop_map(History::Extension),
        (0usize..8).prop_map(History::Changed),
        (0usize..8).prop_map(History::Removed),
        (0usize..8).prop_map(History::Swapped),
    ]
}

fn source_event() -> ChainEvent {
    ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "source.row",
        std::num::NonZeroU32::MIN,
        serde_json::json!({}),
    )
}

fn derive(first: &[ChainEvent], history: &History) -> Vec<ChainEvent> {
    let mut second = first.to_vec();
    let len = first.len();
    match *history {
        History::Same => {}
        History::Prefix(keep) if len > 0 => second.truncate(keep % len),
        History::Extension(added) => second.extend((0..added).map(|_| source_event())),
        History::Changed(index) if len > 0 => second[index % len] = source_event(),
        History::Removed(index) if len > 0 => {
            second.remove(index % len);
        }
        History::Swapped(index) if len > 1 => second.swap(index % (len - 1), index % (len - 1) + 1),
        _ => {}
    }
    second
}

fn source_archive(directory: &Path, events: &[ChainEvent]) -> DiskReplayArchive {
    std::fs::create_dir_all(directory).unwrap();
    let path = directory.join("source.log");
    let identity = JournalIdentity {
        run_id: FlowId::new(),
        journal_id: JournalId::new(),
    };
    write_fixture_identity(&path, identity);
    let mut previous = None;
    let bytes: Vec<u8> = events
        .iter()
        .flat_map(|event| {
            serialize_record(&fixture_record(identity, event.clone(), &mut previous)).unwrap()
        })
        .collect();
    std::fs::write(&path, bytes).unwrap();
    DiskReplayArchive::for_source_journal(
        directory.to_path_buf(),
        "source.log",
        events.len() as u64,
    )
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: CASES,
        ..ProptestConfig::default()
    })]

    #[test]
    fn resume_prefix_accepts_only_prefixes(
        len in 0usize..8,
        history in history(),
    ) {
        let first: Vec<ChainEvent> = (0..len).map(|_| source_event()).collect();
        let second = derive(&first, &history);
        let directory = tempfile::tempdir().unwrap();
        let ancestor = source_archive(&directory.path().join("ancestor"), &first);
        let head = source_archive(&directory.path().join("head"), &second);
        let (shorter, longest, shorter_events, longest_events) = if second.len() > first.len() {
            (&ancestor, &head, &first, &second)
        } else {
            (&head, &ancestor, &second, &first)
        };
        let prefix = shorter_events
            .iter()
            .zip(longest_events.iter())
            .all(|(left, right)| left.id == right.id);
        let verdict = verify_source_prefix(
            shorter,
            longest,
            "src",
            &directory.path().join("head"),
        );
        if prefix {
            prop_assert!(verdict.is_ok(), "{:?}", verdict.err());
        } else {
            prop_assert!(
                matches!(verdict, Err(ReplayError::AncestorDisagrees { .. })),
                "{:?}",
                verdict
            );
        }
    }
}

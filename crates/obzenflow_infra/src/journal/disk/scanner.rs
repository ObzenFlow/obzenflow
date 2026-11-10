// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One current-schema classifier and policy arbiter for every journal read surface.
//! Binary lengths delimit frames; a checked fixed trailer commits the frame.

use super::codec::{frame, Decoder};
use super::log_record::LogFrame;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::journal::TailKind;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameTermination {
    Committed,
    Incomplete,
}

pub(crate) enum ParseOutcome<R: JournalEvent> {
    Complete(Box<LogFrame<R>>),
    Incomplete(ParseProblem),
    Damaged(ParseProblem),
    Corrupt(ParseProblem),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParseProblem {
    Invalid(String),
    SchemaMismatch,
    FrameFollows {
        problem: String,
        offset: u64,
        next: u64,
        damage_file: String,
    },
}
impl std::fmt::Display for ParseProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => f.write_str(message),
            Self::SchemaMismatch => write!(
                f,
                "journal schema marker mismatch (supported: {}); re-record the archive",
                obzenflow_core::journal::JOURNAL_SCHEMA_VERSION
            ),
            Self::FrameFollows {
                problem,
                offset,
                next,
                damage_file,
            } => write!(
                f,
                "{problem} at offset {offset}, and a valid frame follows at offset {next}, so the \
                 damage is not the append that was in flight; restore the file from a copy, or \
                 move the bytes after offset {offset} into {damage_file} to resume without the \
                 facts after it; any effect they record may run again"
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadPolicy {
    LiveTail,
    SealedScan { tolerate_torn_tail: bool },
}

pub(crate) enum Disposition<R: JournalEvent> {
    Yield(Box<LogFrame<R>>),
    Skip,
    EndOfCommittedRecords,
    Interrupted(InterruptedAppend),
    Corrupt(ParseProblem),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct InterruptedAppend {
    pub(crate) committed_end: u64,
    pub(crate) left_bytes: u64,
    pub(crate) kind: TailKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Tail {
    Interrupted(InterruptedAppend),
    FrameFollows {
        offset: u64,
        next: u64,
        damage_file: String,
    },
}

/// The decoder only runs after the whole carrier passes framing and CRC checks.
/// Definitions are resolved directly from committed frames in the same archive.
pub(crate) fn classify_frame<R: JournalEvent>(
    bytes: &[u8],
    decoder: &mut Decoder,
    offset: u64,
) -> ParseOutcome<R> {
    let body = match frame::validate(bytes) {
        Ok(body) => body,
        Err(frame::FrameProblem::Incomplete(message)) => {
            return ParseOutcome::Incomplete(ParseProblem::Invalid(message))
        }
        Err(frame::FrameProblem::SchemaMismatch) => {
            return ParseOutcome::Corrupt(ParseProblem::SchemaMismatch)
        }
        Err(frame::FrameProblem::Corrupt(message)) => {
            return ParseOutcome::Damaged(ParseProblem::Invalid(message))
        }
    };
    match decoder.decode::<R>(body, offset) {
        Ok(record) => ParseOutcome::Complete(Box::new(record)),
        Err(error) => ParseOutcome::Corrupt(ParseProblem::Invalid(error.to_string())),
    }
}

pub(crate) fn dispose<R: JournalEvent>(
    outcome: ParseOutcome<R>,
    termination: FrameTermination,
    policy: ReadPolicy,
    tail: impl FnOnce() -> io::Result<Tail>,
) -> Disposition<R> {
    let (problem, cut_short) = match outcome {
        ParseOutcome::Corrupt(problem) => return Disposition::Corrupt(problem),
        ParseOutcome::Complete(frame) => match termination {
            FrameTermination::Committed => return Disposition::Yield(frame),
            FrameTermination::Incomplete => {
                (ParseProblem::Invalid("missing commit trailer".into()), true)
            }
        },
        ParseOutcome::Incomplete(problem) => (problem, termination == FrameTermination::Incomplete),
        ParseOutcome::Damaged(problem) => (problem, false),
    };
    match policy {
        ReadPolicy::LiveTail if cut_short => Disposition::Skip,
        ReadPolicy::LiveTail
        | ReadPolicy::SealedScan {
            tolerate_torn_tail: false,
        } => Disposition::Corrupt(problem),
        ReadPolicy::SealedScan {
            tolerate_torn_tail: true,
        } => match tail() {
            Ok(Tail::Interrupted(append)) => Disposition::Interrupted(append),
            Ok(Tail::FrameFollows {
                offset,
                next,
                damage_file,
            }) => Disposition::Corrupt(ParseProblem::FrameFollows {
                problem: problem.to_string(),
                offset,
                next,
                damage_file,
            }),
            Err(error) => Disposition::Corrupt(ParseProblem::Invalid(format!(
                "{problem}; the journal after it could not be read: {error}"
            ))),
        },
    }
}

pub(crate) fn unscanned_tail() -> io::Result<Tail> {
    Err(io::Error::other(
        "this read does not look past a damaged frame",
    ))
}

pub(crate) fn damage_path(journal: &Path, committed_end: u64) -> PathBuf {
    journal.with_file_name(damage_file_name(journal, committed_end))
}

fn damage_file_name(journal: &Path, committed_end: u64) -> String {
    format!(
        "{}.{committed_end}.damage",
        journal
            .file_name()
            .map_or_else(|| journal.to_string_lossy(), |name| name.to_string_lossy())
    )
}

const TAIL_CHUNK_BYTES: usize = 64 * 1024;

pub(crate) fn tail_after(
    journal: &Path,
    frame: &[u8],
    offset: u64,
    rest: &mut dyn Read,
) -> io::Result<Tail> {
    let mut scan = TailScan {
        rest,
        window: frame.to_vec(),
        window_offset: offset,
        left_bytes: frame.len() as u64,
        zeros: frame.iter().all(|byte| *byte == 0),
        ended: false,
    };
    let mut at = match frame::frame_length(frame) {
        Ok(length) => offset.saturating_add(length as u64),
        Err(_) => offset + 1,
    };
    while let Some(candidate) = scan.next_marker(at)? {
        if scan.validates_at(candidate)? {
            return Ok(Tail::FrameFollows {
                offset,
                next: candidate,
                damage_file: damage_file_name(journal, offset),
            });
        }
        at = candidate + 1;
    }
    Ok(Tail::Interrupted(InterruptedAppend {
        committed_end: offset,
        left_bytes: scan.left_bytes,
        kind: if scan.zeros {
            TailKind::ZeroFilled
        } else {
            TailKind::TornFrame
        },
    }))
}

pub(crate) fn interrupted_append(journal: &Path) -> io::Result<Option<InterruptedAppend>> {
    let mut reader = io::BufReader::new(std::fs::File::open(journal)?);
    let mut buf = Vec::new();
    let mut offset = 0u64;
    while let Some((consumed, _)) = read_frame_sync(&mut reader, &mut buf)? {
        match frame::validate(&buf) {
            Ok(_) => offset += consumed as u64,
            Err(frame::FrameProblem::SchemaMismatch) => return Ok(None),
            Err(_) => {
                return Ok(match tail_after(journal, &buf, offset, &mut reader)? {
                    Tail::Interrupted(append) => Some(append),
                    Tail::FrameFollows { .. } => None,
                })
            }
        }
    }
    Ok(None)
}

struct TailScan<'a> {
    rest: &'a mut dyn Read,
    window: Vec<u8>,
    window_offset: u64,
    left_bytes: u64,
    zeros: bool,
    ended: bool,
}

impl TailScan<'_> {
    fn fill(&mut self) -> io::Result<bool> {
        if self.ended {
            return Ok(false);
        }
        let start = self.window.len();
        self.window.resize(start + TAIL_CHUNK_BYTES, 0);
        let read = loop {
            match self.rest.read(&mut self.window[start..]) {
                Ok(read) => break read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    self.window.truncate(start);
                    return Err(error);
                }
            }
        };
        self.window.truncate(start + read);
        self.left_bytes += read as u64;
        self.zeros &= self.window[start..].iter().all(|byte| *byte == 0);
        self.ended = read == 0;
        Ok(!self.ended)
    }

    fn window_end(&self) -> u64 {
        self.window_offset + self.window.len() as u64
    }

    fn start_at(&mut self, at: u64) -> io::Result<bool> {
        while at > self.window_end() {
            self.window_offset = self.window_end();
            self.window.clear();
            if !self.fill()? {
                return Ok(false);
            }
        }
        let skip = (at - self.window_offset) as usize;
        self.window.drain(..skip);
        self.window_offset = at;
        Ok(true)
    }

    fn hold_until(&mut self, end: u64) -> io::Result<bool> {
        while self.window_end() < end {
            if !self.fill()? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn next_marker(&mut self, mut at: u64) -> io::Result<Option<u64>> {
        loop {
            if !self.start_at(at)? {
                return Ok(None);
            }
            if let Some(index) = self
                .window
                .windows(frame::MAGIC.len())
                .position(|candidate| candidate == frame::MAGIC)
            {
                return Ok(Some(self.window_offset + index as u64));
            }
            at = self.window_offset
                + self.window.len().saturating_sub(frame::MAGIC.len() - 1) as u64;
            if !self.fill()? {
                return Ok(None);
            }
        }
    }

    fn validates_at(&mut self, at: u64) -> io::Result<bool> {
        if !self.hold_until(at + frame::HEADER_LEN as u64)? {
            return Ok(false);
        }
        let start = (at - self.window_offset) as usize;
        let Ok(length) = frame::frame_length(&self.window[start..start + frame::HEADER_LEN]) else {
            return Ok(false);
        };
        if !self.hold_until(at + length as u64)? {
            return Ok(false);
        }
        Ok(frame::validate(&self.window[start..start + length]).is_ok())
    }
}

/// Read incrementally rather than allocating the untrusted declared length.
/// Invalid headers are handed to the classifier without following their length.
pub(crate) fn read_frame_sync<B: std::io::BufRead>(
    reader: &mut B,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<(usize, FrameTermination)>> {
    buf.clear();
    reader.take(frame::HEADER_LEN as u64).read_to_end(buf)?;
    if buf.is_empty() {
        return Ok(None);
    }
    if let Ok(length) = frame::frame_length(buf) {
        reader
            .take((length - frame::HEADER_LEN) as u64)
            .read_to_end(buf)?;
        return Ok(Some((
            buf.len(),
            if buf.len() == length {
                FrameTermination::Committed
            } else {
                FrameTermination::Incomplete
            },
        )));
    }
    Ok(Some((buf.len(), FrameTermination::Incomplete)))
}

pub(crate) async fn read_frame_async<B: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut B,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<(usize, FrameTermination)>> {
    use tokio::io::AsyncReadExt;
    buf.clear();
    reader
        .take(frame::HEADER_LEN as u64)
        .read_to_end(buf)
        .await?;
    if buf.is_empty() {
        return Ok(None);
    }
    if let Ok(length) = frame::frame_length(buf) {
        reader
            .take((length - frame::HEADER_LEN) as u64)
            .read_to_end(buf)
            .await?;
        return Ok(Some((
            buf.len(),
            if buf.len() == length {
                FrameTermination::Committed
            } else {
                FrameTermination::Incomplete
            },
        )));
    }
    Ok(Some((buf.len(), FrameTermination::Incomplete)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::disk::log_record::{serialize_record, LogRecord};
    use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
    use obzenflow_core::event::JournalRecord;
    use obzenflow_core::{JournalWriterId, StageId, WriterId};
    use serde_json::json;
    use std::io::Cursor;

    fn record() -> LogRecord<ChainEvent> {
        record_with(json!({ "k": "v\n\u{0}🙂" }))
    }
    fn record_with(payload: serde_json::Value) -> LogRecord<ChainEvent> {
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            std::num::NonZeroU32::MIN,
            payload,
        );
        JournalRecord::new(JournalWriterId::new(), event)
    }
    fn classify(bytes: &[u8]) -> ParseOutcome<ChainEvent> {
        classify_frame(bytes, &mut Decoder::new(Path::new("fixture.log")), 0)
    }
    fn nothing_after(bytes: &[u8]) -> impl FnOnce() -> io::Result<Tail> + '_ {
        move || tail_after(Path::new("fixture.log"), bytes, 0, &mut io::empty())
    }
    const TOLERANT: ReadPolicy = ReadPolicy::SealedScan {
        tolerate_torn_tail: true,
    };
    const STRICT: ReadPolicy = ReadPolicy::SealedScan {
        tolerate_torn_tail: false,
    };

    fn scan(bytes: &[u8], policy: ReadPolicy) -> (usize, Disposition<ChainEvent>) {
        let path = Path::new("fixture.log");
        let mut reader = Cursor::new(bytes);
        let mut buf = Vec::new();
        let mut offset = 0u64;
        let mut decoder = Decoder::new(path);
        let mut yielded = 0;
        while let Some((consumed, termination)) = read_frame_sync(&mut reader, &mut buf).unwrap() {
            let at = offset;
            offset += consumed as u64;
            match dispose(
                classify_frame(&buf, &mut decoder, at),
                termination,
                policy,
                || tail_after(path, &buf, at, &mut reader),
            ) {
                Disposition::Yield(_) => yielded += 1,
                other => return (yielded, other),
            }
        }
        (yielded, Disposition::EndOfCommittedRecords)
    }

    fn journal_with_large_last_frame() -> (Vec<u8>, usize) {
        let mut bytes = serialize_record(&record()).unwrap();
        bytes.extend(serialize_record(&record()).unwrap());
        let committed = bytes.len();
        bytes
            .extend(serialize_record(&record_with(json!({ "body": "x".repeat(10_000) }))).unwrap());
        (bytes, committed)
    }

    fn next_block(offset: usize) -> usize {
        (offset / 4096 + 1) * 4096
    }

    fn interrupted(disposition: &Disposition<ChainEvent>) -> Option<InterruptedAppend> {
        match disposition {
            Disposition::Interrupted(append) => Some(*append),
            _ => None,
        }
    }

    #[test]
    fn complete_record_yields_under_every_policy() {
        let bytes = serialize_record(&record()).unwrap();
        for policy in [ReadPolicy::LiveTail, TOLERANT, STRICT] {
            assert!(matches!(
                dispose(
                    classify(&bytes),
                    FrameTermination::Committed,
                    policy,
                    unscanned_tail
                ),
                Disposition::Yield(_)
            ));
        }
    }

    #[test]
    fn every_truncated_header_body_and_trailer_obeys_tail_policy() {
        let bytes = serialize_record(&record()).unwrap();
        for length in 0..bytes.len() {
            let truncated = &bytes[..length];
            assert!(
                matches!(classify(truncated), ParseOutcome::Incomplete(_)),
                "length {length}"
            );
            assert!(matches!(
                dispose(
                    classify(truncated),
                    FrameTermination::Incomplete,
                    ReadPolicy::LiveTail,
                    unscanned_tail
                ),
                Disposition::Skip
            ));
            assert_eq!(
                interrupted(&dispose(
                    classify(truncated),
                    FrameTermination::Incomplete,
                    TOLERANT,
                    nothing_after(truncated)
                )),
                Some(InterruptedAppend {
                    committed_end: 0,
                    left_bytes: length as u64,
                    kind: if length == 0 {
                        TailKind::ZeroFilled
                    } else {
                        TailKind::TornFrame
                    },
                }),
                "length {length}"
            );
            assert!(matches!(
                dispose(
                    classify(truncated),
                    FrameTermination::Incomplete,
                    STRICT,
                    unscanned_tail
                ),
                Disposition::Corrupt(_)
            ));
            assert!(matches!(
                dispose(
                    classify(truncated),
                    FrameTermination::Committed,
                    ReadPolicy::LiveTail,
                    unscanned_tail
                ),
                Disposition::Corrupt(_)
            ));
        }
    }

    #[test]
    fn checksum_mismatch_without_tolerance_refused() {
        let bytes = serialize_record(&record()).unwrap();
        for index in 0..bytes.len() {
            let mut corrupt = bytes.clone();
            corrupt[index] ^= 1;
            for termination in [FrameTermination::Committed, FrameTermination::Incomplete] {
                for policy in [ReadPolicy::LiveTail, STRICT] {
                    assert!(
                        matches!(
                            dispose(classify(&corrupt), termination, policy, unscanned_tail),
                            Disposition::Corrupt(_)
                        ),
                        "byte {index}"
                    );
                }
            }
        }
    }

    #[test]
    fn older_formats_and_trailing_bytes_are_rejected() {
        for bytes in [
            serde_json::to_vec(&record()).unwrap(),
            b"123:456:{\"frame_kind\":\"record_v2\"}\n".to_vec(),
        ] {
            assert!(matches!(
                dispose(
                    classify(&bytes),
                    FrameTermination::Incomplete,
                    STRICT,
                    unscanned_tail
                ),
                Disposition::Corrupt(_)
            ));
        }
        let mut bytes = serialize_record(&record()).unwrap();
        bytes.push(0);
        assert!(matches!(classify(&bytes), ParseOutcome::Damaged(_)));
        assert!(matches!(
            classify(&frame::encode(b"invalid compact body")),
            ParseOutcome::Corrupt(_)
        ));
    }

    #[test]
    fn tolerant_scan_zero_tail_yields_prefix() {
        let mut bytes = serialize_record(&record()).unwrap();
        bytes.extend(serialize_record(&record()).unwrap());
        let committed = bytes.len() as u64;
        bytes.extend([0; 4096]);
        let (yielded, end) = scan(&bytes, TOLERANT);
        assert_eq!(yielded, 2);
        assert_eq!(
            interrupted(&end),
            Some(InterruptedAppend {
                committed_end: committed,
                left_bytes: 4096,
                kind: TailKind::ZeroFilled,
            })
        );
    }

    #[test]
    fn tolerant_scan_partial_frame_yields_prefix() {
        let (mut bytes, committed) = journal_with_large_last_frame();
        let landed = next_block(committed + frame::HEADER_LEN);
        bytes[landed..].fill(0);
        let (yielded, end) = scan(&bytes, TOLERANT);
        assert_eq!(yielded, 2);
        assert_eq!(
            interrupted(&end),
            Some(InterruptedAppend {
                committed_end: committed as u64,
                left_bytes: (bytes.len() - committed) as u64,
                kind: TailKind::TornFrame,
            })
        );
    }

    #[test]
    fn tolerant_scan_headless_frame_yields_prefix() {
        let (mut bytes, committed) = journal_with_large_last_frame();
        bytes[committed..next_block(committed)].fill(0);
        let (yielded, end) = scan(&bytes, TOLERANT);
        assert_eq!(yielded, 2);
        assert_eq!(
            interrupted(&end).map(|append| (append.committed_end, append.kind)),
            Some((committed as u64, TailKind::TornFrame))
        );
    }

    #[test]
    fn tolerant_scan_torn_marker_yields_prefix() {
        let mut bytes = serialize_record(&record()).unwrap();
        let committed = bytes.len() as u64;
        bytes.extend_from_slice(&frame::MAGIC[..5]);
        let (yielded, end) = scan(&bytes, TOLERANT);
        assert_eq!(yielded, 1);
        assert_eq!(
            interrupted(&end),
            Some(InterruptedAppend {
                committed_end: committed,
                left_bytes: 5,
                kind: TailKind::TornFrame,
            })
        );
    }

    #[test]
    fn tolerant_scan_damage_before_frame_refused() {
        let first = serialize_record(&record()).unwrap();
        let mut second = serialize_record(&record()).unwrap();
        let third = serialize_record(&record()).unwrap();
        let body = frame::HEADER_LEN + 2;
        second[body] ^= 0xff;
        let bytes = [first.clone(), second.clone(), third].concat();
        let (yielded, end) = scan(&bytes, TOLERANT);
        assert_eq!(yielded, 1);
        let Disposition::Corrupt(problem) = end else {
            panic!("damage that a valid frame follows must be refused");
        };
        assert_eq!(
            problem,
            ParseProblem::FrameFollows {
                problem: "frame body checksum mismatch".into(),
                offset: first.len() as u64,
                next: (first.len() + second.len()) as u64,
                damage_file: format!("fixture.log.{}.damage", first.len()),
            }
        );
        assert_eq!(
            problem.to_string(),
            format!(
                "frame body checksum mismatch at offset {0}, and a valid frame follows at offset \
                 {1}, so the damage is not the append that was in flight; restore the file from \
                 a copy, or move the bytes after offset {0} into fixture.log.{0}.damage to resume \
                 without the facts after it; any effect they record may run again",
                first.len(),
                first.len() + second.len()
            )
        );
    }

    #[test]
    fn tolerant_scan_other_version_refused() {
        let mut bytes = serialize_record(&record()).unwrap();
        let mut other = serialize_record(&record()).unwrap();
        other[3..7].copy_from_slice(b"21.0");
        bytes.extend(other);
        let (yielded, end) = scan(&bytes, TOLERANT);
        assert_eq!(yielded, 1);
        assert!(matches!(
            end,
            Disposition::Corrupt(ParseProblem::SchemaMismatch)
        ));
    }

    #[test]
    fn strict_scan_partial_frame_refused() {
        let (mut bytes, _) = journal_with_large_last_frame();
        bytes.truncate(bytes.len() - 100);
        let (yielded, end) = scan(&bytes, STRICT);
        assert_eq!(yielded, 2);
        assert!(matches!(end, Disposition::Corrupt(_)));
        let (yielded, end) = scan(&bytes, TOLERANT);
        assert_eq!(yielded, 2);
        assert!(matches!(end, Disposition::Interrupted(_)));
    }

    #[test]
    fn tolerant_scan_unreadable_tail_refused() {
        struct Failing;
        impl Read for Failing {
            fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
                Err(io::Error::other("device gone"))
            }
        }
        let bytes = [0u8; 16];
        assert!(matches!(
            dispose::<ChainEvent>(
                classify(&bytes),
                FrameTermination::Incomplete,
                TOLERANT,
                || tail_after(Path::new("fixture.log"), &bytes, 0, &mut Failing)
            ),
            Disposition::Corrupt(ParseProblem::Invalid(_))
        ));
        assert!(matches!(
            dispose::<ChainEvent>(
                classify(&bytes),
                FrameTermination::Incomplete,
                TOLERANT,
                unscanned_tail
            ),
            Disposition::Corrupt(_)
        ));
    }

    #[test]
    fn interrupted_append_zero_tail_returns_append() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("system.log");
        let mut bytes = serialize_record(&record()).unwrap();
        let committed = bytes.len() as u64;
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(interrupted_append(&path).unwrap(), None);
        bytes.extend([0; 100]);
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            interrupted_append(&path).unwrap(),
            Some(InterruptedAppend {
                committed_end: committed,
                left_bytes: 100,
                kind: TailKind::ZeroFilled,
            })
        );
        bytes.extend(serialize_record(&record()).unwrap());
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(interrupted_append(&path).unwrap(), None);
        assert_eq!(
            damage_path(&path, committed),
            directory
                .path()
                .join(format!("system.log.{committed}.damage"))
        );
    }

    #[tokio::test]
    async fn sync_and_async_reads_agree_on_binary_boundaries_and_partial_tail() {
        let complete = serialize_record(&record()).unwrap();
        for tail_length in [0, 2, frame::HEADER_LEN, complete.len() - 1, complete.len()] {
            let mut bytes = complete.clone();
            bytes.extend_from_slice(&complete[..tail_length]);
            let mut sync = Cursor::new(bytes.clone());
            let mut asynchronous = tokio::io::BufReader::new(Cursor::new(bytes));
            let mut a = Vec::new();
            let mut b = Vec::new();
            loop {
                let left = read_frame_sync(&mut sync, &mut a).unwrap();
                let right = read_frame_async(&mut asynchronous, &mut b).await.unwrap();
                assert_eq!(left, right);
                assert_eq!(a, b);
                match left {
                    None => break,
                    Some((length, FrameTermination::Committed)) => {
                        assert_eq!(length, complete.len());
                        assert!(matches!(classify(&a), ParseOutcome::Complete(_)));
                    }
                    Some((length, FrameTermination::Incomplete)) => assert_eq!(length, tail_length),
                }
            }
        }
    }
}

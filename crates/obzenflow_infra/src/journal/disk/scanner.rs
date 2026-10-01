// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One current-schema classifier and policy arbiter for every journal read surface.
//! Binary lengths delimit frames; a checked fixed trailer commits the frame.

use super::codec::{frame, Decoder};
use super::log_record::LogFrame;
use obzenflow_core::event::JournalEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameTermination {
    Committed,
    Incomplete,
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum ParseOutcome<R: JournalEvent> {
    Complete(LogFrame<R>),
    Incomplete(ParseProblem),
    Corrupt(ParseProblem),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParseProblem {
    Invalid(String),
    SchemaMismatch,
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
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadPolicy {
    LiveTail,
    SealedScan { tolerate_torn_tail: bool },
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum Disposition<R: JournalEvent> {
    Yield(LogFrame<R>),
    Skip,
    EndOfCommittedRecords,
    Corrupt(ParseProblem),
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
            return ParseOutcome::Corrupt(ParseProblem::Invalid(message))
        }
    };
    match decoder.decode::<R>(body, offset) {
        Ok(record) => ParseOutcome::Complete(record),
        Err(error) => ParseOutcome::Corrupt(ParseProblem::Invalid(error.to_string())),
    }
}

pub(crate) fn dispose<R: JournalEvent>(
    outcome: ParseOutcome<R>,
    termination: FrameTermination,
    policy: ReadPolicy,
) -> Disposition<R> {
    let problem = match outcome {
        ParseOutcome::Corrupt(problem) => return Disposition::Corrupt(problem),
        ParseOutcome::Complete(frame) => match termination {
            FrameTermination::Committed => return Disposition::Yield(frame),
            FrameTermination::Incomplete => ParseProblem::Invalid("missing commit trailer".into()),
        },
        ParseOutcome::Incomplete(problem) => match termination {
            FrameTermination::Committed => return Disposition::Corrupt(problem),
            FrameTermination::Incomplete => problem,
        },
    };
    match policy {
        ReadPolicy::LiveTail => Disposition::Skip,
        ReadPolicy::SealedScan {
            tolerate_torn_tail: true,
        } => Disposition::EndOfCommittedRecords,
        ReadPolicy::SealedScan {
            tolerate_torn_tail: false,
        } => Disposition::Corrupt(problem),
    }
}

/// Read incrementally rather than allocating the untrusted declared length.
/// Invalid headers are handed to the classifier without following their length.
pub(crate) fn read_frame_sync<B: std::io::BufRead>(
    reader: &mut B,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<(usize, FrameTermination)>> {
    use std::io::Read;
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
    use std::path::Path;

    fn record() -> LogRecord<ChainEvent> {
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({ "k": "v\n\u{0}🙂" }),
        );
        JournalRecord::new(JournalWriterId::new(), event)
    }
    fn classify(bytes: &[u8]) -> ParseOutcome<ChainEvent> {
        classify_frame(bytes, &mut Decoder::new(Path::new("fixture.log")), 0)
    }

    #[test]
    fn complete_record_yields_under_every_policy() {
        let bytes = serialize_record(&record()).unwrap();
        for policy in [
            ReadPolicy::LiveTail,
            ReadPolicy::SealedScan {
                tolerate_torn_tail: true,
            },
            ReadPolicy::SealedScan {
                tolerate_torn_tail: false,
            },
        ] {
            assert!(matches!(
                dispose(classify(&bytes), FrameTermination::Committed, policy),
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
                    ReadPolicy::LiveTail
                ),
                Disposition::Skip
            ));
            assert!(matches!(
                dispose(
                    classify(truncated),
                    FrameTermination::Incomplete,
                    ReadPolicy::SealedScan {
                        tolerate_torn_tail: true
                    }
                ),
                Disposition::EndOfCommittedRecords
            ));
            assert!(matches!(
                dispose(
                    classify(truncated),
                    FrameTermination::Incomplete,
                    ReadPolicy::SealedScan {
                        tolerate_torn_tail: false
                    }
                ),
                Disposition::Corrupt(_)
            ));
            assert!(matches!(
                dispose(
                    classify(truncated),
                    FrameTermination::Committed,
                    ReadPolicy::LiveTail
                ),
                Disposition::Corrupt(_)
            ));
        }
    }

    #[test]
    fn checksum_mismatch_is_corrupt_under_every_tail_policy() {
        let bytes = serialize_record(&record()).unwrap();
        for index in 0..bytes.len() {
            let mut corrupt = bytes.clone();
            corrupt[index] ^= 1;
            for termination in [FrameTermination::Committed, FrameTermination::Incomplete] {
                assert!(
                    matches!(
                        dispose(classify(&corrupt), termination, ReadPolicy::LiveTail),
                        Disposition::Corrupt(_)
                    ),
                    "byte {index}"
                );
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
                    ReadPolicy::SealedScan {
                        tolerate_torn_tail: true
                    }
                ),
                Disposition::Corrupt(_)
            ));
        }
        let mut bytes = serialize_record(&record()).unwrap();
        bytes.push(0);
        assert!(matches!(classify(&bytes), ParseOutcome::Corrupt(_)));
        assert!(matches!(
            classify(&frame::encode(b"invalid compact body")),
            ParseOutcome::Corrupt(_)
        ));
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

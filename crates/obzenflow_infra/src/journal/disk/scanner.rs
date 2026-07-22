// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared framed-record scanner (FLOWIP-120q).
//!
//! One byte-oriented classifier plus one policy arbiter, reused by every
//! journal reader so corruption semantics cannot drift between call sites.
//!
//! The on-disk frame is `<len>:<crc>:<json>` and the trailing `\n` is the
//! commit marker: a record is committed iff it is newline-terminated. There is
//! no legacy pure-JSON fallback (FLOWIP-120q no-legacy lock); a line without a
//! `<digits>:<digits>:` header is a malformed frame.
//!
//! The split: [`classify_frame`] turns bytes into a [`ParseOutcome`] (I/O-free,
//! the single source of truth), [`dispose`] turns an outcome plus its
//! terminator and a [`ReadPolicy`] into a [`Disposition`], and the two
//! [`read_frame_sync`] / [`read_frame_async`] shims are the only place sync and
//! async readers differ. Both shims read one frame with `read_until(b'\n')`,
//! strip the terminator from `buf`, and report whether it was present.

use super::log_record::{deserialize_frame, LogFrame};
use crc32fast::Hasher;
use obzenflow_core::event::JournalEvent;

/// Whether a frame's line carried its terminating `\n`. The newline is the
/// commit marker, so this is the finality signal `dispose` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineTermination {
    Terminated,
    Unterminated,
}

/// Classification of a single framed line (I/O-free).
pub(crate) enum ParseOutcome<R: JournalEvent> {
    /// A committed-shape record: full body, valid CRC, valid JSON.
    Complete(LogFrame<R>),
    /// Plausibly a torn write: only `dispose` plus the terminator decides
    /// whether this is a tolerable tail or mid-file corruption.
    Incomplete(ParseProblem),
    /// Committed corruption: fatal regardless of terminator or policy.
    Corrupt(ParseProblem),
}

/// Why a frame failed to classify as `Complete`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ParseProblem {
    /// Fewer payload bytes than the header declared (plausibly torn).
    TruncatedFrame { expected: usize, actual: usize },
    /// No valid `<digits>:<digits>:` header (no legacy fallback).
    MalformedHeader,
    /// CRC-valid body with no trailing `\n`: an unacknowledged write. Raised by
    /// `dispose`, never by `classify_frame`.
    UnterminatedRecord,
    /// CRC over the full body did not match the declared CRC (committed
    /// corruption: the body is full length, so this is a bit flip, not a tear).
    ChecksumMismatch { expected: u32, actual: u32 },
    /// More payload bytes than the header declared (committed corruption).
    TrailingBytes { expected: usize, actual: usize },
    /// CRC matched but the JSON did not parse (committed corruption).
    Json(String),
}

impl std::fmt::Display for ParseProblem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseProblem::TruncatedFrame { expected, actual } => write!(
                f,
                "truncated frame: header declared {expected} payload bytes, found {actual}"
            ),
            ParseProblem::MalformedHeader => {
                write!(f, "malformed frame header (no <len>:<crc>: prefix)")
            }
            ParseProblem::UnterminatedRecord => {
                write!(f, "record is not newline-terminated (uncommitted tail)")
            }
            ParseProblem::ChecksumMismatch { expected, actual } => write!(
                f,
                "checksum mismatch: header declared {expected:#010x}, computed {actual:#010x}"
            ),
            ParseProblem::TrailingBytes { expected, actual } => write!(
                f,
                "trailing bytes after framed payload: declared {expected}, found {actual}"
            ),
            ParseProblem::Json(message) => write!(f, "json parse error: {message}"),
        }
    }
}

/// Per-open read policy. Mirrors RocksDB's `WALRecoveryMode`: the reader decides
/// up front how to treat an unterminated tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadPolicy {
    /// `tail -f`: an unterminated final record may complete on a later append.
    LiveTail,
    /// A quiescent archive or full snapshot. `tolerate_torn_tail` is derived
    /// from archive status (`status != Completed`) by the archive reader.
    SealedScan { tolerate_torn_tail: bool },
}

/// What a reader should do with a classified frame under its policy.
pub(crate) enum Disposition<R: JournalEvent> {
    /// A committed record to hand to the caller.
    Yield(LogFrame<R>),
    /// Nothing here yet; a `LiveTail` reader rewinds and retries. A sealed
    /// reader never receives this.
    Skip,
    /// A tolerated final torn tail: stop the scan cleanly.
    EndOfCommittedRecords,
    /// Fail loud.
    Corrupt(ParseProblem),
}

/// Classify one frame body (the line bytes with the trailing `\n` already
/// stripped). I/O-free and the single source of truth; both readers call it.
pub(crate) fn classify_frame<R: JournalEvent>(line: &[u8]) -> ParseOutcome<R> {
    let Some((expected_len, expected_crc, body)) = try_frame_header(line) else {
        // No `<digits>:<digits>:` header and no legacy fallback. The terminator,
        // applied in `dispose`, decides whether this is a torn header or a
        // committed garbage line.
        return ParseOutcome::Incomplete(ParseProblem::MalformedHeader);
    };

    if body.len() < expected_len {
        return ParseOutcome::Incomplete(ParseProblem::TruncatedFrame {
            expected: expected_len,
            actual: body.len(),
        });
    }
    if body.len() > expected_len {
        return ParseOutcome::Corrupt(ParseProblem::TrailingBytes {
            expected: expected_len,
            actual: body.len(),
        });
    }

    let mut hasher = Hasher::new();
    hasher.update(body);
    let actual_crc = hasher.finalize();
    if actual_crc != expected_crc {
        // Full-length body, so this is corruption rather than a torn write.
        return ParseOutcome::Corrupt(ParseProblem::ChecksumMismatch {
            expected: expected_crc,
            actual: actual_crc,
        });
    }

    match deserialize_frame::<R>(body) {
        Ok(frame) => ParseOutcome::Complete(frame),
        Err(error) => ParseOutcome::Corrupt(ParseProblem::Json(error.to_string())),
    }
}

/// Decide what a reader does with a classified frame, given the terminator and
/// the read policy. The newline is the commit marker: a `\n`-terminated line
/// that did not classify `Complete` is corruption, while an unterminated tail is
/// tolerated per policy.
pub(crate) fn dispose<R: JournalEvent>(
    outcome: ParseOutcome<R>,
    termination: LineTermination,
    policy: ReadPolicy,
) -> Disposition<R> {
    let tail_problem = match outcome {
        ParseOutcome::Corrupt(problem) => return Disposition::Corrupt(problem),
        ParseOutcome::Complete(record) => match termination {
            LineTermination::Terminated => return Disposition::Yield(record),
            // CRC-valid body but no `\n`: append never acknowledged this write.
            LineTermination::Unterminated => ParseProblem::UnterminatedRecord,
        },
        ParseOutcome::Incomplete(problem) => match termination {
            // A `\n`-terminated line cannot be a torn write under this writer, so
            // a terminated incomplete line is mid-file corruption.
            LineTermination::Terminated => return Disposition::Corrupt(problem),
            LineTermination::Unterminated => problem,
        },
    };

    // Unterminated tail: provably the final bytes in the file.
    match policy {
        ReadPolicy::LiveTail => Disposition::Skip,
        ReadPolicy::SealedScan {
            tolerate_torn_tail: true,
        } => Disposition::EndOfCommittedRecords,
        ReadPolicy::SealedScan {
            tolerate_torn_tail: false,
        } => Disposition::Corrupt(tail_problem),
    }
}

/// `Some((len, crc, body))` only when `line` begins with
/// `<ascii-digits>:<ascii-digits>:`. The body keeps any `:` it contains, so a
/// JSON payload with colons survives. Records always serialize as a JSON object
/// starting with `{`, so the digit gate cleanly separates frames from any
/// non-framed line.
fn try_frame_header(line: &[u8]) -> Option<(usize, u32, &[u8])> {
    let first = line.iter().position(|&b| b == b':')?;
    if first == 0 || !line[..first].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let after_len = &line[first + 1..];
    let second = after_len.iter().position(|&b| b == b':')?;
    if second == 0 || !after_len[..second].iter().all(u8::is_ascii_digit) {
        return None;
    }
    let expected_len = std::str::from_utf8(&line[..first])
        .ok()?
        .parse::<usize>()
        .ok()?;
    let expected_crc = std::str::from_utf8(&after_len[..second])
        .ok()?
        .parse::<u32>()
        .ok()?;
    Some((expected_len, expected_crc, &after_len[second + 1..]))
}

/// Read one `\n`-delimited frame synchronously. On return, `buf` holds the line
/// bytes with the terminator stripped. `Ok(None)` at a clean EOF; otherwise the
/// number of bytes consumed including the `\n` (for byte-offset tracking) and
/// whether the terminator was present.
pub(crate) fn read_frame_sync<B: std::io::BufRead>(
    reader: &mut B,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<(usize, LineTermination)>> {
    buf.clear();
    let consumed = reader.read_until(b'\n', buf)?;
    if consumed == 0 {
        return Ok(None);
    }
    let termination = if buf.last() == Some(&b'\n') {
        buf.pop();
        LineTermination::Terminated
    } else {
        LineTermination::Unterminated
    };
    Ok(Some((consumed, termination)))
}

/// Async sibling of [`read_frame_sync`] with identical semantics.
pub(crate) async fn read_frame_async<B: tokio::io::AsyncBufRead + Unpin>(
    reader: &mut B,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<(usize, LineTermination)>> {
    use tokio::io::AsyncBufReadExt;
    buf.clear();
    let consumed = reader.read_until(b'\n', buf).await?;
    if consumed == 0 {
        return Ok(None);
    }
    let termination = if buf.last() == Some(&b'\n') {
        buf.pop();
        LineTermination::Terminated
    } else {
        LineTermination::Unterminated
    };
    Ok(Some((consumed, termination)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::disk::log_record::{serialize_record, LogRecord};
    use chrono::Utc;
    use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
    use obzenflow_core::event::vector_clock::VectorClock;
    use obzenflow_core::{JournalId, StageId, WriterId};
    use serde_json::json;
    use std::io::Cursor;
    use ulid::Ulid;

    fn record() -> LogRecord<ChainEvent> {
        let writer_id = WriterId::from(StageId::new());
        let event = ChainEventFactory::data_event(writer_id, "test.event", json!({ "k": "v" }));
        LogRecord {
            journal_id: JournalId::new(),
            event_id: Ulid::new(),
            writer_id,
            vector_clock: VectorClock::new(),
            timestamp: Utc::now(),
            event,
        }
    }

    /// Frame `body` with a correct `<len>:<crc>:` header (no trailing newline).
    fn framed(body: &[u8]) -> Vec<u8> {
        let mut hasher = Hasher::new();
        hasher.update(body);
        let crc = hasher.finalize();
        let mut out = format!("{}:{}:", body.len(), crc).into_bytes();
        out.extend_from_slice(body);
        out
    }

    fn classify(line: &[u8]) -> ParseOutcome<ChainEvent> {
        classify_frame::<ChainEvent>(line)
    }

    #[test]
    fn complete_record_terminated_yields() {
        let body = serialize_record(&record()).unwrap();
        let line = framed(&body);
        assert!(matches!(classify(&line), ParseOutcome::Complete(_)));
        // Terminated complete yields under every policy.
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
                dispose(classify(&line), LineTermination::Terminated, policy),
                Disposition::Yield(_)
            ));
        }
    }

    #[test]
    fn v1_record_body_is_not_silently_mixed_into_v2_journals() {
        let body = serde_json::to_vec(&record()).unwrap();
        let line = framed(&body);
        assert!(matches!(
            classify(&line),
            ParseOutcome::Corrupt(ParseProblem::Json(message))
                if message.contains("frame_kind")
        ));
    }

    #[test]
    fn checksum_mismatch_is_corrupt_regardless_of_terminator() {
        let body = serialize_record(&record()).unwrap();
        // Correct length, wrong CRC.
        let mut line = format!("{}:{}:", body.len(), 12345u32).into_bytes();
        line.extend_from_slice(&body);
        assert!(matches!(
            classify(&line),
            ParseOutcome::Corrupt(ParseProblem::ChecksumMismatch { .. })
        ));
        for termination in [LineTermination::Terminated, LineTermination::Unterminated] {
            assert!(matches!(
                dispose(classify(&line), termination, ReadPolicy::LiveTail),
                Disposition::Corrupt(ParseProblem::ChecksumMismatch { .. })
            ));
        }
    }

    #[test]
    fn short_body_is_incomplete_and_policy_decides() {
        let body = serialize_record(&record()).unwrap();
        // Header claims more bytes than the body carries.
        let mut line = format!("{}:{}:", body.len() + 10, 0u32).into_bytes();
        line.extend_from_slice(&body);
        assert!(matches!(
            classify(&line),
            ParseOutcome::Incomplete(ParseProblem::TruncatedFrame { .. })
        ));
        // Unterminated: LiveTail retries, tolerant sealed stops cleanly, strict fails.
        assert!(matches!(
            dispose(
                classify(&line),
                LineTermination::Unterminated,
                ReadPolicy::LiveTail
            ),
            Disposition::Skip
        ));
        assert!(matches!(
            dispose(
                classify(&line),
                LineTermination::Unterminated,
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: true
                }
            ),
            Disposition::EndOfCommittedRecords
        ));
        assert!(matches!(
            dispose(
                classify(&line),
                LineTermination::Unterminated,
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: false
                }
            ),
            Disposition::Corrupt(ParseProblem::TruncatedFrame { .. })
        ));
        // Terminated incomplete is corruption (a full line that does not parse).
        assert!(matches!(
            dispose(
                classify(&line),
                LineTermination::Terminated,
                ReadPolicy::LiveTail
            ),
            Disposition::Corrupt(ParseProblem::TruncatedFrame { .. })
        ));
    }

    #[test]
    fn non_frame_line_is_malformed_header_no_legacy() {
        // A plain JSON line: legacy fallback is removed, so this is malformed.
        let line = br#"{"event_id":"x"}"#;
        assert!(matches!(
            classify(line),
            ParseOutcome::Incomplete(ParseProblem::MalformedHeader)
        ));
        // Terminated -> corruption; unterminated tolerant -> clean stop.
        assert!(matches!(
            dispose(
                classify(line),
                LineTermination::Terminated,
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: true
                }
            ),
            Disposition::Corrupt(ParseProblem::MalformedHeader)
        ));
        assert!(matches!(
            dispose(
                classify(line),
                LineTermination::Unterminated,
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: true
                }
            ),
            Disposition::EndOfCommittedRecords
        ));
    }

    #[test]
    fn trailing_bytes_is_corrupt() {
        let body = serialize_record(&record()).unwrap();
        // Header claims fewer bytes than the body carries.
        let mut line = format!("{}:{}:", body.len() - 1, 0u32).into_bytes();
        line.extend_from_slice(&body);
        assert!(matches!(
            classify(&line),
            ParseOutcome::Corrupt(ParseProblem::TrailingBytes { .. })
        ));
    }

    #[test]
    fn unterminated_complete_body_is_uncommitted_tail() {
        let body = serialize_record(&record()).unwrap();
        let line = framed(&body); // valid full record
        assert!(matches!(classify(&line), ParseOutcome::Complete(_)));
        // No terminator: treated as a torn tail, not yielded.
        assert!(matches!(
            dispose(
                classify(&line),
                LineTermination::Unterminated,
                ReadPolicy::LiveTail
            ),
            Disposition::Skip
        ));
        assert!(matches!(
            dispose(
                classify(&line),
                LineTermination::Unterminated,
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: false
                }
            ),
            Disposition::Corrupt(ParseProblem::UnterminatedRecord)
        ));
    }

    #[test]
    fn read_frame_sync_reports_termination_and_strips_newline() {
        let body = serialize_record(&record()).unwrap();
        let mut bytes = framed(&body);
        bytes.push(b'\n');
        let unterminated = framed(&body); // second record, no newline
        bytes.extend_from_slice(&unterminated);

        let mut cursor = Cursor::new(bytes);
        let mut buf = Vec::new();

        let (consumed, term) = read_frame_sync(&mut cursor, &mut buf).unwrap().unwrap();
        assert_eq!(term, LineTermination::Terminated);
        assert_eq!(buf.last().copied(), framed(&body).last().copied()); // newline stripped
        assert_eq!(consumed, framed(&body).len() + 1);
        assert!(matches!(classify(&buf), ParseOutcome::Complete(_)));

        let (_, term) = read_frame_sync(&mut cursor, &mut buf).unwrap().unwrap();
        assert_eq!(term, LineTermination::Unterminated);
        assert!(matches!(classify(&buf), ParseOutcome::Complete(_)));

        assert!(read_frame_sync(&mut cursor, &mut buf).unwrap().is_none()); // clean EOF
    }

    #[tokio::test]
    async fn read_frame_async_matches_sync() {
        let body = serialize_record(&record()).unwrap();
        let mut bytes = framed(&body);
        bytes.push(b'\n');
        let mut reader = tokio::io::BufReader::new(Cursor::new(bytes));
        let mut buf = Vec::new();
        let (_, term) = read_frame_async(&mut reader, &mut buf)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(term, LineTermination::Terminated);
        assert!(matches!(classify(&buf), ParseOutcome::Complete(_)));
        assert!(read_frame_async(&mut reader, &mut buf)
            .await
            .unwrap()
            .is_none());
    }
}

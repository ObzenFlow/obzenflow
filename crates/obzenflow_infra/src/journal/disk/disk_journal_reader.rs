// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Efficient cursor-based reader for DiskJournal
//!
//! Maintains an open file handle and tracks position for O(1) sequential reads.

use super::scanner::{classify_frame, dispose, read_frame_async, Disposition, ReadPolicy};
use async_trait::async_trait;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::identity::JournalWriterId;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_reader::JournalReader;
use std::fs::File as StdFile;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::sync::RwLock;

/// Live-tail polls allowed at the same unterminated record before a stuck or
/// crashed writer is treated as a hard error rather than an endless retry
/// (FLOWIP-120q). Sealed readers never reach this path.
const MAX_STALL_POLLS: u32 = 5;

/// Efficient reader for DiskJournal that maintains position
pub struct DiskJournalReader<T: JournalEvent> {
    /// Buffered reader over the file
    reader: BufReader<File>,
    /// Current position (number of committed events read)
    position: u64,
    /// Byte offset of the next unread record. Advances only past a committed
    /// record, so it is both the rewind point for a torn tail (FLOWIP-120q) and
    /// the record position reported in corruption errors.
    read_offset: u64,
    /// Consecutive `Skip` polls at `read_offset` (live-tail stall guard).
    stall_polls: u32,
    /// Read policy: live-tail retries a torn tail, a sealed scan fails loud or
    /// tolerates a final torn tail per `tolerate_torn_tail`.
    policy: ReadPolicy,
    /// Reusable byte buffer for the framed-line reader.
    buf: Vec<u8>,
    /// Path to the journal file (for error messages)
    path: PathBuf,
    /// Journal ID for creating JournalWriterId
    journal_id: JournalId,
    /// Whether we've reached EOF
    at_end: bool,
    /// Shared lock to avoid reading partial writes
    read_write_lock: Arc<RwLock<()>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: JournalEvent> DiskJournalReader<T> {
    /// Create a new live-tail reader starting from the beginning
    pub async fn new(
        path: PathBuf,
        journal_id: JournalId,
        read_write_lock: Arc<RwLock<()>>,
    ) -> Result<Self, JournalError> {
        // If file doesn't exist, create an empty reader at EOF
        if !path.exists() {
            // Create empty file using blocking std I/O, then wrap in async File
            let std_file = StdFile::create(&path).map_err(|e| {
                tracing::error!(
                    path = %path.display(),
                    os_error = %e,
                    "DiskJournalReader failed to create journal file"
                );
                JournalError::Implementation {
                    message: format!("Failed to create journal file: {}", path.display()),
                    source: Box::new(e),
                }
            })?;
            let file = File::from_std(std_file);

            return Ok(Self {
                reader: BufReader::new(file),
                position: 0,
                read_offset: 0,
                stall_polls: 0,
                policy: ReadPolicy::LiveTail,
                buf: Vec::new(),
                path,
                journal_id,
                at_end: true,
                read_write_lock: read_write_lock.clone(),
                _phantom: std::marker::PhantomData,
            });
        }

        // Open using blocking std I/O, then wrap in async File to avoid Tokio's
        // per-open background task failures under high concurrency.
        let std_file = StdFile::open(&path).map_err(|e| {
            tracing::error!(
                path = %path.display(),
                os_error = %e,
                "DiskJournalReader failed to open journal file"
            );
            JournalError::Implementation {
                message: format!("Failed to open journal file: {}", path.display()),
                source: Box::new(e),
            }
        })?;
        let file = File::from_std(std_file);

        Ok(Self {
            reader: BufReader::new(file),
            position: 0,
            read_offset: 0,
            stall_polls: 0,
            policy: ReadPolicy::LiveTail,
            buf: Vec::new(),
            path,
            journal_id,
            at_end: false,
            read_write_lock,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Open an existing journal for read-only sequential access under an explicit
    /// read policy. Unlike `new()`, this never creates the file if missing.
    /// Archive readers pass a sealed policy (FLOWIP-120q).
    pub(crate) async fn open_existing(
        path: PathBuf,
        journal_id: JournalId,
        read_write_lock: Arc<RwLock<()>>,
        policy: ReadPolicy,
    ) -> Result<Self, JournalError> {
        let std_file = StdFile::open(&path).map_err(|e| {
            tracing::error!(
                path = %path.display(),
                os_error = %e,
                "DiskJournalReader failed to open existing journal file"
            );
            JournalError::Implementation {
                message: format!("Failed to open existing journal file: {}", path.display()),
                source: Box::new(e),
            }
        })?;
        let file = File::from_std(std_file);

        Ok(Self {
            reader: BufReader::new(file),
            position: 0,
            read_offset: 0,
            stall_polls: 0,
            policy,
            buf: Vec::new(),
            path,
            journal_id,
            at_end: false,
            read_write_lock,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Create a new live-tail reader starting from a specific event position
    pub async fn from_position(
        path: PathBuf,
        journal_id: JournalId,
        start_position: u64,
        read_write_lock: Arc<RwLock<()>>,
    ) -> Result<Self, JournalError> {
        let std_file = StdFile::open(&path).map_err(|e| {
            tracing::error!(
                path = %path.display(),
                os_error = %e,
                "DiskJournalReader failed to open journal file from_position"
            );
            JournalError::Implementation {
                message: format!("Failed to open journal file: {}", path.display()),
                source: Box::new(e),
            }
        })?;
        let file = File::from_std(std_file);

        let mut reader = BufReader::new(file);
        let mut position = 0;
        let mut read_offset = 0u64;

        // Skip to start position efficiently
        let mut line = String::new();
        while position < start_position {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => {
                    // Reached EOF while skipping
                    return Ok(Self {
                        reader,
                        position,
                        read_offset,
                        stall_polls: 0,
                        policy: ReadPolicy::LiveTail,
                        buf: Vec::new(),
                        path,
                        journal_id,
                        at_end: true,
                        read_write_lock: read_write_lock.clone(),
                        _phantom: std::marker::PhantomData,
                    });
                }
                Ok(bytes) => {
                    read_offset += bytes as u64;
                    if !line.trim().is_empty() {
                        position += 1;
                    }
                }
                Err(e) => {
                    return Err(JournalError::Implementation {
                        message: format!("Failed to skip to position {start_position} in journal"),
                        source: Box::new(e),
                    });
                }
            }
        }

        Ok(Self {
            reader,
            position,
            read_offset,
            stall_polls: 0,
            policy: ReadPolicy::LiveTail,
            buf: Vec::new(),
            path,
            journal_id,
            at_end: false,
            read_write_lock,
            _phantom: std::marker::PhantomData,
        })
    }
}

#[async_trait]
impl<T: JournalEvent> JournalReader<T> for DiskJournalReader<T> {
    async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
        // Don't permanently latch at_end: a live-tail reader retries after EOF to
        // pick up new appends.
        let _read_guard = self.read_write_lock.read().await;

        loop {
            let frame = read_frame_async(&mut self.reader, &mut self.buf)
                .await
                .map_err(|e| JournalError::Implementation {
                    message: format!("Failed to read from journal at offset {}", self.read_offset),
                    source: Box::new(e),
                })?;

            let Some((consumed, termination)) = frame else {
                // Clean EOF: return None for this call, allowing retry on the next.
                self.at_end = true;
                self.stall_polls = 0;
                return Ok(None);
            };
            self.at_end = false;

            if self.buf.iter().all(u8::is_ascii_whitespace) {
                self.read_offset += consumed as u64;
                continue;
            }

            match dispose(classify_frame::<T>(&self.buf), termination, self.policy) {
                Disposition::Yield(record) => {
                    self.read_offset += consumed as u64;
                    self.position += 1;
                    self.stall_polls = 0;
                    return Ok(Some(EventEnvelope {
                        journal_writer_id: JournalWriterId::from(self.journal_id),
                        vector_clock: record.vector_clock,
                        timestamp: record.timestamp,
                        event: record.event,
                    }));
                }
                Disposition::Skip => {
                    // Live-tail unterminated tail: rewind to the record start so
                    // the next poll re-reads the whole record once the writer
                    // completes it, instead of reading forward over the partial.
                    self.reader
                        .seek(SeekFrom::Start(self.read_offset))
                        .await
                        .map_err(|e| JournalError::Implementation {
                            message: format!(
                                "Failed to rewind journal to offset {}",
                                self.read_offset
                            ),
                            source: Box::new(e),
                        })?;
                    self.stall_polls += 1;
                    if self.stall_polls > MAX_STALL_POLLS {
                        let msg = format!(
                            "Partial read retries exceeded at offset {} in {}",
                            self.read_offset,
                            self.path.display()
                        );
                        tracing::error!(
                            read_offset = self.read_offset,
                            path = %self.path.display(),
                            stall_polls = self.stall_polls,
                            "Journal record partial retry budget exceeded"
                        );
                        return Err(JournalError::Implementation {
                            message: msg.clone(),
                            source: Box::new(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                msg,
                            )),
                        });
                    }
                    return Ok(None);
                }
                Disposition::EndOfCommittedRecords => {
                    // Sealed scan tolerated a final torn tail: clean end.
                    self.at_end = true;
                    self.stall_polls = 0;
                    return Ok(None);
                }
                Disposition::Corrupt(problem) => {
                    // FLOWIP-120q live-tail observer resilience: advance past the
                    // unreadable record so a best-effort observer that keeps
                    // polling after the error resumes at the next record rather
                    // than re-reading this one. Strict consumers (replay,
                    // verification, sealed scans) abort on this error, so the
                    // advance is harmless for them.
                    let corrupt_at = self.read_offset;
                    self.read_offset += consumed as u64;
                    self.stall_polls = 0;
                    tracing::error!(
                        read_offset = corrupt_at,
                        path = %self.path.display(),
                        parse_error = %problem,
                        "Failed to parse journal record"
                    );
                    return Err(JournalError::Implementation {
                        message: format!(
                            "Failed to parse journal record at offset {} in {}: {problem}",
                            corrupt_at,
                            self.path.display()
                        ),
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            problem.to_string(),
                        )),
                    });
                }
            }
        }
    }

    async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
        // Don't early return on at_end - allow retry to check for new events

        let mut skipped = 0;
        let mut line = String::new();
        let _read_guard = self.read_write_lock.read().await;

        for _ in 0..n {
            line.clear();
            match self.reader.read_line(&mut line).await {
                Ok(0) => {
                    // EOF reached
                    self.at_end = true;
                    break;
                }
                Ok(bytes) => {
                    // Got data - no longer at EOF
                    self.at_end = false;
                    self.read_offset += bytes as u64;

                    if !line.trim().is_empty() {
                        skipped += 1;
                        self.position += 1;
                    }
                }
                Err(e) => {
                    return Err(JournalError::Implementation {
                        message: format!("Failed to skip in journal at position {}", self.position),
                        source: Box::new(e),
                    });
                }
            }
        }

        Ok(skipped)
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn is_at_end(&self) -> bool {
        self.at_end
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::disk::log_record::LogRecord;
    use chrono::Utc;
    use crc32fast::Hasher;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::event::vector_clock::VectorClock;
    use obzenflow_core::event::JournalEvent;
    use obzenflow_core::{ChainEvent, JournalId, StageId, WriterId};
    use std::io::Write;
    use tempfile::NamedTempFile;
    use ulid::Ulid;

    fn write_framed_record<T: JournalEvent>(file: &mut NamedTempFile, record: &LogRecord<T>) {
        let json_body = serde_json::to_vec(record).unwrap();
        let mut hasher = Hasher::new();
        hasher.update(&json_body);
        let crc = hasher.finalize();
        let mut bytes = format!("{}:{}:", json_body.len(), crc).into_bytes();
        bytes.extend_from_slice(&json_body);
        bytes.push(b'\n');
        file.write_all(&bytes).unwrap();
    }

    #[tokio::test]
    async fn test_sequential_reading() {
        // Create a temporary journal file
        let mut temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        // Write some test records
        let writer_id = WriterId::from(StageId::new());
        let journal_id = JournalId::new();
        for i in 0..5 {
            let event = ChainEventFactory::data_event(
                writer_id,
                "test.event",
                serde_json::json!({"index": i}),
            );
            let record = LogRecord {
                journal_id,
                event_id: Ulid::new(),
                writer_id,
                vector_clock: VectorClock::new(),
                timestamp: Utc::now(),
                event,
            };
            write_framed_record(&mut temp_file, &record);
        }
        temp_file.flush().unwrap();

        let read_write_lock = Arc::new(RwLock::new(()));
        // Create reader and read all events
        let mut reader =
            DiskJournalReader::<ChainEvent>::new(path, journal_id, read_write_lock.clone())
                .await
                .unwrap();

        for i in 0..5 {
            let envelope = reader.next().await.unwrap().expect("Should have event");
            assert_eq!(envelope.event.payload()["index"], i);
            assert_eq!(reader.position(), i as u64 + 1);
        }

        // Should be at end
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.is_at_end());
    }

    #[tokio::test]
    async fn test_skip_and_resume() {
        // Create a temporary journal file
        let mut temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        // Write some test records
        let writer_id = WriterId::from(StageId::new());
        let journal_id = JournalId::new();
        for i in 0..10 {
            let event = ChainEventFactory::data_event(
                writer_id,
                "test.event",
                serde_json::json!({"index": i}),
            );
            let record = LogRecord {
                journal_id,
                event_id: Ulid::new(),
                writer_id,
                vector_clock: VectorClock::new(),
                timestamp: Utc::now(),
                event,
            };
            write_framed_record(&mut temp_file, &record);
        }
        temp_file.flush().unwrap();

        let read_write_lock = Arc::new(RwLock::new(()));
        // Create reader and skip first 5
        let mut reader =
            DiskJournalReader::<ChainEvent>::new(path.clone(), journal_id, read_write_lock.clone())
                .await
                .unwrap();
        let skipped = reader.skip(5).await.unwrap();
        assert_eq!(skipped, 5);
        assert_eq!(reader.position(), 5);

        // Read next event (should be index 5)
        let envelope = reader.next().await.unwrap().expect("Should have event");
        assert_eq!(envelope.event.payload()["index"], 5);

        // Create new reader from position 7
        let mut reader2 =
            DiskJournalReader::<ChainEvent>::from_position(path, journal_id, 7, read_write_lock)
                .await
                .unwrap();
        assert_eq!(reader2.position(), 7);

        // Should read index 7
        let envelope = reader2.next().await.unwrap().expect("Should have event");
        assert_eq!(envelope.event.payload()["index"], 7);
    }
}

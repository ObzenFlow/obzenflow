// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Single event log per flow execution
//!
//! Provides optimal sequential writes and natural event ordering

use super::disk_journal_reader::DiskJournalReader;
use super::log_record::LogRecord;
use super::scanner::{
    classify_frame, dispose, read_frame_async, read_frame_sync, Disposition, ParseOutcome,
    ReadPolicy,
};
use async_trait::async_trait;
use chrono::Utc;
use crc32fast::Hasher;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::identity::{EventId, JournalWriterId, WriterId};
use obzenflow_core::event::vector_clock::{CausalOrderingService, VectorClock};
use obzenflow_core::event::JournalEvent;
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::io::BufReader;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::sync::RwLock;
use ulid::Ulid;

/// Global registry of per-path read/write locks so all DiskJournal instances
/// that point at the same file coordinate access and prevent torn reads.
static JOURNAL_LOCKS: OnceLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, Arc<RwLock<()>>>>,
> = OnceLock::new();

fn shared_lock_for_path(path: &Path) -> Arc<RwLock<()>> {
    // FLOWIP-120q: key by a normalized absolute path so the same file maps to one
    // lock regardless of how the path is expressed (relative vs absolute), so a
    // reader and a writer that reach the same file by different spellings still
    // coordinate. Normalizing never touches the filesystem; fall back to the raw
    // path if it fails (e.g., no current dir).
    let key = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let registry =
        JOURNAL_LOCKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut guard = registry.lock().unwrap();
    guard
        .entry(key)
        .or_insert_with(|| Arc::new(RwLock::new(())))
        .clone()
}

/// Single append-only log for a flow execution
///
/// Uses a mutex to ensure atomic writes from multiple writers
pub struct DiskJournal<T: JournalEvent> {
    /// Owner of this journal (if any)
    owner: Option<JournalOwner>,
    /// Journal ID for this instance
    journal_id: JournalId,
    /// Path to the log file
    path: PathBuf,
    /// Shared synchronous file handle for appends (opened once)
    write_file: Arc<Mutex<StdFile>>,
    /// Atomic write offset for tracking file position
    write_offset: Arc<AtomicU64>,
    /// In-memory index: event_id -> file offset
    index: Arc<RwLock<HashMap<Ulid, u64>>>,
    /// Shared lock to coordinate readers/writers
    ///
    /// Writers take a write lock; readers take a read lock to avoid torn lines.
    read_write_lock: Arc<RwLock<()>>,
    /// Track vector clocks for each writer
    writer_clocks: Arc<RwLock<HashMap<WriterId, VectorClock>>>,
    _phantom: std::marker::PhantomData<T>,
}

/// Buffer size for backwards reading (64KB)
const BACKWARD_READ_BUFFER_SIZE: usize = 64 * 1024;

impl<T: JournalEvent> DiskJournal<T> {
    /// Create a new flow event log
    pub fn new(base_path: PathBuf, flow_id: &str) -> Result<Self, JournalError> {
        std::fs::create_dir_all(&base_path).map_err(|e| JournalError::Implementation {
            message: "Failed to create directory".to_string(),
            source: Box::new(e),
        })?;
        let log_path = base_path.join(format!("{flow_id}.log"));

        // Get current file size if it exists
        let write_offset = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);

        // Build index and writer clocks from the framed log (FLOWIP-120q): one
        // shared rebuild helper, so `new` and `with_owner` cannot drift.
        let (index, writer_clocks) = rebuild_index_from_path::<T>(&log_path)?;

        // Open a single shared file handle for appends
        let std_file = StdFile::options()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| JournalError::Implementation {
                message: format!("Failed to open log file for append: {}", log_path.display()),
                source: Box::new(e),
            })?;
        let write_file = std_file;

        Ok(Self {
            owner: None,
            journal_id: JournalId::new(),
            path: log_path.clone(),
            write_file: Arc::new(Mutex::new(write_file)),
            write_offset: Arc::new(AtomicU64::new(write_offset)),
            index: Arc::new(RwLock::new(index)),
            read_write_lock: shared_lock_for_path(&log_path),
            writer_clocks: Arc::new(RwLock::new(writer_clocks)),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Create a new flow event log with specified owner
    pub fn with_owner(log_path: PathBuf, owner: JournalOwner) -> Result<Self, JournalError> {
        // Create parent directory if needed
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| JournalError::Implementation {
                message: "Failed to create directory".to_string(),
                source: Box::new(e),
            })?;
        }

        // Get current file size if it exists
        let write_offset = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);

        // FLOWIP-120q P3 fix: rebuild via the framed parser through the same
        // shared helper as `new`, not raw `serde_json::from_str` (which fails on
        // framed records and used to leave the index and writer clocks empty).
        let (index, writer_clocks) = rebuild_index_from_path::<T>(&log_path)?;

        // Open a single shared file handle for appends
        let std_file = StdFile::options()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| JournalError::Implementation {
                message: format!("Failed to open log file for append: {}", log_path.display()),
                source: Box::new(e),
            })?;
        let write_file = std_file;

        Ok(Self {
            owner: Some(owner),
            journal_id: JournalId::new(),
            path: log_path.clone(),
            write_file: Arc::new(Mutex::new(write_file)),
            write_offset: Arc::new(AtomicU64::new(write_offset)),
            index: Arc::new(RwLock::new(index)),
            read_write_lock: shared_lock_for_path(&log_path),
            writer_clocks: Arc::new(RwLock::new(writer_clocks)),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Read all events from disk (internal helper)
    async fn read_all_raw(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let mut events = Vec::new();

        if !self.path.exists() {
            return Ok(events);
        }

        // FLOWIP-120q full-read lock: hold the shared read lock so an in-process
        // append cannot be observed mid-frame, and scan with sealed/full-scan
        // policy so corruption fails loud instead of silently shortening the
        // snapshot that backs `read_causally_ordered`/`read_causally_after`.
        let _read_guard = self.read_write_lock.read().await;

        let file = File::open(&self.path)
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to open file".to_string(),
                source: Box::new(e),
            })?;
        let mut reader = tokio::io::BufReader::new(file);
        let mut buf = Vec::new();
        let mut offset = 0u64;

        while let Some((consumed, termination)) = read_frame_async(&mut reader, &mut buf)
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to read line".to_string(),
                source: Box::new(e),
            })?
        {
            let record_offset = offset;
            offset += consumed as u64;
            if buf.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            match dispose(
                classify_frame::<T>(&buf),
                termination,
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: false,
                },
            ) {
                Disposition::Yield(record) => {
                    events.push(EventEnvelope {
                        journal_writer_id: JournalWriterId::from(self.journal_id),
                        vector_clock: record.vector_clock,
                        timestamp: record.timestamp,
                        event: record.event,
                    });
                }
                Disposition::EndOfCommittedRecords | Disposition::Skip => break,
                Disposition::Corrupt(problem) => {
                    return Err(JournalError::Implementation {
                        message: format!(
                            "Corrupt record at offset {record_offset} in {}: {problem}",
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

        Ok(events)
    }
}

/// In-memory index (`event_id -> byte offset`) plus per-writer vector clocks
/// rebuilt from a journal file.
type RebuiltIndex = (HashMap<Ulid, u64>, HashMap<WriterId, VectorClock>);

/// Rebuild a disk journal's in-memory index and writer clocks from the framed
/// log on disk (FLOWIP-120q). Shared by `DiskJournal::new` and
/// `DiskJournal::with_owner` so the two constructors cannot diverge. Uses the
/// sealed full-scan policy: fail loud on committed corruption, tolerate only a
/// final torn tail (a crash mid-append leaves an unterminated last record).
fn rebuild_index_from_path<T: JournalEvent>(log_path: &Path) -> Result<RebuiltIndex, JournalError> {
    let mut index = HashMap::with_capacity(10000);
    let mut writer_clocks = HashMap::new();

    if !log_path.exists() {
        return Ok((index, writer_clocks));
    }

    let file = StdFile::open(log_path).map_err(|e| JournalError::Implementation {
        message: "Failed to open log file".to_string(),
        source: Box::new(e),
    })?;
    let mut reader = BufReader::new(file);
    let mut buf = Vec::new();
    let mut offset = 0u64;

    while let Some((consumed, termination)) =
        read_frame_sync(&mut reader, &mut buf).map_err(|e| JournalError::Implementation {
            message: "Failed to read line".to_string(),
            source: Box::new(e),
        })?
    {
        let record_offset = offset;
        offset += consumed as u64;
        if buf.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match dispose(
            classify_frame::<T>(&buf),
            termination,
            ReadPolicy::SealedScan {
                tolerate_torn_tail: true,
            },
        ) {
            Disposition::Yield(record) => {
                index.insert(record.event_id, record_offset);
                writer_clocks.insert(record.writer_id, record.vector_clock);
            }
            // A tolerated torn tail ends the committed records.
            Disposition::EndOfCommittedRecords | Disposition::Skip => break,
            Disposition::Corrupt(problem) => {
                return Err(JournalError::Implementation {
                    message: format!(
                        "Corrupt record while rebuilding index at offset {record_offset} in {}: {problem}",
                        log_path.display()
                    ),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        problem.to_string(),
                    )),
                });
            }
        }
    }

    Ok((index, writer_clocks))
}

impl<T: JournalEvent> Clone for DiskJournal<T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            journal_id: self.journal_id,
            path: self.path.clone(),
            write_file: self.write_file.clone(),
            write_offset: self.write_offset.clone(),
            index: self.index.clone(),
            read_write_lock: self.read_write_lock.clone(),
            writer_clocks: self.writer_clocks.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> Journal<T> for DiskJournal<T> {
    fn storage_kind(&self) -> obzenflow_core::journal::JournalStorageKind {
        obzenflow_core::journal::JournalStorageKind::Disk
    }

    fn id(&self) -> &JournalId {
        &self.journal_id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,
        event: T,
        parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError> {
        // Safety check: Ensure journal has an owner before allowing writes
        if self.owner.is_none() {
            return Err(JournalError::Implementation {
                message: "Cannot write to an unowned journal. Journal must have an owner."
                    .to_string(),
                source: "Unowned journal write attempt".into(),
            });
        }
        // Get writer_id from the event
        let writer_id = *event.writer_id();

        // Acquire the journal write lock before advancing writer clocks.
        //
        // This serialises append operations and ensures concurrent appends
        // cannot compute the same `writer_seq` from a stale snapshot.
        let _lock = self.read_write_lock.write().await;

        // Get or create vector clock for this writer
        let mut vector_clock = {
            let writer_clocks = self.writer_clocks.read().await;
            writer_clocks
                .get(&writer_id)
                .cloned()
                .unwrap_or_else(VectorClock::new)
        };

        // Update vector clock based on parent
        if let Some(parent_envelope) = parent {
            CausalOrderingService::update_with_parent(
                &mut vector_clock,
                &parent_envelope.vector_clock,
            );
        }

        // Increment for this writer
        CausalOrderingService::increment(&mut vector_clock, &writer_id.to_string());

        // Create envelope
        let envelope = EventEnvelope {
            journal_writer_id: JournalWriterId::from(self.journal_id),
            vector_clock: vector_clock.clone(),
            timestamp: Utc::now(),
            event: event.clone(),
        };

        // Create log record
        let record = LogRecord::<T> {
            event_id: event.id().as_ulid(),
            writer_id,
            journal_id: self.journal_id,
            vector_clock: vector_clock.clone(),
            timestamp: envelope.timestamp,
            event,
        };

        // Serialize with newline
        let json_body = serde_json::to_vec(&record).map_err(|e| JournalError::Implementation {
            message: "Failed to serialize record".to_string(),
            source: Box::new(e),
        })?;

        // Frame with length and checksum so readers can detect torn reads
        let mut hasher = Hasher::new();
        hasher.update(&json_body);
        let crc = hasher.finalize();
        let framed_line = format!("{}:{}:", json_body.len(), crc);

        let mut bytes = framed_line.into_bytes();
        bytes.extend_from_slice(&json_body);
        bytes.push(b'\n');

        // Get current offset and reserve space
        let offset = self.write_offset.load(Ordering::SeqCst);
        self.write_offset
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);

        // Write to file using the shared StdFile handle on a blocking thread.
        let path = self.path.clone();
        let write_file = self.write_file.clone();
        let write_bytes = bytes.clone();

        tokio::task::spawn_blocking(move || -> Result<(), JournalError> {
            use std::io::{Error, Write};

            let mut file = match write_file.lock() {
                Ok(guard) => guard,
                Err(e) => {
                    return Err(JournalError::Implementation {
                        message: format!("Failed to lock journal file: {}", path.display()),
                        source: Box::new(Error::other(format!("Mutex poisoned: {e}"))),
                    });
                }
            };

            file.write_all(&write_bytes).map_err(|e| {
                tracing::error!(
                    path = %path.display(),
                    os_error = %e,
                    "DiskJournal failed to write to file"
                );
                JournalError::Implementation {
                    message: "Failed to write to file".to_string(),
                    source: Box::new(e),
                }
            })?;

            file.flush().map_err(|e| {
                tracing::error!(
                    path = %path.display(),
                    os_error = %e,
                    "DiskJournal failed to flush file"
                );
                JournalError::Implementation {
                    message: "Failed to flush file".to_string(),
                    source: Box::new(e),
                }
            })?;

            Ok(())
        })
        .await
        .map_err(|e| JournalError::Implementation {
            message: format!(
                "Background writer task {} for journal {}",
                if e.is_cancelled() {
                    "was cancelled"
                } else if e.is_panic() {
                    "panicked"
                } else {
                    "failed"
                },
                self.path.display()
            ),
            source: Box::new(e),
        })??;
        tracing::debug!(
            path = %self.path.display(),
            offset,
            bytes = bytes.len(),
            write_lock_ptr = ?Arc::as_ptr(&self.read_write_lock),
            "DiskJournal appended framed record"
        );

        // Update index
        self.index.write().await.insert(record.event_id, offset);

        // Update writer's clock
        self.writer_clocks
            .write()
            .await
            .insert(writer_id, vector_clock);

        Ok(envelope)
    }

    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let events = self.read_all_raw().await?;
        CausalOrderingService::order_envelopes_by_event_id(events)
    }

    async fn read_causally_after(
        &self,
        after_event_id: &EventId,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let all_events = self.read_causally_ordered().await?;

        // Find the position of the reference event
        let position = all_events
            .iter()
            .position(|e| e.event.id() == after_event_id);

        match position {
            Some(pos) => Ok(all_events.into_iter().skip(pos + 1).collect()),
            None => Ok(Vec::new()),
        }
    }

    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let ulid = event_id.as_ulid();

        // Check index
        let index = self.index.read().await;
        let offset = match index.get(&ulid) {
            Some(o) => *o,
            None => return Ok(None),
        };
        drop(index);

        if !self.path.exists() {
            return Ok(None);
        }

        // Read from file at specific offset
        let mut file = File::open(&self.path)
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to open file".to_string(),
                source: Box::new(e),
            })?;
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to seek in file".to_string(),
                source: Box::new(e),
            })?;

        let mut reader = tokio::io::BufReader::new(file);
        let mut buf = Vec::new();

        match read_frame_async(&mut reader, &mut buf).await.map_err(|e| {
            JournalError::Implementation {
                message: "Failed to read line".to_string(),
                source: Box::new(e),
            }
        })? {
            Some((_, termination)) => match dispose(
                classify_frame::<T>(&buf),
                termination,
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: true,
                },
            ) {
                Disposition::Yield(record) => Ok(Some(EventEnvelope {
                    journal_writer_id: JournalWriterId::from(self.journal_id),
                    vector_clock: record.vector_clock,
                    timestamp: record.timestamp,
                    event: record.event,
                })),
                Disposition::EndOfCommittedRecords | Disposition::Skip => Ok(None),
                Disposition::Corrupt(problem) => Err(JournalError::Implementation {
                    message: format!(
                        "Failed to parse record at {}: {problem}",
                        self.path.display()
                    ),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        problem.to_string(),
                    )),
                }),
            },
            None => Ok(None),
        }
    }

    async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(
            DiskJournalReader::new(
                self.path.clone(),
                self.journal_id,
                self.read_write_lock.clone(),
            )
            .await?,
        ))
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(
            DiskJournalReader::from_position(
                self.path.clone(),
                self.journal_id,
                position,
                self.read_write_lock.clone(),
            )
            .await?,
        ))
    }

    async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        use tokio::io::AsyncSeekExt;

        if count == 0 || !self.path.exists() {
            return Ok(Vec::new());
        }

        // Acquire read lock to prevent torn reads
        let _read_guard = self.read_write_lock.read().await;

        let mut file = File::open(&self.path)
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to open file for backwards read".to_string(),
                source: Box::new(e),
            })?;

        // Get file size
        let file_len =
            file.seek(SeekFrom::End(0))
                .await
                .map_err(|e| JournalError::Implementation {
                    message: "Failed to seek to end".to_string(),
                    source: Box::new(e),
                })?;

        if file_len == 0 {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(count);
        let mut pos = file_len;

        // Read backwards chunk by chunk
        while pos > 0 && results.len() < count {
            let chunk_start = pos.saturating_sub(BACKWARD_READ_BUFFER_SIZE as u64);
            let chunk_size = (pos - chunk_start) as usize;

            file.seek(SeekFrom::Start(chunk_start)).await.map_err(|e| {
                JournalError::Implementation {
                    message: "Failed to seek backwards".to_string(),
                    source: Box::new(e),
                }
            })?;

            let mut buffer = vec![0u8; chunk_size];
            use tokio::io::AsyncReadExt;
            let bytes_read =
                file.read(&mut buffer)
                    .await
                    .map_err(|e| JournalError::Implementation {
                        message: "Failed to read chunk".to_string(),
                        source: Box::new(e),
                    })?;
            buffer.truncate(bytes_read);

            // Process lines in this chunk from end to start
            let chunk_str = String::from_utf8_lossy(&buffer);
            let lines: Vec<&str> = chunk_str.lines().collect();

            for line in lines.iter().rev() {
                if results.len() >= count {
                    break;
                }

                if line.trim().is_empty() {
                    continue;
                }

                // read_last_n is a best-effort tail/observability helper
                // (FLOWIP-120q): classify per line and skip anything that is not a
                // committed record. It is never a replay/verification contract.
                match classify_frame::<T>(line.as_bytes()) {
                    ParseOutcome::Complete(record) => {
                        let envelope = EventEnvelope {
                            journal_writer_id: JournalWriterId::from(self.journal_id),
                            vector_clock: record.vector_clock,
                            timestamp: record.timestamp,
                            event: record.event,
                        };
                        results.push(envelope);
                    }
                    ParseOutcome::Incomplete(_) => {
                        // Likely a chunk-boundary fragment; skip.
                        continue;
                    }
                    ParseOutcome::Corrupt(problem) => {
                        tracing::warn!(
                            path = %self.path.display(),
                            parse_error = %problem,
                            "Skipping corrupt record during backwards read"
                        );
                        continue;
                    }
                }
            }

            // Move to next chunk
            pos = chunk_start;
        }

        tracing::debug!(
            path = %self.path.display(),
            requested = count,
            returned = results.len(),
            "read_last_n completed"
        );

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
    use obzenflow_core::id::StageId;
    use tokio::sync::Barrier;

    use uuid::Uuid;

    #[tokio::test]
    async fn test_basic_append_and_read() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!(
            "target/test-logs/test_basic_append_and_read_{test_id}"
        ));
        std::fs::create_dir_all(&test_dir).unwrap();
        // Create a test journal with a proper owner
        let test_stage_id = obzenflow_core::StageId::new();
        let owner = obzenflow_core::JournalOwner::stage(test_stage_id);
        let log_path = test_dir.join("test_flow_1.log");
        let log = DiskJournal::<ChainEvent>::with_owner(log_path, owner).unwrap();

        let writer_id = WriterId::from(StageId::new());
        let event = ChainEventFactory::data_event(
            writer_id,
            "test.event",
            serde_json::json!({"data": "test value"}),
        );

        // Append event
        let envelope = log.append(event.clone(), None).await.unwrap();

        // Read back
        let events = log.read_causally_ordered().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.event_type(), "test.event");
        assert_eq!(events[0].event.payload()["data"], "test value");

        // Read by ID
        let event_by_id = log.read_event(&envelope.event.id).await.unwrap();
        assert!(event_by_id.is_some());
        assert_eq!(event_by_id.unwrap().event.id, envelope.event.id);

        // Cleanup
        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn test_causal_ordering() {
        let test_id = Uuid::new_v4();
        let test_dir =
            std::path::PathBuf::from(format!("target/test-logs/test_causal_ordering_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        // Create a test journal with a proper owner
        let test_system_id = obzenflow_core::SystemId::new();
        let owner = obzenflow_core::JournalOwner::system(test_system_id);
        let log_path = test_dir.join("test_flow_2.log");
        let log = DiskJournal::<ChainEvent>::with_owner(log_path, owner).unwrap();

        let writer1 = WriterId::from(StageId::new());
        let writer2 = WriterId::from(StageId::new());

        // First event from writer1
        let event1 =
            ChainEventFactory::data_event(writer1, "event.1", serde_json::json!({"seq": 1}));
        let envelope1 = log.append(event1, None).await.unwrap();

        // Second event from writer2, causally dependent on event1
        let event2 =
            ChainEventFactory::data_event(writer2, "event.2", serde_json::json!({"seq": 2}));
        let envelope2 = log.append(event2, Some(&envelope1)).await.unwrap();

        // Verify vector clocks show causal relationship
        assert!(CausalOrderingService::happened_before(
            &envelope1.vector_clock,
            &envelope2.vector_clock
        ));

        // Read all events
        let events = log.read_causally_ordered().await.unwrap();
        assert_eq!(events.len(), 2);

        // Verify causal order
        assert_eq!(events[0].event.id, envelope1.event.id);
        assert_eq!(events[1].event.id, envelope2.event.id);

        // Cleanup
        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn test_concurrent_writers() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!(
            "target/test-logs/test_concurrent_writers_{test_id}"
        ));
        std::fs::create_dir_all(&test_dir).unwrap();
        // Create a test journal with a proper owner
        let test_system_id = obzenflow_core::SystemId::new();
        let owner = obzenflow_core::JournalOwner::system(test_system_id);
        let log_path = test_dir.join("test_flow_3.log");
        let log = Arc::new(DiskJournal::<ChainEvent>::with_owner(log_path, owner).unwrap());

        // Spawn multiple concurrent writers
        let mut handles = vec![];

        for i in 0..5 {
            let log_clone = log.clone();
            let handle = tokio::spawn(async move {
                let writer_id = WriterId::from(StageId::new());
                let event = ChainEventFactory::data_event(
                    writer_id,
                    "concurrent.event",
                    serde_json::json!({"writer": i}),
                );
                log_clone.append(event, None).await
            });
            handles.push(handle);
        }

        // Wait for all writers
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // Verify all events were written
        let events = log.read_causally_ordered().await.unwrap();
        assert_eq!(events.len(), 5);

        // Verify each event has unique writer
        let writer_ids: std::collections::HashSet<_> = events
            .iter()
            .map(|e| e.event.writer_id().as_ulid().to_string())
            .collect();
        assert_eq!(writer_ids.len(), 5);

        // Cleanup
        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn test_same_writer_concurrent_appends_have_unique_writer_seq() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!(
            "target/test-logs/test_same_writer_concurrent_appends_have_unique_writer_seq_{test_id}"
        ));
        std::fs::create_dir_all(&test_dir).unwrap();

        let test_stage_id = obzenflow_core::StageId::new();
        let owner = obzenflow_core::JournalOwner::stage(test_stage_id);
        let log_path = test_dir.join("same_writer_concurrent.log");
        let log = Arc::new(DiskJournal::<ChainEvent>::with_owner(log_path, owner).unwrap());

        let writer_id = WriterId::from(StageId::new());
        let writer_key = writer_id.to_string();

        let task_count: usize = 20;
        let barrier = Arc::new(Barrier::new(task_count));

        let mut handles = Vec::with_capacity(task_count);
        for i in 0..task_count {
            let log_clone = log.clone();
            let barrier_clone = barrier.clone();
            let handle = tokio::spawn(async move {
                barrier_clone.wait().await;
                let event = ChainEventFactory::data_event(
                    writer_id,
                    "concurrent.same_writer",
                    serde_json::json!({ "i": i }),
                );
                log_clone.append(event, None).await
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        let events = log.read_causally_ordered().await.unwrap();
        assert_eq!(events.len(), task_count);

        let writer_seqs: Vec<u64> = events
            .iter()
            .map(|e| e.vector_clock.get(&writer_key))
            .collect();

        let expected: Vec<u64> = (1..=task_count as u64).collect();
        assert_eq!(writer_seqs, expected);

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn with_owner_reopen_rebuilds_index_and_writer_clocks() {
        // FLOWIP-120q P3: reopening an owned framed journal must rebuild the
        // index (so read_event by id works) and the writer clocks (so a new
        // append continues the sequence) via the framed parser.
        let test_id = Uuid::new_v4();
        let test_dir =
            std::path::PathBuf::from(format!("target/test-logs/with_owner_reopen_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("reopen.log");
        let writer_id = WriterId::from(StageId::new());
        let writer_key = writer_id.to_string();

        let kept_id;
        {
            let log = DiskJournal::<ChainEvent>::with_owner(
                log_path.clone(),
                obzenflow_core::JournalOwner::stage(StageId::new()),
            )
            .unwrap();
            let e1 = ChainEventFactory::data_event(writer_id, "a", serde_json::json!({"n": 1}));
            let env1 = log.append(e1, None).await.unwrap();
            let e2 = ChainEventFactory::data_event(writer_id, "b", serde_json::json!({"n": 2}));
            log.append(e2, Some(&env1)).await.unwrap();
            kept_id = env1.event.id;
        }

        let reopened = DiskJournal::<ChainEvent>::with_owner(
            log_path,
            obzenflow_core::JournalOwner::stage(StageId::new()),
        )
        .unwrap();

        assert!(
            reopened.read_event(&kept_id).await.unwrap().is_some(),
            "read_event by id must work after reopen (index rebuilt)"
        );
        assert_eq!(reopened.read_causally_ordered().await.unwrap().len(), 2);

        let e3 = ChainEventFactory::data_event(writer_id, "c", serde_json::json!({"n": 3}));
        let env3 = reopened.append(e3, None).await.unwrap();
        assert!(
            env3.vector_clock.get(&writer_key) >= 3,
            "writer clock must continue after reopen, not reset"
        );

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn read_causally_ordered_fails_loud_on_mid_file_corruption() {
        // FLOWIP-120q: a corrupt committed record must fail loud rather than be
        // silently skipped to produce a shortened stream.
        let test_id = Uuid::new_v4();
        let test_dir =
            std::path::PathBuf::from(format!("target/test-logs/read_all_corrupt_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("corrupt.log");
        let writer_id = WriterId::from(StageId::new());

        let log = DiskJournal::<ChainEvent>::with_owner(
            log_path.clone(),
            obzenflow_core::JournalOwner::stage(StageId::new()),
        )
        .unwrap();
        for i in 0..3 {
            let e = ChainEventFactory::data_event(writer_id, "e", serde_json::json!({"i": i}));
            log.append(e, None).await.unwrap();
        }

        // Flip a byte inside the first record's body (mid-file): its CRC no
        // longer matches, so it is committed corruption.
        let mut bytes = std::fs::read(&log_path).unwrap();
        let brace = bytes.iter().position(|&b| b == b'{').unwrap();
        bytes[brace + 5] ^= 0xff;
        std::fs::write(&log_path, &bytes).unwrap();

        assert!(
            log.read_causally_ordered().await.is_err(),
            "mid-file corruption must fail loud"
        );

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn live_tail_reader_handles_concurrent_appends_without_misalignment() {
        // FLOWIP-120q: a live-tail reader sharing the per-path lock with the
        // writer must read every committed record under concurrent append, never
        // misaligning into a mid-record "malformed" read and never erroring.
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!("target/test-logs/live_tail_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("tail.log");
        let log = Arc::new(
            DiskJournal::<ChainEvent>::with_owner(
                log_path,
                obzenflow_core::JournalOwner::stage(StageId::new()),
            )
            .unwrap(),
        );
        let writer_id = WriterId::from(StageId::new());

        const N: usize = 50;
        let writer_log = log.clone();
        let writer = tokio::spawn(async move {
            for i in 0..N {
                let e = ChainEventFactory::data_event(
                    writer_id,
                    "tail.event",
                    serde_json::json!({"i": i}),
                );
                writer_log.append(e, None).await.unwrap();
                tokio::task::yield_now().await;
            }
        });

        let mut reader = log.reader().await.unwrap();
        let mut seen = 0usize;
        let mut polls = 0u32;
        while seen < N {
            match reader.next().await {
                Ok(Some(_)) => seen += 1,
                Ok(None) => {
                    tokio::task::yield_now().await;
                    polls += 1;
                    assert!(polls < 1_000_000, "live-tail reader stalled at {seen}/{N}");
                }
                Err(e) => panic!("live-tail reader must not error on concurrent append: {e}"),
            }
        }
        writer.await.unwrap();
        assert_eq!(seen, N);

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn live_tail_reader_resumes_after_a_corrupt_record() {
        // FLOWIP-120q live-tail observer resilience: a live reader that hits a
        // corrupt committed record reports it once, then resumes at the next
        // record on the following poll, so a best-effort observer that keeps
        // polling recovers rather than losing the rest of the stream.
        let test_id = Uuid::new_v4();
        let test_dir =
            std::path::PathBuf::from(format!("target/test-logs/resume_corrupt_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("resume.log");
        let writer_id = WriterId::from(StageId::new());

        let log = DiskJournal::<ChainEvent>::with_owner(
            log_path.clone(),
            obzenflow_core::JournalOwner::stage(StageId::new()),
        )
        .unwrap();
        for i in 0..3 {
            let e = ChainEventFactory::data_event(writer_id, "e", serde_json::json!({"i": i}));
            log.append(e, None).await.unwrap();
        }

        // Flip a byte inside the second record's body (just before its
        // terminating newline) so its CRC no longer matches: committed corruption
        // with intact records on either side.
        let mut bytes = std::fs::read(&log_path).unwrap();
        let newlines: Vec<usize> = bytes
            .iter()
            .enumerate()
            .filter(|(_, &b)| b == b'\n')
            .map(|(i, _)| i)
            .collect();
        let target = newlines[1] - 5;
        bytes[target] ^= 0xff;
        std::fs::write(&log_path, &bytes).unwrap();

        let mut reader = log.reader().await.unwrap();
        assert!(
            reader.next().await.unwrap().is_some(),
            "the first record reads cleanly"
        );
        assert!(
            reader.next().await.is_err(),
            "the corrupt middle record is reported as an error"
        );
        assert!(
            reader.next().await.unwrap().is_some(),
            "the reader resumes at the record after the corrupt one"
        );

        std::fs::remove_dir_all(&test_dir).ok();
    }
}

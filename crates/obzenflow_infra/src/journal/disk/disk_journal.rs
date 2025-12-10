//! Single event log per flow execution
//!
//! Provides optimal sequential writes and natural event ordering

use super::disk_journal_reader::DiskJournalReader;
use super::log_record::LogRecord;
use async_trait::async_trait;
use chrono::Utc;
use crc32fast::Hasher;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::identity::{EventId, JournalWriterId, WriterId};
use obzenflow_core::event::vector_clock::{CausalOrderingService, VectorClock};
use obzenflow_core::event::JournalEvent;
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::io::SeekFrom;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt};
use tokio::sync::RwLock;
use ulid::Ulid;

/// Global registry of per-path read/write locks so all DiskJournal instances
/// that point at the same file coordinate access and prevent torn reads.
static JOURNAL_LOCKS: OnceLock<
    std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, Arc<RwLock<()>>>>,
> = OnceLock::new();

fn shared_lock_for_path(path: &PathBuf) -> Arc<RwLock<()>> {
    let registry =
        JOURNAL_LOCKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut guard = registry.lock().unwrap();
    guard
        .entry(path.clone())
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
        let log_path = base_path.join(format!("{}.log", flow_id));

        // Get current file size if it exists
        let write_offset = std::fs::metadata(&log_path).map(|m| m.len()).unwrap_or(0);

        // Build index if file exists
        let mut index = HashMap::with_capacity(10000);
        let mut writer_clocks = HashMap::new();

        if log_path.exists() {
            // Read through file to rebuild index and writer clocks
            let file = StdFile::open(&log_path).map_err(|e| JournalError::Implementation {
                message: "Failed to open log file".to_string(),
                source: Box::new(e),
            })?;
            let reader = BufReader::new(file);
            let mut offset = 0u64;

            for line in reader.lines() {
                let line = line.map_err(|e| JournalError::Implementation {
                    message: "Failed to read line".to_string(),
                    source: Box::new(e),
                })?;
                if !line.trim().is_empty() {
                    match parse_framed_record::<T>(&line) {
                        ParseOutcome::Complete(record) => {
                            index.insert(record.event_id, offset);

                            // Update writer clocks
                            writer_clocks.insert(record.writer_id, record.vector_clock);
                        }
                        // Skip partial lines when rebuilding index; they'll be retried on read
                        ParseOutcome::Partial => {}
                        ParseOutcome::Corrupt(e) => {
                            tracing::warn!(
                                offset,
                                path = %log_path.display(),
                                parse_error = %e,
                                "Skipping corrupt record while rebuilding index"
                            );
                        }
                    }
                    offset += line.len() as u64 + 1; // +1 for newline
                }
            }
        }

        // Open a single shared file handle for appends
        let std_file = StdFile::options()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| JournalError::Implementation {
                message: format!(
                    "Failed to open log file for append: {}",
                    log_path.display()
                ),
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

        // Build index if file exists
        let mut index = HashMap::with_capacity(10000);
        let mut writer_clocks = HashMap::new();

        if log_path.exists() {
            // Read through file to rebuild index and writer clocks
            let file = StdFile::open(&log_path).map_err(|e| JournalError::Implementation {
                message: "Failed to open log file".to_string(),
                source: Box::new(e),
            })?;
            let reader = BufReader::new(file);
            let mut offset = 0u64;

            for line in reader.lines() {
                let line = line.map_err(|e| JournalError::Implementation {
                    message: "Failed to read line".to_string(),
                    source: Box::new(e),
                })?;
                if !line.trim().is_empty() {
                    if let Ok(record) = serde_json::from_str::<LogRecord<T>>(&line) {
                        index.insert(record.event_id, offset);

                        // Update writer clocks
                        writer_clocks.insert(record.writer_id, record.vector_clock);
                    }
                    offset += line.len() as u64 + 1; // +1 for newline
                }
            }
        }

        // Open a single shared file handle for appends
        let std_file = StdFile::options()
            .create(true)
            .append(true)
            .open(&log_path)
            .map_err(|e| JournalError::Implementation {
                message: format!(
                    "Failed to open log file for append: {}",
                    log_path.display()
                ),
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

        let file = File::open(&self.path)
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to open file".to_string(),
                source: Box::new(e),
            })?;
        let reader = tokio::io::BufReader::new(file);
        let mut lines = reader.lines();

        while let Some(line) =
            lines
                .next_line()
                .await
                .map_err(|e| JournalError::Implementation {
                    message: "Failed to read line".to_string(),
                    source: Box::new(e),
                })?
        {
            if line.trim().is_empty() {
                continue;
            }
            match parse_framed_record::<T>(&line) {
                ParseOutcome::Complete(record) => {
                    events.push(EventEnvelope {
                        journal_writer_id: JournalWriterId::from(self.journal_id),
                        vector_clock: record.vector_clock,
                        timestamp: record.timestamp,
                        event: record.event,
                    });
                }
                // For read_all, treat partial as transient and skip
                ParseOutcome::Partial => continue,
                ParseOutcome::Corrupt(e) => {
                    return Err(JournalError::Implementation {
                        message: format!("Failed to parse record: {}", e),
                        source: Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
                    });
                }
            }
        }

        Ok(events)
    }
}

/// Outcome of attempting to parse a framed log line
pub(super) enum ParseOutcome<R: JournalEvent> {
    Complete(LogRecord<R>),
    Partial,
    Corrupt(String),
}

pub(super) fn parse_framed_record<R: JournalEvent>(line: &str) -> ParseOutcome<R> {
    let trimmed = line.trim_end_matches('\n');

    // Attempt framed format: <len>:<crc>:<json>
    let mut parts = trimmed.splitn(3, ':');
    if let (Some(len_str), Some(crc_str), Some(payload)) =
        (parts.next(), parts.next(), parts.next())
    {
        if let (Ok(expected_len), Ok(expected_crc)) =
            (len_str.parse::<usize>(), crc_str.parse::<u32>())
        {
            // Check length first
            let payload_bytes = payload.as_bytes();
            if payload_bytes.len() < expected_len {
                return ParseOutcome::Partial;
            }

            let body = &payload_bytes[..expected_len];
            let mut hasher = Hasher::new();
            hasher.update(body);
            let actual_crc = hasher.finalize();
            if actual_crc != expected_crc {
                return ParseOutcome::Partial;
            }

            match serde_json::from_slice::<LogRecord<R>>(body) {
                Ok(rec) => return ParseOutcome::Complete(rec),
                // CRC matched, so this is truly malformed JSON rather than a torn read
                Err(e) => return ParseOutcome::Corrupt(format!("json parse error: {}", e)),
            }
        } else {
            // Header present but not parseable - likely read mid-line
            return ParseOutcome::Partial;
        }
    } else {
        // No header delimiters, likely a torn prefix/suffix
        // Fall through to legacy JSON parsing below
    }

    // Fallback to legacy pure-JSON line
    match serde_json::from_str::<LogRecord<R>>(trimmed) {
        Ok(rec) => ParseOutcome::Complete(rec),
        Err(_) => {
            // Legacy JSON parse failed: treat as partial to allow retry instead of hard-corrupt
            ParseOutcome::Partial
        }
    }
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
        let writer_id = event.writer_id().clone();

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
            writer_id: writer_id.clone(),
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

        // Acquire write lock for atomic write (blocks readers)
        let _lock = self.read_write_lock.write().await;

        // Get current offset and reserve space
        let offset = self.write_offset.load(Ordering::SeqCst);
        self.write_offset
            .fetch_add(bytes.len() as u64, Ordering::SeqCst);

        // Write to file using the shared StdFile handle on a blocking thread.
        let path = self.path.clone();
        let write_file = self.write_file.clone();
        let write_bytes = bytes.clone();

        tokio::task::spawn_blocking(move || -> Result<(), JournalError> {
            use std::io::{Error, ErrorKind, Write};

            let mut file = match write_file.lock() {
                Ok(guard) => guard,
                Err(e) => {
                    return Err(JournalError::Implementation {
                        message: format!("Failed to lock journal file: {}", path.display()),
                        source: Box::new(Error::new(
                            ErrorKind::Other,
                            format!("Mutex poisoned: {}", e),
                        )),
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
                "Background writer task panicked for journal {}",
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
        let mut events = self.read_all_raw().await?;

        // Sort by vector clock for causal ordering
        events.sort_by(|a, b| {
            CausalOrderingService::causal_compare(&a.vector_clock, &b.vector_clock).unwrap_or_else(
                || {
                    // For concurrent events, use timestamp as tiebreaker
                    a.timestamp.cmp(&b.timestamp)
                },
            )
        });

        Ok(events)
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

        let reader = tokio::io::BufReader::new(file);
        let mut lines = reader.lines();

        if let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to read line".to_string(),
                source: Box::new(e),
            })?
        {
            match parse_framed_record::<T>(&line) {
                ParseOutcome::Complete(record) => Ok(Some(EventEnvelope {
                    journal_writer_id: JournalWriterId::from(self.journal_id),
                    vector_clock: record.vector_clock,
                    timestamp: record.timestamp,
                    event: record.event,
                })),
                ParseOutcome::Partial => Ok(None),
                ParseOutcome::Corrupt(e) => Err(JournalError::Implementation {
                    message: format!("Failed to parse record: {}", e),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        e,
                    )),
                }),
            }
        } else {
            Ok(None)
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
        let file_len = file
            .seek(SeekFrom::End(0))
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

            file.seek(SeekFrom::Start(chunk_start))
                .await
                .map_err(|e| JournalError::Implementation {
                    message: "Failed to seek backwards".to_string(),
                    source: Box::new(e),
                })?;

            let mut buffer = vec![0u8; chunk_size];
            use tokio::io::AsyncReadExt;
            let bytes_read = file
                .read(&mut buffer)
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

                match parse_framed_record::<T>(line) {
                    ParseOutcome::Complete(record) => {
                        let envelope = EventEnvelope {
                            journal_writer_id: JournalWriterId::from(self.journal_id),
                            vector_clock: record.vector_clock,
                            timestamp: record.timestamp,
                            event: record.event,
                        };
                        results.push(envelope);
                    }
                    ParseOutcome::Partial => {
                        // Skip partial records - they might be at chunk boundary
                        continue;
                    }
                    ParseOutcome::Corrupt(e) => {
                        tracing::warn!(
                            path = %self.path.display(),
                            parse_error = %e,
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
    use obzenflow_core::id::{StageId, SystemId};
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_basic_append_and_read() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!(
            "target/test-logs/test_basic_append_and_read_{}",
            test_id
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
            std::path::PathBuf::from(format!("target/test-logs/test_causal_ordering_{}", test_id));
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
            "target/test-logs/test_concurrent_writers_{}",
            test_id
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
}

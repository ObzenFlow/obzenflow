//! Efficient cursor-based reader for DiskJournal
//!
//! Maintains an open file handle and tracks position for O(1) sequential reads.

use async_trait::async_trait;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::identity::JournalWriterId;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_reader::JournalReader;
use std::fs::File as StdFile;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::RwLock;

/// Efficient reader for DiskJournal that maintains position
pub struct DiskJournalReader<T: JournalEvent> {
    /// Buffered reader over the file
    reader: BufReader<File>,
    /// Current position (number of events read)
    position: u64,
    /// Consecutive partial read attempts at the current position
    partial_retries: u32,
    /// Position associated with the current partial retry budget
    last_partial_position: Option<u64>,
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
    /// Create a new reader starting from the beginning
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
                partial_retries: 0,
                last_partial_position: None,
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
            partial_retries: 0,
            last_partial_position: None,
            path,
            journal_id,
            at_end: false,
            read_write_lock,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Create a new reader starting from a specific position
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
                        partial_retries: 0,
                        last_partial_position: None,
                        path,
                        journal_id,
                        at_end: true,
                        read_write_lock: read_write_lock.clone(),
                        _phantom: std::marker::PhantomData,
                    });
                }
                Ok(_) => {
                    if !line.trim().is_empty() {
                        position += 1;
                    }
                }
                Err(e) => {
                    return Err(JournalError::Implementation {
                        message: format!(
                            "Failed to skip to position {} in journal",
                            start_position
                        ),
                        source: Box::new(e),
                    });
                }
            }
        }

        Ok(Self {
            reader,
            position,
            partial_retries: 0,
            last_partial_position: None,
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
        // Remove the early return for at_end - we want to retry after EOF
        // to check for new events (like tail -f behavior)

        let mut line = String::new();
        // Prevent reading partial writes by holding a shared lock
        let _read_guard = self.read_write_lock.read().await;

        loop {
            line.clear();
            match self.reader.read_line(&mut line).await {
                Ok(0) => {
                    // EOF reached - but don't permanently set at_end
                    // Just return None for this call, allowing retry on next call
                    self.at_end = true;
                    self.partial_retries = 0;
                    self.last_partial_position = None;
                    return Ok(None);
                }
                Ok(_) => {
                    // We got data - no longer at EOF
                    self.at_end = false;

                    // Skip empty lines
                    if line.trim().is_empty() {
                        continue;
                    }
                    tracing::debug!(
                        path = %self.path.display(),
                        position = self.position,
                        line_len = line.len(),
                        lock_ptr = ?Arc::as_ptr(&self.read_write_lock),
                        line_preview = %if line.len() > 120 { format!("{}... [truncated]", &line[..120]) } else { line.clone() },
                        "DiskJournalReader read line"
                    );

                    // Inspect header before parsing for diagnostics
                    let mut parts = line.trim_end_matches('\n').splitn(3, ':');
                    let expected_len = parts.next().and_then(|s| s.parse::<usize>().ok());
                    let expected_crc = parts.next().and_then(|s| s.parse::<u32>().ok());
                    let header_present = expected_len.is_some() && expected_crc.is_some();
                    let payload = parts.next().unwrap_or("");
                    let payload_len = payload.len();
                    let crc_ok = expected_crc.map(|crc| {
                        let mut hasher = crc32fast::Hasher::new();
                        hasher.update(payload.as_bytes());
                        hasher.finalize() == crc
                    });

                    match super::disk_journal::parse_framed_record::<T>(&line) {
                        super::disk_journal::ParseOutcome::Complete(record) => {
                            let envelope = EventEnvelope {
                                journal_writer_id: JournalWriterId::from(self.journal_id),
                                vector_clock: record.vector_clock,
                                timestamp: record.timestamp,
                                event: record.event,
                            };

                            self.position += 1;
                            self.partial_retries = 0;
                            self.last_partial_position = None;
                            return Ok(Some(envelope));
                        }
                        super::disk_journal::ParseOutcome::Partial => {
                            if self.last_partial_position == Some(self.position) {
                                self.partial_retries += 1;
                            } else {
                                self.partial_retries = 1;
                                self.last_partial_position = Some(self.position);
                            }

                            if self.partial_retries > 5 {
                                let err_msg = format!(
                                    "Partial read retries exceeded at position {}",
                                    self.position
                                );
                                tracing::error!(
                                    position = self.position,
                                    path = %self.path.display(),
                                    line_len = line.len(),
                                    header_present,
                                    expected_len,
                                    expected_crc,
                                    payload_len,
                                    crc_ok,
                                    lock_ptr = ?Arc::as_ptr(&self.read_write_lock),
                                    partial_retries = self.partial_retries,
                                    "Journal record partial retry budget exceeded"
                                );
                                return Err(JournalError::Implementation {
                                    message: err_msg.clone(),
                                    source: Box::new(std::io::Error::new(
                                        std::io::ErrorKind::UnexpectedEof,
                                        err_msg,
                                    )),
                                });
                            }

                            tracing::info!(
                                position = self.position,
                                path = %self.path.display(),
                                line_len = line.len(),
                                header_present,
                                expected_len,
                                expected_crc,
                                payload_len,
                                crc_ok,
                                lock_ptr = ?Arc::as_ptr(&self.read_write_lock),
                                partial_retries = self.partial_retries,
                                "Journal record appears partial; will retry"
                            );
                            return Ok(None);
                        }
                        super::disk_journal::ParseOutcome::Corrupt(e) => {
                            let line_preview = if line.len() > 200 {
                                format!(
                                    "{}... [truncated, {} bytes total]",
                                    &line[..200],
                                    line.len()
                                )
                            } else {
                                line.clone()
                            };

                            tracing::error!(
                                position = self.position,
                                path = %self.path.display(),
                                line_len = line.len(),
                                header_present,
                                expected_len,
                                expected_crc,
                                payload_len,
                                crc_ok,
                                lock_ptr = ?Arc::as_ptr(&self.read_write_lock),
                                parse_error = %e,
                                line_preview = %line_preview,
                                "Failed to parse journal record"
                            );

                            self.partial_retries = 0;
                            self.last_partial_position = None;
                            return Err(JournalError::Implementation {
                                message: format!(
                                    "Failed to parse journal record at position {}: {}",
                                    self.position, e
                                ),
                                source: Box::new(std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    e,
                                )),
                            });
                        }
                    }
                }
                Err(e) => {
                    return Err(JournalError::Implementation {
                        message: format!(
                            "Failed to read from journal at position {}",
                            self.position
                        ),
                        source: Box::new(e),
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
                Ok(_) => {
                    // Got data - no longer at EOF
                    self.at_end = false;

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
    use chrono::Utc;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::event::JournalEvent;
    use obzenflow_core::event::vector_clock::VectorClock;
    use obzenflow_core::{ChainEvent, EventId, JournalId, StageId, WriterId};
    use crc32fast::Hasher;
    use crate::journal::disk::log_record::LogRecord;
    use std::io::Write;
    use tempfile::NamedTempFile;
    use ulid::Ulid;

    fn write_framed_record<T: JournalEvent>(
        file: &mut NamedTempFile,
        record: &LogRecord<T>,
    ) {
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

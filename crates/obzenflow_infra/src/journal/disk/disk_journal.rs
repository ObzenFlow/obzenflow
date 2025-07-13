//! Single event log per flow execution
//!
//! Provides optimal sequential writes and natural event ordering

use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::vector_clock::{VectorClock, CausalOrderingService};
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_reader::JournalReader;
use async_trait::async_trait;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{RwLock, Mutex};
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, AsyncSeekExt};
use tokio::fs::File;
use std::io::{BufRead, BufReader};
use std::fs::File as StdFile;
use std::collections::HashMap;
use ulid::Ulid;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use std::io::SeekFrom;

use super::disk_journal_reader::DiskJournalReader;
use super::log_record::LogRecord;

/// Single append-only log for a flow execution
///
/// Uses a mutex to ensure atomic writes from multiple writers
pub struct DiskJournal {
    /// Owner of this journal (if any)
    owner: Option<JournalOwner>,
    /// Path to the log file
    path: PathBuf,
    /// Atomic write offset for tracking file position
    write_offset: Arc<AtomicU64>,
    /// In-memory index: event_id -> file offset
    index: Arc<RwLock<HashMap<Ulid, u64>>>,
    /// Write mutex to ensure atomic writes
    write_lock: Arc<Mutex<()>>,
    /// Track vector clocks for each writer
    writer_clocks: Arc<RwLock<HashMap<WriterId, VectorClock>>>,
}

impl DiskJournal {
    /// Create a new flow event log
    pub fn new(base_path: PathBuf, flow_id: &str) -> Result<Self, JournalError> {
        std::fs::create_dir_all(&base_path)
            .map_err(|e| JournalError::Implementation {
                message: "Failed to create directory".to_string(),
                source: Box::new(e),
            })?;
        let log_path = base_path.join(format!("{}.log", flow_id));

        // Get current file size if it exists
        let write_offset = std::fs::metadata(&log_path)
            .map(|m| m.len())
            .unwrap_or(0);

        // Build index if file exists
        let mut index = HashMap::with_capacity(10000);
        let mut writer_clocks = HashMap::new();

        if log_path.exists() {
            // Read through file to rebuild index and writer clocks
            let file = StdFile::open(&log_path)
                .map_err(|e| JournalError::Implementation {
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
                    if let Ok(record) = serde_json::from_str::<LogRecord>(&line) {
                        index.insert(record.event_id, offset);

                        // Update writer clocks
                        if let Ok(writer_id) = WriterId::try_from(record.writer_id.as_str()) {
                            writer_clocks.insert(writer_id, record.vector_clock);
                        }
                    }
                    offset += line.len() as u64 + 1; // +1 for newline
                }
            }
        }

        Ok(Self {
            owner: None,
            path: log_path,
            write_offset: Arc::new(AtomicU64::new(write_offset)),
            index: Arc::new(RwLock::new(index)),
            write_lock: Arc::new(Mutex::new(())),
            writer_clocks: Arc::new(RwLock::new(writer_clocks)),
        })
    }

    /// Create a new flow event log with specified owner
    pub fn with_owner(log_path: PathBuf, owner: JournalOwner) -> Result<Self, JournalError> {
        // Create parent directory if needed
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| JournalError::Implementation {
                    message: "Failed to create directory".to_string(),
                    source: Box::new(e),
                })?;
        }

        // Get current file size if it exists
        let write_offset = std::fs::metadata(&log_path)
            .map(|m| m.len())
            .unwrap_or(0);

        // Build index if file exists
        let mut index = HashMap::with_capacity(10000);
        let mut writer_clocks = HashMap::new();

        if log_path.exists() {
            // Read through file to rebuild index and writer clocks
            let file = StdFile::open(&log_path)
                .map_err(|e| JournalError::Implementation {
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
                    if let Ok(record) = serde_json::from_str::<LogRecord>(&line) {
                        index.insert(record.event_id, offset);

                        // Update writer clocks
                        if let Ok(writer_id) = WriterId::try_from(record.writer_id.as_str()) {
                            writer_clocks.insert(writer_id, record.vector_clock);
                        }
                    }
                    offset += line.len() as u64 + 1; // +1 for newline
                }
            }
        }

        Ok(Self {
            owner: Some(owner),
            path: log_path,
            write_offset: Arc::new(AtomicU64::new(write_offset)),
            index: Arc::new(RwLock::new(index)),
            write_lock: Arc::new(Mutex::new(())),
            writer_clocks: Arc::new(RwLock::new(writer_clocks)),
        })
    }

    /// Read all events from disk (internal helper)
    async fn read_all_raw(&self) -> Result<Vec<EventEnvelope>, JournalError> {
        let mut events = Vec::new();

        if !self.path.exists() {
            return Ok(events);
        }

        let file = File::open(&self.path).await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to open file".to_string(),
                source: Box::new(e),
            })?;
        let reader = tokio::io::BufReader::new(file);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to read line".to_string(),
                source: Box::new(e),
            })? {
            if !line.trim().is_empty() {
                let record: LogRecord = serde_json::from_str(&line)
                    .map_err(|e| JournalError::Implementation {
                        message: "Failed to parse record".to_string(),
                        source: Box::new(e),
                    })?;

                let writer_id = WriterId::try_from(record.writer_id.as_str())
                    .map_err(|_| JournalError::Implementation {
                        message: format!("Invalid writer ID: {}", record.writer_id),
                        source: "Invalid WriterId format".into(),
                    })?;

                events.push(EventEnvelope {
                    writer_id,
                    vector_clock: record.vector_clock,
                    timestamp: record.timestamp,
                    event: record.event,
                });
            }
        }

        Ok(events)
    }
}

impl Clone for DiskJournal {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            path: self.path.clone(),
            write_offset: self.write_offset.clone(),
            index: self.index.clone(),
            write_lock: self.write_lock.clone(),
            writer_clocks: self.writer_clocks.clone(),
        }
    }
}

#[async_trait]
impl Journal for DiskJournal {
    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,
        writer_id: &WriterId,
        event: ChainEvent,
        parent: Option<&EventEnvelope>
    ) -> Result<EventEnvelope, JournalError> {
        // Safety check: Ensure journal has an owner before allowing writes
        if self.owner.is_none() {
            return Err(JournalError::Implementation {
                message: "Cannot write to an unowned journal. Journal must have an owner.".to_string(),
                source: "Unowned journal write attempt".into(),
            });
        }
        // Get or create vector clock for this writer
        let mut vector_clock = {
            let writer_clocks = self.writer_clocks.read().await;
            writer_clocks
                .get(writer_id)
                .cloned()
                .unwrap_or_else(VectorClock::new)
        };

        // Update vector clock based on parent
        if let Some(parent_envelope) = parent {
            CausalOrderingService::update_with_parent(&mut vector_clock, &parent_envelope.vector_clock);
        }

        // Increment for this writer
        CausalOrderingService::increment(&mut vector_clock, &writer_id.to_string());

        // Create envelope
        let envelope = EventEnvelope {
            writer_id: writer_id.clone(),
            vector_clock: vector_clock.clone(),
            timestamp: Utc::now(),
            event: event.clone(),
        };

        // Create log record
        let record = LogRecord {
            event_id: event.id.as_ulid(),
            writer_id: writer_id.to_string(),
            vector_clock: vector_clock.clone(),
            timestamp: envelope.timestamp,
            event,
        };

        // Serialize with newline
        let mut json_line = serde_json::to_string(&record)
            .map_err(|e| JournalError::Implementation {
                message: "Failed to serialize record".to_string(),
                source: Box::new(e),
            })?;
        json_line.push('\n');
        let bytes = json_line.into_bytes();

        // Acquire write lock for atomic write
        let _lock = self.write_lock.lock().await;

        // Get current offset and reserve space
        let offset = self.write_offset.load(Ordering::SeqCst);
        self.write_offset.fetch_add(bytes.len() as u64, Ordering::SeqCst);

        // Write to file
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to open file for append".to_string(),
                source: Box::new(e),
            })?;

        file.write_all(&bytes).await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to write to file".to_string(),
                source: Box::new(e),
            })?;
        file.flush().await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to flush file".to_string(),
                source: Box::new(e),
            })?;

        // Update index
        self.index.write().await.insert(record.event_id, offset);

        // Update writer's clock
        self.writer_clocks.write().await
            .insert(writer_id.clone(), vector_clock);

        Ok(envelope)
    }

    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope>, JournalError> {
        let mut events = self.read_all_raw().await?;

        // Sort by vector clock for causal ordering
        events.sort_by(|a, b| {
            CausalOrderingService::causal_compare(&a.vector_clock, &b.vector_clock)
                .unwrap_or_else(|| {
                    // For concurrent events, use timestamp as tiebreaker
                    a.timestamp.cmp(&b.timestamp)
                })
        });

        Ok(events)
    }

    async fn read_causally_after(&self, after_event_id: &EventId) -> Result<Vec<EventEnvelope>, JournalError> {
        let all_events = self.read_causally_ordered().await?;

        // Find the position of the reference event
        let position = all_events.iter()
            .position(|e| &e.event.id == after_event_id);

        match position {
            Some(pos) => Ok(all_events.into_iter().skip(pos + 1).collect()),
            None => Ok(Vec::new()),
        }
    }

    async fn read_event(&self, event_id: &EventId) -> Result<Option<EventEnvelope>, JournalError> {
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
        let mut file = File::open(&self.path).await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to open file".to_string(),
                source: Box::new(e),
            })?;
        file.seek(SeekFrom::Start(offset)).await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to seek in file".to_string(),
                source: Box::new(e),
            })?;

        let reader = tokio::io::BufReader::new(file);
        let mut lines = reader.lines();

        if let Some(line) = lines.next_line().await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to read line".to_string(),
                source: Box::new(e),
            })? {
            let record: LogRecord = serde_json::from_str(&line)
                .map_err(|e| JournalError::Implementation {
                    message: "Failed to parse record".to_string(),
                    source: Box::new(e),
                })?;

            let writer_id = WriterId::try_from(record.writer_id.as_str())
                .map_err(|_| JournalError::Implementation {
                    message: format!("Invalid writer ID: {}", record.writer_id),
                    source: "Invalid WriterId format".into(),
                })?;

            Ok(Some(EventEnvelope {
                writer_id,
                vector_clock: record.vector_clock,
                timestamp: record.timestamp,
                event: record.event,
            }))
        } else {
            Ok(None)
        }
    }
    
    async fn reader(&self) -> Result<Box<dyn JournalReader>, JournalError> {
        Ok(Box::new(DiskJournalReader::new(self.path.clone()).await?))
    }
    
    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader>, JournalError> {
        Ok(Box::new(DiskJournalReader::from_position(self.path.clone(), position).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_basic_append_and_read() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!("target/test-logs/test_basic_append_and_read_{}", test_id));
        std::fs::create_dir_all(&test_dir).unwrap();
        // Create a test journal with a proper owner
        let test_stage_id = obzenflow_core::StageId::new();
        let owner = obzenflow_core::JournalOwner::stage(test_stage_id);
        let log_path = test_dir.join("test_flow_1.log");
        let log = DiskJournal::with_owner(log_path, owner).unwrap();

        let writer_id = WriterId::new();
        let event = ChainEvent::new(
            EventId::new(),
            writer_id.clone(),
            "test.event",
            serde_json::json!({"data": "test value"})
        );

        // Append event
        let envelope = log.append(&writer_id, event.clone(), None).unwrap();

        // Read back
        let events = log.read_causally_ordered().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.event_type, "test.event");
        assert_eq!(events[0].event.payload["data"], "test value");

        // Read by ID
        let event_by_id = log.read_event(&envelope.event.id).unwrap();
        assert!(event_by_id.is_some());
        assert_eq!(event_by_id.unwrap().event.id, envelope.event.id);
        
        // Cleanup
        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn test_causal_ordering() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!("target/test-logs/test_causal_ordering_{}", test_id));
        std::fs::create_dir_all(&test_dir).unwrap();
        // Create a test journal with a proper owner
        let test_pipeline_id = obzenflow_core::PipelineId::new();
        let owner = obzenflow_core::JournalOwner::pipeline(test_pipeline_id);
        let log_path = test_dir.join("test_flow_2.log");
        let log = DiskJournal::with_owner(log_path, owner).unwrap();

        let writer1 = WriterId::new();
        let writer2 = WriterId::new();

        // First event from writer1
        let event1 = ChainEvent::new(
            EventId::new(),
            writer1.clone(),
            "event.1",
            serde_json::json!({"seq": 1})
        );
        let envelope1 = log.append(&writer1, event1, None).unwrap();

        // Second event from writer2, causally dependent on event1
        let event2 = ChainEvent::new(
            EventId::new(),
            writer2.clone(),
            "event.2",
            serde_json::json!({"seq": 2})
        );
        let envelope2 = log.append(&writer2, event2, Some(&envelope1)).unwrap();

        // Verify vector clocks show causal relationship
        assert!(CausalOrderingService::happened_before(
            &envelope1.vector_clock,
            &envelope2.vector_clock
        ));

        // Read all events
        let events = log.read_causally_ordered().unwrap();
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
        let test_dir = std::path::PathBuf::from(format!("target/test-logs/test_concurrent_writers_{}", test_id));
        std::fs::create_dir_all(&test_dir).unwrap();
        // Create a test journal with a proper owner
        let test_metrics_id = obzenflow_core::MetricsId::new();
        let owner = obzenflow_core::JournalOwner::metrics(test_metrics_id);
        let log_path = test_dir.join("test_flow_3.log");
        let log = Arc::new(DiskJournal::with_owner(log_path, owner).unwrap());

        // Spawn multiple concurrent writers
        let mut handles = vec![];

        for i in 0..5 {
            let log_clone = log.clone();
            let handle = tokio::spawn(async move {
                let writer_id = WriterId::new();
                let event = ChainEvent::new(
                    EventId::new(),
                    writer_id.clone(),
                    "concurrent.event",
                    serde_json::json!({"writer": i})
                );
                log_clone.append(&writer_id, event, None).await
            });
            handles.push(handle);
        }

        // Wait for all writers
        for handle in handles {
            handle.unwrap().unwrap();
        }

        // Verify all events were written
        let events = log.read_causally_ordered().unwrap();
        assert_eq!(events.len(), 5);

        // Verify each event has unique writer
        let writer_ids: std::collections::HashSet<_> =
            events.iter().map(|e| e.writer_id.to_string()).collect();
        assert_eq!(writer_ids.len(), 5);
        
        // Cleanup
        std::fs::remove_dir_all(&test_dir).ok();
    }
}

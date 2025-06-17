//! Single event log per flow execution
//! 
//! Provides optimal sequential writes and natural event ordering

use crate::chain_event::ChainEvent;
use crate::step::Result;
use crate::event_store::{EventEnvelope, VectorClock, WriterId};
use crate::topology::StageId;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{RwLock, Mutex};
use tokio::io::{AsyncWriteExt, AsyncBufReadExt, AsyncSeekExt, BufReader};
use tokio::fs::{File, OpenOptions};
use std::collections::HashMap;
use ulid::Ulid;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use std::io::SeekFrom;

/// Compact log record format
#[derive(Debug, Serialize, Deserialize)]
struct LogRecord {
    event_id: Ulid,
    sequence: u64,
    writer_id: String,  // Keep as string for serialization compatibility
    vector_clock: VectorClock,
    timestamp: DateTime<Utc>,
    event: ChainEvent,
}

/// Single append-only log for a flow execution
/// 
/// Uses a mutex to ensure atomic writes from multiple writers
pub struct FlowEventLog {
    /// Path to the log file
    path: PathBuf,
    /// Atomic write offset for tracking file position
    write_offset: Arc<AtomicU64>,
    /// In-memory index: event_id -> file offset
    index: Arc<RwLock<HashMap<Ulid, u64>>>,
    /// Write mutex to ensure atomic writes
    write_lock: Arc<Mutex<()>>,
}

impl FlowEventLog {
    /// Create a new flow event log
    pub async fn new(base_path: PathBuf, flow_id: &str) -> Result<Self> {
        std::fs::create_dir_all(&base_path)?;
        let log_path = base_path.join(format!("{}.log", flow_id));
        
        // Get current file size
        let write_offset = std::fs::metadata(&log_path)
            .map(|m| m.len())
            .unwrap_or(0);
        
        Ok(Self {
            path: log_path,
            write_offset: Arc::new(AtomicU64::new(write_offset)),
            index: Arc::new(RwLock::new(HashMap::with_capacity(10000))),
            write_lock: Arc::new(Mutex::new(())),
        })
    }
    
    /// Append an event from a specific writer
    /// Each writer opens its own file handle - no shared state!
    pub async fn append(&self, writer_id: &WriterId, sequence: u64, event: ChainEvent, parent: Option<&EventEnvelope>) -> Result<EventEnvelope> {
        // Create vector clock
        let vector_clock = if let Some(parent) = parent {
            let mut clock = parent.vector_clock.clone();
            clock.update(writer_id, &parent.vector_clock);
            clock
        } else {
            let mut clock = VectorClock::new();
            clock.tick(writer_id);
            clock
        };
        
        // Create envelope
        let envelope = EventEnvelope {
            sequence,
            writer_id: writer_id.clone(),
            vector_clock: vector_clock.clone(),
            timestamp: Utc::now(),
            event: event.clone(),
        };
        
        // Create log record
        let record = LogRecord {
            event_id: event.ulid,
            sequence,
            writer_id: writer_id.to_string(),  // String for serialization
            vector_clock,
            timestamp: envelope.timestamp,
            event,
        };
        
        // Serialize with newline included
        let mut json_line = serde_json::to_string(&record)?;
        json_line.push('\n');
        let bytes = json_line.into_bytes();
        
        // Acquire write lock to ensure atomic writes
        let _lock = self.write_lock.lock().await;
        
        // Get current offset and reserve space
        let offset = self.write_offset.load(Ordering::SeqCst);
        self.write_offset.fetch_add(bytes.len() as u64, Ordering::SeqCst);
        
        // Open file in append mode
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)
            .await?;
            
        // Write complete record atomically under lock
        file.write_all(&bytes).await?;
        // Flush to ensure data is written to OS buffer
        file.flush().await?;
        
        // Update index before releasing write lock to prevent read races
        self.index.write().await.insert(envelope.event.ulid, offset);
        
        // Lock released here
        
        Ok(envelope)
    }
    
    /// Read event by ID - O(1) with index
    pub async fn read_event(&self, event_id: &Ulid) -> Result<Option<EventEnvelope>> {
        // Check index
        let index = self.index.read().await;
        let offset = match index.get(event_id) {
            Some(o) => *o,
            None => return Ok(None),
        };
        drop(index);
        
        // Return None if file doesn't exist
        if !self.path.exists() {
            return Ok(None);
        }
        
        // Seek and read
        let mut file = File::open(&self.path).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        
        if let Some(line) = lines.next_line().await? {
            let record: LogRecord = serde_json::from_str(&line)?;
            let writer_id = WriterId::from_string(&record.writer_id)
                .ok_or_else(|| format!("Invalid writer ID in log: {}", record.writer_id))?;
            Ok(Some(EventEnvelope {
                sequence: record.sequence,
                writer_id,
                vector_clock: record.vector_clock,
                timestamp: record.timestamp,
                event: record.event,
            }))
        } else {
            Ok(None)
        }
    }
    
    /// Read all events in order (for replay)
    pub async fn read_all(&self) -> Result<Vec<EventEnvelope>> {
        let mut events = Vec::new();
        
        // Return empty if file doesn't exist yet
        if !self.path.exists() {
            return Ok(events);
        }
        
        let file = File::open(&self.path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        
        while let Some(line) = lines.next_line().await? {
            if !line.trim().is_empty() {
                let record: LogRecord = serde_json::from_str(&line)?;
                let writer_id = WriterId::from_string(&record.writer_id)
                    .ok_or_else(|| format!("Invalid writer ID in log: {}", record.writer_id))?;
                events.push(EventEnvelope {
                    sequence: record.sequence,
                    writer_id,
                    vector_clock: record.vector_clock,
                    timestamp: record.timestamp,
                    event: record.event,
                });
            }
        }
        
        Ok(events)
    }
    
    /// Read events from a specific stage
    pub async fn read_stage_events(&self, stage_id: StageId) -> Result<Vec<EventEnvelope>> {
        let all_events = self.read_all().await?;
        Ok(all_events.into_iter()
            .filter(|e| e.writer_id.stage_id() == stage_id)
            .collect())
    }
}

impl Clone for FlowEventLog {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            write_offset: self.write_offset.clone(),
            index: self.index.clone(),
            write_lock: self.write_lock.clone(),
        }
    }
}
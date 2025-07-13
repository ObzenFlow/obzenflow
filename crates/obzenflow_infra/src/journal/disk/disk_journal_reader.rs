//! Efficient cursor-based reader for DiskJournal
//!
//! Maintains an open file handle and tracks position for O(1) sequential reads.

use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_reader::JournalReader;
use async_trait::async_trait;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::fs::File;
use std::path::PathBuf;

use super::log_record::LogRecord;

/// Efficient reader for DiskJournal that maintains position
pub struct DiskJournalReader {
    /// Buffered reader over the file
    reader: BufReader<File>,
    /// Current position (number of events read)
    position: u64,
    /// Path to the journal file (for error messages)
    path: PathBuf,
    /// Whether we've reached EOF
    at_end: bool,
}

impl DiskJournalReader {
    /// Create a new reader starting from the beginning
    pub async fn new(path: PathBuf) -> Result<Self, JournalError> {
        // If file doesn't exist, create an empty reader at EOF
        if !path.exists() {
            // Create empty file
            let file = File::create(&path).await
                .map_err(|e| JournalError::Implementation {
                    message: format!("Failed to create journal file: {}", path.display()),
                    source: Box::new(e),
                })?;
            
            return Ok(Self {
                reader: BufReader::new(file),
                position: 0,
                path,
                at_end: true,
            });
        }
        
        let file = File::open(&path).await
            .map_err(|e| JournalError::Implementation {
                message: format!("Failed to open journal file: {}", path.display()),
                source: Box::new(e),
            })?;
        
        Ok(Self {
            reader: BufReader::new(file),
            position: 0,
            path,
            at_end: false,
        })
    }
    
    /// Create a new reader starting from a specific position
    pub async fn from_position(path: PathBuf, start_position: u64) -> Result<Self, JournalError> {
        let file = File::open(&path).await
            .map_err(|e| JournalError::Implementation {
                message: format!("Failed to open journal file: {}", path.display()),
                source: Box::new(e),
            })?;
        
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
                        path,
                        at_end: true,
                    });
                }
                Ok(_) => {
                    if !line.trim().is_empty() {
                        position += 1;
                    }
                }
                Err(e) => {
                    return Err(JournalError::Implementation {
                        message: format!("Failed to skip to position {} in journal", start_position),
                        source: Box::new(e),
                    });
                }
            }
        }
        
        Ok(Self {
            reader,
            position,
            path,
            at_end: false,
        })
    }
}

#[async_trait]
impl JournalReader for DiskJournalReader {
    async fn next(&mut self) -> Result<Option<EventEnvelope>, JournalError> {
        // Remove the early return for at_end - we want to retry after EOF
        // to check for new events (like tail -f behavior)
        
        let mut line = String::new();
        
        loop {
            line.clear();
            match self.reader.read_line(&mut line).await {
                Ok(0) => {
                    // EOF reached - but don't permanently set at_end
                    // Just return None for this call, allowing retry on next call
                    self.at_end = true;
                    return Ok(None);
                }
                Ok(_) => {
                    // We got data - no longer at EOF
                    self.at_end = false;
                    
                    // Skip empty lines
                    if line.trim().is_empty() {
                        continue;
                    }
                    
                    // Parse the log record
                    let record: LogRecord = serde_json::from_str(&line)
                        .map_err(|e| JournalError::Implementation {
                            message: format!("Failed to parse journal record at position {}", self.position),
                            source: Box::new(e),
                        })?;
                    
                    // Convert WriterId
                    let writer_id = WriterId::try_from(record.writer_id.as_str())
                        .map_err(|_| JournalError::Implementation {
                            message: format!("Invalid writer ID: {}", record.writer_id),
                            source: "Invalid WriterId format".into(),
                        })?;
                    
                    // Create envelope
                    let envelope = EventEnvelope {
                        writer_id,
                        vector_clock: record.vector_clock,
                        timestamp: record.timestamp,
                        event: record.event,
                    };
                    
                    self.position += 1;
                    return Ok(Some(envelope));
                }
                Err(e) => {
                    return Err(JournalError::Implementation {
                        message: format!("Failed to read from journal at position {}", self.position),
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
    use obzenflow_core::{ChainEvent, EventId, WriterId};
    use std::io::Write;
    use tempfile::NamedTempFile;
    use chrono::Utc;
    use ulid::Ulid;
    
    #[tokio::test]
    async fn test_sequential_reading() {
        // Create a temporary journal file
        let mut temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();
        
        // Write some test records
        let writer_id = WriterId::new();
        for i in 0..5 {
            let record = LogRecord {
                event_id: Ulid::new(),
                writer_id: writer_id.to_string(),
                vector_clock: VectorClock::new(),
                timestamp: Utc::now(),
                event: ChainEvent::new(
                    EventId::new(),
                    writer_id.clone(),
                    "test.event",
                    serde_json::json!({"index": i}),
                ),
            };
            writeln!(temp_file, "{}", serde_json::to_string(&record).unwrap()).unwrap();
        }
        temp_file.flush().unwrap();
        
        // Create reader and read all events
        let mut reader = DiskJournalReader::new(path).await.unwrap();
        
        for i in 0..5 {
            let envelope = reader.next().await.unwrap().expect("Should have event");
            assert_eq!(envelope.event.payload["index"], i);
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
        let writer_id = WriterId::new();
        for i in 0..10 {
            let record = LogRecord {
                event_id: Ulid::new(),
                writer_id: writer_id.to_string(),
                vector_clock: VectorClock::new(),
                timestamp: Utc::now(),
                event: ChainEvent::new(
                    EventId::new(),
                    writer_id.clone(),
                    "test.event",
                    serde_json::json!({"index": i}),
                ),
            };
            writeln!(temp_file, "{}", serde_json::to_string(&record).unwrap()).unwrap();
        }
        temp_file.flush().unwrap();
        
        // Create reader and skip first 5
        let mut reader = DiskJournalReader::new(path.clone()).await.unwrap();
        let skipped = reader.skip(5).await.unwrap();
        assert_eq!(skipped, 5);
        assert_eq!(reader.position(), 5);
        
        // Read next event (should be index 5)
        let envelope = reader.next().await.unwrap().expect("Should have event");
        assert_eq!(envelope.event.payload["index"], 5);
        
        // Create new reader from position 7
        let mut reader2 = DiskJournalReader::from_position(path, 7).await.unwrap();
        assert_eq!(reader2.position(), 7);
        
        // Should read index 7
        let envelope = reader2.next().await.unwrap().expect("Should have event");
        assert_eq!(envelope.event.payload["index"], 7);
    }
}
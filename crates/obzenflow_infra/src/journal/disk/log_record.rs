//! Log record format for disk-based journals
//!
//! Shared between DiskJournal and DiskJournalReader for efficient file-based operations.

use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::vector_clock::VectorClock;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use ulid::Ulid;

/// Compact log record format for disk storage
#[derive(Debug, Serialize, Deserialize)]
pub struct LogRecord {
    pub event_id: Ulid,
    pub writer_id: String,  // Keep as string for serialization compatibility
    pub vector_clock: VectorClock,
    pub timestamp: DateTime<Utc>,
    pub event: ChainEvent,
}
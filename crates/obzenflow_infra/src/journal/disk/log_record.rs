// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Log record format for disk-based journals
//!
//! Shared between DiskJournal and DiskJournalReader for efficient file-based operations.

use chrono::{DateTime, Utc};
use obzenflow_core::event::identity::WriterId;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::id::JournalId;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

/// Compact log record format for disk storage
#[derive(Debug)]
pub struct LogRecord<T: JournalEvent> {
    pub event_id: Ulid,
    pub writer_id: WriterId,
    pub journal_id: JournalId,
    pub vector_clock: VectorClock,
    pub timestamp: DateTime<Utc>,
    pub event: T,
}

// Manual implementations to avoid serde trait bound issues
impl<T> Serialize for LogRecord<T>
where
    T: JournalEvent,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("LogRecord", 6)?;
        state.serialize_field("event_id", &self.event_id)?;
        state.serialize_field("writer_id", &self.writer_id)?;
        state.serialize_field("journal_id", &self.journal_id)?;
        state.serialize_field("vector_clock", &self.vector_clock)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("event", &self.event)?;
        state.end()
    }
}

impl<'de, T> Deserialize<'de> for LogRecord<T>
where
    T: JournalEvent,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct LogRecordData<T> {
            event_id: Ulid,
            writer_id: WriterId,
            journal_id: JournalId,
            vector_clock: VectorClock,
            timestamp: DateTime<Utc>,
            event: T,
        }

        let data = LogRecordData::<T>::deserialize(deserializer)?;
        Ok(LogRecord {
            event_id: data.event_id,
            writer_id: data.writer_id,
            journal_id: data.journal_id,
            vector_clock: data.vector_clock,
            timestamp: data.timestamp,
            event: data.event,
        })
    }
}

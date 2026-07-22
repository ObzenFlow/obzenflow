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

pub const RECORD_FRAME_KIND: &str = "record_v2";
pub const ATOMIC_GROUP_FRAME_KIND: &str = "atomic_group_v2";

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

/// One physical journal-format-v2 disk frame. Every frame uses the same tagged
/// envelope: ordinary appends carry one record and atomic groups carry all
/// members that become visible at the envelope's single commit marker.
#[derive(Debug)]
pub(crate) enum LogFrame<T: JournalEvent> {
    Record(LogRecord<T>),
    AtomicGroup {
        group_id: String,
        records: Vec<LogRecord<T>>,
    },
}

pub(crate) fn serialize_record<T: JournalEvent>(
    record: &LogRecord<T>,
) -> Result<Vec<u8>, serde_json::Error> {
    use serde::ser::SerializeStruct;

    struct RecordFrame<'a, T: JournalEvent>(&'a LogRecord<T>);
    impl<T: JournalEvent> Serialize for RecordFrame<'_, T> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut state = serializer.serialize_struct("LogRecordFrame", 2)?;
            state.serialize_field("frame_kind", RECORD_FRAME_KIND)?;
            state.serialize_field("record", self.0)?;
            state.end()
        }
    }

    serde_json::to_vec(&RecordFrame(record))
}

impl<T: JournalEvent> LogFrame<T> {
    pub(crate) fn into_records(self) -> Vec<LogRecord<T>> {
        match self {
            Self::Record(record) => vec![record],
            Self::AtomicGroup { group_id, records } => {
                debug_assert!(!group_id.is_empty());
                records
            }
        }
    }
}

pub(crate) fn serialize_atomic_group<T: JournalEvent>(
    group_id: &str,
    records: &[LogRecord<T>],
) -> Result<Vec<u8>, serde_json::Error> {
    use serde::ser::{SerializeSeq, SerializeStruct};

    struct Records<'a, T: JournalEvent>(&'a [LogRecord<T>]);
    impl<T: JournalEvent> Serialize for Records<'_, T> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
            for record in self.0 {
                sequence.serialize_element(record)?;
            }
            sequence.end()
        }
    }

    struct Group<'a, T: JournalEvent> {
        group_id: &'a str,
        records: &'a [LogRecord<T>],
    }
    impl<T: JournalEvent> Serialize for Group<'_, T> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut state = serializer.serialize_struct("AtomicLogGroup", 3)?;
            state.serialize_field("frame_kind", ATOMIC_GROUP_FRAME_KIND)?;
            state.serialize_field("group_id", self.group_id)?;
            state.serialize_field("records", &Records(self.records))?;
            state.end()
        }
    }

    serde_json::to_vec(&Group { group_id, records })
}

pub(crate) fn deserialize_frame<T: JournalEvent>(
    body: &[u8],
) -> Result<LogFrame<T>, serde_json::Error> {
    let value: serde_json::Value = serde_json::from_slice(body)?;
    let frame_kind = value
        .get("frame_kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            <serde_json::Error as serde::de::Error>::custom(
                "journal v2 frame requires a frame_kind",
            )
        })?;

    if frame_kind == RECORD_FRAME_KIND {
        #[derive(Deserialize)]
        struct RecordData<T> {
            record: T,
        }
        let record: RecordData<LogRecord<T>> = serde_json::from_value(value)?;
        return Ok(LogFrame::Record(record.record));
    }

    if frame_kind != ATOMIC_GROUP_FRAME_KIND {
        return Err(<serde_json::Error as serde::de::Error>::custom(format!(
            "unknown journal v2 frame kind '{frame_kind}'"
        )));
    }

    #[derive(Deserialize)]
    struct AtomicGroupData<T> {
        group_id: String,
        records: Vec<T>,
    }

    let group: AtomicGroupData<LogRecord<T>> = serde_json::from_value(value)?;
    if group.group_id.is_empty() || group.records.is_empty() {
        return Err(<serde_json::Error as serde::de::Error>::custom(
            "atomic journal group requires a non-empty id and at least one record",
        ));
    }
    Ok(LogFrame::AtomicGroup {
        group_id: group.group_id,
        records: group.records,
    })
}

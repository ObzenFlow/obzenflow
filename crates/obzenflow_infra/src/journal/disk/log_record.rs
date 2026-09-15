// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Log record format for disk-based journals
//!
//! Shared between DiskJournal and DiskJournalReader for efficient file-based operations.

use obzenflow_core::event::event_envelope::JournalGroupMember;
use obzenflow_core::event::journal_record::{JournalPayload, JournalRecord};
use obzenflow_core::event::JournalEvent;
use serde::{Deserialize, Serialize};

pub const RECORD_FRAME_KIND: &str = "record_v2";
pub const ATOMIC_GROUP_FRAME_KIND: &str = "atomic_group_v2";

pub type LogRecord<T> = JournalRecord<<T as JournalEvent>::Payload>;

/// One physical journal-format-v2 disk frame. Every frame uses the same tagged
/// envelope: ordinary appends carry one record and atomic groups carry all
/// members that become visible at the envelope's single commit marker.
#[derive(Debug)]
// These transient scanner values keep the existing inline record handoff;
// boxing would add an allocation to every ordinary journal read.
#[allow(clippy::large_enum_variant)]
pub(crate) enum LogFrame<T: JournalEvent> {
    Record(LogRecord<T>),
    AtomicGroup {
        group_id: String,
        records: Vec<LogRecord<T>>,
    },
}

pub(crate) fn serialize_record<P: JournalPayload>(
    record: &JournalRecord<P>,
) -> Result<Vec<u8>, serde_json::Error> {
    use serde::ser::SerializeStruct;

    struct RecordFrame<'a, P: JournalPayload>(&'a JournalRecord<P>);
    impl<P: JournalPayload> Serialize for RecordFrame<'_, P> {
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

    serde_json::to_vec(&RecordFrame::<P>(record))
}

impl<T: JournalEvent> LogFrame<T> {
    pub(crate) fn group_id(&self) -> Option<&str> {
        match self {
            Self::Record(_) => None,
            Self::AtomicGroup { group_id, .. } => Some(group_id),
        }
    }

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

pub(crate) fn serialize_atomic_group<P: JournalPayload>(
    group_id: &str,
    records: &[JournalRecord<P>],
) -> Result<Vec<u8>, serde_json::Error> {
    use serde::ser::{SerializeSeq, SerializeStruct};

    struct Records<'a, P: JournalPayload>(&'a [JournalRecord<P>]);
    impl<P: JournalPayload> Serialize for Records<'_, P> {
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

    struct Group<'a, P: JournalPayload> {
        group_id: &'a str,
        records: &'a [JournalRecord<P>],
    }
    impl<P: JournalPayload> Serialize for Group<'_, P> {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let mut state = serializer.serialize_struct("AtomicLogGroup", 3)?;
            state.serialize_field("frame_kind", ATOMIC_GROUP_FRAME_KIND)?;
            state.serialize_field("group_id", self.group_id)?;
            state.serialize_field("records", &Records::<P>(self.records))?;
            state.end()
        }
    }

    serde_json::to_vec(&Group::<P> { group_id, records })
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
        let provenance = &record.record.envelope.provenance.journal;
        if provenance.journal_group_id.is_some() || provenance.journal_group_member.is_some() {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "single frame has atomic membership",
            ));
        }
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
    let size = u32::try_from(group.records.len())
        .map_err(|_| <serde_json::Error as serde::de::Error>::custom("atomic group too large"))?;
    for (index, record) in group.records.iter().enumerate() {
        let provenance = &record.envelope.provenance.journal;
        if provenance.journal_group_id.as_deref() != Some(group.group_id.as_str())
            || provenance.journal_group_member
                != Some(JournalGroupMember {
                    index: index as u32,
                    size,
                })
        {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "record membership disagrees with physical atomic frame",
            ));
        }
    }
    Ok(LogFrame::AtomicGroup {
        group_id: group.group_id,
        records: group.records,
    })
}

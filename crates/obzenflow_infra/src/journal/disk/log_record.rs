// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Logical handoff shared by all current-schema disk readers.

use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::JournalEvent;

pub type LogRecord<T> = JournalRecord<<T as JournalEvent>::Payload>;

/// All members of a group become visible at one physical commit trailer.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum LogFrame<T: JournalEvent> {
    Record(LogRecord<T>),
    AtomicGroup {
        group_id: String,
        records: Vec<LogRecord<T>>,
    },
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
            Self::AtomicGroup { records, .. } => records,
        }
    }
}

/// Fixtures use the production codec, with complete local definitions so they
/// remain independent of any other test archive or process cache.
#[cfg(test)]
pub(crate) fn serialize_record<P: obzenflow_core::event::payloads::JournalPayload>(
    record: &JournalRecord<P>,
) -> Result<Vec<u8>, super::codec::Error> {
    Ok(super::codec::prepare(
        std::slice::from_ref(record),
        None,
        std::path::Path::new("fixture.log"),
        super::codec::DefinitionStore::default(),
    )?
    .bytes)
}

#[cfg(test)]
pub(crate) fn serialize_atomic_group<P: obzenflow_core::event::payloads::JournalPayload>(
    group: &str,
    records: &[JournalRecord<P>],
) -> Result<Vec<u8>, super::codec::Error> {
    Ok(super::codec::prepare(
        records,
        Some(group),
        std::path::Path::new("fixture.log"),
        super::codec::DefinitionStore::default(),
    )?
    .bytes)
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Private format-4 storage adapter for the current Core provenance schema.
//! See README.md for the wire contract and scalar-preservation invariants.

mod definitions;
mod deserialize;
pub(crate) mod frame;
#[cfg(test)]
mod performance_tests;
mod primitives;
mod schema;
mod serialize;
#[cfg(test)]
mod test_data;
#[cfg(test)]
mod tests;
mod values;

pub(crate) use definitions::DefinitionStore;
use definitions::{ReadTable, WriteTable};
use obzenflow_core::event::event_envelope::JournalGroupMember;
use obzenflow_core::event::journal_record::{JournalPayload, JournalRecord};
use obzenflow_core::event::JournalEvent;
use primitives::{bytes, text, unsigned, Cursor};
use schema::{Kind, Shape};
use std::path::{Path, PathBuf};

use super::log_record::{LogFrame, LogRecord};

#[derive(Debug, thiserror::Error)]
pub(crate) enum Error {
    #[error("invalid compact journal: {0}")]
    Invalid(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
type Result<T> = std::result::Result<T, Error>;
fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

/// Pending definitions cannot be published by a caller without consuming the
/// prepared append after its physical commit. Dropping a failed append is inert.
pub(crate) struct PreparedFrame {
    pub(crate) bytes: Vec<u8>,
    definitions: WriteTable,
}

impl PreparedFrame {
    pub(crate) fn commit(self, offset: u64) {
        self.definitions.commit(offset);
    }
}

pub(crate) fn prepare<P: JournalPayload>(
    records: &[JournalRecord<P>],
    group: Option<&str>,
    path: &Path,
    store: DefinitionStore,
) -> Result<PreparedFrame> {
    validate_membership(records, group)?;
    let mut definitions = WriteTable::new(store, path)?;
    let mut content = Vec::new();
    match group {
        None => content.push(0),
        Some(group) => {
            content.push(1);
            text(group, &mut content);
            unsigned(records.len() as u64, &mut content);
        }
    }
    for record in records {
        definitions.begin_record();
        record.payload.validate(&record.envelope.provenance.event)?;
        let mut provenance = Vec::new();
        serialize::write(
            Kind::Struct(Shape::Provenance),
            &record.envelope.provenance,
            None,
            &mut provenance,
            &mut definitions,
        )?;
        bytes(&provenance, &mut content);
        match record.envelope.observability.as_ref() {
            None => content.push(0),
            Some(observation) => {
                content.push(2);
                let mut encoded = Vec::new();
                serialize::write(
                    Kind::Struct(Shape::Observation),
                    observation,
                    None,
                    &mut encoded,
                    &mut definitions,
                )?;
                bytes(&encoded, &mut content);
            }
        }
        bytes(&serde_json::to_vec(&record.payload)?, &mut content);
    }
    let mut body = Vec::new();
    definitions.encode(&mut body);
    body.extend_from_slice(&content);
    Ok(PreparedFrame {
        bytes: frame::encode(&body),
        definitions,
    })
}

pub(crate) struct Decoder {
    path: PathBuf,
    store: DefinitionStore,
}

/// Exact disjoint byte attribution. Shared framing/definitions stay visible;
/// callers may amortise them but cannot omit them from an envelope budget.
#[derive(Default, Debug, Clone, serde::Serialize)]
pub(crate) struct FrameSizes {
    pub(crate) provenance: usize,
    pub(crate) observability: usize,
    pub(crate) payload: usize,
    pub(crate) shared: usize,
    pub(crate) records: usize,
    pub(crate) packets: usize,
    pub(crate) inline_definition_counts: [usize; 7],
    pub(crate) inline_definition_bytes: [usize; 7],
    pub(crate) definition_reference_bytes: usize,
}

impl Decoder {
    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn cache_stats(&self) -> definitions::StoreStats {
        self.store.stats()
    }
    pub(crate) fn new(path: &Path) -> Self {
        Self {
            path: path.into(),
            store: DefinitionStore::for_archive(path),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn cold(path: &Path) -> Self {
        Self {
            path: path.into(),
            store: DefinitionStore::default(),
        }
    }

    pub(crate) fn decode<T: JournalEvent>(
        &mut self,
        body: &[u8],
        offset: u64,
    ) -> Result<LogFrame<T>> {
        self.decode_measured(body, offset).map(|(frame, _)| frame)
    }

    pub(crate) fn decode_measured<T: JournalEvent>(
        &mut self,
        body: &[u8],
        offset: u64,
    ) -> Result<(LogFrame<T>, FrameSizes)> {
        let mut sizes = FrameSizes::default();
        let mut input = Cursor::new(body);
        let mut definitions = ReadTable::new(&mut input, &self.store, &self.path, offset)?;
        let (group, count) = match input.byte()? {
            0 => (None, 1),
            1 => {
                let group = input.text()?;
                (Some(group), values::bounded_count(&mut input)?)
            }
            _ => return Err(invalid("unknown frame kind")),
        };
        let mut records = Vec::new();
        for _ in 0..count {
            definitions.begin_record();
            sizes.records += 1;
            let start = input.position();
            definitions.section(1);
            let mut provenance_input = Cursor::new(input.bytes()?);
            let provenance = deserialize::read::<
                obzenflow_core::event::provenance::Provenance<
                    <T::Payload as JournalPayload>::Provenance,
                >,
            >(
                Kind::Struct(Shape::Provenance),
                &mut provenance_input,
                &mut definitions,
            )?;
            provenance_input.finish()?;
            sizes.provenance += input.position() - start;
            let start = input.position();
            definitions.section(2);
            let observability = match input.byte()? {
                0 | 1 => None,
                2 => {
                    sizes.packets += 1;
                    let mut observation_input = Cursor::new(input.bytes()?);
                    let observation = deserialize::read(
                        Kind::Struct(Shape::Observation),
                        &mut observation_input,
                        &mut definitions,
                    )?;
                    observation_input.finish()?;
                    Some(observation)
                }
                _ => return Err(invalid("unknown observation presence tag")),
            };
            if observability.is_some() {
                sizes.observability += input.position() - start;
            }
            let start = input.position();
            let payload: serde_json::Value = serde_json::from_slice(input.bytes()?)?;
            sizes.payload += input.position() - start;
            let payload = T::Payload::decode(&provenance.event, payload)?;
            payload.validate(&provenance.event)?;
            let record: LogRecord<T> = JournalRecord {
                envelope: obzenflow_core::event::provenance::EventEnvelope {
                    provenance,
                    observability,
                },
                payload,
            };
            records.push(record);
        }
        input.finish()?;
        let (provenance, observations) = definitions.attributed_bytes();
        (
            sizes.inline_definition_counts,
            sizes.inline_definition_bytes,
            sizes.definition_reference_bytes,
        ) = definitions.definition_costs();
        sizes.provenance += provenance;
        sizes.observability += observations;
        sizes.shared = body.len() + frame::HEADER_LEN + frame::TRAILER_LEN
            - sizes.provenance
            - sizes.observability
            - sizes.payload;
        validate_membership(&records, group.as_deref())?;
        Ok((
            match group {
                Some(group_id) => LogFrame::AtomicGroup { group_id, records },
                None => LogFrame::Record(
                    records
                        .pop()
                        .ok_or_else(|| invalid("empty ordinary frame"))?,
                ),
            },
            sizes,
        ))
    }
}

fn validate_membership<P: JournalPayload>(
    records: &[JournalRecord<P>],
    group: Option<&str>,
) -> Result<()> {
    let size = u32::try_from(records.len()).map_err(|_| invalid("atomic group too large"))?;
    match group {
        None if records.len() != 1 => {
            return Err(invalid("ordinary frame requires exactly one record"))
        }
        Some(group) if group.is_empty() || records.is_empty() => {
            return Err(invalid("atomic group requires a non-empty id and members"))
        }
        _ => {}
    }
    for (index, record) in records.iter().enumerate() {
        let journal = &record.envelope.provenance.journal;
        let member = group.map(|_| JournalGroupMember {
            index: index as u32,
            size,
        });
        if journal.journal_group_id.as_deref() != group || journal.journal_group_member != member {
            return Err(invalid("record membership disagrees with physical frame"));
        }
    }
    Ok(())
}

/// Check that a referenced metadata table belongs to an ordinary record/group
/// carrier. Skip its length-delimited records without chasing their references:
/// resolving metadata never expands an ancestor record or another definition.
fn validate_carrier(input: &mut Cursor<'_>) -> Result<()> {
    let count = match input.byte()? {
        0 => 1,
        1 => {
            if input.text()?.is_empty() {
                return Err(invalid("empty carrier group id"));
            }
            let count = values::bounded_count(input)?;
            if count == 0 {
                return Err(invalid("empty carrier group"));
            }
            count
        }
        _ => return Err(invalid("definition carrier is not a record/group frame")),
    };
    for _ in 0..count {
        if input.bytes()?.is_empty() {
            return Err(invalid("empty carrier provenance"));
        }
        match input.byte()? {
            0 | 1 => {}
            2 => {
                if input.bytes()?.is_empty() {
                    return Err(invalid("empty carrier observation"));
                }
            }
            _ => return Err(invalid("invalid carrier observation tag")),
        }
        if input.bytes()?.is_empty() {
            return Err(invalid("empty carrier payload"));
        }
    }
    if input.remaining() != 0 {
        return Err(invalid("trailing carrier bytes"));
    }
    Ok(())
}

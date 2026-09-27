// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Private current-schema storage adapter for the current Core provenance schema.
//! See README.md for the wire contract and scalar-preservation invariants.

mod definitions;
#[cfg(feature = "test-support")]
pub(crate) mod benchmark;
mod deserialize;
pub(crate) mod frame;
mod layout;
#[cfg(test)]
mod performance_tests;
mod primitives;
mod routing;
#[cfg(test)]
mod selection_tests;
mod serialize;
#[cfg(test)]
mod test_data;
#[cfg(test)]
mod tests;
mod values;

pub(crate) use definitions::DefinitionStore;
use definitions::{ReadTable, WriteTable};
use layout::{Kind, Layout};
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::payloads::JournalPayload;
use obzenflow_core::event::provenance::JournalGroupMember;
use obzenflow_core::event::JournalEvent;
use primitives::{bytes, Cursor};
use std::path::{Path, PathBuf};

use super::log_record::{LogFrame, LogRecord};

#[derive(serde::Serialize)]
struct BodyProvenanceRef<'a, E> {
    event: &'a E,
    vector_clock: &'a obzenflow_core::event::vector_clock::VectorClock,
    causal: WitnessesRef<'a>,
    timestamp: &'a chrono::DateTime<chrono::Utc>,
}

#[derive(serde::Serialize)]
struct WitnessesRef<'a> {
    witnesses: &'a [obzenflow_core::event::CommittedCausalRef],
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct BodyProvenance<E> {
    event: E,
    vector_clock: obzenflow_core::event::vector_clock::VectorClock,
    causal: obzenflow_core::event::CausalWitnesses,
    timestamp: chrono::DateTime<chrono::Utc>,
}

fn read_provenance<E: serde::de::DeserializeOwned, const MEASURE: bool>(
    bytes: &[u8],
    definitions: &mut ReadTable<'_, MEASURE>,
) -> Result<BodyProvenance<E>> {
    let mut input = Cursor::new(bytes);
    let stored: BodyProvenance<E> =
        deserialize::read(Kind::Struct(Layout::RecordBody), &mut input, definitions)?;
    input.finish()?;
    if stored.causal.previous.is_some() {
        return Err(invalid("predecessor must appear only in routing metadata"));
    }
    Ok(stored)
}

fn read_payload<P: JournalPayload>(provenance: &P::Provenance, bytes: &[u8]) -> Result<P> {
    if bytes.len() > obzenflow_core::journal::limits::MAX_RECORD_BYTES {
        return Err(invalid("payload byte budget exceeded"));
    }
    #[cfg(feature = "bench-instrumentation")]
    obzenflow_core::benchmark::payload_decode(provenance);
    let payload: serde_json::Value = serde_json::from_slice(bytes)?;
    let payload = P::decode(provenance, payload)?;
    payload.validate(provenance)?;
    Ok(payload)
}

pub(crate) struct SelectedFrame<T: JournalEvent> {
    pub records: Vec<JournalRecord<T::Payload>>,
    pub routing: routing::RouteSummary,
}

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
    obzenflow_core::journal::limits::validate_group(records)
        .map_err(|error| invalid(error.to_string()))?;
    let mut definitions = WriteTable::new(store, path)?;
    let mut content = Vec::new();
    let mut lengths = Vec::with_capacity(records.len());
    for record in records {
        let member_start = content.len();
        definitions.begin_record();
        record.payload.validate(&record.envelope.provenance.event)?;
        let mut provenance = Vec::new();
        serialize::write(
            Kind::Struct(Layout::RecordBody),
            &BodyProvenanceRef {
                event: &record.envelope.provenance.event,
                vector_clock: &record.envelope.provenance.journal.vector_clock,
                causal: WitnessesRef {
                    witnesses: &record.envelope.provenance.journal.causal.witnesses,
                },
                timestamp: &record.envelope.provenance.journal.timestamp,
            },
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
                    Kind::Struct(Layout::Observation),
                    observation,
                    None,
                    &mut encoded,
                    &mut definitions,
                )?;
                bytes(&encoded, &mut content);
            }
        }
        bytes(&serde_json::to_vec(&record.payload)?, &mut content);
        lengths.push(content.len() - member_start);
    }
    let mut body = routing::encode(records, group, &lengths)?;
    let mut table = Vec::new();
    definitions.encode(&mut table);
    bytes(&table, &mut body);
    body.extend_from_slice(&content);
    if body.len() > obzenflow_core::journal::limits::MAX_GROUP_BYTES {
        return Err(invalid("frame byte budget exceeded"));
    }
    Ok(PreparedFrame {
        bytes: frame::encode(&body),
        definitions,
    })
}

#[derive(Clone)]
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
        self.decode_inner::<T, false>(body, offset, false)
            .map(|(frame, _)| Self::full_frame(frame))
    }

    pub(crate) fn decode_selected<T: JournalEvent>(
        &mut self,
        body: &[u8],
        offset: u64,
    ) -> Result<SelectedFrame<T>> {
        self.decode_inner::<T, false>(body, offset, true)
            .map(|(frame, _)| frame)
    }

    fn full_frame<T: JournalEvent>(mut frame: SelectedFrame<T>) -> LogFrame<T> {
        match frame.routing.group {
            Some(group_id) => LogFrame::AtomicGroup {
                group_id,
                records: frame.records,
            },
            None => LogFrame::Record(frame.records.pop().expect("validated ordinary member")),
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn decode_measured<T: JournalEvent>(
        &mut self,
        body: &[u8],
        offset: u64,
    ) -> Result<(LogFrame<T>, FrameSizes)> {
        self.decode_inner::<T, true>(body, offset, false)
            .map(|(frame, sizes)| (Self::full_frame(frame), sizes))
    }

    // Both modes use identical parsing and validation. Byte attribution is a
    // diagnostic cost, not part of a normal journal read.
    fn decode_inner<T: JournalEvent, const MEASURE: bool>(
        &mut self,
        body: &[u8],
        offset: u64,
        selective: bool,
    ) -> Result<(SelectedFrame<T>, FrameSizes)> {
        let mut sizes = FrameSizes::default();
        let envelope = routing::Envelope::parse(body)?;
        // Business-only frames need neither definition materialisation nor a decoder.
        if selective && envelope.members.iter().all(|member| !member.candidate) {
            return Ok((
                SelectedFrame {
                    records: Vec::new(),
                    routing: envelope.summary,
                },
                sizes,
            ));
        }
        let mut table = Cursor::new(envelope.definitions);
        let mut definitions =
            ReadTable::<MEASURE>::new(&mut table, &self.store, &self.path, offset)?;
        table.finish()?;
        let mut decoded_bytes = 0usize;
        let mut records = Vec::new();
        let mut previous = envelope.summary.previous;
        for (index, member) in envelope.members.iter().enumerate() {
            let reference = obzenflow_core::event::CommittedCausalRef {
                event_id: member.id,
                sequence: envelope.summary.first.sequence + index as u64,
                ..envelope.summary.first
            };
            let predecessor = previous;
            previous = Some(reference);
            if selective && !member.candidate {
                continue;
            }
            let mut input = Cursor::new(member.body);
            definitions.begin_record();
            if MEASURE {
                sizes.records += 1;
            }
            let start = input.position();
            definitions.section(1);
            let stored = read_provenance::<<T::Payload as JournalPayload>::Provenance, MEASURE>(
                input.bytes()?, &mut definitions,
            )?;
            let provenance = obzenflow_core::event::provenance::Provenance {
                event: stored.event,
                journal: obzenflow_core::event::provenance::JournalProvenance {
                    run_id: reference.run_id,
                    journal_writer_id: reference.journal_writer_id,
                    causal: obzenflow_core::event::CausalWitnesses {
                        previous: predecessor,
                        witnesses: stored.causal.witnesses,
                    },
                    vector_clock: stored.vector_clock,
                    timestamp: stored.timestamp,
                    journal_group_id: envelope.summary.group.clone(),
                    journal_group_member: envelope.summary.group.as_ref().map(|_| {
                        JournalGroupMember {
                            index: index as u32,
                            size: envelope.summary.count as u32,
                        }
                    }),
                },
            };
            if MEASURE {
                sizes.provenance += input.position() - start;
            }
            let start = input.position();
            definitions.section(2);
            let observability = match input.byte()? {
                0 | 1 => None,
                2 => {
                    if MEASURE {
                        sizes.packets += 1;
                    }
                    let mut observation_input = Cursor::new(input.bytes()?);
                    let observation = deserialize::read(
                        Kind::Struct(Layout::Observation),
                        &mut observation_input,
                        &mut definitions,
                    )?;
                    observation_input.finish()?;
                    Some(observation)
                }
                _ => return Err(invalid("unknown observation presence tag")),
            };
            if MEASURE && observability.is_some() {
                sizes.observability += input.position() - start;
            }
            let start = input.position();
            let payload_bytes = input.bytes()?;
            let payload = read_payload::<T::Payload>(&provenance.event, payload_bytes)?;
            if MEASURE {
                sizes.payload += input.position() - start;
            }
            if payload.is_supervision_candidate() != member.candidate {
                return Err(invalid("supervision classification disagrees with payload"));
            }
            let record: LogRecord<T> = JournalRecord::from_parts(
                obzenflow_core::event::envelope::EventEnvelope {
                    provenance,
                    observability,
                },
                payload,
            );
            if *record.id() != reference.event_id || record.local_sequence() != reference.sequence {
                return Err(invalid("record disagrees with routing commitment"));
            }
            decoded_bytes += obzenflow_core::journal::limits::record_bytes(&record)?;
            if decoded_bytes > obzenflow_core::journal::limits::MAX_GROUP_BYTES {
                return Err(invalid("decoded frame byte budget exceeded"));
            }
            records.push(record);
            input.finish()?;
        }
        if MEASURE {
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
        }
        Ok((
            SelectedFrame {
                records,
                routing: envelope.summary,
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

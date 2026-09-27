// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Development access to the same codec operations used by normal readers.
use super::*;
use obzenflow_core::event::provenance::ChainEventProvenance;
use obzenflow_core::event::{CausalWitnesses, ChainPayload, VectorClock};

pub struct ReconstructionInput {
    path: PathBuf,
    bytes: Vec<u8>,
    offset: u64,
    event: ChainEventProvenance,
    store: DefinitionStore,
}

pub struct MetadataValue {
    pub event: ChainEventProvenance,
    pub clock: VectorClock,
    pub causal: CausalWitnesses,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

pub struct MemberRead<'a> {
    definitions: ReadTable<'a, false>,
    provenance: &'a [u8],
    payload: &'a [u8],
    event: &'a ChainEventProvenance,
}

impl ReconstructionInput {
    pub(crate) fn new(path: PathBuf, bytes: Vec<u8>, offset: u64, event: ChainEventProvenance) -> Self {
        Self { store: DefinitionStore::for_archive(&path), path, bytes, offset, event }
    }

    /// Untimed prerequisites for the individual member operations. Complete-frame
    /// benchmarks independently include framing and definition-table setup.
    pub fn member(&self) -> MemberRead<'_> {
        let body = frame::validate(&self.bytes).unwrap();
        let envelope = routing::Envelope::parse(body).unwrap();
        assert_eq!(envelope.members.len(), 1);
        let mut definitions = Cursor::new(envelope.definitions);
        let mut table = ReadTable::new(&mut definitions, &self.store, &self.path, self.offset).unwrap();
        definitions.finish().unwrap();
        table.begin_record();
        let mut fields = Cursor::new(envelope.members[0].body);
        let provenance = fields.bytes().unwrap();
        match fields.byte().unwrap() {
            0 | 1 => {},
            2 => { fields.bytes().unwrap(); },
            _ => unreachable!(),
        }
        let payload = fields.bytes().unwrap();
        fields.finish().unwrap();
        MemberRead { definitions: table, provenance, payload, event: &self.event }
    }

    pub fn verify_and_route(&self) -> usize {
        let body = frame::validate(&self.bytes).unwrap();
        let envelope = routing::Envelope::parse(body).unwrap();
        std::hint::black_box(envelope.summary.count)
    }
}

impl MemberRead<'_> {
    pub fn metadata(&mut self) -> MetadataValue {
        let stored: BodyProvenance<ChainEventProvenance> =
            read_provenance(self.provenance, &mut self.definitions).unwrap();
        MetadataValue { event: stored.event, clock: stored.vector_clock, causal: stored.causal, timestamp: stored.timestamp }
    }
    pub fn payload(&self) -> ChainPayload {
        read_payload::<ChainPayload>(self.event, self.payload).unwrap()
    }
}

/// Stateful encoding of a fresh prefix using the real preparation and definition
/// publication code. A new cursor is required for every measured iteration.
pub struct EncodingCursor {
    path: PathBuf,
    store: DefinitionStore,
    offset: u64,
}
impl EncodingCursor {
    pub fn new(path: PathBuf) -> Self {
        Self { path, store: DefinitionStore::default(), offset: 0 }
    }
    pub fn encode(&mut self, rows: &[JournalRecord<ChainPayload>], group: Option<&str>) -> Vec<u8> {
        let mut prepared = prepare(rows, group, &self.path, self.store.clone()).unwrap();
        let bytes = std::mem::take(&mut prepared.bytes);
        prepared.commit(self.offset);
        self.offset += bytes.len() as u64;
        bytes
    }
}

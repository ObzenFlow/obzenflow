// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The manifest-4 record contract. Payload interpretation follows the descriptor;
//! arbitrary application JSON is never used as an untagged decoder fallback.

use super::envelope::{AuthoredEnvelope, EventEnvelope};
use super::journal_event::JournalEvent;
use super::payloads::{ChainPayload, JournalPayload, SystemPayload};
use super::provenance::{AuthoredProvenance, JournalProvenance, Provenance, RecordProvenance};
use crate::event::CorrelationId;
use crate::{AdmissionSeq, EventId, JournalWriterId, WriterId};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::ops::{Deref, DerefMut};

/// Record data plus an ephemeral admission capability. Only the core journal
/// ports admit records after successful storage operations. Decoding and public
/// constructors produce unadmitted data; any mutable access revokes admission.
#[derive(Debug, Clone)]
pub struct JournalRecord<P: JournalPayload> {
    data: JournalRecordData<P>,
    admitted: bool,
}

#[derive(Debug, Clone)]
pub struct JournalRecordData<P: JournalPayload> {
    pub envelope: EventEnvelope<P::Provenance>,
    pub payload: P,
}

impl<P: JournalPayload> Deref for JournalRecord<P> {
    type Target = JournalRecordData<P>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<P: JournalPayload> DerefMut for JournalRecord<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.admitted = false;
        &mut self.data
    }
}

impl<P: JournalPayload> JournalRecord<P> {
    pub fn from_parts(envelope: EventEnvelope<P::Provenance>, payload: P) -> Self {
        Self {
            data: JournalRecordData { envelope, payload },
            admitted: false,
        }
    }

    pub fn into_parts(self) -> (EventEnvelope<P::Provenance>, P) {
        (self.data.envelope, self.data.payload)
    }

    pub(crate) fn is_admitted(&self) -> bool {
        self.admitted
    }

    pub(crate) fn admit(mut self) -> Result<Self, super::CausalError> {
        super::PreparedCausalCommit::from_record(&self)?;
        self.admitted = true;
        Ok(self)
    }

    pub fn new<E: JournalEvent<Payload = P>>(journal_writer_id: JournalWriterId, event: E) -> Self {
        let (authored, payload) = event.into_parts();
        let mut vector_clock = super::vector_clock::VectorClock::new();
        vector_clock
            .clocks
            .insert(super::CausalCoordinate::new(journal_writer_id), 1);
        Self::from_parts(
            EventEnvelope {
                provenance: Provenance {
                    event: authored.provenance.event,
                    journal: JournalProvenance {
                        run_id: crate::FlowId::new(),
                        causal: Default::default(),
                        journal_writer_id,
                        vector_clock,
                        timestamp: chrono::Utc::now(),
                        journal_group_id: None,
                        journal_group_member: None,
                    },
                },
                observability: authored.observability.and_then(|packet| packet.validated()),
            },
            payload,
        )
    }

    /// Prepare record data with journal-assigned metadata. This does not commit
    /// storage or admit evidence; the successful journal operation does that.
    pub fn commit_event<E: JournalEvent<Payload = P>>(
        event: E,
        journal: JournalProvenance,
    ) -> Result<Self, serde_json::Error> {
        let (authored, payload) = event.into_parts();
        Self::commit(authored, payload, journal)
    }

    pub fn causal_coordinate(&self) -> super::CausalCoordinate {
        super::CausalCoordinate::new(self.envelope.provenance.journal.journal_writer_id)
    }

    pub fn local_sequence(&self) -> u64 {
        self.envelope
            .provenance
            .journal
            .vector_clock
            .get(&self.causal_coordinate())
    }

    pub fn id(&self) -> &EventId {
        self.envelope.provenance.event.id()
    }
    pub fn writer_id(&self) -> &WriterId {
        self.envelope.provenance.event.writer_id()
    }
    pub fn event_type_name(&self) -> &str {
        self.envelope.provenance.event.event_type()
    }
    pub fn admission_seq(&self) -> Option<AdmissionSeq> {
        self.envelope.provenance.event.admission_seq()
    }
    pub fn into_authored(self) -> P::Event {
        let (envelope, payload) = self.into_parts();
        P::Event::from_parts(
            AuthoredEnvelope {
                provenance: AuthoredProvenance {
                    event: envelope.provenance.event,
                },
                observability: envelope.observability,
            },
            payload,
        )
    }
    pub fn authored(&self) -> P::Event {
        self.clone().into_authored()
    }

    pub fn commit(
        authored: AuthoredEnvelope<P::Provenance>,
        payload: P,
        journal: JournalProvenance,
    ) -> Result<Self, serde_json::Error> {
        payload.validate(&authored.provenance.event)?;
        let record = Self::from_parts(
            EventEnvelope {
                provenance: Provenance {
                    event: authored.provenance.event,
                    journal,
                },
                observability: authored.observability.and_then(|packet| packet.validated()),
            },
            payload,
        );
        super::PreparedCausalCommit::from_record(&record)
            .map_err(<serde_json::Error as serde::de::Error>::custom)?;
        Ok(record)
    }
}

impl<P: JournalPayload> Serialize for JournalRecord<P> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeStruct};
        self.payload
            .validate(&self.envelope.provenance.event)
            .map_err(S::Error::custom)?;
        super::PreparedCausalCommit::from_record(self).map_err(S::Error::custom)?;
        let mut record = serializer.serialize_struct("JournalRecord", 2)?;
        record.serialize_field("envelope", &self.envelope)?;
        record.serialize_field("payload", &self.payload)?;
        record.end()
    }
}

impl<'de, P: JournalPayload> Deserialize<'de> for JournalRecord<P> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let record = super::record_serde::deserialize::<_, EventEnvelope<P::Provenance>, Value>(
            deserializer,
        )?;
        let payload = P::decode(&record.envelope.provenance.event, record.payload)
            .map_err(D::Error::custom)?;
        payload
            .validate(&record.envelope.provenance.event)
            .map_err(D::Error::custom)?;
        let record = Self::from_parts(record.envelope, payload);
        super::PreparedCausalCommit::from_record(&record).map_err(D::Error::custom)?;
        Ok(record)
    }
}

#[cfg(test)]
mod tests;

pub type ChainJournalRecord = JournalRecord<ChainPayload>;
pub type SystemJournalRecord = JournalRecord<SystemPayload>;

impl JournalRecord<ChainPayload> {
    pub fn event_type(&self) -> String {
        self.envelope.provenance.event.event_type.clone()
    }
    pub fn payload(&self) -> Value {
        serde_json::to_value(&self.payload).expect("closed payload serialization")
    }
    pub fn is_fact(&self) -> bool {
        matches!(self.payload, ChainPayload::Fact(_))
    }
    pub fn is_typed_input(&self) -> bool {
        matches!(
            self.payload,
            ChainPayload::Fact(_) | ChainPayload::CompositeData(_)
        )
    }
    pub fn consumes_data_credit(&self) -> bool {
        self.payload.consumes_data_credit()
    }
    pub fn is_eof(&self) -> bool {
        matches!(
            self.payload,
            ChainPayload::FlowControl(
                super::payloads::flow_control_payload::FlowControlPayload::Eof { .. }
            )
        )
    }
    pub fn is_control(&self) -> bool {
        matches!(self.payload, ChainPayload::FlowControl(_))
    }
    pub fn is_delivery(&self) -> bool {
        matches!(self.payload, ChainPayload::Delivery(_))
    }
    pub fn is_system(&self) -> bool {
        false
    }
    pub fn is_lifecycle(&self) -> bool {
        matches!(&self.payload, ChainPayload::Execution(execution) if !execution.consumes_data_credit())
    }
    pub fn composite_activations(&self) -> &[super::provenance::CompositeActivationContext] {
        &self.envelope.provenance.event.composite_activations
    }
    pub fn correlation_ids(&self) -> Option<&[CorrelationId]> {
        self.envelope
            .provenance
            .event
            .correlation
            .as_ref()
            .map(|c| c.ids.as_slice())
    }
    pub fn correlation_id(&self) -> Option<CorrelationId> {
        self.envelope
            .provenance
            .event
            .correlation
            .as_ref()
            .and_then(|c| c.single_id())
    }
    pub fn correlation_payload(
        &self,
    ) -> Option<&super::payloads::correlation_payload::CorrelationPayload> {
        self.envelope
            .provenance
            .event
            .correlation
            .as_ref()
            .and_then(|c| c.payload.as_ref())
    }
    pub fn correlation_ids_truncated(&self) -> bool {
        self.envelope
            .provenance
            .event
            .correlation
            .as_ref()
            .is_some_and(|c| c.truncated)
    }
    pub fn replay_disposition(&self) -> super::chain_event::ReplayDisposition {
        self.payload.replay_disposition()
    }
    pub fn is_source_replayable(&self) -> bool {
        self.replay_disposition() == super::chain_event::ReplayDisposition::ReAdmit
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ChainEvent;
use crate::event::journal_event::{JournalEvent, Sealed};
use crate::event::types::{AdmissionSeq, EventId, WriterId};

impl Sealed for ChainEvent {}

impl JournalEvent for ChainEvent {
    type Payload = crate::event::ChainPayload;
    fn into_parts(
        self,
    ) -> (
        crate::event::provenance::AuthoredEnvelope<crate::event::provenance::ChainEventProvenance>,
        Self::Payload,
    ) {
        (self.envelope, self.payload)
    }
    fn from_parts(
        envelope: crate::event::provenance::AuthoredEnvelope<
            crate::event::provenance::ChainEventProvenance,
        >,
        payload: Self::Payload,
    ) -> Self {
        Self { envelope, payload }
    }

    fn id(&self) -> &EventId {
        &self.id
    }

    fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }

    fn admission_seq(&self) -> Option<AdmissionSeq> {
        self.admission_seq
    }

    fn set_admission_seq(&mut self, seq: AdmissionSeq) {
        self.admission_seq = Some(seq);
    }

    fn event_type_name(&self) -> &str {
        &self.envelope.provenance.event.event_type
    }
}

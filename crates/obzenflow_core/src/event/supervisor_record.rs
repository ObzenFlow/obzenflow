// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A consumed supervisor fact and its original journal metadata. Conversion
//! moves the payload; it does not retain a second copy of the source record.
use super::{CausalError, CausalFrontier, ChainPayload, JournalRecord, SystemPayload};
use crate::journal::JournalError;
use crate::{EventId, JournalId, WriterId};

#[derive(Debug, Clone)]
pub struct SupervisorRecord {
    pub payload: SystemPayload,
    journal: super::provenance::JournalProvenance,
    id: EventId,
    writer: WriterId,
    timestamp: u64,
    observability: Option<super::observability::ObservabilityContext>,
    admitted: bool,
}

impl SupervisorRecord {
    pub fn journal(&self) -> &super::provenance::JournalProvenance {
        &self.journal
    }
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }
    pub fn observability(&self) -> Option<&super::observability::ObservabilityContext> {
        self.observability.as_ref()
    }
    pub fn commitment(&self) -> super::JournalCommitRef {
        super::JournalCommitRef {
            run_id: self.journal.run_id,
            journal_writer_id: self.journal.journal_writer_id,
            sequence: self.position(),
            event_id: self.id,
        }
    }
    pub fn journal_id(&self) -> JournalId {
        *self.journal.journal_writer_id.as_journal_id()
    }
    pub fn position(&self) -> u64 {
        self.journal.vector_clock.get(&super::CausalCoordinate::new(
            self.journal.journal_writer_id,
        ))
    }
    pub fn id(&self) -> &EventId {
        &self.id
    }
    pub fn writer_id(&self) -> &WriterId {
        &self.writer
    }
    pub fn frontier(&self) -> Result<CausalFrontier, JournalError> {
        if !self.admitted {
            return Err(CausalError::UnadmittedRecord.into());
        }
        Ok(CausalFrontier {
            clock: self.journal.vector_clock.clone(),
        })
    }

    pub fn from_chain(record: JournalRecord<ChainPayload>) -> Option<Self> {
        let admitted = record.is_admitted();
        let position = record.local_sequence();
        let (envelope, payload) = record.into_parts();
        let ChainPayload::Execution(payload) = payload else {
            return None;
        };
        let event = envelope.provenance.event;
        use super::payloads::execution_payload::{
            CircuitBreakerFact as C, ExecutionPayload as E, MiddlewareFact, RateLimiterFact as R,
        };
        let payload = match payload {
            E::CircuitBreaker(
                fact @ (C::Opened { .. }
                | C::Closed { .. }
                | C::HalfOpen { .. }
                | C::StateChanged { .. }),
            ) => Some(MiddlewareFact::CircuitBreaker(fact)),
            E::RateLimiter(fact @ (R::ModeChange { .. } | R::ConfigChanged { .. })) => {
                Some(MiddlewareFact::RateLimiter(fact))
            }
            payload => {
                return Some(Self {
                    payload: payload.into_supervision_report()?,
                    journal: envelope.provenance.journal,
                    id: event.id,
                    writer: event.writer_id,
                    timestamp: event.processing.event_time,
                    observability: envelope.observability,
                    admitted,
                });
            }
        };
        Some(Self {
            payload: SystemPayload::MiddlewareLifecycle {
                stage_id: event.flow_context.stage_id,
                stage_name: Some(event.flow_context.stage_name),
                flow_id: Some(event.flow_context.flow_id),
                flow_name: Some(event.flow_context.flow_name),
                origin: super::payloads::system_payload::MiddlewareEventOrigin {
                    event_id: event.id,
                    writer_key: event.writer_id.to_string(),
                    seq: super::types::SeqNo(position),
                },
                middleware: payload?,
            },
            journal: envelope.provenance.journal,
            id: event.id,
            writer: event.writer_id,
            timestamp: event.processing.event_time,
            observability: envelope.observability,
            admitted,
        })
    }
}

impl From<JournalRecord<SystemPayload>> for SupervisorRecord {
    fn from(record: JournalRecord<SystemPayload>) -> Self {
        let admitted = record.is_admitted();
        let (envelope, payload) = record.into_parts();
        let event = envelope.provenance.event;
        Self {
            payload,
            journal: envelope.provenance.journal,
            id: event.id,
            writer: event.writer_id,
            timestamp: event.timestamp,
            observability: envelope.observability,
            admitted,
        }
    }
}

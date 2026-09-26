// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Read-only semantic view of a supervisor fact with its original commitment.
use super::{CausalFrontier, ChainPayload, JournalRecord, SystemPayload};
use crate::journal::JournalError;
use crate::{EventId, JournalId, WriterId};

#[derive(Debug, Clone)]
enum CommittedReport {
    Stage(Box<JournalRecord<ChainPayload>>),
    System(Box<JournalRecord<SystemPayload>>),
}

/// A semantic view plus its unchanged, original journal commitment.
#[derive(Debug, Clone)]
pub struct SupervisorRecord {
    pub payload: SystemPayload,
    committed: CommittedReport,
}

impl SupervisorRecord {
    pub fn journal(&self) -> &super::provenance::JournalProvenance {
        match &self.committed {
            CommittedReport::Stage(row) => &row.envelope.provenance.journal,
            CommittedReport::System(row) => &row.envelope.provenance.journal,
        }
    }

    pub fn timestamp(&self) -> u64 {
        match &self.committed {
            CommittedReport::Stage(row) => row.envelope.provenance.event.processing.event_time,
            CommittedReport::System(row) => row.envelope.provenance.event.timestamp,
        }
    }

    pub fn observability(&self) -> Option<&super::observability::ObservabilityContext> {
        match &self.committed {
            CommittedReport::Stage(row) => row.envelope.observability.as_ref(),
            CommittedReport::System(row) => row.envelope.observability.as_ref(),
        }
    }

    pub fn commitment(&self) -> Result<super::CausalCommit, super::CausalError> {
        match &self.committed {
            CommittedReport::Stage(row) => super::CausalCommit::from_record(row),
            CommittedReport::System(row) => super::CausalCommit::from_record(row),
        }
    }

    pub fn journal_id(&self) -> JournalId {
        match &self.committed {
            CommittedReport::Stage(row) => {
                *row.causal_coordinate().journal_writer_id.as_journal_id()
            }
            CommittedReport::System(row) => {
                *row.causal_coordinate().journal_writer_id.as_journal_id()
            }
        }
    }

    pub fn position(&self) -> u64 {
        match &self.committed {
            CommittedReport::Stage(row) => row.local_sequence(),
            CommittedReport::System(row) => row.local_sequence(),
        }
    }
    pub fn from_chain(record: JournalRecord<ChainPayload>) -> Option<Self> {
        let ChainPayload::Execution(payload) = &record.payload else {
            return None;
        };
        Some(Self {
            payload: payload.supervision_report().or_else(|| {
                use super::payloads::execution_payload::{
                    CircuitBreakerFact, ExecutionPayload, MiddlewareFact, RateLimiterFact,
                };
                let middleware = match payload {
                    ExecutionPayload::CircuitBreaker(
                        fact @ (CircuitBreakerFact::Opened { .. }
                        | CircuitBreakerFact::Closed { .. }
                        | CircuitBreakerFact::HalfOpen { .. }
                        | CircuitBreakerFact::StateChanged { .. }),
                    ) => MiddlewareFact::CircuitBreaker(fact.clone()),
                    ExecutionPayload::RateLimiter(
                        fact @ (RateLimiterFact::ModeChange { .. }
                        | RateLimiterFact::ConfigChanged { .. }),
                    ) => MiddlewareFact::RateLimiter(fact.clone()),
                    _ => return None,
                };
                let event = &record.envelope.provenance.event;
                Some(SystemPayload::MiddlewareLifecycle {
                    stage_id: event.flow_context.stage_id,
                    stage_name: Some(event.flow_context.stage_name.clone()),
                    flow_id: Some(event.flow_context.flow_id.clone()),
                    flow_name: Some(event.flow_context.flow_name.clone()),
                    origin: super::payloads::system_payload::MiddlewareEventOrigin {
                        event_id: event.id,
                        writer_key: event.writer_id.to_string(),
                        seq: super::types::SeqNo(record.local_sequence()),
                    },
                    middleware,
                })
            })?,
            committed: CommittedReport::Stage(Box::new(record)),
        })
    }

    pub fn id(&self) -> &EventId {
        match &self.committed {
            CommittedReport::Stage(row) => row.id(),
            CommittedReport::System(row) => row.id(),
        }
    }

    pub fn writer_id(&self) -> &WriterId {
        match &self.committed {
            CommittedReport::Stage(row) => row.writer_id(),
            CommittedReport::System(row) => row.writer_id(),
        }
    }

    pub fn frontier(&self) -> Result<CausalFrontier, JournalError> {
        Ok(match &self.committed {
            CommittedReport::Stage(row) => CausalFrontier::from_record(row)?,
            CommittedReport::System(row) => CausalFrontier::from_record(row)?,
        })
    }
}

impl From<JournalRecord<SystemPayload>> for SupervisorRecord {
    fn from(record: JournalRecord<SystemPayload>) -> Self {
        Self {
            payload: record.payload.clone(),
            committed: CommittedReport::System(Box::new(record)),
        }
    }
}

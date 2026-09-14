// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Event authorship and journal commitment have separate authorities.

use super::chain_event::CorrelationContext;
use super::context::causality_context::CausalityContext;
use super::context::{
    CompositeActivationContext, FlowContext, IntentContext, ReplayContext, RuntimeProvenance,
};
use super::event_envelope::JournalGroupMember;
use super::observation::ObservabilityContext;
use super::payloads::chain_payload::EventKind;
use super::payloads::effect_payload::EffectProvenance;
use super::status::processing_status::ProcessingStatus;
use super::vector_clock::VectorClock;
use crate::id::{CycleDepth, SccId};
use crate::ingress::IngressContext;
use crate::{AdmissionSeq, EventId, JournalWriterId, WriterId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Processing outcome and occurrence time survive removal of measurements.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessingProvenance {
    pub processed_by: String,
    pub event_time: u64,
    pub status: ProcessingStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_hops_remaining: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChainEventProvenance {
    pub id: EventId,
    pub writer_id: WriterId,
    pub event_kind: EventKind,
    pub event_type: String,
    pub causality: CausalityContext,
    pub flow_context: FlowContext,
    pub processing: ProcessingProvenance,
    pub intent: Option<IntentContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation: Option<CorrelationContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay_context: Option<ReplayContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ingress_context: Option<IngressContext>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cycle_depth: Option<CycleDepth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cycle_scc_id: Option<SccId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub effect_provenance: Option<EffectProvenance>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admission_seq: Option<AdmissionSeq>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<RuntimeProvenance>,
    pub composite_activations: Vec<CompositeActivationContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SystemEventProvenance {
    pub id: EventId,
    pub writer_id: WriterId,
    pub event_kind: EventKind,
    pub event_type: String,
    /// Creation time in milliseconds; independent of journal append time.
    pub timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JournalProvenance {
    pub journal_writer_id: JournalWriterId,
    pub vector_clock: VectorClock,
    pub timestamp: DateTime<Utc>,
    pub journal_group_id: Option<String>,
    pub journal_group_member: Option<JournalGroupMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provenance<E> {
    pub event: E,
    pub journal: JournalProvenance,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoredProvenance<E> {
    pub event: E,
}

/// An author cannot supply physical journal commitment through this type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoredEnvelope<E> {
    pub provenance: AuthoredProvenance<E>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observability: Option<ObservabilityContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventEnvelope<E> {
    pub provenance: Provenance<E>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observability: Option<ObservabilityContext>,
}

/// Shared identity access for the two protected event provenance families.
pub trait RecordProvenance {
    fn id(&self) -> &EventId;
    fn writer_id(&self) -> &WriterId;
    fn event_type(&self) -> &str;
    fn admission_seq(&self) -> Option<AdmissionSeq>;
}
impl RecordProvenance for ChainEventProvenance {
    fn id(&self) -> &EventId {
        &self.id
    }
    fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }
    fn event_type(&self) -> &str {
        &self.event_type
    }
    fn admission_seq(&self) -> Option<AdmissionSeq> {
        self.admission_seq
    }
}
impl RecordProvenance for SystemEventProvenance {
    fn id(&self) -> &EventId {
        &self.id
    }
    fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }
    fn event_type(&self) -> &str {
        &self.event_type
    }
    fn admission_seq(&self) -> Option<AdmissionSeq> {
        None
    }
}

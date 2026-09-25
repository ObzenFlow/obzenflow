// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Event authorship and journal commitment have separate authorities.

use crate::event::chain_event::CorrelationContext;
use crate::event::payloads::chain_payload::EventKind;
use crate::event::payloads::effect_payload::EffectProvenance;
use crate::event::provenance::causality_context::CausalityContext;
use crate::event::status::processing_status::ProcessingStatus;
use crate::event::vector_clock::VectorClock;
use crate::id::{CycleDepth, SccId};
use crate::ingress::IngressContext;
use crate::{AdmissionSeq, EventId, JournalWriterId, WriterId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Processing outcome and occurrence time survive removal of measurements.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessingProvenance {
    pub event_time: u64,
    pub status: ProcessingStatus,
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
    pub run_id: crate::FlowId,
    pub journal_writer_id: JournalWriterId,
    pub vector_clock: VectorClock,
    pub causal: crate::event::CausalWitnesses,
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

/// Position of one logical event inside the physical atomic journal frame
/// that committed it.
///
/// The zero-based index and total size make a repeated use of one
/// deterministic group identity observable during replay. Without this
/// witness, two adjacent physical frames with the same `journal_group_id`
/// collapse into one indistinguishable list of logical events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalGroupMember {
    pub index: u32,
    pub size: u32,
}

pub mod causality_context;
pub mod composite_activation_context;
pub mod flow_context;
pub mod replay_context;
pub mod runtime_provenance;

pub use composite_activation_context::CompositeActivationContext;
pub use flow_context::FlowContext;
pub use replay_context::ReplayContext;
pub use runtime_provenance::{
    EventTypeCountContext, ExecutionAccounting, RuntimeProvenance, UpstreamEventTypeCountContext,
};

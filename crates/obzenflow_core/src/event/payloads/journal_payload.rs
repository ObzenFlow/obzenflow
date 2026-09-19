// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Closed payload decoding and descriptor validation.

use super::chain_payload::EventKind;
use super::{ChainPayload, SystemPayload};
use crate::event::provenance::{ChainEventProvenance, RecordProvenance, SystemEventProvenance};
use crate::event::JournalEvent;
use crate::journal::metrics_tail::MetricsTailKey;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;

mod sealed {
    pub trait Sealed {}
}

/// Only Core's closed chain/system families can define journal decoding.
pub trait JournalPayload:
    sealed::Sealed + 'static + Clone + std::fmt::Debug + Serialize + Send + Sync
{
    type Event: JournalEvent<Payload = Self>;
    type Provenance: 'static
        + RecordProvenance
        + Clone
        + std::fmt::Debug
        + Serialize
        + DeserializeOwned
        + Send
        + Sync;
    fn decode(provenance: &Self::Provenance, payload: Value) -> Result<Self, serde_json::Error>;
    fn validate(&self, provenance: &Self::Provenance) -> Result<(), serde_json::Error>;
    fn visit_metrics_keys(
        &self,
        provenance: &Self::Provenance,
        visit: &mut dyn FnMut(MetricsTailKey),
    );
}

impl sealed::Sealed for ChainPayload {}
impl JournalPayload for ChainPayload {
    type Event = crate::event::ChainEvent;
    type Provenance = ChainEventProvenance;

    fn visit_metrics_keys(
        &self,
        provenance: &Self::Provenance,
        visit: &mut dyn FnMut(MetricsTailKey),
    ) {
        use super::execution_payload::ExecutionPayload;
        let stage = provenance.flow_context.stage_id;
        // Forwarded carriers cannot replace the originating stage's counters.
        if provenance.writer_id != crate::WriterId::from(stage) {
            return;
        }
        if provenance.runtime.is_some() {
            visit(MetricsTailKey::Accounting(stage));
        }
        match self {
            Self::Execution(ExecutionPayload::HttpPullState(_)) => {
                visit(MetricsTailKey::HttpPull(stage))
            }
            Self::Execution(ExecutionPayload::CircuitBreaker(fact)) => {
                use super::execution_payload::CircuitBreakerFact;
                if matches!(
                    fact,
                    CircuitBreakerFact::Opened { .. }
                        | CircuitBreakerFact::Closed { .. }
                        | CircuitBreakerFact::HalfOpen { .. }
                        | CircuitBreakerFact::StateChanged { .. }
                ) {
                    visit(MetricsTailKey::CircuitBreaker(stage));
                }
            }
            _ => {}
        }
    }

    fn decode(provenance: &Self::Provenance, payload: Value) -> Result<Self, serde_json::Error> {
        ChainPayload::decode(provenance.event_kind, &provenance.event_type, payload)
    }

    fn validate(&self, provenance: &Self::Provenance) -> Result<(), serde_json::Error> {
        if self.kind() != provenance.event_kind
            || self
                .framework_event_type()
                .is_some_and(|expected| expected != provenance.event_type)
        {
            return Err(descriptor_mismatch());
        }
        if let ChainPayload::Execution(
            crate::event::payloads::execution_payload::ExecutionPayload::SourcePollError(failure),
        ) = self
        {
            use crate::event::status::processing_status::ProcessingStatus;
            match &provenance.processing.status {
                ProcessingStatus::Error { kind, .. }
                    if *kind == Some(failure.error_type.processing_error_kind()) => {}
                _ => return Err(descriptor_mismatch()),
            }
        }
        Ok(())
    }
}

impl sealed::Sealed for SystemPayload {}
impl JournalPayload for SystemPayload {
    type Event = crate::event::SystemEvent;
    type Provenance = SystemEventProvenance;

    fn visit_metrics_keys(
        &self,
        provenance: &Self::Provenance,
        visit: &mut dyn FnMut(MetricsTailKey),
    ) {
        use super::system_payload::StageLifecycleEvent;
        match self {
            Self::StageLifecycle { stage_id, event } => {
                visit(MetricsTailKey::StageLifecycle(*stage_id));
                if matches!(
                    event,
                    StageLifecycleEvent::Draining {
                        accounting: Some(_)
                    } | StageLifecycleEvent::Completed {
                        accounting: Some(_)
                    } | StageLifecycleEvent::Cancelled {
                        accounting: Some(_),
                        ..
                    } | StageLifecycleEvent::Failed {
                        accounting: Some(_),
                        ..
                    }
                ) {
                    visit(MetricsTailKey::Accounting(*stage_id));
                }
            }
            Self::PipelineLifecycle(event) => {
                visit(MetricsTailKey::PipelineLifecycle(provenance.writer_id));
                use super::system_payload::PipelineLifecycleEvent;
                if matches!(
                    event,
                    PipelineLifecycleEvent::Completed { .. }
                        | PipelineLifecycleEvent::Failed { .. }
                        | PipelineLifecycleEvent::Cancelled { .. }
                        | PipelineLifecycleEvent::NotStarted
                ) {
                    visit(MetricsTailKey::PipelineOutcome(provenance.writer_id));
                }
            }
            Self::ContractResult {
                upstream,
                reader,
                selected_event_type,
                feed_role,
                contract_name,
                ..
            } => {
                visit(MetricsTailKey::Contract(
                    crate::metrics::ContractMetricEdgeKey {
                        upstream: *upstream,
                        downstream: *reader,
                        selected_event_type: selected_event_type.clone(),
                        feed_role: *feed_role,
                        contract: contract_name.clone(),
                    },
                ));
            }
            _ => {}
        }
    }

    fn decode(provenance: &Self::Provenance, payload: Value) -> Result<Self, serde_json::Error> {
        if provenance.event_kind != EventKind::System {
            return Err(descriptor_mismatch());
        }
        serde_json::from_value(payload)
    }

    fn validate(&self, provenance: &Self::Provenance) -> Result<(), serde_json::Error> {
        if provenance.event_kind != EventKind::System || self.event_type() != provenance.event_type
        {
            return Err(descriptor_mismatch());
        }
        Ok(())
    }
}

fn descriptor_mismatch() -> serde_json::Error {
    <serde_json::Error as serde::de::Error>::custom("event descriptor does not match payload")
}

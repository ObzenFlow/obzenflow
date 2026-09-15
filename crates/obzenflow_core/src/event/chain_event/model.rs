// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::factory::ChainEventFactory;
use crate::event::context::causality_context::CausalityContext;
use crate::event::context::{FlowContext, RuntimeProvenance, RuntimeSnapshot};
use crate::event::journal_record::JournalPayload;
use crate::event::observation::ObservabilityContext;
use crate::event::payloads::correlation_payload::CorrelationPayload;
use crate::event::payloads::effect_payload::EffectProvenance;
use crate::event::payloads::flow_control_payload::FlowControlPayload;
use crate::event::provenance::{AuthoredEnvelope, ChainEventProvenance};
use crate::event::status::processing_status::{ErrorKind, ProcessingStatus};
use crate::event::types::CorrelationId;
use crate::ingress::IngressContext;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Correlation metadata carried through a flow.
///
/// `ids` contains one id for ordinary 1:1 or fan-out lineage, and multiple ids
/// for fan-in aggregates. When `truncated` is true, `ids` is a bounded sample.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CorrelationContext {
    pub ids: Vec<CorrelationId>,

    #[serde(default, skip_serializing_if = "is_false")]
    pub truncated: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<CorrelationPayload>,
}

impl CorrelationContext {
    pub fn single(id: CorrelationId, payload: Option<CorrelationPayload>) -> Self {
        Self {
            ids: vec![id],
            truncated: false,
            payload,
        }
    }

    pub fn sample(ids: Vec<CorrelationId>, truncated: bool) -> Self {
        Self {
            ids,
            truncated,
            payload: None,
        }
    }

    pub fn single_id(&self) -> Option<CorrelationId> {
        if self.ids.len() == 1 && !self.truncated {
            self.ids.first().copied()
        } else {
            None
        }
    }
}

/// An authored chain record. Only the journal can supply commitment provenance.
#[derive(Debug, Clone)]
pub struct ChainEvent {
    pub envelope: AuthoredEnvelope<ChainEventProvenance>,
    pub payload: ChainPayload,
}

pub use crate::event::payloads::chain_payload::ChainPayload;

impl std::ops::Deref for ChainEvent {
    type Target = ChainEventProvenance;
    fn deref(&self) -> &Self::Target {
        &self.envelope.provenance.event
    }
}
impl std::ops::DerefMut for ChainEvent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.envelope.provenance.event
    }
}

impl Serialize for ChainEvent {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeStruct};
        JournalPayload::validate(&self.payload, &self.envelope.provenance.event)
            .map_err(S::Error::custom)?;
        let mut event = serializer.serialize_struct("ChainEvent", 2)?;
        event.serialize_field("envelope", &self.envelope)?;
        event.serialize_field("payload", &self.payload)?;
        event.end()
    }
}

impl<'de> Deserialize<'de> for ChainEvent {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let raw = crate::event::record_serde::deserialize::<
            _,
            AuthoredEnvelope<ChainEventProvenance>,
            Value,
        >(deserializer)?;
        let payload = ChainPayload::decode(
            raw.envelope.provenance.event.event_kind,
            &raw.envelope.provenance.event.event_type,
            raw.payload,
        )
        .map_err(D::Error::custom)?;
        JournalPayload::validate(&payload, &raw.envelope.provenance.event)
            .map_err(D::Error::custom)?;
        Ok(Self {
            envelope: raw.envelope,
            payload,
        })
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Source-replay disposition of an event class (FLOWIP-120n).
/// See [`ChainEvent::replay_disposition`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayDisposition {
    /// Source-authored, position-bearing: the `ReplayDriver` re-injects it.
    ReAdmit,
    /// Runtime-computed: the re-running stage regenerates it.
    ReAuthor,
}

impl ChainEvent {
    /// Attach observability context to any event (wide events pattern)
    pub fn with_observability_context(mut self, observability: ObservabilityContext) -> Self {
        self.envelope.observability = Some(observability);
        self
    }

    pub fn with_effect_provenance(mut self, provenance: EffectProvenance) -> Self {
        self.effect_provenance = Some(provenance);
        self
    }

    pub fn with_runtime_provenance(mut self, ctx: RuntimeProvenance) -> Self {
        self.runtime = Some(ctx);
        self
    }

    /// Attach the local runtime's diagnostic snapshot without re-stamping any
    /// existing handler or forwarded measurements.
    pub fn with_runtime_snapshot(mut self, snapshot: RuntimeSnapshot) -> Self {
        let capture = snapshot.capture;
        self.envelope
            .observability
            .get_or_insert_with(|| ObservabilityContext::new(capture))
            .runtime_snapshot = Some(snapshot);
        self
    }

    pub fn with_ingress_context(mut self, ctx: IngressContext) -> Self {
        self.ingress_context = Some(ctx);
        self
    }

    /// Replace the flow-context block and return the updated event.
    pub fn with_flow_context(mut self, ctx: FlowContext) -> Self {
        self.flow_context = ctx;
        self
    }

    /// Set causality information for this event
    pub fn with_causality(mut self, causality: CausalityContext) -> Self {
        self.causality = causality;
        self
    }

    /// Check event type helpers
    pub fn is_eof(&self) -> bool {
        matches!(
            self.payload,
            ChainPayload::FlowControl(FlowControlPayload::Eof { .. })
        )
    }

    pub fn is_control(&self) -> bool {
        matches!(self.payload, ChainPayload::FlowControl(_))
    }

    pub fn is_system(&self) -> bool {
        // ChainEvent never contains system events - those are SystemEvent type
        false
    }

    pub fn is_fact(&self) -> bool {
        matches!(self.payload, ChainPayload::Fact(_))
    }

    /// Physical input/output accounting and credit population, independent of
    /// the meaning of a record. Framework effect rows retain their old charge.
    pub fn consumes_data_credit(&self) -> bool {
        self.payload.consumes_data_credit()
    }

    /// Payloads eligible for typed handler contracts and selected feeds.
    pub fn is_typed_input(&self) -> bool {
        matches!(
            self.payload,
            ChainPayload::Fact(_) | ChainPayload::CompositeData(_)
        )
    }

    /// Protected execution facts formerly in the lifecycle lane are not
    /// delivered to handlers. Effect evidence has its own history/credit rules.
    pub fn is_transport_excluded_execution(&self) -> bool {
        matches!(&self.payload, ChainPayload::Execution(p) if !p.consumes_data_credit())
    }

    pub fn typed_payload(&self) -> Option<Value> {
        match &self.payload {
            ChainPayload::Fact(value) => Some(value.clone()),
            ChainPayload::CompositeData(value) => serde_json::to_value(value).ok(),
            ChainPayload::Execution(_)
            | ChainPayload::FlowControl(_)
            | ChainPayload::Delivery(_) => None,
        }
    }

    pub fn is_delivery(&self) -> bool {
        matches!(self.payload, ChainPayload::Delivery(_))
    }

    pub fn is_lifecycle(&self) -> bool {
        matches!(&self.payload, ChainPayload::Execution(execution) if !execution.consumes_data_credit())
    }

    /// Source-replay disposition (FLOWIP-120n phase 6). `ReAdmit` rows are
    /// source-authored and position-bearing; the `ReplayDriver` re-injects them
    /// in place. `ReAuthor` rows are runtime-computed; re-running stages
    /// regenerate them. Exhaustive with no wildcard arm, so a new variant fails
    /// to compile until its disposition is declared.
    ///
    /// The one data-dependent case: framework-owned effect records ride the
    /// separate `EffectHistory::load` path, never the source re-injection.
    pub fn replay_disposition(&self) -> ReplayDisposition {
        self.payload.replay_disposition()
    }

    /// Whether this event should be re-injected as a fresh source event during
    /// source replay (FLOWIP-095a). Derived from [`Self::replay_disposition`].
    pub fn is_source_replayable(&self) -> bool {
        self.replay_disposition() == ReplayDisposition::ReAdmit
    }

    /// Mark this event as an error with a structured ErrorKind.
    ///
    /// This sets `processing_info.status` to `ProcessingStatus::Error` with
    /// the provided message and kind, and primes `error_hops_remaining` so
    /// stage supervisors can route the event according to FLOWIP-082e/082g.
    pub fn mark_as_error(mut self, reason: impl Into<String>, kind: ErrorKind) -> Self {
        self.processing.status = ProcessingStatus::error_with_kind(reason.into(), Some(kind));
        self.processing.error_hops_remaining = Some(1);
        self
    }

    /// Convenience: mark this event as a domain/validation error.
    pub fn mark_as_validation_error(self, reason: impl Into<String>) -> Self {
        self.mark_as_error(reason, ErrorKind::Validation)
    }

    /// Convenience: mark this event as an infra/remote error.
    pub fn mark_as_infra_error(self, reason: impl Into<String>) -> Self {
        self.mark_as_error(reason, ErrorKind::Remote)
    }

    /// Create a derived error event from this event.
    ///
    /// This helper combines `ChainEventFactory::derived_data_event` with
    /// `mark_as_error`, preserving causality/correlation while marking the
    /// new event as an error with the provided `ErrorKind`.
    pub fn derive_error_event(
        &self,
        event_type: impl Into<String>,
        payload: Value,
        reason: impl Into<String>,
        kind: ErrorKind,
        lineage: crate::config::LineagePolicy,
    ) -> ChainEvent {
        let reason_str = reason.into();
        ChainEventFactory::derived_data_event(self.writer_id, self, event_type, payload, lineage)
            .mark_as_error(reason_str, kind)
    }

    /// Declared application label or descriptor derived from the typed payload.
    pub fn event_type(&self) -> String {
        self.envelope.provenance.event.event_type.clone()
    }

    /// JSON body, without infrastructure wrappers.
    pub fn payload(&self) -> Value {
        serde_json::to_value(&self.payload).expect("closed payloads serialize to JSON")
    }
}

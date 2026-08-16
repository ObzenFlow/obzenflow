// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::backpressure::{BackpressurePlan, BackpressureRegistry};
use crate::id_conversions::StageIdExt;
use crate::stages::observer::{
    EffectObserver, EffectObserverContext, EffectObserverOutcome, ObserverBinding, ObserverTarget,
    StageObserverBindings, StageObserverBundle,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::event_envelope::JournalGroupMember;
use obzenflow_core::event::{EventEnvelope, JournalEvent};
use obzenflow_core::journal::{ArchiveStatus, JournalError, JournalReader, StatusDerivation};
use obzenflow_core::{
    BoundedBindingEvidence, JournalId, JournalOwner, JournalWriterId, TypedPayload,
};
use obzenflow_topology::{TopologyBuilder, TypeHintInfo};
use serde_json::json;
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

struct MemoryJournal<T: JournalEvent> {
    id: JournalId,
    owner: Option<JournalOwner>,
    events: Mutex<Vec<EventEnvelope<T>>>,
    fail_group_prefixes: Mutex<Vec<String>>,
}

impl<T: JournalEvent> MemoryJournal<T> {
    fn new(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            events: Mutex::new(Vec::new()),
            fail_group_prefixes: Mutex::new(Vec::new()),
        }
    }

    fn failing_group(owner: JournalOwner, prefix: impl Into<String>) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            events: Mutex::new(Vec::new()),
            fail_group_prefixes: Mutex::new(vec![prefix.into()]),
        }
    }

    fn events(&self) -> Vec<EventEnvelope<T>> {
        self.events.lock().expect("events lock poisoned").clone()
    }
}

struct MemoryJournalReader<T: JournalEvent> {
    events: Vec<EventEnvelope<T>>,
    position: usize,
}

#[async_trait]
impl<T: JournalEvent + 'static> JournalReader<T> for MemoryJournalReader<T> {
    async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let next = self.events.get(self.position).cloned();
        if next.is_some() {
            self.position += 1;
        }
        Ok(next)
    }

    fn position(&self) -> u64 {
        self.position as u64
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> Journal<T> for MemoryJournal<T> {
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,
        event: T,
        _parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError> {
        let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
        self.events
            .lock()
            .expect("events lock poisoned")
            .push(envelope.clone());
        Ok(envelope)
    }

    async fn append_group(
        &self,
        group_id: &str,
        events: Vec<T>,
        _parent: Option<&EventEnvelope<T>>,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let mut failures = self
            .fail_group_prefixes
            .lock()
            .expect("group failures lock poisoned");
        if failures
            .first()
            .is_some_and(|prefix| group_id.starts_with(prefix))
        {
            let prefix = failures.remove(0);
            return Err(JournalError::Implementation {
                message: format!("injected atomic-group failure for '{group_id}'"),
                source: format!("test journal rejected group prefix '{prefix}'").into(),
            });
        }
        drop(failures);
        let size = u32::try_from(events.len()).map_err(|_| JournalError::Implementation {
            message: format!("test group '{group_id}' exceeds u32 member capacity"),
            source: "test group too large".into(),
        })?;
        let envelopes = events
            .into_iter()
            .enumerate()
            .map(|(index, event)| {
                let mut envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
                envelope.journal_group_id = Some(group_id.to_string());
                envelope.journal_group_member = Some(JournalGroupMember {
                    index: u32::try_from(index).expect("group size was checked"),
                    size,
                });
                envelope
            })
            .collect::<Vec<_>>();
        self.events
            .lock()
            .expect("events lock poisoned")
            .extend(envelopes.iter().cloned());
        Ok(envelopes)
    }

    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(self.events())
    }

    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError> {
        Ok(self
            .events()
            .into_iter()
            .find(|envelope| *envelope.event.id() == *event_id))
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(MemoryJournalReader {
            events: self.events(),
            position: position as usize,
        }))
    }

    async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let events = self.events();
        let start = events.len().saturating_sub(count);
        Ok(events[start..].iter().rev().cloned().collect())
    }
}

struct InspectingFailJournal {
    id: JournalId,
    owner: JournalOwner,
    registry: BackpressureRegistry,
    upstream: StageId,
    downstream: StageId,
}

struct FailingStartJournal {
    id: JournalId,
    owner: JournalOwner,
    attempted_event_types: Mutex<Vec<String>>,
}

impl FailingStartJournal {
    fn new(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner,
            attempted_event_types: Mutex::new(Vec::new()),
        }
    }

    fn attempted_event_types(&self) -> Vec<String> {
        self.attempted_event_types
            .lock()
            .expect("attempted event types lock poisoned")
            .clone()
    }
}

#[async_trait]
impl Journal<ChainEvent> for FailingStartJournal {
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        Some(&self.owner)
    }

    async fn append(
        &self,
        event: ChainEvent,
        _parent: Option<&EventEnvelope<ChainEvent>>,
    ) -> Result<EventEnvelope<ChainEvent>, JournalError> {
        self.attempted_event_types
            .lock()
            .expect("attempted event types lock poisoned")
            .push(event.event_type());
        Err(JournalError::Implementation {
            message: "injected Start append failure".to_string(),
            source: "test journal rejected Start".into(),
        })
    }

    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        _event_id: &EventId,
    ) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(None)
    }

    async fn reader_from(
        &self,
        _position: u64,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        Err(JournalError::Implementation {
            message: "failing Start journal has no reader".to_string(),
            source: "test-only journal".into(),
        })
    }

    async fn read_last_n(
        &self,
        _count: usize,
    ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl Journal<ChainEvent> for InspectingFailJournal {
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        Some(&self.owner)
    }

    async fn append(
        &self,
        event: ChainEvent,
        _parent: Option<&EventEnvelope<ChainEvent>>,
    ) -> Result<EventEnvelope<ChainEvent>, JournalError> {
        assert!(event.is_data());
        assert_eq!(
            self.registry.edge_in_flight(self.upstream, self.downstream),
            Some(1),
            "direct Data must reserve its physical row before append"
        );
        Err(JournalError::Implementation {
            message: "injected append failure".to_string(),
            source: "test journal rejected append".into(),
        })
    }

    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        _event_id: &EventId,
    ) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(None)
    }

    async fn reader_from(
        &self,
        _position: u64,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        Err(JournalError::Implementation {
            message: "failing append journal has no reader".to_string(),
            source: "test-only journal".into(),
        })
    }

    async fn read_last_n(
        &self,
        _count: usize,
    ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(Vec::new())
    }
}

fn effect_backpressure_fixture(window: u64) -> (BackpressureRegistry, StageId, StageId) {
    let mut builder = TopologyBuilder::new();
    let upstream_top = builder.add_stage(Some("effect_stage".to_string()));
    let downstream_top = builder.add_stage(Some("downstream".to_string()));
    let topology = builder.build_unchecked().expect("topology");
    let upstream = StageId::from_topology_id(upstream_top);
    let downstream = StageId::from_topology_id(downstream_top);
    let plan = BackpressurePlan::disabled().with_stage_enforced(
        upstream,
        NonZeroU64::new(window).expect("window"),
        std::time::Duration::from_secs(30),
    );
    (
        BackpressureRegistry::new(&topology, &plan),
        upstream,
        downstream,
    )
}

#[derive(Clone, Debug)]
struct CountingEffect {
    value: u64,
    label: &'static str,
    calls: Arc<AtomicUsize>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct CountingOutput {
    value: u64,
}

impl TypedPayload for CountingOutput {
    const EVENT_TYPE: &'static str = "test.counting_output";
}

#[async_trait]
impl Effect for CountingEffect {
    const EFFECT_TYPE: &'static str = "test.counting";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = crate::effects::Portless;

    type Outcome = CountingOutput;
    type OutcomeSemantics = crate::effects::DomainFacts;

    fn label(&self) -> &str {
        self.label
    }

    fn canonical_input(&self) -> Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(CountingOutput {
            value: self.value + 1,
        })
    }
}

#[derive(Clone, Debug)]
struct RecordedReplyEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct RecordedReplyValue {
    value: u64,
    provider_trace: String,
}

fn assert_effect_outcome_fits<E, Output, Proof>()
where
    E: EffectOutcomeFitsOutput<Output, Proof>,
    Output: obzenflow_core::StageFactSet,
{
}

#[async_trait]
impl Effect for RecordedReplyEffect {
    const EFFECT_TYPE: &'static str = "test.recorded_reply";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = crate::effects::Portless;

    type Outcome = RecordedReplyValue;
    type OutcomeSemantics = crate::effects::RecordedReply;

    fn label(&self) -> &str {
        "recorded-reply"
    }

    fn canonical_input(&self) -> Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(RecordedReplyValue {
            value: self.value + 1,
            provider_trace: "integration-material".to_string(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum AdapterSettlement {
    Success,
    Nonfatal,
    Fatal,
}

#[derive(Clone, Debug)]
struct ConsumeOneEffectThenSettle {
    calls: Arc<AtomicUsize>,
    settlement: AdapterSettlement,
}

#[derive(Clone, Debug)]
struct CatchBindingFaultThenSettle {
    invocation_binding: EffectBinding<ZeroSlotNamedEffect>,
    calls: Arc<AtomicUsize>,
    settlement: AdapterSettlement,
}

#[async_trait]
impl crate::stages::common::handlers::EffectfulTransformHandler for CatchBindingFaultThenSettle {
    type Input = FirstOutput;
    type Output = CountingOutput;
    type AllowedEffects = crate::effect_set![ZeroSlotNamedEffect];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, crate::stages::common::handler_error::HandlerError>
    {
        // Build the receipt before the authority fault, then deliberately
        // catch the effect error. Adapter postflight must still make false
        // success impossible.
        let prebuilt_receipt = fx
            .complete_empty()
            .map_err(Into::<crate::stages::common::handler_error::HandlerError>::into)?;
        let _caught = fx
            .perform(ZeroSlotNamedEffect {
                value: input.value,
                calls: self.calls.clone(),
                binding: self.invocation_binding.invocation(),
            })
            .await
            .expect_err("the fixture intentionally mixes construction families");

        match self.settlement {
            AdapterSettlement::Success => Ok(prebuilt_receipt),
            AdapterSettlement::Nonfatal => {
                Err(crate::stages::common::handler_error::HandlerError::Domain(
                    "caught binding fault".to_string(),
                ))
            }
            AdapterSettlement::Fatal => {
                Err(crate::stages::common::handler_error::HandlerError::Fatal(
                    crate::stages::common::handler_error::StageFatal::new(
                        obzenflow_core::event::StageFatalCode::Protocol,
                        obzenflow_core::event::StageFatalReason::ProtocolInputIntegrity,
                        "handler fatal must lose to the binding latch",
                    ),
                ))
            }
        }
    }
}

#[derive(Clone, Debug)]
struct FoldThenCatchBindingFault {
    invocation_binding: EffectBinding<ZeroSlotNamedEffect>,
    valid_calls: Arc<AtomicUsize>,
    mismatched_calls: Arc<AtomicUsize>,
    apply_calls: Arc<AtomicUsize>,
    reject_fold: bool,
    settlement: AdapterSettlement,
}

#[async_trait]
impl crate::stages::common::handlers::EffectfulStatefulHandler for FoldThenCatchBindingFault {
    type State = u64;
    type Input = FirstOutput;
    type Output = CountingOutput;
    type AllowedEffects = crate::effect_set![CountingEffect, ZeroSlotNamedEffect];

    fn initial_state(&self) -> Self::State {
        0
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, crate::stages::common::handler_error::HandlerError>
    {
        fx.perform(CountingEffect {
            value: input.value,
            label: "stateful-before-binding-fault",
            calls: self.valid_calls.clone(),
        })
        .await
        .map_err(crate::stages::common::handler_error::HandlerError::from)?;
        let prebuilt_receipt = fx
            .complete()
            .map_err(crate::stages::common::handler_error::HandlerError::from)?;
        let _caught = fx
            .perform(ZeroSlotNamedEffect {
                value: input.value,
                calls: self.mismatched_calls.clone(),
                binding: self.invocation_binding.invocation(),
            })
            .await
            .expect_err("the fixture intentionally mixes construction families");

        match self.settlement {
            AdapterSettlement::Success => Ok(prebuilt_receipt),
            AdapterSettlement::Nonfatal => {
                Err(crate::stages::common::handler_error::HandlerError::Domain(
                    "caught binding fault after a committed fact".to_string(),
                ))
            }
            AdapterSettlement::Fatal => {
                Err(crate::stages::common::handler_error::HandlerError::Fatal(
                    crate::stages::common::handler_error::StageFatal::new(
                        obzenflow_core::event::StageFatalCode::Protocol,
                        obzenflow_core::event::StageFatalReason::ProtocolInputIntegrity,
                        "handler fatal must lose after the committed fact folds",
                    ),
                ))
            }
        }
    }

    fn apply(
        &mut self,
        state: &mut Self::State,
        fact: Self::Output,
    ) -> Result<(), crate::stages::common::handler_error::HandlerError> {
        self.apply_calls.fetch_add(1, Ordering::SeqCst);
        if self.reject_fold {
            return Err(crate::stages::common::handler_error::HandlerError::Domain(
                "injected fold rejection".to_string(),
            ));
        }
        *state += fact.value;
        Ok(())
    }
}

#[async_trait]
impl crate::stages::common::handlers::EffectfulTransformHandler for ConsumeOneEffectThenSettle {
    type Input = FirstOutput;
    type Output = CountingOutput;
    type AllowedEffects = crate::effect_set![CountingEffect];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, crate::stages::common::handler_error::HandlerError>
    {
        fx.perform(CountingEffect {
            value: input.value,
            label: "first",
            calls: self.calls.clone(),
        })
        .await
        .map_err(|error| {
            crate::stages::common::handler_error::HandlerError::Other(error.to_string())
        })?;
        match self.settlement {
            AdapterSettlement::Success => fx.complete().map_err(Into::into),
            AdapterSettlement::Nonfatal => {
                Err(crate::stages::common::handler_error::HandlerError::Domain(
                    "failed between effect cursors".to_string(),
                ))
            }
            AdapterSettlement::Fatal => {
                Err(crate::stages::common::handler_error::HandlerError::Fatal(
                    crate::stages::common::handler_error::StageFatal::new(
                        obzenflow_core::event::StageFatalCode::Protocol,
                        obzenflow_core::event::StageFatalReason::ProtocolInputIntegrity,
                        "original fatal wins",
                    ),
                ))
            }
        }
    }
}

#[derive(Clone, Debug)]
struct AffineCountingEffect {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for AffineCountingEffect {
    const EFFECT_TYPE: &'static str = "test.affine_counting";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentAtLeastOnce;
    type BindingMode = crate::effects::Portless;

    type Outcome = CountingOutput;
    type OutcomeSemantics = crate::effects::DomainFacts;

    fn label(&self) -> &str {
        "affine-counting"
    }

    fn canonical_input(&self) -> Value {
        json!({ "kind": "affine-counting" })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(CountingOutput { value: 1 })
    }
}

#[derive(Clone, Debug)]
struct InvariantAffineEffect {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for InvariantAffineEffect {
    const EFFECT_TYPE: &'static str = "test.invariant_affine";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentAtLeastOnce;
    type BindingMode = crate::effects::Portless;

    type Outcome = CountingOutput;
    type OutcomeSemantics = crate::effects::DomainFacts;

    fn label(&self) -> &str {
        "invariant-affine"
    }

    fn canonical_input(&self) -> Value {
        json!({ "kind": "invariant-affine" })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(EffectError::target_invariant_violation(
            EffectPortSlot::<()>::new("chat"),
        ))
    }
}

#[derive(Clone, Debug)]
struct FailingEffect {
    label: &'static str,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for FailingEffect {
    const EFFECT_TYPE: &'static str = "test.failing";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = crate::effects::Portless;

    type Outcome = CountingOutput;
    type OutcomeSemantics = crate::effects::DomainFacts;

    fn label(&self) -> &str {
        self.label
    }

    fn canonical_input(&self) -> Value {
        json!({ "kind": "failing" })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(EffectError::Execution("simulated_failure".to_string()))
    }
}

#[derive(Clone, Debug)]
struct TransactionalCountingEffect {
    value: u64,
    normal_calls: Arc<AtomicUsize>,
    binding: EffectBindingUse<Self>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TransactionalCountingEvidence;

impl EffectBindingEvidence for TransactionalCountingEvidence {
    const SCHEMA_VERSION: u32 = 1;

    fn canonical_bytes(&self) -> BoundedBindingEvidence {
        BoundedBindingEvidence::try_new(b"transactional-counting-fixture".to_vec()).unwrap()
    }
}

#[async_trait]
impl Effect for TransactionalCountingEffect {
    const EFFECT_TYPE: &'static str = "test.transactional_counting";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Transactional;
    type BindingMode = Named<TransactionalCountingEvidence>;

    type Outcome = CountingOutput;
    type OutcomeSemantics = crate::effects::DomainFacts;

    fn label(&self) -> &str {
        "transactional"
    }

    fn canonical_input(&self) -> Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.normal_calls.fetch_add(1, Ordering::SeqCst);
        Ok(CountingOutput {
            value: self.value + 10,
        })
    }
}

impl NamedEffect for TransactionalCountingEffect {
    type BindingEvidence = TransactionalCountingEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(transactional_effect_port_slot::<Self>())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct VersionedBindingEvidence(u64);

impl EffectBindingEvidence for VersionedBindingEvidence {
    const SCHEMA_VERSION: u32 = 1;

    fn canonical_bytes(&self) -> BoundedBindingEvidence {
        BoundedBindingEvidence::try_new(self.0.to_be_bytes().to_vec()).unwrap()
    }
}

#[derive(Clone, Debug)]
struct ZeroSlotNamedEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
    binding: EffectBindingUse<Self>,
}

#[async_trait]
impl Effect for ZeroSlotNamedEffect {
    const EFFECT_TYPE: &'static str = "test.zero_slot_named";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = Named<VersionedBindingEvidence>;
    type Outcome = CountingOutput;
    type OutcomeSemantics = crate::effects::DomainFacts;

    fn label(&self) -> &str {
        "zero-slot-named"
    }

    fn canonical_input(&self) -> Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(CountingOutput {
            value: self.value + 1,
        })
    }
}

impl NamedEffect for ZeroSlotNamedEffect {
    type BindingEvidence = VersionedBindingEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::new()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct FirstOutput {
    value: u64,
}

impl TypedPayload for FirstOutput {
    const EVENT_TYPE: &'static str = "test.first_output";
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SecondOutput {
    value: String,
}

impl TypedPayload for SecondOutput {
    const EVENT_TYPE: &'static str = "test.second_output";
}

/// Product carrier for the multi-fact effect tests, derived per FLOWIP-120m:
/// exact reconstruction, one fact per field.
#[derive(Clone, Debug, PartialEq, Eq, obzenflow_core::EffectOutcomeFacts)]
struct MultiFactOutcome {
    first: FirstOutput,
    second: SecondOutput,
}

#[derive(Clone, Debug, obzenflow_core::StageOutputFacts)]
enum OrderedStatefulOutput {
    First(FirstOutput),
    Second(SecondOutput),
}

// `OneFactStageOutput` is an open, law-bearing marker. This deliberately false
// implementation proves the fatal runtime backstop for a trusted manual claim.
impl obzenflow_core::OneFactStageOutput for MultiFactOutcome {}

#[derive(Clone, Debug)]
struct DishonestProductStateful {
    apply_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl crate::stages::common::handlers::EffectfulStatefulHandler for DishonestProductStateful {
    type State = u64;
    type Input = FirstOutput;
    type Output = MultiFactOutcome;
    type AllowedEffects = crate::effect_set![];

    fn initial_state(&self) -> Self::State {
        0
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, crate::stages::common::handler_error::HandlerError>
    {
        fx.emit(FirstOutput { value: input.value }).await?;
        Err(crate::stages::common::handler_error::HandlerError::Domain(
            "failure after dishonest emit".to_string(),
        ))
    }

    fn apply(
        &mut self,
        state: &mut Self::State,
        _fact: Self::Output,
    ) -> Result<(), crate::stages::common::handler_error::HandlerError> {
        self.apply_calls.fetch_add(1, Ordering::SeqCst);
        *state += 1;
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct EmitThenErrorStateful;

#[async_trait]
impl crate::stages::common::handlers::EffectfulStatefulHandler for EmitThenErrorStateful {
    type State = Vec<String>;
    type Input = FirstOutput;
    type Output = OrderedStatefulOutput;
    type AllowedEffects = crate::effect_set![];

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, crate::stages::common::handler_error::HandlerError>
    {
        fx.emit(FirstOutput { value: input.value }).await?;
        fx.emit(SecondOutput {
            value: format!("second-{}", input.value),
        })
        .await?;
        Err(crate::stages::common::handler_error::HandlerError::Domain(
            "failure after committed facts".to_string(),
        ))
    }

    fn apply(
        &mut self,
        state: &mut Self::State,
        fact: Self::Output,
    ) -> Result<(), crate::stages::common::handler_error::HandlerError> {
        match fact {
            OrderedStatefulOutput::First(first) => state.push(format!("first:{}", first.value)),
            OrderedStatefulOutput::Second(second) => state.push(second.value),
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct SingleFactErrorStateful {
    emit: bool,
    fail_apply: bool,
}

#[async_trait]
impl crate::stages::common::handlers::EffectfulStatefulHandler for SingleFactErrorStateful {
    type State = Vec<u64>;
    type Input = FirstOutput;
    type Output = FirstOutput;
    type AllowedEffects = crate::effect_set![];

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, crate::stages::common::handler_error::HandlerError>
    {
        if self.emit {
            fx.emit(FirstOutput { value: input.value }).await?;
        }
        Err(crate::stages::common::handler_error::HandlerError::Domain(
            "decide failed".to_string(),
        ))
    }

    fn apply(
        &mut self,
        state: &mut Self::State,
        fact: Self::Output,
    ) -> Result<(), crate::stages::common::handler_error::HandlerError> {
        if self.fail_apply {
            return Err(
                crate::stages::common::handler_error::HandlerError::Validation(
                    "apply failed".to_string(),
                ),
            );
        }
        state.push(fact.value);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct SecondFactApplyErrorStateful;

#[async_trait]
impl crate::stages::common::handlers::EffectfulStatefulHandler for SecondFactApplyErrorStateful {
    type State = Vec<String>;
    type Input = FirstOutput;
    type Output = OrderedStatefulOutput;
    type AllowedEffects = crate::effect_set![];

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, crate::stages::common::handler_error::HandlerError>
    {
        fx.emit(FirstOutput { value: input.value }).await?;
        fx.emit(SecondOutput {
            value: format!("second-{}", input.value),
        })
        .await?;
        Ok(fx.complete()?)
    }

    fn apply(
        &mut self,
        state: &mut Self::State,
        fact: Self::Output,
    ) -> Result<(), crate::stages::common::handler_error::HandlerError> {
        match fact {
            OrderedStatefulOutput::First(first) => {
                state.push(format!("first:{}", first.value));
                Ok(())
            }
            OrderedStatefulOutput::Second(_) => Err(
                crate::stages::common::handler_error::HandlerError::Validation(
                    "second apply failed".to_string(),
                ),
            ),
        }
    }
}

#[derive(Clone, Debug)]
struct MultiFactEffect {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for MultiFactEffect {
    const EFFECT_TYPE: &'static str = "test.multi_fact";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = crate::effects::Portless;

    type Outcome = MultiFactOutcome;
    type OutcomeSemantics = crate::effects::DomainFacts;

    fn label(&self) -> &str {
        "multi"
    }

    fn canonical_input(&self) -> Value {
        json!({ "kind": "multi" })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(MultiFactOutcome {
            first: FirstOutput { value: 10 },
            second: SecondOutput {
                value: "twenty".to_string(),
            },
        })
    }
}

fn output_contract_for<T: TypedPayload>() -> StageOutputContract {
    StageOutputContract::single(crate::feed_plan::PayloadTypeDescriptor {
        type_hint: TypeHintInfo::Exact {
            name: std::any::type_name::<T>().to_string(),
        },
        event_type: Some(T::versioned_event_type()),
        schema_version: Some(T::SCHEMA_VERSION),
        visibility: crate::feed_plan::FactVisibility::Routable,
    })
}

fn output_contract_for_many(
    outputs: Vec<crate::feed_plan::PayloadTypeDescriptor>,
) -> StageOutputContract {
    StageOutputContract { outputs }
}

fn output_descriptor_for<T: TypedPayload>() -> crate::feed_plan::PayloadTypeDescriptor {
    crate::feed_plan::PayloadTypeDescriptor {
        type_hint: TypeHintInfo::Exact {
            name: std::any::type_name::<T>().to_string(),
        },
        event_type: Some(T::versioned_event_type()),
        schema_version: Some(T::SCHEMA_VERSION),
        visibility: crate::feed_plan::FactVisibility::Routable,
    }
}

#[test]
fn deterministic_typed_output_events_preserve_ordinals() {
    let writer_id = WriterId::from(StageId::new());
    let parent = ChainEventFactory::data_event(writer_id, "test.input.v1", json!({ "id": 1 }));

    let first = deterministic_typed_output_event(
        writer_id,
        &parent,
        FirstOutput { value: 1 },
        "flow-a",
        "stage-a",
        StageInputPosition(4),
        2,
        obzenflow_core::config::LineagePolicy::default(),
    )
    .expect("first output event");
    let second = deterministic_typed_output_event(
        writer_id,
        &parent,
        SecondOutput {
            value: "two".to_string(),
        },
        "flow-a",
        "stage-a",
        StageInputPosition(4),
        3,
        obzenflow_core::config::LineagePolicy::default(),
    )
    .expect("second output event");
    let events = [first, second];

    assert_eq!(events.len(), 2);
    assert!(matches!(
        &events[0].content,
        ChainEventContent::Data { event_type, .. } if event_type == "test.first_output.v1"
    ));
    assert!(matches!(
        &events[1].content,
        ChainEventContent::Data { event_type, .. } if event_type == "test.second_output.v1"
    ));
    assert_eq!(
        events[0].id,
        deterministic_event_id("flow-a", "stage-a", StageInputPosition(4), 2)
    );
    assert_eq!(
        events[1].id,
        deterministic_event_id("flow-a", "stage-a", StageInputPosition(4), 3)
    );
    assert_eq!(events[0].processing_info.event_time, 4_002);
    assert_eq!(events[1].processing_info.event_time, 4_003);
}

struct TransactionalCountingPort {
    calls: Arc<AtomicUsize>,
    commit: bool,
}

fn transactional_counting_binding(
    port: Arc<dyn TransactionalEffectPort<TransactionalCountingEffect>>,
) -> (
    EffectBinding<TransactionalCountingEffect>,
    EffectRegistration<TransactionalCountingEffect>,
) {
    EffectRegistrationBuilder::<TransactionalCountingEffect>::new(
        LogicalEffectBindingName::new("tx").unwrap(),
        TransactionalCountingEvidence,
    )
    .bind_eager(
        transactional_effect_port_slot::<TransactionalCountingEffect>(),
        port,
    )
    .unwrap()
    .finish()
    .unwrap()
}

fn zero_slot_named_binding(
    evidence: u64,
) -> (
    EffectBinding<ZeroSlotNamedEffect>,
    EffectRegistration<ZeroSlotNamedEffect>,
) {
    EffectRegistrationBuilder::<ZeroSlotNamedEffect>::new(
        LogicalEffectBindingName::new("zero_slot").unwrap(),
        VersionedBindingEvidence(evidence),
    )
    .finish()
    .unwrap()
}

fn registry_with_transactional_counting(
    registration: EffectRegistration<TransactionalCountingEffect>,
) -> EffectPortRegistry {
    let mut registry = EffectPortRegistry::new();
    registry.install(registration).unwrap();
    registry
}

#[async_trait]
impl TransactionalEffectPort<TransactionalCountingEffect> for TransactionalCountingPort {
    async fn execute_and_commit(
        &self,
        effect: TransactionalCountingEffect,
        _ctx: &mut EffectContext,
        commit: EffectCommitHandle<CountingOutput>,
    ) -> Result<CountingOutput, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let output = CountingOutput {
            value: effect.value + 1_000,
        };
        if self.commit {
            commit.commit_success(&output).await?;
        }
        Ok(output)
    }
}

/// FLOWIP-120a: a deliberately misbehaving port that commits one value through
/// the handle but returns a different value, used to prove the runtime derives
/// the live return from the committed record rather than the port's return value.
struct DivergentTransactionalPort {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl TransactionalEffectPort<TransactionalCountingEffect> for DivergentTransactionalPort {
    async fn execute_and_commit(
        &self,
        effect: TransactionalCountingEffect,
        _ctx: &mut EffectContext,
        commit: EffectCommitHandle<CountingOutput>,
    ) -> Result<CountingOutput, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let committed = CountingOutput {
            value: effect.value + 1_000,
        };
        commit.commit_success(&committed).await?;
        // Return a value that disagrees with what was committed.
        Ok(CountingOutput {
            value: effect.value + 9_999,
        })
    }
}

/// Commits a failure but returns an ordinary success value, proving that the
/// committed failure remains authoritative through a single-use boundary.
struct CommittedFailureTransactionalPort {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl TransactionalEffectPort<TransactionalCountingEffect> for CommittedFailureTransactionalPort {
    async fn execute_and_commit(
        &self,
        effect: TransactionalCountingEffect,
        _ctx: &mut EffectContext,
        commit: EffectCommitHandle<CountingOutput>,
    ) -> Result<CountingOutput, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        commit
            .commit_failure(&EffectError::Timeout(
                "committed transactional timeout".to_string(),
            ))
            .await?;
        Ok(CountingOutput {
            value: effect.value + 9_999,
        })
    }
}

fn parent_envelope(writer_id: WriterId) -> EventEnvelope<ChainEvent> {
    let event = ChainEventFactory::data_event(writer_id, "test.input", json!({"id": 1}));
    EventEnvelope::new(JournalWriterId::new(), event)
}

fn invocation_context(
    journal: Arc<dyn Journal<ChainEvent>>,
    parent: EventEnvelope<ChainEvent>,
    effect_history: Option<Arc<EffectHistory>>,
) -> EffectInvocationContext {
    let effect_runtime_mode = if effect_history.is_some() {
        EffectRuntimeMode::ReplayStrict
    } else {
        EffectRuntimeMode::Live
    };
    invocation_context_with_mode(
        journal,
        parent,
        effect_history,
        effect_runtime_mode,
        EffectPortRegistry::new(),
    )
}

fn invocation_context_with_mode(
    journal: Arc<dyn Journal<ChainEvent>>,
    parent: EventEnvelope<ChainEvent>,
    effect_history: Option<Arc<EffectHistory>>,
    effect_runtime_mode: EffectRuntimeMode,
    effect_ports: EffectPortRegistry,
) -> EffectInvocationContext {
    let stage_id = StageId::new();
    EffectInvocationContext {
        flow_id: FlowId::new(),
        stage_id,
        stage_key: "effect_stage".to_string(),
        writer_id: WriterId::from(stage_id),
        input_seq: StageInputPosition(1),
        lineage: obzenflow_core::config::LineagePolicy::default(),
        stage_logic_version: "test-v1".to_string(),
        data_journal: journal,
        flow_context: None,
        observers: None,
        system_journal: None,
        instrumentation: None,
        heartbeat_state: None,
        parent,
        effect_history,
        runtime_execution: crate::execution::RuntimeExecution::from_effect_runtime_mode(
            effect_runtime_mode,
            None,
        ),
        effect_ports,
        effect_declarations: vec![
            EffectDeclaration::of::<CountingEffect>(),
            EffectDeclaration::of::<RecordedReplyEffect>(),
            EffectDeclaration::at_least_once::<AffineCountingEffect>(),
            EffectDeclaration::at_least_once::<InvariantAffineEffect>(),
            EffectDeclaration::of::<FailingEffect>(),
            EffectDeclaration::of::<MultiFactEffect>(),
            EffectDeclaration::of::<KeylessEffect>(),
            EffectDeclaration::of::<KeyedEffect>(),
        ],
        output_contract: StageOutputContract::empty(),
        backpressure_writer: BackpressureWriter::disabled(),
        emit_enabled: false,
        effect_boundary: None,
    }
}

fn transactional_invocation_context_with_mode(
    journal: Arc<dyn Journal<ChainEvent>>,
    parent: EventEnvelope<ChainEvent>,
    effect_history: Option<Arc<EffectHistory>>,
    effect_runtime_mode: EffectRuntimeMode,
    effect_ports: EffectPortRegistry,
    binding: &EffectBinding<TransactionalCountingEffect>,
) -> EffectInvocationContext {
    let mut context = invocation_context_with_mode(
        journal,
        parent,
        effect_history,
        effect_runtime_mode,
        effect_ports,
    );
    context
        .effect_declarations
        .push(EffectDeclaration::transactional(binding));
    context
}

fn zero_slot_named_invocation_context_with_mode(
    journal: Arc<dyn Journal<ChainEvent>>,
    parent: EventEnvelope<ChainEvent>,
    effect_history: Option<Arc<EffectHistory>>,
    effect_runtime_mode: EffectRuntimeMode,
    effect_ports: EffectPortRegistry,
    binding: &EffectBinding<ZeroSlotNamedEffect>,
) -> EffectInvocationContext {
    let mut context = invocation_context_with_mode(
        journal,
        parent,
        effect_history,
        effect_runtime_mode,
        effect_ports,
    );
    context
        .effect_declarations
        .push(EffectDeclaration::named(binding));
    context
}

/// Metadata-only archive used to exercise the real resume strategy. Effect
/// history is supplied directly to the invocation context, so none of the
/// archive reader methods should be reached by these unit fixtures.
struct ScopeMatrixArchive;

#[async_trait]
impl crate::replay::ReplayArchive for ScopeMatrixArchive {
    async fn open_source_reader(
        &self,
        _stage_key: &str,
        _expected_type: StageType,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, crate::replay::ReplayError> {
        unreachable!("the execution-scope fixture does not open source readers")
    }

    async fn open_effect_history(
        &self,
        _stage_key: &str,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, crate::replay::ReplayError> {
        unreachable!("the execution-scope fixture supplies effect history directly")
    }

    fn source_data_journal_path(
        &self,
        _stage_key: &str,
    ) -> Result<PathBuf, crate::replay::ReplayError> {
        unreachable!("the execution-scope fixture does not resolve archive paths")
    }

    fn archive_flow_id(&self) -> &str {
        "scope-matrix-archive"
    }

    fn archived_stage_id(&self, _stage_key: &str) -> Result<StageId, crate::replay::ReplayError> {
        unreachable!("the execution-scope fixture does not resolve archived stage ids")
    }

    fn archive_status(&self) -> ArchiveStatus {
        ArchiveStatus::Cancelled
    }

    fn status_derivation(&self) -> StatusDerivation {
        StatusDerivation {
            terminal_events_found: 1,
            chosen: ArchiveStatus::Cancelled,
            warning: None,
        }
    }

    fn allow_incomplete_archive(&self) -> bool {
        false
    }

    fn source_stage_keys(&self) -> Vec<String> {
        Vec::new()
    }

    fn archive_path(&self) -> &Path {
        Path::new("scope-matrix-archive")
    }
}

fn resume_execution() -> crate::execution::RuntimeExecution {
    crate::execution::RuntimeExecution::new(
        crate::execution::RuntimeMode::Resume,
        Some(Arc::new(ScopeMatrixArchive)),
    )
}

#[tokio::test]
async fn generated_pre_effect_terminal_keeps_reconstruction_track_only() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let admission = crate::backpressure::DirectFactAdmission::new(
        obzenflow_core::EventType::from("test.generated-terminal"),
        std::num::NonZeroU64::new(3).expect("non-zero generated bound"),
    );
    let mut ctx = invocation_context_with_mode(
        journal,
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
    );
    ctx.backpressure_writer =
        BackpressureWriter::disabled().with_direct_fact_admission(admission.clone());
    let effects = EffectsCore::new(ctx);

    effects
        .request_generated_live_admission()
        .await
        .expect("reconstruction must not cross the live admission barrier");

    assert!(
        !admission.is_requested(),
        "a reconstruction-authored pre-effect terminal must remain track-only"
    );
    assert_eq!(
        admission
            .close()
            .expect("the reconstruction lease remains closable"),
        0
    );
}

#[tokio::test]
async fn generated_pre_effect_preflight_distinguishes_miss_hit_and_in_doubt() {
    let stage_id = StageId::new();
    let parent = parent_envelope(WriterId::from(stage_id));

    let fresh_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let fresh = EffectsCore::new(invocation_context_with_mode(
        fresh_journal.clone(),
        parent.clone(),
        None,
        EffectRuntimeMode::Live,
        EffectPortRegistry::new(),
    ));
    fresh
        .preflight_next_effect_cursor_is_empty()
        .await
        .expect("a fresh live position is a Miss");
    assert!(fresh_journal.events().is_empty());

    let completed_cursor = EffectCursor::new("archived_flow", "effect_stage", 1_u64, 0_u32);
    let completed_record = EffectRecord {
        cursor: completed_cursor,
        descriptor_hash: EffectDescriptorHash::new("archived-descriptor"),
        descriptor: EffectDescriptor::new(
            CountingEffect::EFFECT_TYPE,
            "archived",
            CountingEffect::SCHEMA_VERSION,
            "test-v1",
            "archived-input",
        ),
        outcome: EffectOutcomePayload::Succeeded {
            output: json!({"value": 10}),
        },
        origin: None,
    };
    let completed_history = Arc::new(
        EffectHistory::from_records("archived_flow", vec![completed_record])
            .expect("completed history indexes"),
    );
    let completed_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let completed = EffectsCore::new(invocation_context_with_mode(
        completed_journal.clone(),
        parent.clone(),
        Some(completed_history),
        EffectRuntimeMode::ResumeIncomplete,
        EffectPortRegistry::new(),
    ));
    let completed_error = completed
        .preflight_next_effect_cursor_is_empty()
        .await
        .expect_err("a pre-effect failure cannot replace an archived completion");
    assert!(matches!(
        completed_error,
        EffectError::EffectProvenanceMismatch(ref message)
            if message.contains("replace an existing terminal")
    ));
    assert!(
        completed_journal.events().is_empty(),
        "preflight is read-only and preserves the archived terminal"
    );

    let in_doubt_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut in_doubt_ctx = invocation_context_with_mode(
        in_doubt_journal.clone(),
        parent.clone(),
        None,
        EffectRuntimeMode::ResumeIncomplete,
        EffectPortRegistry::new(),
    );
    let cursor = EffectCursor::new(
        "archived_flow",
        in_doubt_ctx.stage_key.clone(),
        in_doubt_ctx.input_seq.0,
        0_u32,
    );
    let affine = AffineCountingEffect {
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let descriptor = descriptor_for_effect(
        &affine,
        in_doubt_ctx.stage_logic_version.clone(),
        AffineCountingEffect::EFFECT_TYPE,
        AffineCountingEffect::SCHEMA_VERSION,
        obzenflow_core::EffectBindingIdentity::Portless,
    )
    .expect("affine descriptor");
    let descriptor_hash = descriptor_hash(&descriptor).expect("affine descriptor hash");
    let started = EffectAttemptStarted {
        cursor: cursor.clone(),
        descriptor_hash,
        effect_type: EffectType::new(AffineCountingEffect::EFFECT_TYPE),
        attempt: EffectAttemptOrdinal::new(1),
        outcome_group_id: effect_outcome_group_id(&cursor),
        causal_input_id: parent.event.id,
    };
    let start = build_effect_attempt_started_event(
        in_doubt_ctx.writer_id,
        &parent,
        started,
        descriptor,
        in_doubt_ctx.lineage,
    )
    .expect("Start event");
    let archived_start = EffectHistory::from_cursor_history_for_test(
        "archived_flow",
        cursor,
        EffectCursorHistory {
            attempts: vec![EffectAttemptStarted::try_from_event(&start)
                .expect("archived Start payload decodes")],
            attempt_events: std::collections::BTreeMap::from([(
                EffectAttemptOrdinal::new(1),
                start,
            )]),
            ..EffectCursorHistory::default()
        },
    )
    .expect("archived in-doubt history is valid");
    in_doubt_ctx.effect_history = Some(Arc::new(archived_start));
    let in_doubt = EffectsCore::new(in_doubt_ctx);
    let in_doubt_error = in_doubt
        .preflight_next_effect_cursor_is_empty()
        .await
        .expect_err("a pre-effect failure cannot erase an archived Start");
    assert!(matches!(
        in_doubt_error,
        EffectError::EffectProvenanceMismatch(ref message)
            if message.contains("erase in-doubt Start(1)")
    ));
    assert!(
        in_doubt_journal.events().is_empty(),
        "preflight preserves the archived Start without rematerialising or abandoning it"
    );
}

async fn affine_scope_matrix_histories(
    parent: &EventEnvelope<ChainEvent>,
) -> (Arc<EffectHistory>, Arc<EffectHistory>) {
    let stage_id = StageId::new();
    let completed_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut completed = EffectsCore::new(invocation_context(
        completed_journal.clone(),
        parent.clone(),
        None,
    ));
    completed
        .perform(AffineCountingEffect {
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .await
        .expect("scope-matrix completed archive");
    let completed_cursor = cursor_started_in(&completed_journal);
    let completed_history = archive_current_cursor(&completed_journal, &completed_cursor).await;

    let in_doubt_journal = Arc::new(MemoryJournal::failing_group(
        JournalOwner::stage(stage_id),
        "effect-outcome:v1:",
    ));
    let mut in_doubt = EffectsCore::new(invocation_context(
        in_doubt_journal.clone(),
        parent.clone(),
        None,
    ));
    assert!(matches!(
        in_doubt
            .perform(AffineCountingEffect {
                calls: Arc::new(AtomicUsize::new(0)),
            })
            .await,
        Err(EffectError::Journal(_))
    ));
    let in_doubt_cursor = cursor_started_in(&in_doubt_journal);
    let in_doubt_history = archive_current_cursor(&in_doubt_journal, &in_doubt_cursor).await;

    (completed_history, in_doubt_history)
}

fn direct_fact_scope(
    runtime_execution: &crate::execution::RuntimeExecution,
    stage_id: StageId,
) -> obzenflow_core::MiddlewareExecutionScope {
    runtime_execution.handler_scope_for(crate::execution::ExecutionPositionSource::Data {
        stage_id,
        position: StageInputPosition(1),
        generation: None,
    })
}

async fn assert_scope_matrix_hit(
    runtime_execution: crate::execution::RuntimeExecution,
    history: Arc<EffectHistory>,
    parent: EventEnvelope<ChainEvent>,
    expected_scope: obzenflow_core::MiddlewareExecutionScope,
) {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let admission = crate::backpressure::DirectFactAdmission::new(
        obzenflow_core::EventType::from("test.scope-matrix"),
        NonZeroU64::new(3).expect("non-zero direct-fact bound"),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = invocation_context_with_mode(
        journal.clone(),
        parent,
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
    );
    ctx.runtime_execution = runtime_execution.clone();
    ctx.backpressure_writer =
        BackpressureWriter::disabled().with_direct_fact_admission(admission.clone());
    assert_eq!(
        direct_fact_scope(&runtime_execution, ctx.stage_id),
        expected_scope
    );

    let mut effects = EffectsCore::new(ctx);
    effects
        .perform(AffineCountingEffect {
            calls: calls.clone(),
        })
        .await
        .expect("a history Hit reconstructs without live admission");

    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(!admission.is_requested());
    assert_eq!(
        admission.close().expect("reconstruction lease closes"),
        2,
        "the archived Start and terminal are reconstructed track-only"
    );
    assert_eq!(journal.events().len(), 2);
}

async fn assert_scope_matrix_executable(
    runtime_execution: crate::execution::RuntimeExecution,
    history: Option<Arc<EffectHistory>>,
    parent: EventEnvelope<ChainEvent>,
    expected_prefix_rows: usize,
) {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let admission = crate::backpressure::DirectFactAdmission::new(
        obzenflow_core::EventType::from("test.scope-matrix"),
        NonZeroU64::new(3).expect("non-zero direct-fact bound"),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = invocation_context_with_mode(
        journal.clone(),
        parent,
        history,
        EffectRuntimeMode::ResumeIncomplete,
        EffectPortRegistry::new(),
    );
    ctx.runtime_execution = runtime_execution.clone();
    ctx.backpressure_writer =
        BackpressureWriter::disabled().with_direct_fact_admission(admission.clone());
    assert_eq!(
        direct_fact_scope(&runtime_execution, ctx.stage_id),
        obzenflow_core::MiddlewareExecutionScope::ResumeHandler
    );

    let effect_calls = calls.clone();
    let mut effects = EffectsCore::new(ctx);
    let task = tokio::spawn(async move {
        effects
            .perform(AffineCountingEffect {
                calls: effect_calls,
            })
            .await
    });

    for _ in 0..32 {
        if admission.is_requested() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        admission.is_requested(),
        "the executable branch must park at the live admission barrier"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        journal.events().len(),
        expected_prefix_rows,
        "only the reconstruction prefix may exist before live admission"
    );

    let lease = crate::backpressure::DirectFactLease::try_acquire(
        &BackpressureWriter::disabled(),
        NonZeroU64::new(3).expect("non-zero direct-fact bound"),
    )
    .expect("local-bound admission succeeds")
    .expect("off mode grants a local affine lease");
    admission.grant(lease).expect("single live grant");
    task.await
        .expect("scope-matrix task joins")
        .expect("admitted affine execution succeeds");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        direct_fact_scope(&runtime_execution, stage_id),
        obzenflow_core::MiddlewareExecutionScope::ResumeHandler,
        "live admission must not mutate the frozen handler observer scope"
    );
    assert_eq!(
        admission.close().expect("admitted lease closes"),
        expected_prefix_rows as u64 + 2,
        "one live Start and one terminal settle after any reconstructed prefix"
    );
}

#[tokio::test]
async fn direct_fact_execution_scope_matrix_separates_reconstruction_from_live_admission() {
    let stage_id = StageId::new();
    let parent = parent_envelope(WriterId::from(stage_id));
    let (completed_history, in_doubt_history) = affine_scope_matrix_histories(&parent).await;

    let strict = crate::execution::RuntimeExecution::from_effect_runtime_mode(
        EffectRuntimeMode::ReplayStrict,
        None,
    );
    assert_scope_matrix_hit(
        strict.clone(),
        completed_history.clone(),
        parent.clone(),
        obzenflow_core::MiddlewareExecutionScope::StrictReplayHandler,
    )
    .await;

    let strict_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let strict_admission = crate::backpressure::DirectFactAdmission::new(
        obzenflow_core::EventType::from("test.scope-matrix"),
        NonZeroU64::new(3).expect("non-zero direct-fact bound"),
    );
    let strict_calls = Arc::new(AtomicUsize::new(0));
    let mut strict_ctx = invocation_context_with_mode(
        strict_journal.clone(),
        parent.clone(),
        Some(in_doubt_history.clone()),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
    );
    strict_ctx.runtime_execution = strict;
    strict_ctx.backpressure_writer =
        BackpressureWriter::disabled().with_direct_fact_admission(strict_admission.clone());
    let mut strict_effects = EffectsCore::new(strict_ctx);
    assert!(matches!(
        strict_effects
            .perform(AffineCountingEffect {
                calls: strict_calls.clone(),
            })
            .await,
        Err(EffectError::EffectInDoubt { .. })
    ));
    assert_eq!(strict_calls.load(Ordering::SeqCst), 0);
    assert!(!strict_admission.is_requested());
    assert!(strict_journal.events().is_empty());
    assert_eq!(
        strict_admission
            .close()
            .expect("strict reconstruction lease closes"),
        0
    );

    let incomplete = crate::execution::RuntimeExecution::from_effect_runtime_mode(
        EffectRuntimeMode::ResumeIncomplete,
        None,
    );
    assert_scope_matrix_hit(
        incomplete.clone(),
        completed_history.clone(),
        parent.clone(),
        obzenflow_core::MiddlewareExecutionScope::ResumeHandler,
    )
    .await;
    assert_scope_matrix_executable(incomplete, None, parent.clone(), 0).await;

    let resume_hit = resume_execution();
    assert_scope_matrix_hit(
        resume_hit,
        completed_history,
        parent.clone(),
        obzenflow_core::MiddlewareExecutionScope::ResumeHandler,
    )
    .await;
    assert_scope_matrix_executable(resume_execution(), None, parent.clone(), 0).await;
    assert_scope_matrix_executable(resume_execution(), Some(in_doubt_history), parent, 1).await;
}

#[tokio::test]
async fn affine_start_append_failure_authors_no_terminal_and_never_executes() {
    let stage_id = StageId::new();
    let journal = Arc::new(FailingStartJournal::new(JournalOwner::stage(stage_id)));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut effects = EffectsCore::new(invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));

    let error = effects
        .perform(AffineCountingEffect {
            calls: calls.clone(),
        })
        .await
        .expect_err("injected Start append failure must escape");

    assert!(matches!(error, EffectError::Journal(_)));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        journal.attempted_event_types(),
        vec![EffectAttemptStarted::versioned_event_type()],
        "a failed Start cut must not attempt an effect terminal"
    );
    assert!(journal
        .read_all_unordered()
        .await
        .expect("failed journal remains readable")
        .is_empty());
}

#[tokio::test]
async fn emit_rejects_contexts_without_output_emission_support() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut effects = EffectsCore::new(invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));

    let err = effects
        .emit(FirstOutput { value: 1 })
        .await
        .expect_err("perform-only contexts must reject emitted outputs");

    assert!(matches!(
        err,
        EffectError::EmitUnsupported { stage_key } if stage_key == "effect_stage"
    ));
    assert!(journal.events().is_empty());
}

#[tokio::test]
async fn emit_rejects_fact_type_outside_stage_output_contract() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.emit_enabled = true;
    ctx.output_contract = output_contract_for::<FirstOutput>();
    let mut effects = EffectsCore::new(ctx);

    let err = effects
        .emit(SecondOutput {
            value: "second".to_string(),
        })
        .await
        .expect_err("undeclared output fact types must fail closed");

    assert!(matches!(
        err,
        EffectError::UndeclaredOutput {
            stage_key,
            event_type,
        } if stage_key == "effect_stage" && event_type == SecondOutput::versioned_event_type()
    ));
    assert!(journal.events().is_empty());
}

#[tokio::test]
async fn emit_commits_declared_fact_type_immediately() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.emit_enabled = true;
    ctx.output_contract = output_contract_for::<FirstOutput>();
    let mut effects = EffectsCore::new(ctx);

    effects
        .emit(FirstOutput { value: 7 })
        .await
        .expect("declared output fact should be accepted");

    let events = journal.events();
    assert_eq!(events.len(), 1);
    assert!(matches!(
        &events[0].event.content,
        ChainEventContent::Data { event_type, .. }
            if event_type == FirstOutput::versioned_event_type().as_str()
    ));
}

#[test]
fn typed_completion_requires_explicit_empty_success() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let ctx = invocation_context(journal, parent_envelope(WriterId::from(stage_id)), None);
    let effects = Effects::<FirstOutput, crate::effect_set![]>::new(ctx);

    assert!(matches!(
        effects.complete(),
        Err(EffectError::CompletedWithoutOutput { stage_key })
            if stage_key == "effect_stage"
    ));

    let receipt = effects
        .complete_empty()
        .expect("an explicitly empty invocation completes");
    assert_eq!(receipt.committed_fact_count(), 0);
    assert!(receipt.committed_fact_types().is_empty());
}

#[tokio::test]
async fn typed_completion_reports_direct_facts_in_commit_order() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut ctx = invocation_context(journal, parent_envelope(WriterId::from(stage_id)), None);
    ctx.emit_enabled = true;
    ctx.output_contract = output_contract_for_many(vec![
        output_descriptor_for::<FirstOutput>(),
        output_descriptor_for::<SecondOutput>(),
    ]);
    let mut effects = Effects::<
        obzenflow_core::stage_fact_set![FirstOutput, SecondOutput],
        crate::effect_set![],
    >::new(ctx);

    effects.emit(FirstOutput { value: 1 }).await.unwrap();
    effects
        .emit(SecondOutput {
            value: "second".to_string(),
        })
        .await
        .unwrap();

    assert!(matches!(
        effects.complete_empty(),
        Err(EffectError::CompletedEmptyWithOutput {
            stage_key,
            committed: 2,
        }) if stage_key == "effect_stage"
    ));
    let receipt = effects
        .complete()
        .expect("committed facts complete normally");
    assert_eq!(receipt.committed_fact_count(), 2);
    assert_eq!(
        receipt
            .committed_fact_types()
            .iter()
            .map(obzenflow_core::EventType::as_str)
            .collect::<Vec<_>>(),
        vec![
            FirstOutput::versioned_event_type(),
            SecondOutput::versioned_event_type(),
        ]
    );
}

#[tokio::test]
async fn typed_completion_counts_effect_outcome_facts() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut ctx = invocation_context(journal, parent_envelope(WriterId::from(stage_id)), None);
    ctx.output_contract = output_contract_for::<CountingOutput>();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut effects = Effects::<CountingOutput, crate::effect_set![CountingEffect]>::new(ctx);

    effects
        .perform(CountingEffect {
            value: 7,
            label: "completion",
            calls: calls.clone(),
        })
        .await
        .expect("effect succeeds");

    let receipt = effects.complete().expect("effect fact counts as output");
    assert_eq!(receipt.committed_fact_count(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        receipt.committed_fact_types()[0].as_str(),
        CountingOutput::versioned_event_type()
    );
}

/// Completion is a receipt, not a transaction: an authoring error after this
/// immediate emit cannot roll the fact back.
#[tokio::test]
async fn typed_emit_remains_durable_when_handler_later_errors() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.emit_enabled = true;
    ctx.output_contract = output_contract_for::<FirstOutput>();
    let mut effects = Effects::<FirstOutput, crate::effect_set![]>::new(ctx);

    effects.emit(FirstOutput { value: 9 }).await.unwrap();
    let handler_result: Result<(), crate::stages::common::handler_error::HandlerError> =
        Err(crate::stages::common::handler_error::HandlerError::Domain(
            "failure after emit".to_string(),
        ));
    assert!(handler_result.is_err());
    drop(effects);

    assert_eq!(journal.events().len(), 1, "the emitted fact stays durable");
}

#[tokio::test]
async fn effectful_stateful_folds_committed_facts_before_returning_decide_error() {
    use crate::stages::common::handlers::{
        EffectfulStatefulHandlerAdapter, UnifiedStatefulHandler,
    };

    let stage_id = StageId::new();
    let writer_id = WriterId::from(stage_id);
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let input = ChainEventFactory::data_event(
        writer_id,
        FirstOutput::versioned_event_type(),
        json!({ "value": 9 }),
    );
    let parent = EventEnvelope::new(JournalWriterId::new(), input.clone());
    let mut effect_context = invocation_context(journal.clone(), parent, None);
    effect_context.emit_enabled = true;
    effect_context.output_contract = output_contract_for_many(vec![
        output_descriptor_for::<FirstOutput>(),
        output_descriptor_for::<SecondOutput>(),
    ]);

    let mut adapter = EffectfulStatefulHandlerAdapter(EmitThenErrorStateful);
    let mut state = Vec::new();

    let error = adapter
        .accumulate(
            &mut state,
            input,
            Some(effect_context),
            obzenflow_core::MiddlewareExecutionScope::LiveHandler,
        )
        .await
        .expect_err("the original decide error must still propagate");

    assert!(
        matches!(
            &error,
            crate::stages::common::handler_error::HandlerError::Domain(message)
                if message == "failure after committed facts"
        ),
        "unexpected adapter error: {error:?}"
    );
    assert_eq!(
        state,
        vec!["first:9".to_string(), "second-9".to_string()],
        "committed facts must fold in order"
    );

    let events = journal.events();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].event.event_type(),
        FirstOutput::versioned_event_type().as_str()
    );
    assert_eq!(
        events[1].event.event_type(),
        SecondOutput::versioned_event_type().as_str()
    );
}

#[tokio::test]
async fn effectful_stateful_decide_error_without_commit_leaves_state_unchanged() {
    use crate::stages::common::handlers::{
        EffectfulStatefulHandlerAdapter, UnifiedStatefulHandler,
    };

    let stage_id = StageId::new();
    let writer_id = WriterId::from(stage_id);
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let input = ChainEventFactory::data_event(
        writer_id,
        FirstOutput::versioned_event_type(),
        json!({ "value": 9 }),
    );
    let parent = EventEnvelope::new(JournalWriterId::new(), input.clone());
    let mut effect_context = invocation_context(journal.clone(), parent, None);
    effect_context.emit_enabled = true;
    effect_context.output_contract = output_contract_for::<FirstOutput>();

    let mut adapter = EffectfulStatefulHandlerAdapter(SingleFactErrorStateful {
        emit: false,
        fail_apply: false,
    });
    let mut state = vec![7];

    let error = adapter
        .accumulate(
            &mut state,
            input,
            Some(effect_context),
            obzenflow_core::MiddlewareExecutionScope::LiveHandler,
        )
        .await
        .expect_err("the decide error must propagate");

    assert!(matches!(
        error,
        crate::stages::common::handler_error::HandlerError::Domain(ref message)
            if message == "decide failed"
    ));
    assert_eq!(state, vec![7]);
    assert!(journal.events().is_empty());
}

#[tokio::test]
async fn effectful_stateful_apply_error_takes_precedence_and_discards_draft() {
    use crate::stages::common::handlers::{
        EffectfulStatefulHandlerAdapter, UnifiedStatefulHandler,
    };

    let stage_id = StageId::new();
    let writer_id = WriterId::from(stage_id);
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let input = ChainEventFactory::data_event(
        writer_id,
        FirstOutput::versioned_event_type(),
        json!({ "value": 9 }),
    );
    let parent = EventEnvelope::new(JournalWriterId::new(), input.clone());
    let mut effect_context = invocation_context(journal.clone(), parent, None);
    effect_context.emit_enabled = true;
    effect_context.output_contract = output_contract_for::<FirstOutput>();

    let mut adapter = EffectfulStatefulHandlerAdapter(SingleFactErrorStateful {
        emit: true,
        fail_apply: true,
    });
    let mut state = vec![7];

    let error = adapter
        .accumulate(
            &mut state,
            input,
            Some(effect_context),
            obzenflow_core::MiddlewareExecutionScope::LiveHandler,
        )
        .await
        .expect_err("the apply error must supersede the pending decide error");

    let events = journal.events();
    assert_eq!(events.len(), 1, "the fact remains durable");
    let committed_fact = &events[0].event;
    assert!(matches!(
        error,
        crate::stages::common::handler_error::HandlerError::ContractViolation(ref message)
            if message.contains("effectful_stateful_apply")
                && message.contains(&committed_fact.id.to_string())
                && message.contains(&FirstOutput::versioned_event_type())
                && message.contains("Validation error: apply failed")
    ));
    assert_eq!(state, vec![7], "the failed draft must not be installed");
}

#[tokio::test]
async fn effectful_stateful_second_apply_error_discards_the_whole_ordered_draft() {
    use crate::stages::common::handlers::{
        EffectfulStatefulHandlerAdapter, UnifiedStatefulHandler,
    };

    let stage_id = StageId::new();
    let writer_id = WriterId::from(stage_id);
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let input = ChainEventFactory::data_event(
        writer_id,
        FirstOutput::versioned_event_type(),
        json!({ "value": 9 }),
    );
    let parent = EventEnvelope::new(JournalWriterId::new(), input.clone());
    let mut effect_context = invocation_context(journal.clone(), parent, None);
    effect_context.emit_enabled = true;
    effect_context.output_contract = output_contract_for_many(vec![
        output_descriptor_for::<FirstOutput>(),
        output_descriptor_for::<SecondOutput>(),
    ]);

    let mut adapter = EffectfulStatefulHandlerAdapter(SecondFactApplyErrorStateful);
    let mut state = vec!["installed".to_string()];

    let error = adapter
        .accumulate(
            &mut state,
            input,
            Some(effect_context),
            obzenflow_core::MiddlewareExecutionScope::LiveHandler,
        )
        .await
        .expect_err("the second apply error must be a contract violation");

    let events = journal.events();
    assert_eq!(events.len(), 2, "both facts remain durable");
    assert_eq!(
        events[0].event.event_type(),
        FirstOutput::versioned_event_type(),
        "facts remain in commit order"
    );
    assert_eq!(
        events[1].event.event_type(),
        SecondOutput::versioned_event_type(),
        "facts remain in commit order"
    );
    assert!(matches!(
        error,
        crate::stages::common::handler_error::HandlerError::ContractViolation(ref message)
            if message.contains("effectful_stateful_apply")
                && message.contains(&events[1].event.id.to_string())
                && message.contains(&SecondOutput::versioned_event_type())
                && message.contains("Validation error: second apply failed")
    ));
    assert_eq!(
        state,
        vec!["installed"],
        "the successful first fold must remain confined to the discarded draft"
    );
}

#[tokio::test]
async fn false_one_fact_assertion_takes_precedence_over_decide_error() {
    use crate::stages::common::handlers::{
        EffectfulStatefulHandlerAdapter, UnifiedStatefulHandler,
    };

    let stage_id = StageId::new();
    let writer_id = WriterId::from(stage_id);
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let input = ChainEventFactory::data_event(
        writer_id,
        FirstOutput::versioned_event_type(),
        json!({ "value": 9 }),
    );
    let parent = EventEnvelope::new(JournalWriterId::new(), input.clone());
    let mut effect_context = invocation_context(journal.clone(), parent, None);
    effect_context.emit_enabled = true;
    effect_context.output_contract = output_contract_for_many(vec![
        output_descriptor_for::<FirstOutput>(),
        output_descriptor_for::<SecondOutput>(),
    ]);

    let apply_calls = Arc::new(AtomicUsize::new(0));
    let mut adapter = EffectfulStatefulHandlerAdapter(DishonestProductStateful {
        apply_calls: apply_calls.clone(),
    });
    let mut state = 0;

    let error = adapter
        .accumulate(
            &mut state,
            input,
            Some(effect_context),
            obzenflow_core::MiddlewareExecutionScope::LiveHandler,
        )
        .await
        .expect_err("false one-fact assertion must fail after singleton decoding");

    assert!(error.is_contract_violation());
    assert!(error.to_string().contains("one_fact_stage_output"));
    assert_eq!(state, 0, "the draft state must not be installed");
    assert_eq!(apply_calls.load(Ordering::SeqCst), 0);

    let events = journal.events();
    assert_eq!(events.len(), 1, "the emitted fact was already durable");
    assert!(matches!(
        &events[0].event.content,
        obzenflow_core::event::ChainEventContent::Data { event_type, .. }
            if event_type == FirstOutput::versioned_event_type().as_str()
    ));
}

#[tokio::test]
async fn direct_emit_tracks_honest_over_window_physical_debt() {
    let (registry, stage_id, downstream) = effect_backpressure_fixture(1);
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.stage_id = stage_id;
    ctx.writer_id = WriterId::from(stage_id);
    ctx.backpressure_writer = registry.writer(stage_id);
    ctx.emit_enabled = true;
    let mut effects = EffectsCore::new(ctx);

    for value in 0..3 {
        effects
            .emit(FirstOutput { value })
            .await
            .expect("direct emit does not wait after input admission");
    }

    assert_eq!(journal.events().len(), 3);
    let snapshot = registry.metrics_snapshot();
    assert_eq!(snapshot.stage_writer_seq.get(&stage_id), Some(&3));
    assert_eq!(
        snapshot.edge_in_flight.get(&(stage_id, downstream)),
        Some(&3)
    );
    assert_eq!(
        snapshot.edge_credits.get(&(stage_id, downstream)),
        Some(&0),
        "credits saturate at zero while direct debt remains visible"
    );

    registry.reader(stage_id, downstream).ack_consumed(3);
    assert_eq!(registry.edge_in_flight(stage_id, downstream), Some(0));
}

#[tokio::test]
async fn failed_direct_append_releases_tracked_position_without_writer_advance() {
    let (registry, stage_id, downstream) = effect_backpressure_fixture(2);
    let journal: Arc<dyn Journal<ChainEvent>> = Arc::new(InspectingFailJournal {
        id: JournalId::new(),
        owner: JournalOwner::stage(stage_id),
        registry: registry.clone(),
        upstream: stage_id,
        downstream,
    });
    let mut ctx = invocation_context(journal, parent_envelope(WriterId::from(stage_id)), None);
    ctx.stage_id = stage_id;
    ctx.writer_id = WriterId::from(stage_id);
    ctx.backpressure_writer = registry.writer(stage_id);
    ctx.emit_enabled = true;
    let mut effects = EffectsCore::new(ctx);

    let error = effects
        .emit(FirstOutput { value: 7 })
        .await
        .expect_err("injected append failure");
    assert!(matches!(error, EffectError::Journal(_)));

    let snapshot = registry.metrics_snapshot();
    assert_eq!(snapshot.stage_writer_seq.get(&stage_id), Some(&0));
    assert_eq!(
        snapshot.edge_in_flight.get(&(stage_id, downstream)),
        Some(&0)
    );
    assert_eq!(snapshot.edge_credits.get(&(stage_id, downstream)), Some(&2));
}

#[tokio::test]
async fn emit_rejects_routed_fanout_beyond_contract_member_bound() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.emit_enabled = true;
    ctx.output_contract = output_contract_for::<FirstOutput>();
    let mut effects = EffectsCore::new(ctx);

    effects
        .emit(FirstOutput { value: 1 })
        .await
        .expect("first routed fact should be accepted");
    let err = effects
        .emit(FirstOutput { value: 2 })
        .await
        .expect_err("second routed fact should exceed strict v1 fanout bound");

    assert!(
        matches!(err, EffectError::Execution(message) if message.contains("bounded fanout limit"))
    );
    assert_eq!(journal.events().len(), 1);
}

fn effect_records(journal: &MemoryJournal<ChainEvent>) -> Vec<EffectRecord> {
    journal
        .events()
        .into_iter()
        .filter_map(|envelope| {
            effect_record_from_event(&envelope.event).expect("effect record decode")
        })
        .collect()
}

#[test]
fn effect_history_uses_record_root_over_archive_fallback() {
    let root_cursor = EffectCursor::new("root_flow", "effect_stage", 1, 0);
    let record = EffectRecord {
        cursor: root_cursor,
        descriptor_hash: "hash".into(),
        descriptor: EffectDescriptor::new(
            CountingEffect::EFFECT_TYPE,
            "same",
            CountingEffect::SCHEMA_VERSION,
            "test-v1",
            "input",
        ),
        outcome: EffectOutcomePayload::Succeeded {
            output: json!({ "value": 10 }),
        },
        origin: None,
    };

    let history = EffectHistory::from_records("replay_flow".to_string(), vec![record])
        .expect("history should use record root");

    assert_eq!(history.recorded_flow_id().as_str(), "root_flow");
}

#[test]
fn effect_history_rejects_mixed_record_roots() {
    let descriptor = EffectDescriptor::new(
        CountingEffect::EFFECT_TYPE,
        "same",
        CountingEffect::SCHEMA_VERSION,
        "test-v1",
        "input",
    );
    let first = EffectRecord {
        cursor: EffectCursor::new("root_a", "effect_stage", 1, 0),
        descriptor_hash: "hash".into(),
        descriptor: descriptor.clone(),
        outcome: EffectOutcomePayload::Succeeded {
            output: json!({ "value": 10 }),
        },
        origin: None,
    };
    let second = EffectRecord {
        cursor: EffectCursor::new("root_b", "effect_stage", 2, 0),
        descriptor_hash: "hash".into(),
        descriptor,
        outcome: EffectOutcomePayload::Succeeded {
            output: json!({ "value": 11 }),
        },
        origin: None,
    };

    let err = EffectHistory::from_records("replay_flow".to_string(), vec![first, second])
        .expect_err("mixed roots must fail closed");

    assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
}

#[tokio::test]
async fn live_perform_records_effect_data_fact() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut effects = EffectsCore::new(invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));

    let output = effects
        .perform(CountingEffect {
            value: 41,
            label: "same",
            calls: calls.clone(),
        })
        .await
        .expect("effect should succeed");

    assert_eq!(output.value, 42);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let events = journal.events();
    assert!(matches!(
        events[0].event.content,
        ChainEventContent::Data { .. }
    ));
    let records = effect_records(&journal);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].cursor.input_seq, 1);
    assert_eq!(records[0].cursor.effect_ordinal, 0);
    let provenance = events[0]
        .event
        .effect_provenance
        .as_ref()
        .expect("effect data fact should carry provenance");
    assert_eq!(
        provenance.group_id.as_ref(),
        Some(&effect_outcome_group_id(&records[0].cursor))
    );
}

#[tokio::test]
async fn recorded_reply_is_replay_authority_but_not_a_public_output_fact() {
    assert_effect_outcome_fits::<RecordedReplyEffect, CountingOutput, _>();

    let stage_id = StageId::new();
    let parent = parent_envelope(WriterId::from(stage_id));
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut live_ctx = invocation_context(live_journal.clone(), parent.clone(), None);
    live_ctx.emit_enabled = true;
    live_ctx.output_contract =
        output_contract_for_many(vec![output_descriptor_for::<CountingOutput>()]);
    let live_flow_id = live_ctx.flow_id.to_string();
    let mut live = EffectsCore::new(live_ctx);

    let declaration = EffectDeclaration::of::<RecordedReplyEffect>();
    assert_eq!(declaration.outcome_kind(), EffectOutcomeKind::RecordedReply);
    assert!(declaration.public_outcome_fact_types().is_empty());

    let reply = live
        .perform(RecordedReplyEffect {
            value: 41,
            calls: calls.clone(),
        })
        .await
        .expect("recorded-reply effect succeeds");
    live.emit(CountingOutput { value: reply.value })
        .await
        .expect("domain translation commits");

    assert_eq!(
        reply,
        RecordedReplyValue {
            value: 42,
            provider_trace: "integration-material".to_string(),
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(live.committed_fact_evidence().0, 1);

    let live_events = live_journal.events();
    assert_eq!(live_events.len(), 2);
    assert!(is_framework_effect_event_type(
        &live_events[0].event.event_type()
    ));
    let reply_provenance = live_events[0]
        .event
        .effect_provenance
        .as_ref()
        .expect("recorded reply carries provenance");
    assert!(reply_provenance.fact_owner.is_framework());
    assert_eq!(
        live_events[1].event.id,
        deterministic_event_id(&live_flow_id, "effect_stage", StageInputPosition(1), 0),
        "the recorded reply must not consume a user output ordinal"
    );

    let live_records = effect_records(&live_journal);
    assert_eq!(live_records.len(), 1);
    assert!(matches!(
        live_records[0].outcome,
        EffectOutcomePayload::Succeeded { .. }
    ));
    let history = Arc::new(
        EffectHistory::from_records(live_flow_id, live_records)
            .expect("recorded-reply history loads"),
    );
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut replay_ctx = invocation_context(replay_journal.clone(), parent, Some(history));
    replay_ctx.emit_enabled = true;
    replay_ctx.output_contract =
        output_contract_for_many(vec![output_descriptor_for::<CountingOutput>()]);
    let mut replay = EffectsCore::new(replay_ctx);

    let replayed_reply = replay
        .perform(RecordedReplyEffect {
            value: 41,
            calls: calls.clone(),
        })
        .await
        .expect("recorded reply reconstructs");
    replay
        .emit(CountingOutput {
            value: replayed_reply.value,
        })
        .await
        .expect("replayed domain translation commits");

    assert_eq!(replayed_reply, reply);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "strict replay must not execute the integration effect"
    );
    let replay_events = replay_journal.events();
    assert_eq!(replay_events.len(), 2);
    assert_eq!(replay_events[0].event.id, live_events[0].event.id);
    assert_eq!(replay_events[1].event.id, live_events[1].event.id);
    assert_eq!(replay.committed_fact_evidence().0, 1);
}

async fn adapter_history_fixture(
    effect_count: usize,
) -> (EventEnvelope<ChainEvent>, Arc<EffectHistory>) {
    let stage_id = StageId::new();
    let input = ChainEventFactory::data_event(
        WriterId::from(stage_id),
        FirstOutput::versioned_event_type(),
        json!({ "value": 9 }),
    );
    let parent = EventEnvelope::new(JournalWriterId::new(), input);
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let live_ctx = invocation_context(journal.clone(), parent.clone(), None);
    let recorded_flow_id = live_ctx.flow_id.to_string();
    let mut live = EffectsCore::new(live_ctx);
    live.perform(CountingEffect {
        value: 9,
        label: "first",
        calls: Arc::new(AtomicUsize::new(0)),
    })
    .await
    .expect("first archived effect");
    live.perform(CountingEffect {
        value: 10,
        label: "second",
        calls: Arc::new(AtomicUsize::new(0)),
    })
    .await
    .expect("second archived effect");
    let mut records = effect_records(&journal);
    records.truncate(effect_count);
    (
        parent,
        Arc::new(
            EffectHistory::from_records(recorded_flow_id, records)
                .expect("adapter history indexes"),
        ),
    )
}

async fn run_consume_one_adapter(
    parent: EventEnvelope<ChainEvent>,
    history: Arc<EffectHistory>,
    settlement: AdapterSettlement,
    calls: Arc<AtomicUsize>,
) -> Result<Vec<ChainEvent>, crate::stages::common::handler_error::HandlerError> {
    use crate::stages::common::handlers::{
        EffectfulTransformHandlerAdapter, UnifiedTransformHandler,
    };

    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
    let mut context = invocation_context(journal, parent.clone(), Some(history));
    context.output_contract = output_contract_for::<CountingOutput>();
    let adapter = EffectfulTransformHandlerAdapter::new(
        ConsumeOneEffectThenSettle { calls, settlement },
        Arc::new(AbortingBoundary),
    );
    UnifiedTransformHandler::process(
        &adapter,
        parent.event,
        Some(context),
        obzenflow_core::MiddlewareExecutionScope::StrictReplayHandler,
    )
    .await
}

async fn run_caught_binding_fault_adapter(
    settlement: AdapterSettlement,
) -> (
    Result<Vec<ChainEvent>, crate::stages::common::handler_error::HandlerError>,
    Arc<AtomicUsize>,
    Arc<MemoryJournal<ChainEvent>>,
) {
    use crate::stages::common::handlers::{
        EffectfulTransformHandlerAdapter, UnifiedTransformHandler,
    };

    let stage_id = StageId::new();
    let input = ChainEventFactory::data_event(
        WriterId::from(stage_id),
        FirstOutput::versioned_event_type(),
        json!({ "value": 9 }),
    );
    let parent = EventEnvelope::new(JournalWriterId::new(), input);
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let (declared_binding, declared_registration) = zero_slot_named_binding(7);
    let (invocation_binding, invocation_registration) = zero_slot_named_binding(7);
    drop(invocation_registration);
    let mut registry = EffectPortRegistry::new();
    registry.install(declared_registration).unwrap();
    let mut context = zero_slot_named_invocation_context_with_mode(
        journal.clone(),
        parent.clone(),
        None,
        EffectRuntimeMode::Live,
        registry,
        &declared_binding,
    );
    context.output_contract = output_contract_for::<CountingOutput>();
    let calls = Arc::new(AtomicUsize::new(0));
    let adapter = EffectfulTransformHandlerAdapter::new(
        CatchBindingFaultThenSettle {
            invocation_binding,
            calls: calls.clone(),
            settlement,
        },
        Arc::new(AbortingBoundary),
    );
    let result = UnifiedTransformHandler::process(
        &adapter,
        parent.event,
        Some(context),
        obzenflow_core::MiddlewareExecutionScope::LiveHandler,
    )
    .await;
    (result, calls, journal)
}

#[tokio::test]
async fn transform_adapter_binding_latch_supersedes_every_handler_settlement() {
    for settlement in [
        AdapterSettlement::Success,
        AdapterSettlement::Nonfatal,
        AdapterSettlement::Fatal,
    ] {
        let (result, calls, journal) = run_caught_binding_fault_adapter(settlement).await;
        let error = result.expect_err("a caught binding fault must remain invocation-terminal");
        assert!(matches!(
            error,
            crate::stages::common::handler_error::HandlerError::Fatal(ref fatal)
                if fatal.code == obzenflow_core::event::StageFatalCode::Configuration
                    && fatal.reason
                        == obzenflow_core::event::StageFatalReason::EffectPortBindingMismatch
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(journal.events().is_empty());
    }
}

async fn run_stateful_binding_fault_adapter(
    settlement: AdapterSettlement,
    reject_fold: bool,
) -> (
    Result<(), crate::stages::common::handler_error::HandlerError>,
    u64,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<MemoryJournal<ChainEvent>>,
) {
    use crate::stages::common::handlers::{
        EffectfulStatefulHandlerAdapter, UnifiedStatefulHandler,
    };

    let stage_id = StageId::new();
    let input = ChainEventFactory::data_event(
        WriterId::from(stage_id),
        FirstOutput::versioned_event_type(),
        json!({ "value": 9 }),
    );
    let parent = EventEnvelope::new(JournalWriterId::new(), input.clone());
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let (declared_binding, declared_registration) = zero_slot_named_binding(7);
    let (invocation_binding, invocation_registration) = zero_slot_named_binding(7);
    drop(invocation_registration);
    let mut registry = EffectPortRegistry::new();
    registry.install(declared_registration).unwrap();
    let mut context = zero_slot_named_invocation_context_with_mode(
        journal.clone(),
        parent,
        None,
        EffectRuntimeMode::Live,
        registry,
        &declared_binding,
    );
    context.output_contract = output_contract_for::<CountingOutput>();
    let valid_calls = Arc::new(AtomicUsize::new(0));
    let mismatched_calls = Arc::new(AtomicUsize::new(0));
    let apply_calls = Arc::new(AtomicUsize::new(0));
    let mut adapter = EffectfulStatefulHandlerAdapter(FoldThenCatchBindingFault {
        invocation_binding,
        valid_calls: valid_calls.clone(),
        mismatched_calls: mismatched_calls.clone(),
        apply_calls: apply_calls.clone(),
        reject_fold,
        settlement,
    });
    let mut state = 0;
    let result = UnifiedStatefulHandler::accumulate(
        &mut adapter,
        &mut state,
        input,
        Some(context),
        obzenflow_core::MiddlewareExecutionScope::LiveHandler,
    )
    .await;
    (
        result,
        state,
        valid_calls,
        mismatched_calls,
        apply_calls,
        journal,
    )
}

#[tokio::test]
async fn stateful_adapter_folds_once_then_applies_binding_fatal_precedence() {
    for settlement in [
        AdapterSettlement::Success,
        AdapterSettlement::Nonfatal,
        AdapterSettlement::Fatal,
    ] {
        let (result, state, valid_calls, mismatched_calls, apply_calls, journal) =
            run_stateful_binding_fault_adapter(settlement, false).await;
        let error = result.expect_err("the binding latch wins after the committed fact folds");
        assert!(matches!(
            error,
            crate::stages::common::handler_error::HandlerError::Fatal(ref fatal)
                if fatal.code == obzenflow_core::event::StageFatalCode::Configuration
                    && fatal.reason
                        == obzenflow_core::event::StageFatalReason::EffectPortBindingMismatch
        ));
        assert_eq!(state, 10, "the committed CountingOutput folds exactly once");
        assert_eq!(valid_calls.load(Ordering::SeqCst), 1);
        assert_eq!(mismatched_calls.load(Ordering::SeqCst), 0);
        assert_eq!(apply_calls.load(Ordering::SeqCst), 1);
        assert_eq!(effect_records(&journal).len(), 1);
    }
}

#[tokio::test]
async fn stateful_fold_rejection_precedes_binding_latch_and_preserves_installed_state() {
    let (result, state, valid_calls, mismatched_calls, apply_calls, journal) =
        run_stateful_binding_fault_adapter(AdapterSettlement::Fatal, true).await;
    let error = result.expect_err("rejection of an already committed fact is priority one");
    assert!(matches!(
        error,
        crate::stages::common::handler_error::HandlerError::ContractViolation(ref detail)
            if detail.contains("injected fold rejection")
    ));
    assert_eq!(
        state, 0,
        "a rejected draft must not replace installed state"
    );
    assert_eq!(valid_calls.load(Ordering::SeqCst), 1);
    assert_eq!(mismatched_calls.load(Ordering::SeqCst), 0);
    assert_eq!(apply_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        effect_records(&journal).len(),
        1,
        "the already committed fact is not rolled back"
    );
}

#[tokio::test]
async fn effectful_transform_nonfatal_error_checks_the_next_unused_cursor() {
    let (parent, one_cursor) = adapter_history_fixture(1).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let miss = run_consume_one_adapter(
        parent.clone(),
        one_cursor,
        AdapterSettlement::Nonfatal,
        calls.clone(),
    )
    .await
    .expect_err("the fixture settles with a nonfatal error");
    assert!(matches!(
        miss,
        crate::stages::common::handler_error::HandlerError::Domain(ref message)
            if message == "failed between effect cursors"
    ));

    let (parent, two_cursors) = adapter_history_fixture(2).await;
    let hit = run_consume_one_adapter(
        parent.clone(),
        two_cursors.clone(),
        AdapterSettlement::Nonfatal,
        calls.clone(),
    )
    .await
    .expect_err("unused history must replace the nonfatal error");
    assert!(matches!(
        hit,
        crate::stages::common::handler_error::HandlerError::Fatal(ref fatal)
            if fatal.code == obzenflow_core::event::StageFatalCode::Replay
                && fatal.reason == obzenflow_core::event::StageFatalReason::ReplayDivergence
                && fatal.detail.contains("would replace an existing terminal")
    ));

    let fatal =
        run_consume_one_adapter(parent, two_cursors, AdapterSettlement::Fatal, calls.clone())
            .await
            .expect_err("the fixture settles with its original fatal error");
    assert!(matches!(
        fatal,
        crate::stages::common::handler_error::HandlerError::Fatal(ref fatal)
            if fatal.code == obzenflow_core::event::StageFatalCode::Protocol
                && fatal.reason
                    == obzenflow_core::event::StageFatalReason::ProtocolInputIntegrity
                && fatal.detail == "original fatal wins"
    ));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "history selection and error settlement resolve no live authority"
    );
}

#[tokio::test]
async fn effectful_transform_success_checks_the_next_unused_cursor() {
    let (parent, one_cursor) = adapter_history_fixture(1).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let settled = run_consume_one_adapter(
        parent.clone(),
        one_cursor,
        AdapterSettlement::Success,
        calls.clone(),
    )
    .await
    .expect("settlement at the end of archived history succeeds");
    assert!(settled.is_empty());

    let (parent, two_cursors) = adapter_history_fixture(2).await;
    let divergence = run_consume_one_adapter(
        parent,
        two_cursors,
        AdapterSettlement::Success,
        calls.clone(),
    )
    .await
    .expect_err("successful settlement cannot abandon archived history");
    assert!(matches!(
        divergence,
        crate::stages::common::handler_error::HandlerError::Fatal(ref fatal)
            if fatal.code == obzenflow_core::event::StageFatalCode::Replay
                && fatal.reason == obzenflow_core::event::StageFatalReason::ReplayDivergence
                && fatal.detail.contains("would replace an existing terminal")
    ));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "successful settlement resolves archived effects without live calls"
    );
}

#[tokio::test]
async fn effect_and_capture_rows_reconstruct_identical_debt_without_waiting() {
    let (live_registry, live_stage, live_downstream) = effect_backpressure_fixture(1);
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(live_stage)));
    let parent = parent_envelope(WriterId::from(live_stage));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut live_ctx = invocation_context(live_journal.clone(), parent.clone(), None);
    live_ctx.stage_id = live_stage;
    live_ctx.writer_id = WriterId::from(live_stage);
    live_ctx.backpressure_writer = live_registry.writer(live_stage);
    let live_flow_id = live_ctx.flow_id.to_string();
    let mut live = EffectsCore::new(live_ctx);

    live.perform(CountingEffect {
        value: 41,
        label: "same",
        calls: calls.clone(),
    })
    .await
    .expect("live effect");
    let captured: u64 = live.capture("side_value", 7).await.expect("live capture");
    assert_eq!(captured, 7);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        live_registry.edge_in_flight(live_stage, live_downstream),
        Some(2),
        "effect outcome and capture are both physical Data rows"
    );
    assert_eq!(
        live_registry
            .metrics_snapshot()
            .edge_credits
            .get(&(live_stage, live_downstream)),
        Some(&0)
    );

    let history = Arc::new(
        EffectHistory::from_records(live_flow_id, effect_records(&live_journal))
            .expect("effect history"),
    );

    for mode in [
        EffectRuntimeMode::ReplayStrict,
        EffectRuntimeMode::ResumeIncomplete,
    ] {
        let (registry, stage_id, downstream) = effect_backpressure_fixture(1);
        let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let mut ctx = invocation_context_with_mode(
            journal.clone(),
            parent.clone(),
            Some(history.clone()),
            mode,
            EffectPortRegistry::new(),
        );
        ctx.stage_id = stage_id;
        ctx.writer_id = WriterId::from(stage_id);
        ctx.backpressure_writer = registry.writer(stage_id);
        let mut reconstructed = EffectsCore::new(ctx);

        reconstructed
            .perform(CountingEffect {
                value: 41,
                label: "same",
                calls: calls.clone(),
            })
            .await
            .expect("recorded effect reconstructs");
        let captured: u64 = reconstructed
            .capture("side_value", 7)
            .await
            .expect("recorded capture reconstructs");
        assert_eq!(captured, 7);
        assert_eq!(journal.events().len(), 2);
        assert_eq!(registry.edge_in_flight(stage_id, downstream), Some(2));
        assert_eq!(
            registry.metrics_snapshot().stage_writer_seq.get(&stage_id),
            Some(&2)
        );
    }

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "replay and resume do not re-execute the effect"
    );
}

#[tokio::test]
async fn perform_records_and_replays_multi_fact_effect_outcome_group() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let parent = parent_envelope(WriterId::from(stage_id));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut live_ctx = invocation_context(live_journal.clone(), parent.clone(), None);
    live_ctx.output_contract = output_contract_for_many(vec![
        output_descriptor_for::<FirstOutput>(),
        output_descriptor_for::<SecondOutput>(),
    ]);
    let live_flow_id = live_ctx.flow_id.to_string();
    let mut live_effects = EffectsCore::new(live_ctx);

    let live_output = live_effects
        .perform(MultiFactEffect {
            calls: calls.clone(),
        })
        .await
        .expect("live multi-fact effect succeeds");

    assert_eq!(
        live_output,
        MultiFactOutcome {
            first: FirstOutput { value: 10 },
            second: SecondOutput {
                value: "twenty".to_string()
            },
        }
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let events = live_journal.events();
    assert_eq!(events.len(), 2);
    assert!(matches!(
        &events[0].event.content,
        ChainEventContent::Data { event_type, .. } if event_type == "test.first_output.v1"
    ));
    assert!(matches!(
        &events[1].event.content,
        ChainEventContent::Data { event_type, .. } if event_type == "test.second_output.v1"
    ));
    assert_eq!(
        events[0]
            .event
            .effect_provenance
            .as_ref()
            .and_then(|provenance| provenance.outcome_fact_ordinal),
        Some(OutcomeFactOrdinal::new(0))
    );
    assert_eq!(
        events[1]
            .event
            .effect_provenance
            .as_ref()
            .and_then(|provenance| provenance.outcome_fact_ordinal),
        Some(OutcomeFactOrdinal::new(1))
    );
    assert_eq!(
        events[0]
            .event
            .effect_provenance
            .as_ref()
            .and_then(|provenance| provenance.group_id.as_ref()),
        events[1]
            .event
            .effect_provenance
            .as_ref()
            .and_then(|provenance| provenance.group_id.as_ref())
    );
    assert_eq!(
        events[0].event.id,
        deterministic_event_id(&live_flow_id, "effect_stage", StageInputPosition(1), 0)
    );
    assert_eq!(
        events[1].event.id,
        deterministic_event_id(&live_flow_id, "effect_stage", StageInputPosition(1), 1)
    );

    let records = effect_records(&live_journal);
    let history = Arc::new(
        EffectHistory::from_records(live_flow_id.clone(), records).expect("grouped history loads"),
    );
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut replay_ctx = invocation_context(replay_journal.clone(), parent, Some(history));
    replay_ctx.output_contract = output_contract_for_many(vec![
        output_descriptor_for::<FirstOutput>(),
        output_descriptor_for::<SecondOutput>(),
    ]);
    let mut replay_effects = EffectsCore::new(replay_ctx);

    let replay_output = replay_effects
        .perform(MultiFactEffect {
            calls: calls.clone(),
        })
        .await
        .expect("replay reconstructs multi-fact effect output");

    assert_eq!(replay_output, live_output);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let replay_events = replay_journal.events();
    assert_eq!(replay_events.len(), 2);
    assert_eq!(replay_events[0].event.id, events[0].event.id);
    assert_eq!(replay_events[1].event.id, events[1].event.id);
    assert_eq!(
        replay_events[0].event.effect_provenance,
        events[0].event.effect_provenance
    );
    assert_eq!(
        replay_events[1].event.effect_provenance,
        events[1].event.effect_provenance
    );
}

#[tokio::test]
async fn replay_success_effect_fact_advances_output_ordinals_before_emit() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let parent = parent_envelope(WriterId::from(stage_id));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut live_ctx = invocation_context(live_journal.clone(), parent.clone(), None);
    live_ctx.emit_enabled = true;
    live_ctx.output_contract = output_contract_for_many(vec![
        output_descriptor_for::<CountingOutput>(),
        output_descriptor_for::<SecondOutput>(),
    ]);
    let live_flow_id = live_ctx.flow_id.to_string();
    let mut live_effects = EffectsCore::new(live_ctx);

    let live_output = live_effects
        .perform(CountingEffect {
            value: 41,
            label: "count",
            calls: calls.clone(),
        })
        .await
        .expect("live effect succeeds");
    live_effects
        .emit(SecondOutput {
            value: "after-effect".to_string(),
        })
        .await
        .expect("live emit succeeds");

    assert_eq!(live_output, CountingOutput { value: 42 });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let live_events = live_journal.events();
    assert_eq!(live_events.len(), 2);
    assert_eq!(
        live_events[0].event.id,
        deterministic_event_id(&live_flow_id, "effect_stage", StageInputPosition(1), 0)
    );
    assert_eq!(
        live_events[1].event.id,
        deterministic_event_id(&live_flow_id, "effect_stage", StageInputPosition(1), 1)
    );

    let records = effect_records(&live_journal);
    let history = Arc::new(
        EffectHistory::from_records(live_flow_id.clone(), records).expect("grouped history loads"),
    );
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut replay_ctx = invocation_context(replay_journal.clone(), parent, Some(history));
    replay_ctx.emit_enabled = true;
    replay_ctx.output_contract = output_contract_for_many(vec![
        output_descriptor_for::<CountingOutput>(),
        output_descriptor_for::<SecondOutput>(),
    ]);
    let mut replay_effects = EffectsCore::new(replay_ctx);

    let replay_output = replay_effects
        .perform(CountingEffect {
            value: 41,
            label: "count",
            calls: calls.clone(),
        })
        .await
        .expect("replay reconstructs effect output");
    replay_effects
        .emit(SecondOutput {
            value: "after-effect".to_string(),
        })
        .await
        .expect("replay emit succeeds");

    assert_eq!(replay_output, live_output);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let replay_events = replay_journal.events();
    assert_eq!(replay_events.len(), 2);
    assert_eq!(replay_events[0].event.id, live_events[0].event.id);
    assert_eq!(replay_events[1].event.id, live_events[1].event.id);
    assert_ne!(replay_events[1].event.id, live_events[0].event.id);

    let replay_records = effect_records(&replay_journal);
    let replay_of_replay_history = Arc::new(
        EffectHistory::from_records("replay_archive_flow".to_string(), replay_records)
            .expect("replay history should infer the original root"),
    );
    assert_eq!(
        replay_of_replay_history.recorded_flow_id().as_str(),
        live_flow_id.as_str()
    );
    let replay_of_replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let replay_of_replay_calls = Arc::new(AtomicUsize::new(0));
    let mut replay_of_replay_ctx = invocation_context(
        replay_of_replay_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(replay_of_replay_history),
    );
    replay_of_replay_ctx.emit_enabled = true;
    replay_of_replay_ctx.output_contract = output_contract_for_many(vec![
        output_descriptor_for::<CountingOutput>(),
        output_descriptor_for::<SecondOutput>(),
    ]);
    let mut replay_of_replay_effects = EffectsCore::new(replay_of_replay_ctx);

    let replay_of_replay_output = replay_of_replay_effects
        .perform(CountingEffect {
            value: 41,
            label: "count",
            calls: replay_of_replay_calls.clone(),
        })
        .await
        .expect("replay-of-replay reconstructs effect output");
    replay_of_replay_effects
        .emit(SecondOutput {
            value: "after-effect".to_string(),
        })
        .await
        .expect("replay-of-replay emit succeeds");

    assert_eq!(replay_of_replay_output, live_output);
    assert_eq!(replay_of_replay_calls.load(Ordering::SeqCst), 0);
    let replay_of_replay_events = replay_of_replay_journal.events();
    assert_eq!(replay_of_replay_events[0].event.id, live_events[0].event.id);
    assert_eq!(replay_of_replay_events[1].event.id, live_events[1].event.id);
}

#[test]
fn effect_history_rejects_partial_multi_fact_outcome_group() {
    let cursor = EffectCursor::new("flow", "effect_stage", 1, 0);
    let descriptor = EffectDescriptor::new(
        MultiFactEffect::EFFECT_TYPE,
        "multi",
        MultiFactEffect::SCHEMA_VERSION,
        "test-v1",
        "input",
    );
    let records = vec![
        EffectRecord {
            cursor: cursor.clone(),
            descriptor_hash: "hash".into(),
            descriptor: descriptor.clone(),
            outcome: EffectOutcomePayload::SucceededFact {
                event_type: FirstOutput::versioned_event_type().into(),
                output: json!({ "value": 10 }),
                outcome_fact_ordinal: OutcomeFactOrdinal::new(0),
                outcome_fact_count: OutcomeFactCount::new(3),
            },
            origin: None,
        },
        EffectRecord {
            cursor,
            descriptor_hash: "hash".into(),
            descriptor,
            outcome: EffectOutcomePayload::SucceededFact {
                event_type: SecondOutput::versioned_event_type().into(),
                output: json!({ "value": "twenty" }),
                outcome_fact_ordinal: OutcomeFactOrdinal::new(2),
                outcome_fact_count: OutcomeFactCount::new(3),
            },
            origin: None,
        },
    ];

    let err = EffectHistory::from_records("flow".to_string(), records)
        .expect_err("missing ordinal 1 must fail loud");

    assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
}

#[test]
fn incomplete_outcome_group_torn_tail_is_dropped_as_absent() {
    // FLOWIP-120q: a group missing its top ordinal (a torn tail dropped fact 2)
    // is detected via the recorded count and treated as absent, so load
    // succeeds and the cursor is not found (replay re-executes / errors absent).
    let cursor = EffectCursor::new("flow", "stage", 0, 0);
    let descriptor = EffectDescriptor::new("fx", "fx", 1, "1", "input");
    let fact = |ordinal: u32| EffectRecord {
        cursor: cursor.clone(),
        descriptor_hash: "hash".into(),
        descriptor: descriptor.clone(),
        outcome: EffectOutcomePayload::SucceededFact {
            event_type: "fx.out".into(),
            output: json!({ "ordinal": ordinal }),
            outcome_fact_ordinal: OutcomeFactOrdinal::new(ordinal),
            // The group declared 3 facts, but only 0 and 1 survived the tail.
            outcome_fact_count: OutcomeFactCount::new(3),
        },
        origin: None,
    };

    let history = EffectHistory::from_records("flow".to_string(), vec![fact(0), fact(1)])
        .expect("an incomplete final group is dropped, not an error");

    assert!(
        history.find_group(&cursor).is_none(),
        "a torn-tail outcome group must be treated as absent"
    );
}

#[test]
fn incomplete_outcome_group_on_completed_archive_fails_loud() {
    // FLOWIP-120q: the same incomplete group that a torn-tail-tolerant load drops
    // (above) must fail loud under the strict policy a `Completed` archive uses,
    // because a clean archive flushed every record and permits no torn tail.
    let cursor = EffectCursor::new("flow", "stage", 0, 0);
    let descriptor = EffectDescriptor::new("fx", "fx", 1, "1", "input");
    let fact = |ordinal: u32| EffectRecord {
        cursor: cursor.clone(),
        descriptor_hash: "hash".into(),
        descriptor: descriptor.clone(),
        outcome: EffectOutcomePayload::SucceededFact {
            event_type: "fx.out".into(),
            output: json!({ "ordinal": ordinal }),
            outcome_fact_ordinal: OutcomeFactOrdinal::new(ordinal),
            outcome_fact_count: OutcomeFactCount::new(3),
        },
        origin: None,
    };

    let err =
        EffectHistory::from_records_with_policy("flow".to_string(), vec![fact(0), fact(1)], false)
            .expect_err("an incomplete group on a completed archive must fail loud");

    assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
}

#[test]
fn interleaved_incomplete_groups_all_drop_on_interrupted_archive() {
    // FLOWIP-120q: torn-tail tolerance is position-independent. Interleaved
    // commits can leave more than one group incomplete; an interrupted archive
    // drops them all as absent regardless of journal order (resume re-executes).
    // Locks out any "only the last group may be incomplete" positional assumption.
    let cursor_a = EffectCursor::new("flow", "stage", 0, 0);
    let cursor_b = EffectCursor::new("flow", "stage", 1, 0);
    let descriptor = EffectDescriptor::new("fx", "fx", 1, "1", "input");
    let fact = |cursor: &EffectCursor, ordinal: u32, count: u32| EffectRecord {
        cursor: cursor.clone(),
        descriptor_hash: "hash".into(),
        descriptor: descriptor.clone(),
        outcome: EffectOutcomePayload::SucceededFact {
            event_type: "fx.out".into(),
            output: json!({ "ordinal": ordinal }),
            outcome_fact_ordinal: OutcomeFactOrdinal::new(ordinal),
            outcome_fact_count: OutcomeFactCount::new(count),
        },
        origin: None,
    };

    // Journal order interleaves the groups, each missing its top ordinal(s):
    // A is {0,1} of 3, B is {0} of 2, B's fact sits between A's two facts.
    let records = vec![
        fact(&cursor_a, 0, 3),
        fact(&cursor_b, 0, 2),
        fact(&cursor_a, 1, 3),
    ];

    let history = EffectHistory::from_records("flow".to_string(), records)
        .expect("an interrupted archive tolerates multiple torn-tail groups");

    assert!(
        history.find_group(&cursor_a).is_none(),
        "group A is dropped as absent"
    );
    assert!(
        history.find_group(&cursor_b).is_none(),
        "group B is dropped as absent"
    );
}

#[tokio::test]
async fn perform_rejects_undeclared_effect_before_execution() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.effect_declarations.clear();
    let mut effects = EffectsCore::new(ctx);

    let err = effects
        .perform(CountingEffect {
            value: 41,
            label: "same",
            calls: calls.clone(),
        })
        .await
        .expect_err("undeclared effects must fail before execution");

    match err {
        EffectError::UndeclaredEffect {
            stage_key,
            effect_type,
        } => {
            assert_eq!(stage_key, "effect_stage");
            assert_eq!(effect_type, CountingEffect::EFFECT_TYPE);
        }
        other => panic!("unexpected effect error: {other:?}"),
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(effect_records(&journal).is_empty());
}

#[tokio::test]
async fn strict_replay_rejects_undeclared_effect_before_history_lookup() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_parent = parent_envelope(WriterId::from(stage_id));
    let mut live = EffectsCore::new(invocation_context(live_journal.clone(), live_parent, None));
    live.perform(CountingEffect {
        value: 41,
        label: "same",
        calls: live_calls,
    })
    .await
    .expect("live effect should succeed");

    let live_records = effect_records(&live_journal);
    let history = Arc::new(
        EffectHistory::from_records(
            live_records[0].cursor.recorded_flow_id.clone(),
            live_records,
        )
        .expect("history should index"),
    );
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = invocation_context(
        replay_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
    );
    ctx.stage_logic_version = "test-v2".to_string();
    ctx.effect_declarations.clear();
    let mut replay = EffectsCore::new(ctx);

    let err = replay
        .perform(CountingEffect {
            value: 41,
            label: "same",
            calls: replay_calls.clone(),
        })
        .await
        .expect_err("undeclared effects must fail before replay history lookup");

    match err {
        EffectError::UndeclaredEffect {
            stage_key,
            effect_type,
        } => {
            assert_eq!(stage_key, "effect_stage");
            assert_eq!(effect_type, CountingEffect::EFFECT_TYPE);
        }
        other => panic!("unexpected effect error: {other:?}"),
    }
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert!(effect_records(&replay_journal).is_empty());
}

#[tokio::test]
async fn capture_is_exempt_from_declared_effect_list() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.effect_declarations.clear();
    let mut effects = EffectsCore::new(ctx);

    let captured: u64 = effects
        .capture("side_value", 7)
        .await
        .expect("capture should not require an effect declaration");

    assert_eq!(captured, 7);
    let events = journal.events();
    assert!(matches!(
        &events[0].event.content,
        ChainEventContent::Data { event_type, .. } if event_type == CAPTURE_EVENT_TYPE
    ));
    assert!(events[0].event.effect_provenance.is_some());
    assert_eq!(effect_records(&journal).len(), 1);
}

#[tokio::test]
async fn replay_perform_uses_recorded_output_without_execute() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_parent = parent_envelope(WriterId::from(stage_id));
    let mut live = EffectsCore::new(invocation_context(journal.clone(), live_parent, None));
    let output = live
        .perform(CountingEffect {
            value: 9,
            label: "same",
            calls: live_calls,
        })
        .await
        .expect("live effect should succeed");
    assert_eq!(output.value, 10);

    let records = effect_records(&journal);
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("history should index"),
    );
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let mut replay = EffectsCore::new(invocation_context(
        Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
    ));

    let replayed = replay
        .perform(CountingEffect {
            value: 9,
            label: "same",
            calls: replay_calls.clone(),
        })
        .await
        .expect("replay should read recorded output");

    assert_eq!(replayed.value, 10);
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn strict_replay_missing_effect_record_fails_without_execute() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let history = Arc::new(
        EffectHistory::from_records("archived_flow".to_string(), Vec::new())
            .expect("empty history should index"),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let mut effects = EffectsCore::new(invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
    ));

    let err = effects
        .perform(CountingEffect {
            value: 9,
            label: "same",
            calls: calls.clone(),
        })
        .await
        .expect_err("strict replay must fail when the cursor is missing");

    assert!(matches!(err, EffectError::MissingRecordedEffect { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(effect_records(&journal).is_empty());
}

#[tokio::test]
async fn failed_effect_records_are_replayed_into_replay_history() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let live_calls = Arc::new(AtomicUsize::new(0));
    let mut live = EffectsCore::new(invocation_context(
        live_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));

    let live_err = live
        .perform(FailingEffect {
            label: "fail",
            calls: live_calls.clone(),
        })
        .await
        .expect_err("live effect should fail");

    assert!(matches!(live_err, EffectError::Execution(_)));
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);
    let live_events = live_journal.events();
    assert_eq!(live_events.len(), 1);
    let live_records = effect_records(&live_journal);
    assert_eq!(live_records.len(), 1);
    let live_record = live_records[0].clone();
    let root_recorded_flow_id = live_record.cursor.recorded_flow_id.clone();
    assert!(matches!(
        live_record.outcome,
        EffectOutcomePayload::Failed { .. }
    ));
    assert_eq!(
        live_events[0].event.id,
        deterministic_effect_record_event_id(&live_record.cursor, EFFECT_RECORD_EVENT_TYPE)
    );

    let replay_history = Arc::new(
        EffectHistory::from_records("replay_archive_flow".to_string(), live_records)
            .expect("history should infer the live root from records"),
    );
    assert_eq!(replay_history.recorded_flow_id(), &root_recorded_flow_id);
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let mut replay = EffectsCore::new(invocation_context(
        replay_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(replay_history),
    ));

    let replay_err = replay
        .perform(FailingEffect {
            label: "fail",
            calls: replay_calls.clone(),
        })
        .await
        .expect_err("strict replay should return the recorded failure");

    assert!(matches!(replay_err, EffectError::RecordedFailure { .. }));
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    let replay_events = replay_journal.events();
    assert_eq!(replay_events.len(), 1);
    assert_eq!(replay_events[0].event.id, live_events[0].event.id);
    let replay_records = effect_records(&replay_journal);
    assert_eq!(replay_records, vec![live_record.clone()]);

    let replay_of_replay_history = Arc::new(
        EffectHistory::from_records("second_replay_archive_flow".to_string(), replay_records)
            .expect("replay archive history should stay keyed by the live root"),
    );
    assert_eq!(
        replay_of_replay_history.recorded_flow_id(),
        &root_recorded_flow_id
    );
    let replay_of_replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let replay_of_replay_calls = Arc::new(AtomicUsize::new(0));
    let mut replay_of_replay = EffectsCore::new(invocation_context(
        replay_of_replay_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(replay_of_replay_history),
    ));

    let replay_of_replay_err = replay_of_replay
        .perform(FailingEffect {
            label: "fail",
            calls: replay_of_replay_calls.clone(),
        })
        .await
        .expect_err("replay-of-replay should return the recorded failure");

    assert!(matches!(
        replay_of_replay_err,
        EffectError::RecordedFailure { .. }
    ));
    assert_eq!(replay_of_replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        replay_of_replay_journal.events()[0].event.id,
        live_events[0].event.id
    );
}

#[tokio::test]
async fn resume_incomplete_missing_effect_executes_with_recorded_cursor() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let history = Arc::new(
        EffectHistory::from_records("archived_flow".to_string(), Vec::new())
            .expect("empty history should index"),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let mut effects = EffectsCore::new(invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ResumeIncomplete,
        EffectPortRegistry::new(),
    ));

    let output = effects
        .perform(CountingEffect {
            value: 9,
            label: "same",
            calls: calls.clone(),
        })
        .await
        .expect("resume should execute missing effect live");

    assert_eq!(output.value, 10);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let records = effect_records(&journal);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].cursor.recorded_flow_id, "archived_flow");
    assert_eq!(records[0].cursor.stage_key, "effect_stage");
    assert_eq!(records[0].cursor.input_seq, 1);
    assert_eq!(records[0].cursor.effect_ordinal, 0);
}

#[tokio::test]
async fn resume_incomplete_recorded_effect_suppresses_execution() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let live_calls = Arc::new(AtomicUsize::new(0));
    let mut live = EffectsCore::new(invocation_context(
        live_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));
    live.perform(CountingEffect {
        value: 9,
        label: "same",
        calls: live_calls,
    })
    .await
    .expect("live effect should record");
    let records = effect_records(&live_journal);
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("history should index"),
    );
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let mut resume = EffectsCore::new(invocation_context_with_mode(
        Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ResumeIncomplete,
        EffectPortRegistry::new(),
    ));

    let output = resume
        .perform(CountingEffect {
            value: 9,
            label: "same",
            calls: replay_calls.clone(),
        })
        .await
        .expect("resume should use recorded output");

    assert_eq!(output.value, 10);
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn transactional_effect_uses_registered_port_and_commits_once() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let transactional_calls = Arc::new(AtomicUsize::new(0));
    let (binding, registration) =
        transactional_counting_binding(Arc::new(TransactionalCountingPort {
            calls: transactional_calls.clone(),
            commit: true,
        }));
    let ports = registry_with_transactional_counting(registration);
    let mut effects = EffectsCore::new(transactional_invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
        &binding,
    ));

    let output = effects
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect("transactional port should commit");

    assert_eq!(output.value, 1_007);
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
    assert_eq!(transactional_calls.load(Ordering::SeqCst), 1);
    let records = effect_records(&journal);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].cursor.effect_ordinal, 0);
}

#[tokio::test]
async fn transactional_effect_live_return_comes_from_committed_record_not_port_return() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let port_calls = Arc::new(AtomicUsize::new(0));
    let (binding, registration) =
        transactional_counting_binding(Arc::new(DivergentTransactionalPort {
            calls: port_calls.clone(),
        }));
    let ports = registry_with_transactional_counting(registration);
    let mut live = EffectsCore::new(transactional_invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
        &binding,
    ));

    let live_output = live
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect("transactional effect should commit");

    // The live return is the committed value (7 + 1000), NOT the value the port
    // returned (7 + 9999). Without the structural fix this would be 10_006 live
    // and 1_007 on replay, a divergence.
    assert_eq!(live_output.value, 1_007);
    assert_eq!(port_calls.load(Ordering::SeqCst), 1);

    let records = effect_records(&journal);
    assert_eq!(records.len(), 1);
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("history should index"),
    );
    let mut replay = EffectsCore::new(transactional_invocation_context_with_mode(
        Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
        &binding,
    ));

    let replay_output = replay
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect("strict replay should reconstruct the committed value");

    assert_eq!(
        replay_output, live_output,
        "live and replay must agree on the committed outcome"
    );
    assert_eq!(
        port_calls.load(Ordering::SeqCst),
        1,
        "replay must not invoke the transactional port"
    );
}

#[tokio::test]
async fn transactional_effect_replay_does_not_require_port_or_execute() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let transactional_calls = Arc::new(AtomicUsize::new(0));
    let (binding, registration) =
        transactional_counting_binding(Arc::new(TransactionalCountingPort {
            calls: transactional_calls,
            commit: true,
        }));
    let ports = registry_with_transactional_counting(registration);
    let mut live = EffectsCore::new(transactional_invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
        &binding,
    ));
    live.perform(TransactionalCountingEffect {
        value: 7,
        normal_calls: normal_calls.clone(),
        binding: binding.invocation(),
    })
    .await
    .expect("live transactional effect should commit");

    let records = effect_records(&journal);
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("history should index"),
    );
    let mut replay = EffectsCore::new(transactional_invocation_context_with_mode(
        Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
        &binding,
    ));

    let output = replay
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect("strict replay should use recorded transactional output");

    assert_eq!(output.value, 1_007);
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn transactional_effect_missing_commit_fails() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let transactional_calls = Arc::new(AtomicUsize::new(0));
    let (binding, registration) =
        transactional_counting_binding(Arc::new(TransactionalCountingPort {
            calls: transactional_calls.clone(),
            commit: false,
        }));
    let ports = registry_with_transactional_counting(registration);
    let mut effects = EffectsCore::new(transactional_invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
        &binding,
    ));

    let err = effects
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect_err("transactional port returning without commit must fail");

    assert!(matches!(
        err,
        EffectError::TransactionalCommitMissing { .. }
    ));
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
    assert_eq!(transactional_calls.load(Ordering::SeqCst), 1);
    assert!(effect_records(&journal).is_empty());
}

#[tokio::test]
async fn transactional_effect_missing_port_fails_before_execute() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let (binding, registration) =
        transactional_counting_binding(Arc::new(TransactionalCountingPort {
            calls: Arc::new(AtomicUsize::new(0)),
            commit: true,
        }));
    drop(registration);
    let mut effects = EffectsCore::new(transactional_invocation_context_with_mode(
        journal,
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        EffectPortRegistry::new(),
        &binding,
    ));

    let err = effects
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect_err("missing transactional port must fail before execution");

    let EffectError::BindingAuthority { fault } = err else {
        panic!("expected a binding-authority fault")
    };
    assert_eq!(
        fault.reason(),
        obzenflow_core::event::StageFatalReason::EffectPortRegistrationMissing
    );
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn named_binding_family_mismatch_latches_before_cursor_or_io() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let declared_port_calls = Arc::new(AtomicUsize::new(0));
    let invocation_port_calls = Arc::new(AtomicUsize::new(0));
    let normal_calls = Arc::new(AtomicUsize::new(0));

    let (declared_binding, declared_registration) =
        transactional_counting_binding(Arc::new(TransactionalCountingPort {
            calls: declared_port_calls.clone(),
            commit: true,
        }));
    let (invocation_binding, invocation_registration) =
        transactional_counting_binding(Arc::new(TransactionalCountingPort {
            calls: invocation_port_calls.clone(),
            commit: true,
        }));
    drop(invocation_registration);

    // Both constructions have the same public name and evidence, but each
    // builder mints a distinct, opaque construction-family token. A binding
    // from one family must never borrow another family's installed authority.
    assert_eq!(
        EffectDeclaration::transactional(&declared_binding).binding_identity(),
        EffectDeclaration::transactional(&invocation_binding).binding_identity(),
        "durable descriptor identity is evidence-based, not process-family-based"
    );

    let ports = registry_with_transactional_counting(declared_registration);
    let mut effects = EffectsCore::new(transactional_invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
        &declared_binding,
    ));

    let first_error = effects
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: invocation_binding.invocation(),
        })
        .await
        .expect_err("an independently constructed binding must be rejected");
    let first_fault = match first_error {
        EffectError::BindingAuthority { fault } => fault,
        other => panic!("expected a binding-authority fault, got {other:?}"),
    };
    assert_eq!(
        first_fault.mismatch_kind(),
        Some(BindingMismatchKind::ConstructionFamily)
    );
    assert_eq!(
        effects.next_effect_ordinal_for_test(),
        EffectOrdinal::new(0),
        "authority is checked before reserving a durable effect cursor"
    );
    assert!(journal.events().is_empty());
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
    assert_eq!(declared_port_calls.load(Ordering::SeqCst), 0);
    assert_eq!(invocation_port_calls.load(Ordering::SeqCst), 0);

    let ensure_fault = match effects
        .ensure_authoring_open()
        .expect_err("the first authority fault closes the invocation")
    {
        EffectError::BindingAuthority { fault } => fault,
        other => panic!("expected the latched binding-authority fault, got {other:?}"),
    };
    assert_eq!(ensure_fault, first_fault);

    let capture_fault = match effects
        .capture("after-binding-fault", 1_u64)
        .await
        .expect_err("capture must be closed after a binding-authority fault")
    {
        EffectError::BindingAuthority { fault } => fault,
        other => panic!("expected the latched binding-authority fault, got {other:?}"),
    };
    assert_eq!(capture_fault, first_fault);

    let emit_fault = match effects
        .emit(CountingOutput { value: 1 })
        .await
        .expect_err("emit must be closed after a binding-authority fault")
    {
        EffectError::BindingAuthority { fault } => fault,
        other => panic!("expected the latched binding-authority fault, got {other:?}"),
    };
    assert_eq!(emit_fault, first_fault);

    let later_perform_fault = match effects
        .perform(TransactionalCountingEffect {
            value: 8,
            normal_calls: normal_calls.clone(),
            binding: declared_binding.invocation(),
        })
        .await
        .expect_err("even a valid later binding cannot reopen the invocation")
    {
        EffectError::BindingAuthority { fault } => fault,
        other => panic!("expected the latched binding-authority fault, got {other:?}"),
    };
    assert_eq!(later_perform_fault, first_fault);

    let fatal = effects
        .binding_fault_fatal()
        .expect("the latched authority fault must map to a stage fatal");
    assert_eq!(
        fatal.code,
        obzenflow_core::event::StageFatalCode::Configuration
    );
    assert_eq!(
        fatal.reason,
        obzenflow_core::event::StageFatalReason::EffectPortBindingMismatch
    );
    assert!(journal.events().is_empty());
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
    assert_eq!(declared_port_calls.load(Ordering::SeqCst), 0);
    assert_eq!(invocation_port_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn rebuilt_named_binding_replays_by_evidence_and_changed_evidence_rejects() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let live_calls = Arc::new(AtomicUsize::new(0));
    let (live_binding, live_registration) = zero_slot_named_binding(7);
    let mut live_registry = EffectPortRegistry::new();
    live_registry.install(live_registration).unwrap();
    let mut live = EffectsCore::new(zero_slot_named_invocation_context_with_mode(
        live_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        live_registry,
        &live_binding,
    ));

    let live_output = live
        .perform(ZeroSlotNamedEffect {
            value: 11,
            calls: live_calls.clone(),
            binding: live_binding.invocation(),
        })
        .await
        .expect("the live zero-slot named effect should execute");
    assert_eq!(live_output, CountingOutput { value: 12 });
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);

    let records = effect_records(&live_journal);
    let live_descriptor = records[0].descriptor.clone();
    assert!(matches!(
        live_descriptor.binding,
        obzenflow_core::EffectBindingIdentity::Named { .. }
    ));
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("live history should index"),
    );

    // A new materialisation necessarily mints a different construction
    // family. Equal versioned evidence must nevertheless reproduce the
    // descriptor and authorise strict replay without a registration.
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let (rebuilt_binding, rebuilt_registration) = zero_slot_named_binding(7);
    drop(rebuilt_registration);
    assert!(!live_binding.shares_construction_family(&rebuilt_binding));
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
    let mut replay = EffectsCore::new(zero_slot_named_invocation_context_with_mode(
        replay_journal,
        parent_envelope(WriterId::from(stage_id)),
        Some(history.clone()),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
        &rebuilt_binding,
    ));
    let replay_output = replay
        .perform(ZeroSlotNamedEffect {
            value: 11,
            calls: replay_calls.clone(),
            binding: rebuilt_binding.invocation(),
        })
        .await
        .expect("same evidence in a rebuilt family should replay");
    assert_eq!(replay_output, live_output);
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);

    // Changing only evidence changes the durable descriptor. Declaration and
    // invocation still share their new family, so rejection is a replay
    // descriptor mismatch, before any live authority can be consulted.
    let changed_calls = Arc::new(AtomicUsize::new(0));
    let (changed_binding, changed_registration) = zero_slot_named_binding(8);
    drop(changed_registration);
    let changed_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
    let mut changed_replay = EffectsCore::new(zero_slot_named_invocation_context_with_mode(
        changed_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
        &changed_binding,
    ));
    let error = changed_replay
        .perform(ZeroSlotNamedEffect {
            value: 11,
            calls: changed_calls.clone(),
            binding: changed_binding.invocation(),
        })
        .await
        .expect_err("changed evidence must reject the archived descriptor");
    assert!(matches!(error, EffectError::DescriptorMismatch { .. }));
    assert_eq!(changed_calls.load(Ordering::SeqCst), 0);
    assert!(changed_journal.events().is_empty());
}

#[tokio::test]
async fn replay_fails_on_descriptor_hash_mismatch() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut live = EffectsCore::new(invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));
    live.perform(CountingEffect {
        value: 1,
        label: "original",
        calls,
    })
    .await
    .expect("live effect should succeed");
    let records = effect_records(&journal);
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("history should index"),
    );
    let mut replay = EffectsCore::new(invocation_context(
        Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
    ));

    let err = replay
        .perform(CountingEffect {
            value: 1,
            label: "changed",
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .await
        .expect_err("descriptor mismatch must fail replay");

    assert!(matches!(err, EffectError::DescriptorMismatch { .. }));
}

#[tokio::test]
async fn effect_history_fails_loud_on_duplicate_scalar_cursor_record() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut live = EffectsCore::new(invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));
    live.perform(CountingEffect {
        value: 1,
        label: "same",
        calls: Arc::new(AtomicUsize::new(0)),
    })
    .await
    .expect("live effect should succeed");

    let mut records = effect_records(&journal);
    records.push(records[0].clone());

    let err = EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
        .expect_err("duplicate scalar cursor records must fail loud");

    assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
}

#[tokio::test]
async fn effect_record_decode_rejects_payload_provenance_cursor_mismatch() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut live = EffectsCore::new(invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));
    live.perform(CountingEffect {
        value: 1,
        label: "same",
        calls: Arc::new(AtomicUsize::new(0)),
    })
    .await
    .expect("live effect should succeed");

    let mut event = journal.events()[0].event.clone();
    event
        .effect_provenance
        .as_mut()
        .expect("effect event should carry provenance")
        .cursor
        .effect_ordinal = EffectOrdinal::new(99);

    let err = effect_record_from_event(&event)
        .expect_err("payload/provenance cursor mismatch must fail loud");

    assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
}

#[test]
fn effect_record_decode_rejects_reserved_event_without_provenance() {
    let stage_id = StageId::new();
    let cursor = EffectCursor::new("flow", "effect_stage", 1, 0);
    let record = EffectRecord {
        cursor,
        descriptor_hash: "hash".into(),
        descriptor: EffectDescriptor::new(
            CountingEffect::EFFECT_TYPE,
            "same",
            CountingEffect::SCHEMA_VERSION,
            "test-v1",
            "input",
        ),
        outcome: EffectOutcomePayload::Succeeded {
            output: json!(2_u64),
        },
        origin: None,
    };
    let event = ChainEventFactory::data_event(
        WriterId::from(stage_id),
        EFFECT_RECORD_EVENT_TYPE,
        serde_json::to_value(record).expect("record should serialize"),
    );

    let err = effect_record_from_event(&event)
        .expect_err("reserved effect record without provenance must fail loud");

    assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
}

// ---------------------------------------------------------------------------
// FLOWIP-120m: replay origin read-back
// ---------------------------------------------------------------------------

#[tokio::test]
async fn effect_record_from_event_reads_back_provenance_origin() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut live = EffectsCore::new(invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));
    live.perform(CountingEffect {
        value: 1,
        label: "same",
        calls: Arc::new(AtomicUsize::new(0)),
    })
    .await
    .expect("live effect should succeed");

    // The live commit stamps Effect origin; the record reconstructed from the
    // journaled event must carry it back.
    let records = effect_records(&journal);
    assert_eq!(records[0].origin, Some(EffectFactOrigin::Effect));
}

#[tokio::test]
async fn replayed_group_without_recorded_origin_falls_back_to_derivation() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut live = EffectsCore::new(invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));
    live.perform(CountingEffect {
        value: 1,
        label: "same",
        calls: Arc::new(AtomicUsize::new(0)),
    })
    .await
    .expect("live effect should succeed");

    // Strip the origin to simulate a pre-120h journal.
    let records: Vec<EffectRecord> = effect_records(&journal)
        .into_iter()
        .map(|mut record| {
            record.origin = None;
            record
        })
        .collect();
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("history should index"),
    );
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
    let mut replay = EffectsCore::new(invocation_context(
        replay_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
    ));
    replay
        .perform(CountingEffect {
            value: 1,
            label: "same",
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .await
        .expect("replay should reconstruct the recorded outcome");

    let replayed_origin = replay_journal.events()[0]
        .event
        .effect_provenance
        .as_ref()
        .expect("replayed fact should carry provenance")
        .origin
        .clone();
    assert_eq!(
        replayed_origin,
        Some(EffectFactOrigin::Effect),
        "an older record defaults to the effect itself"
    );
}

#[tokio::test]
async fn mixed_origin_group_is_rejected_as_provenance_mismatch() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut live = EffectsCore::new(invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));
    live.perform(MultiFactEffect {
        calls: Arc::new(AtomicUsize::new(0)),
    })
    .await
    .expect("live multi-fact effect should succeed");

    let mut records = effect_records(&journal);
    assert_eq!(records.len(), 2, "multi-fact outcome should record a group");
    records[1].origin = None;

    let record_refs: Vec<&EffectRecord> = records.iter().collect();
    let err = effect_record_group_materialization(&record_refs)
        .expect_err("a group disagreeing on origin must fail loud");

    assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
}

#[tokio::test]
async fn capture_replays_recorded_value_without_using_live_value() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut live = EffectsCore::new(invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));

    let captured: u64 = live.capture("side_value", 7).await.expect("capture");
    assert_eq!(captured, 7);

    let records = effect_records(&journal);
    let live_record = records[0].clone();
    let live_event_id = journal.events()[0].event.id;
    assert_eq!(
        live_event_id,
        deterministic_effect_record_event_id(&live_record.cursor, CAPTURE_EVENT_TYPE)
    );
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("history should index"),
    );
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
    let mut replay = EffectsCore::new(invocation_context(
        replay_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
    ));

    let replayed: u64 = replay
        .capture("side_value", 999)
        .await
        .expect("capture should replay");

    assert_eq!(replayed, 7);
    let replay_events = replay_journal.events();
    assert_eq!(replay_events.len(), 1);
    assert_eq!(replay_events[0].event.id, live_event_id);
    let replay_records = effect_records(&replay_journal);
    assert_eq!(replay_records, vec![live_record]);

    let replay_of_replay_history = Arc::new(
        EffectHistory::from_records("replay_archive_flow".to_string(), replay_records)
            .expect("capture replay history should infer the original root"),
    );
    let replay_of_replay_journal =
        Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
    let mut replay_of_replay = EffectsCore::new(invocation_context(
        replay_of_replay_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(replay_of_replay_history),
    ));

    let replayed_again: u64 = replay_of_replay
        .capture("side_value", 1234)
        .await
        .expect("capture should replay from a replay archive");

    assert_eq!(replayed_again, 7);
    assert_eq!(replay_of_replay_journal.events()[0].event.id, live_event_id);
}

// ---------------------------------------------------------------------------
// FLOWIP-120h: boundary rejections must be recorded under the effect cursor
// ---------------------------------------------------------------------------

struct AbortingBoundary;

#[derive(Default)]
struct EffectObserverLog {
    outcomes: Mutex<Vec<(String, EffectObserverOutcome)>>,
}

impl EffectObserver for EffectObserverLog {
    fn after_effect(&self, ctx: &EffectObserverContext<'_>) {
        self.outcomes
            .lock()
            .expect("effect observer log lock poisoned")
            .push((ctx.effect_type().to_string(), ctx.outcome()));
    }
}

fn attach_effect_observer(
    ctx: &mut EffectInvocationContext,
    effect_type: &'static str,
    observer: Arc<dyn EffectObserver>,
) {
    let mut bindings = StageObserverBindings::default();
    bindings.push(
        ObserverBinding::effect("effect-outcome-proof", effect_type, observer)
            .expect("effect observer binding is valid"),
    );
    ctx.observers = Some(
        StageObserverBundle::compose_checked(
            &ctx.stage_key,
            ObserverTarget::Transform {
                effects: &ctx.effect_declarations,
            },
            bindings,
        )
        .expect("effect observer matches the declared subject"),
    );
}

#[async_trait]
impl EffectBoundary for AbortingBoundary {
    async fn around_repeatable_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        _operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Aborted(EffectAbortReason {
                cause: EffectFailureCause {
                    source: "circuit_breaker".into(),
                    code: "circuit_open".into(),
                },
                message: "circuit breaker rejected effect execution".to_string(),
                retry: RetryDisposition::Retryable,
            }),
            control_events: Vec::new(),
        }
    }

    async fn around_single_use_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        operation.abort(
            EffectAbortReason {
                cause: EffectFailureCause {
                    source: "circuit_breaker".into(),
                    code: "circuit_open".into(),
                },
                message: "circuit breaker rejected effect execution".to_string(),
                retry: RetryDisposition::Retryable,
            },
            Vec::new(),
        )
    }

    async fn around_affine_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        operation: AffineEffectOperation,
    ) -> AffineEffectBoundaryReport {
        operation.abort(
            EffectAbortReason {
                cause: EffectFailureCause {
                    source: "circuit_breaker".into(),
                    code: "circuit_open".into(),
                },
                message: "circuit breaker rejected effect execution".to_string(),
                retry: RetryDisposition::Retryable,
            },
            Vec::new(),
        )
    }
}

struct RecoveryRejectingBoundary {
    consults: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectBoundary for RecoveryRejectingBoundary {
    async fn around_repeatable_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        _operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        panic!("recovery fixture only supports affine effects")
    }

    async fn around_single_use_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        _operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        panic!("recovery fixture only supports affine effects")
    }

    async fn around_affine_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        operation: AffineEffectOperation,
    ) -> AffineEffectBoundaryReport {
        self.consults.fetch_add(1, Ordering::SeqCst);
        operation.abort(
            EffectAbortReason {
                cause: EffectFailureCause {
                    source: "circuit_breaker".into(),
                    code: "circuit_open".into(),
                },
                message: "recovery attempt rejected by open boundary".to_string(),
                retry: RetryDisposition::Retryable,
            },
            Vec::new(),
        )
    }
}

struct PanicBoundary;

#[async_trait]
impl EffectBoundary for PanicBoundary {
    async fn around_repeatable_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        _operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        panic!("replay must not consult the effect boundary")
    }

    async fn around_single_use_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        _operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        panic!("replay must not consult the effect boundary")
    }

    async fn around_affine_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        _operation: AffineEffectOperation,
    ) -> AffineEffectBoundaryReport {
        panic!("replay must not consult the effect boundary")
    }
}

struct InvariantEvidenceBoundary {
    include_preterminal: bool,
    consults: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectBoundary for InvariantEvidenceBoundary {
    async fn around_repeatable_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        _operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        panic!("invariant fixture only supports affine effects")
    }

    async fn around_single_use_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        _operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        panic!("invariant fixture only supports affine effects")
    }

    async fn around_affine_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        operation: AffineEffectOperation,
    ) -> AffineEffectBoundaryReport {
        use obzenflow_core::event::chain_event::{
            CircuitBreakerAttemptSettledEventParams, CircuitBreakerRecoveryCompletedEventParams,
        };
        use obzenflow_core::event::payloads::observability_payload::CircuitBreakerHealthClassification;

        assert_eq!(identity.safety, EffectSafety::NonIdempotentAtLeastOnce);
        self.consults.fetch_add(1, Ordering::SeqCst);
        let execution = operation.execute().await;
        assert!(matches!(
            execution.result(),
            Err(EffectError::EffectTargetInvariantViolation { .. })
        ));
        let attempt = execution.attempt();
        let evidence_writer = WriterId::from(StageId::new());
        let mut controls = Vec::new();
        if self.include_preterminal {
            controls.push(ChainEventFactory::circuit_breaker_retry_scheduled(
                evidence_writer,
                identity.cursor.clone(),
                attempt.saturating_add(1),
                0,
                event.id,
            ));
        }
        controls.push(ChainEventFactory::circuit_breaker_attempt_settled(
            evidence_writer,
            CircuitBreakerAttemptSettledEventParams {
                cursor: identity.cursor.clone(),
                attempt,
                health_classification: CircuitBreakerHealthClassification::Ignored,
                slow: false,
                dependency_elapsed_ms: 0,
                admission_wait_ms: 0,
            },
            event.id,
        ));
        controls.push(ChainEventFactory::circuit_breaker_recovery_completed(
            evidence_writer,
            CircuitBreakerRecoveryCompletedEventParams {
                cursor: identity.cursor.clone(),
                total_attempts: attempt,
                backoff_elapsed_ms: 0,
                recovery_elapsed_ms: 0,
            },
            event.id,
        ));
        execution.into_report(controls)
    }
}

async fn archive_current_cursor(
    journal: &Arc<MemoryJournal<ChainEvent>>,
    cursor: &EffectCursor,
) -> Arc<EffectHistory> {
    let erased: Arc<dyn Journal<ChainEvent>> = journal.clone();
    let history = current_cursor_history(&erased, cursor)
        .await
        .expect("current cursor history is valid");
    Arc::new(
        EffectHistory::from_cursor_history_for_test(
            cursor.recorded_flow_id.clone(),
            cursor.clone(),
            history,
        )
        .expect("cursor history can become the next resume archive"),
    )
}

fn cursor_started_in(journal: &MemoryJournal<ChainEvent>) -> EffectCursor {
    journal
        .events()
        .iter()
        .find_map(|envelope| EffectAttemptStarted::from_event(&envelope.event))
        .expect("fixture journal contains a Start")
        .cursor
}

type ComparableEffectJournalEntry = (
    EventId,
    String,
    Option<String>,
    Option<JournalGroupMember>,
    serde_json::Value,
);

fn comparable_effect_journal(
    journal: &MemoryJournal<ChainEvent>,
) -> Vec<ComparableEffectJournalEntry> {
    journal
        .events()
        .into_iter()
        .map(|envelope| {
            (
                envelope.event.id,
                envelope.event.event_type(),
                envelope.journal_group_id,
                envelope.journal_group_member,
                serde_json::to_value(envelope.event.content)
                    .expect("effect journal content serialises"),
            )
        })
        .collect()
}

#[tokio::test]
async fn invariant_preterminal_and_terminal_group_cuts_are_independently_atomic() {
    let stage_id = StageId::new();
    let parent = parent_envelope(WriterId::from(stage_id));

    let preterminal_cut = Arc::new(MemoryJournal::failing_group(
        JournalOwner::stage(stage_id),
        "effect-escape-controls:v1:",
    ));
    let preterminal_calls = Arc::new(AtomicUsize::new(0));
    let mut preterminal_ctx = invocation_context(preterminal_cut.clone(), parent.clone(), None);
    preterminal_ctx.effect_boundary = Some(Arc::new(InvariantEvidenceBoundary {
        include_preterminal: true,
        consults: Arc::new(AtomicUsize::new(0)),
    }));
    let mut preterminal = EffectsCore::new(preterminal_ctx);
    let error = preterminal
        .perform(InvariantAffineEffect {
            calls: preterminal_calls.clone(),
        })
        .await
        .expect_err("preterminal publication failure is journal-fatal");
    assert!(matches!(error, EffectError::Journal(_)));
    assert_eq!(preterminal_calls.load(Ordering::SeqCst), 1);
    let preterminal_events = preterminal_cut.events();
    assert_eq!(preterminal_events.len(), 1);
    assert!(EffectAttemptStarted::event_type_matches(
        &preterminal_events[0].event.event_type()
    ));
    assert!(preterminal_events[0].journal_group_id.is_none());

    let terminal_cut = Arc::new(MemoryJournal::failing_group(
        JournalOwner::stage(stage_id),
        "effect-outcome:v1:",
    ));
    let terminal_calls = Arc::new(AtomicUsize::new(0));
    let mut terminal_ctx = invocation_context(terminal_cut.clone(), parent, None);
    terminal_ctx.effect_boundary = Some(Arc::new(InvariantEvidenceBoundary {
        include_preterminal: true,
        consults: Arc::new(AtomicUsize::new(0)),
    }));
    let mut terminal = EffectsCore::new(terminal_ctx);
    let error = terminal
        .perform(InvariantAffineEffect {
            calls: terminal_calls.clone(),
        })
        .await
        .expect_err("terminal publication failure is journal-fatal");
    assert!(matches!(error, EffectError::Journal(_)));
    assert_eq!(terminal_calls.load(Ordering::SeqCst), 1);
    let terminal_events = terminal_cut.events();
    assert_eq!(terminal_events.len(), 2);
    assert!(EffectAttemptStarted::event_type_matches(
        &terminal_events[0].event.event_type()
    ));
    assert!(terminal_events[1]
        .journal_group_id
        .as_deref()
        .is_some_and(|group| group.starts_with("effect-escape-controls:v1:")));
    assert!(
        terminal_events
            .iter()
            .all(|envelope| envelope.event.event_type() != EFFECT_RECORD_EVENT_TYPE),
        "a failed terminal frame exposes no typed invariant terminal"
    );
}

#[tokio::test]
async fn invariant_escape_resume_sequence_preserves_attempt_scoped_identity() {
    let stage_id = StageId::new();
    let parent = parent_envelope(WriterId::from(stage_id));

    let first = Arc::new(MemoryJournal::failing_group(
        JournalOwner::stage(stage_id),
        "effect-outcome:v1:",
    ));
    let first_calls = Arc::new(AtomicUsize::new(0));
    let mut first_ctx = invocation_context(first.clone(), parent.clone(), None);
    first_ctx.effect_boundary = Some(Arc::new(InvariantEvidenceBoundary {
        include_preterminal: true,
        consults: Arc::new(AtomicUsize::new(0)),
    }));
    let mut first_run = EffectsCore::new(first_ctx);
    assert!(matches!(
        first_run
            .perform(InvariantAffineEffect {
                calls: first_calls.clone(),
            })
            .await,
        Err(EffectError::Journal(_))
    ));
    let cursor = cursor_started_in(&first);
    let first_history = archive_current_cursor(&first, &cursor).await;

    let second = Arc::new(MemoryJournal::failing_group(
        JournalOwner::stage(stage_id),
        "effect-outcome:v1:",
    ));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let mut second_ctx = invocation_context_with_mode(
        second.clone(),
        parent.clone(),
        Some(first_history),
        EffectRuntimeMode::ResumeIncomplete,
        EffectPortRegistry::new(),
    );
    second_ctx.effect_boundary = Some(Arc::new(InvariantEvidenceBoundary {
        include_preterminal: true,
        consults: Arc::new(AtomicUsize::new(0)),
    }));
    let mut second_run = EffectsCore::new(second_ctx);
    assert!(matches!(
        second_run
            .perform(InvariantAffineEffect {
                calls: second_calls.clone(),
            })
            .await,
        Err(EffectError::Journal(_))
    ));
    let second_history = archive_current_cursor(&second, &cursor).await;

    let terminal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let terminal_calls = Arc::new(AtomicUsize::new(0));
    let terminal_consults = Arc::new(AtomicUsize::new(0));
    let mut terminal_ctx = invocation_context_with_mode(
        terminal.clone(),
        parent.clone(),
        Some(second_history),
        EffectRuntimeMode::ResumeIncomplete,
        EffectPortRegistry::new(),
    );
    terminal_ctx.effect_boundary = Some(Arc::new(InvariantEvidenceBoundary {
        include_preterminal: false,
        consults: terminal_consults.clone(),
    }));
    let mut terminal_run = EffectsCore::new(terminal_ctx);
    let terminal_error = terminal_run
        .perform(InvariantAffineEffect {
            calls: terminal_calls.clone(),
        })
        .await
        .expect_err("the successful terminal retains the typed invariant failure");
    assert!(matches!(
        terminal_error,
        EffectError::EffectTargetInvariantViolation { .. }
    ));

    assert_eq!(first_calls.load(Ordering::SeqCst), 1);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    assert_eq!(terminal_calls.load(Ordering::SeqCst), 1);
    assert_eq!(terminal_consults.load(Ordering::SeqCst), 1);

    let terminal_history = archive_current_cursor(&terminal, &cursor).await;
    let selected = terminal_history.cursor_history(&cursor);
    assert_eq!(
        selected
            .attempts
            .iter()
            .map(|started| started.attempt.get())
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    let escape_attempts = selected
        .escape_control_batches
        .keys()
        .map(|attempt| attempt.get())
        .collect::<Vec<_>>();
    assert_eq!(escape_attempts, vec![1, 2]);
    for attempt in [1_u32, 2] {
        let expected = effect_escape_controls_group_id(&cursor, EffectAttemptOrdinal::new(attempt));
        assert!(terminal
            .events()
            .iter()
            .any(|envelope| { envelope.journal_group_id.as_deref() == Some(expected.as_str()) }));
    }
    assert_eq!(
        selected.terminal_attempt,
        Some(Some(EffectAttemptOrdinal::new(3)))
    );

    let replay = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_consults = Arc::new(AtomicUsize::new(0));
    let mut replay_ctx = invocation_context_with_mode(
        replay.clone(),
        parent,
        Some(terminal_history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
    );
    replay_ctx.effect_boundary = Some(Arc::new(InvariantEvidenceBoundary {
        include_preterminal: true,
        consults: replay_consults.clone(),
    }));
    let mut replay_run = EffectsCore::new(replay_ctx);
    let replay_error = replay_run
        .perform(InvariantAffineEffect {
            calls: replay_calls.clone(),
        })
        .await
        .expect_err("strict replay returns the archived failure");
    assert!(matches!(replay_error, EffectError::RecordedFailure { .. }));
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(replay_consults.load(Ordering::SeqCst), 0);
    assert_eq!(
        comparable_effect_journal(&terminal),
        comparable_effect_journal(&replay),
        "strict replay rematerialises Starts, attempt-scoped escape batches, and terminal byte-for-byte"
    );
}

#[tokio::test]
async fn recovery_abandonment_names_the_archived_attempt_and_replays_without_a_boundary() {
    let stage_id = StageId::new();
    let parent = parent_envelope(WriterId::from(stage_id));
    let (_, in_doubt_history) = affine_scope_matrix_histories(&parent).await;
    let recovery_parent = parent_envelope(WriterId::from(stage_id));
    assert_ne!(
        parent.event.id, recovery_parent.event.id,
        "the recovery invocation must not accidentally share the archived input identity"
    );

    let recovery_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let recovery_calls = Arc::new(AtomicUsize::new(0));
    let recovery_consults = Arc::new(AtomicUsize::new(0));
    let recovery_observations = Arc::new(EffectObserverLog::default());
    let mut recovery_ctx = invocation_context_with_mode(
        recovery_journal.clone(),
        recovery_parent.clone(),
        Some(in_doubt_history),
        EffectRuntimeMode::ResumeIncomplete,
        EffectPortRegistry::new(),
    );
    attach_effect_observer(
        &mut recovery_ctx,
        AffineCountingEffect::EFFECT_TYPE,
        recovery_observations.clone(),
    );
    recovery_ctx.effect_boundary = Some(Arc::new(RecoveryRejectingBoundary {
        consults: recovery_consults.clone(),
    }));
    let mut recovery = EffectsCore::new(recovery_ctx);
    let recovery_error = recovery
        .perform(AffineCountingEffect {
            calls: recovery_calls.clone(),
        })
        .await
        .expect_err("a rejected recovery becomes a typed abandonment");
    assert!(matches!(
        recovery_error,
        EffectError::RecoveryAbandoned {
            last_started_attempt,
            ref failure_source,
            ref code,
            ..
        } if last_started_attempt == EffectAttemptOrdinal::new(1)
            && failure_source.as_str() == "circuit_breaker"
            && code.as_str() == "circuit_open"
    ));
    assert_eq!(recovery_calls.load(Ordering::SeqCst), 0);
    assert_eq!(recovery_consults.load(Ordering::SeqCst), 1);
    assert_eq!(
        *recovery_observations
            .outcomes
            .lock()
            .expect("recovery observer assertion lock"),
        [(
            AffineCountingEffect::EFFECT_TYPE.to_string(),
            EffectObserverOutcome::Failed,
        )],
        "a live recovery abandonment is one failed affine-effect result"
    );

    let recovery_events = recovery_journal.events();
    let starts = recovery_events
        .iter()
        .filter_map(|envelope| EffectAttemptStarted::from_event(&envelope.event))
        .collect::<Vec<_>>();
    assert_eq!(
        starts
            .iter()
            .map(|started| started.attempt.get())
            .collect::<Vec<_>>(),
        vec![1],
        "recovery rejection must not author Start(2)"
    );
    let abandonments = recovery_events
        .iter()
        .filter(|envelope| {
            EffectRecoveryAbandoned::event_type_matches(&envelope.event.event_type())
        })
        .map(|envelope| {
            EffectRecoveryAbandoned::try_from_event(&envelope.event)
                .expect("abandonment payload decodes")
        })
        .collect::<Vec<_>>();
    assert_eq!(abandonments.len(), 1);
    assert_eq!(
        abandonments[0].highest_started_attempt,
        EffectAttemptOrdinal::new(1)
    );
    let records = effect_records(&recovery_journal);
    assert_eq!(records.len(), 1);
    assert!(matches!(
        records[0].outcome,
        EffectOutcomePayload::Failed {
            ref error_type,
            ref cause,
            ..
        } if error_type.as_str() == "recovery_abandoned"
            && cause.as_ref().is_some_and(|cause| {
                cause.source.as_str() == "circuit_breaker"
                    && cause.code.as_str() == "circuit_open"
            })
    ));

    let cursor = cursor_started_in(&recovery_journal);
    let settled_history = archive_current_cursor(&recovery_journal, &cursor).await;
    for mode in [
        EffectRuntimeMode::ReplayStrict,
        EffectRuntimeMode::ResumeIncomplete,
    ] {
        let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
        let replay_calls = Arc::new(AtomicUsize::new(0));
        let replay_observations = Arc::new(EffectObserverLog::default());
        let mut replay_ctx = invocation_context_with_mode(
            replay_journal.clone(),
            recovery_parent.clone(),
            Some(settled_history.clone()),
            mode,
            EffectPortRegistry::new(),
        );
        attach_effect_observer(
            &mut replay_ctx,
            AffineCountingEffect::EFFECT_TYPE,
            replay_observations.clone(),
        );
        replay_ctx.effect_boundary = Some(Arc::new(PanicBoundary));
        let mut replay = EffectsCore::new(replay_ctx);
        let replay_error = replay
            .perform(AffineCountingEffect {
                calls: replay_calls.clone(),
            })
            .await
            .expect_err("abandonment remains the deterministic terminal");
        assert!(matches!(
            replay_error,
            EffectError::RecoveryAbandoned {
                last_started_attempt,
                ..
            } if last_started_attempt == EffectAttemptOrdinal::new(1)
        ));
        assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
        assert!(
            replay_observations
                .outcomes
                .lock()
                .expect("replay observer assertion lock")
                .is_empty(),
            "replaying an archived abandonment must not dispatch an observer"
        );
        assert_eq!(
            comparable_effect_journal(&recovery_journal),
            comparable_effect_journal(&replay_journal),
            "strict replay and resume-of-resume reproduce the abandonment byte-for-byte"
        );
    }
}

#[tokio::test]
async fn affine_boundary_abort_notifies_effect_observer_once() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let effect_calls = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(EffectObserverLog::default());
    let mut ctx = invocation_context(journal, parent_envelope(WriterId::from(stage_id)), None);
    attach_effect_observer(
        &mut ctx,
        AffineCountingEffect::EFFECT_TYPE,
        observations.clone(),
    );
    ctx.effect_boundary = Some(Arc::new(AbortingBoundary));
    let mut effects = EffectsCore::new(ctx);

    let result = effects
        .perform(AffineCountingEffect {
            calls: effect_calls.clone(),
        })
        .await;

    assert!(matches!(result, Err(EffectError::BoundaryRejected { .. })));
    assert_eq!(effect_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        *observations
            .outcomes
            .lock()
            .expect("affine observer assertion lock"),
        [(
            AffineCountingEffect::EFFECT_TYPE.to_string(),
            EffectObserverOutcome::Failed,
        )]
    );
}

#[tokio::test]
async fn boundary_abort_records_failure_with_cause_and_replays_deterministically() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let live_calls = Arc::new(AtomicUsize::new(0));
    let mut live_ctx = invocation_context(
        live_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    live_ctx.effect_boundary = Some(Arc::new(AbortingBoundary));
    let mut live = EffectsCore::new(live_ctx);

    let live_err = live
        .perform(CountingEffect {
            value: 1,
            label: "guarded",
            calls: live_calls.clone(),
        })
        .await
        .expect_err("boundary abort should fail the perform");

    match &live_err {
        EffectError::BoundaryRejected {
            rejected_by, code, ..
        } => {
            assert_eq!(rejected_by.as_str(), "circuit_breaker");
            assert_eq!(code.as_str(), "circuit_open");
        }
        other => panic!("expected BoundaryRejected, got {other:?}"),
    }
    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        0,
        "boundary abort must prevent effect execution"
    );

    let live_records = effect_records(&live_journal);
    assert_eq!(
        live_records.len(),
        1,
        "boundary abort must record a failure under the effect cursor"
    );
    match &live_records[0].outcome {
        EffectOutcomePayload::Failed { cause, retry, .. } => {
            let cause = cause
                .as_ref()
                .expect("recorded failure must carry the cause");
            assert_eq!(cause.source, "circuit_breaker");
            assert_eq!(cause.code, "circuit_open");
            assert!(retry.is_retryable());
        }
        other => panic!("expected Failed outcome, got {other:?}"),
    }

    // Strict replay: same deterministic error, no MissingRecordedEffect, no
    // boundary consultation, zero executions.
    let replay_history = Arc::new(
        EffectHistory::from_records("replay_archive_flow".to_string(), live_records.clone())
            .expect("history from live records"),
    );
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let mut replay = EffectsCore::new(invocation_context(
        replay_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(replay_history),
    ));

    let replay_err = replay
        .perform(CountingEffect {
            value: 1,
            label: "guarded",
            calls: replay_calls.clone(),
        })
        .await
        .expect_err("strict replay should return the recorded rejection");

    match &replay_err {
        EffectError::RecordedFailure { cause, .. } => {
            let cause = cause
                .as_ref()
                .expect("replayed failure must carry the cause");
            assert_eq!(cause.source, "circuit_breaker");
            assert_eq!(cause.code, "circuit_open");
        }
        other => panic!("expected RecordedFailure on replay, got {other:?}"),
    }
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(effect_records(&replay_journal), live_records);
}

// ---------------------------------------------------------------------------
// FLOWIP-120c G10: the missing idempotency-key check sits above the
// effect-history lookup and the boundary consult, so live and replay
// recompute the same deterministic error and admission is never charged.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct KeylessEffect {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for KeylessEffect {
    const EFFECT_TYPE: &'static str = "test.keyless";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type BindingMode = crate::effects::Portless;

    type Outcome = CountingOutput;
    type OutcomeSemantics = crate::effects::DomainFacts;

    fn label(&self) -> &str {
        "keyless"
    }

    fn canonical_input(&self) -> Value {
        json!({ "kind": "keyless" })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(CountingOutput { value: 1 })
    }
}

#[derive(Clone, Debug)]
struct KeyedEffect {
    key: IdempotencyKey,
    seen_keys: Arc<Mutex<Vec<IdempotencyKey>>>,
}

#[async_trait]
impl Effect for KeyedEffect {
    const EFFECT_TYPE: &'static str = "test.keyed";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type BindingMode = crate::effects::Portless;

    type Outcome = CountingOutput;
    type OutcomeSemantics = crate::effects::DomainFacts;

    fn label(&self) -> &str {
        "keyed"
    }

    fn canonical_input(&self) -> Value {
        json!({ "key": self.key.0 })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.seen_keys
            .lock()
            .expect("seen keys lock poisoned")
            .push(self.key.clone());
        Ok(CountingOutput { value: 1 })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(self.key.clone())
    }
}

struct CountingBoundary {
    consults: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectBoundary for CountingBoundary {
    async fn around_repeatable_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        mut operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        self.consults.fetch_add(1, Ordering::SeqCst);
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
            control_events: Vec::new(),
        }
    }

    async fn around_single_use_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        self.consults.fetch_add(1, Ordering::SeqCst);
        operation.execute().await.into_report(Vec::new())
    }
}

/// Deliberately violates the boundary contract: it executes the supplied
/// transaction, discards that receipt, then returns an abort report minted by
/// another operation. The runtime must preserve the committed outcome.
struct ExecutedThenForeignAbortBoundary;

#[async_trait]
impl EffectBoundary for ExecutedThenForeignAbortBoundary {
    async fn around_repeatable_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        mut operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
            control_events: Vec::new(),
        }
    }

    async fn around_single_use_effect(
        &self,
        _identity: &EffectIdentity,
        event: &ChainEvent,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        let _supplied_execution = operation.execute().await;
        SingleUseEffectOperation::new(|| async { Ok(Vec::new()) }).abort(
            EffectAbortReason {
                cause: EffectFailureCause {
                    source: "foreign_boundary_report".into(),
                    code: "foreign_abort".into(),
                },
                message: "abort report belongs to another operation".to_string(),
                retry: RetryDisposition::NotRetryable,
            },
            vec![event.clone()],
        )
    }
}

/// Deliberately drops the supplied transaction and returns an execution
/// receipt minted by another operation. With no settled supplied operation,
/// the runtime must fail closed and journal the provenance violation.
struct ForeignExecutionBoundary {
    foreign_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectBoundary for ForeignExecutionBoundary {
    async fn around_repeatable_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        mut operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
            control_events: Vec::new(),
        }
    }

    async fn around_single_use_effect(
        &self,
        _identity: &EffectIdentity,
        event: &ChainEvent,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        drop(operation);
        let calls = self.foreign_calls.clone();
        SingleUseEffectOperation::new(move || async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(Vec::new())
        })
        .execute()
        .await
        .into_report(vec![event.clone()])
    }
}

struct ThreeCallBoundary {
    consults: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectBoundary for ThreeCallBoundary {
    async fn around_repeatable_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        mut operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        self.consults.fetch_add(1, Ordering::SeqCst);
        operation.execute().await.expect("first physical call");
        operation.execute().await.expect("second physical call");
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
            control_events: Vec::new(),
        }
    }

    async fn around_single_use_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        self.consults.fetch_add(1, Ordering::SeqCst);
        operation.execute().await.into_report(Vec::new())
    }
}

struct KeyCheckingThreeCallBoundary {
    expected_key: IdempotencyKey,
}

#[async_trait]
impl EffectBoundary for KeyCheckingThreeCallBoundary {
    async fn around_repeatable_effect(
        &self,
        identity: &EffectIdentity,
        _event: &ChainEvent,
        mut operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        assert_eq!(identity.idempotency_key.as_ref(), Some(&self.expected_key));
        operation.execute().await.expect("first physical call");
        operation.execute().await.expect("second physical call");
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
            control_events: Vec::new(),
        }
    }

    async fn around_single_use_effect(
        &self,
        identity: &EffectIdentity,
        _event: &ChainEvent,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        assert_eq!(identity.idempotency_key.as_ref(), Some(&self.expected_key));
        operation.execute().await.into_report(Vec::new())
    }
}

#[tokio::test]
async fn runtime_enters_the_boundary_once_and_commits_only_the_terminal_call() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let consults = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.effect_boundary = Some(Arc::new(ThreeCallBoundary {
        consults: consults.clone(),
    }));
    let mut effects = EffectsCore::new(ctx);

    let outcome = effects
        .perform(CountingEffect {
            value: 41,
            label: "three physical calls",
            calls: calls.clone(),
        })
        .await
        .expect("terminal physical call should succeed");

    assert_eq!(outcome, CountingOutput { value: 42 });
    assert_eq!(consults.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        effect_records(&journal).len(),
        1,
        "intermediate physical calls must not commit effect records"
    );
}

#[tokio::test]
async fn repeated_physical_calls_preserve_the_invocations_idempotency_key() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let key = IdempotencyKey("stable-key".to_string());
    let seen_keys = Arc::new(Mutex::new(Vec::new()));
    let mut ctx = invocation_context(journal, parent_envelope(WriterId::from(stage_id)), None);
    ctx.effect_boundary = Some(Arc::new(KeyCheckingThreeCallBoundary {
        expected_key: key.clone(),
    }));
    let mut effects = EffectsCore::new(ctx);

    effects
        .perform(KeyedEffect {
            key: key.clone(),
            seen_keys: seen_keys.clone(),
        })
        .await
        .expect("terminal physical call should succeed");

    assert_eq!(
        *seen_keys.lock().expect("seen keys lock poisoned"),
        vec![key; 3]
    );
}

#[tokio::test]
async fn missing_idempotency_key_is_unrecorded_and_unadmitted() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let consults = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.effect_boundary = Some(Arc::new(CountingBoundary {
        consults: consults.clone(),
    }));
    let mut effects = EffectsCore::new(ctx);

    let err = effects
        .perform(KeylessEffect {
            calls: calls.clone(),
        })
        .await
        .expect_err("missing key must fail before execution");

    assert!(matches!(err, EffectError::MissingIdempotencyKey { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        consults.load(Ordering::SeqCst),
        0,
        "boundary admission must never be charged for a doomed call"
    );
    assert!(
        effect_records(&journal).is_empty(),
        "a deterministic validation error records nothing under the cursor"
    );
}

#[tokio::test]
async fn missing_idempotency_key_replays_as_same_error() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let history = Arc::new(
        EffectHistory::from_records("archived_flow".to_string(), Vec::new())
            .expect("empty history should index"),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let mut effects = EffectsCore::new(invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
    ));

    let err = effects
        .perform(KeylessEffect {
            calls: calls.clone(),
        })
        .await
        .expect_err("strict replay must recompute the live validation error");

    assert!(
        matches!(err, EffectError::MissingIdempotencyKey { .. }),
        "replay must reproduce MissingIdempotencyKey, not MissingRecordedEffect, got {err:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(effect_records(&journal).is_empty());
}

#[tokio::test]
async fn stale_recorded_effect_fails_loud_when_key_dropped() {
    // Documented G10 caveat: an archive recorded before a code change that
    // later dropped the idempotency key fails loud at the check instead of
    // reading the old record, consistent with descriptor-hash fail-loud.
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let record = EffectRecord {
        cursor: EffectCursor::new("archived_flow", "effect_stage", 1, 0),
        descriptor_hash: "hash".into(),
        descriptor: EffectDescriptor::new(
            KeylessEffect::EFFECT_TYPE,
            "keyless",
            KeylessEffect::SCHEMA_VERSION,
            "test-v1",
            "input",
        ),
        outcome: EffectOutcomePayload::Succeeded {
            output: json!({ "value": 10 }),
        },
        origin: None,
    };
    let history = Arc::new(
        EffectHistory::from_records("archived_flow".to_string(), vec![record])
            .expect("history should index"),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let mut effects = EffectsCore::new(invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
    ));

    let err = effects
        .perform(KeylessEffect {
            calls: calls.clone(),
        })
        .await
        .expect_err("the dropped key fails loud even with a record at the cursor");

    assert!(matches!(err, EffectError::MissingIdempotencyKey { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

// ---------------------------------------------------------------------------
// FLOWIP-120c H5: transactional effects route through the boundary for
// admission and observation; rejections record under the effect cursor.
// ---------------------------------------------------------------------------

/// Aborts transactional effects only, keyed off the seam's effect identity
/// (FLOWIP-120c gap G3: the boundary can tell which effect it guards).
struct TransactionalOnlyAbortBoundary;

#[async_trait]
impl EffectBoundary for TransactionalOnlyAbortBoundary {
    async fn around_repeatable_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        mut operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
            control_events: Vec::new(),
        }
    }

    async fn around_single_use_effect(
        &self,
        identity: &EffectIdentity,
        _event: &ChainEvent,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        assert!(matches!(identity.safety, EffectSafety::Transactional));
        operation.abort(
            EffectAbortReason {
                cause: EffectFailureCause {
                    source: "circuit_breaker".into(),
                    code: "circuit_open".into(),
                },
                message: "circuit breaker rejected transactional effect".to_string(),
                retry: RetryDisposition::Retryable,
            },
            Vec::new(),
        )
    }
}

#[tokio::test]
async fn transactional_boundary_executes_and_commits_the_single_use_operation_once() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let transactional_calls = Arc::new(AtomicUsize::new(0));
    let boundary_consults = Arc::new(AtomicUsize::new(0));
    let (binding, registration) =
        transactional_counting_binding(Arc::new(TransactionalCountingPort {
            calls: transactional_calls.clone(),
            commit: true,
        }));
    let ports = registry_with_transactional_counting(registration);
    let mut ctx = transactional_invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
        &binding,
    );
    ctx.effect_boundary = Some(Arc::new(CountingBoundary {
        consults: boundary_consults.clone(),
    }));
    let mut effects = EffectsCore::new(ctx);

    let output = effects
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect("single-use boundary execution should commit");

    assert_eq!(output, CountingOutput { value: 1_007 });
    assert_eq!(boundary_consults.load(Ordering::SeqCst), 1);
    assert_eq!(transactional_calls.load(Ordering::SeqCst), 1);
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
    let records = effect_records(&journal);
    assert_eq!(records.len(), 1);
    assert!(matches!(
        records[0].outcome,
        EffectOutcomePayload::SucceededFact { .. }
    ));
}

#[tokio::test]
async fn transactional_boundary_committed_failure_overrides_port_return_and_replays() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let port_calls = Arc::new(AtomicUsize::new(0));
    let live_boundary_consults = Arc::new(AtomicUsize::new(0));
    let (binding, registration) =
        transactional_counting_binding(Arc::new(CommittedFailureTransactionalPort {
            calls: port_calls.clone(),
        }));
    let ports = registry_with_transactional_counting(registration);
    let mut live_ctx = transactional_invocation_context_with_mode(
        live_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
        &binding,
    );
    live_ctx.effect_boundary = Some(Arc::new(CountingBoundary {
        consults: live_boundary_consults.clone(),
    }));
    let mut live = EffectsCore::new(live_ctx);

    let live_err = live
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect_err("the committed failure must override the port's success return");

    assert!(matches!(live_err, EffectError::RecordedFailure { .. }));
    assert_eq!(port_calls.load(Ordering::SeqCst), 1);
    assert_eq!(live_boundary_consults.load(Ordering::SeqCst), 1);
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
    let records = effect_records(&live_journal);
    assert_eq!(records.len(), 1);
    assert!(matches!(
        records[0].outcome,
        EffectOutcomePayload::Failed { .. }
    ));

    let history = Arc::new(
        EffectHistory::from_records(
            records[0].cursor.recorded_flow_id.as_str().to_string(),
            records,
        )
        .expect("committed transactional failure indexes"),
    );
    let replay_boundary_consults = Arc::new(AtomicUsize::new(0));
    let mut replay_ctx = transactional_invocation_context_with_mode(
        Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id))),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
        &binding,
    );
    replay_ctx.effect_boundary = Some(Arc::new(CountingBoundary {
        consults: replay_boundary_consults.clone(),
    }));
    let mut replay = EffectsCore::new(replay_ctx);

    let replay_err = replay
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls,
            binding: binding.invocation(),
        })
        .await
        .expect_err("strict replay must return the recorded committed failure");

    assert!(matches!(replay_err, EffectError::RecordedFailure { .. }));
    assert_eq!(port_calls.load(Ordering::SeqCst), 1);
    assert_eq!(replay_boundary_consults.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn transactional_boundary_foreign_abort_cannot_reclassify_a_committed_operation() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let transactional_calls = Arc::new(AtomicUsize::new(0));
    let (binding, registration) =
        transactional_counting_binding(Arc::new(TransactionalCountingPort {
            calls: transactional_calls.clone(),
            commit: true,
        }));
    let ports = registry_with_transactional_counting(registration);
    let mut ctx = transactional_invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
        &binding,
    );
    ctx.effect_boundary = Some(Arc::new(ExecutedThenForeignAbortBoundary));
    let mut effects = EffectsCore::new(ctx);

    let output = effects
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect("the supplied operation's committed outcome must remain terminal");

    assert_eq!(output, CountingOutput { value: 1_007 });
    assert_eq!(transactional_calls.load(Ordering::SeqCst), 1);
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
    let records = effect_records(&journal);
    assert_eq!(records.len(), 1);
    assert!(matches!(
        records[0].outcome,
        EffectOutcomePayload::SucceededFact { .. }
    ));
}

#[tokio::test]
async fn transactional_boundary_foreign_execution_fails_closed_and_replays() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let transactional_calls = Arc::new(AtomicUsize::new(0));
    let foreign_calls = Arc::new(AtomicUsize::new(0));
    let (binding, registration) =
        transactional_counting_binding(Arc::new(TransactionalCountingPort {
            calls: transactional_calls.clone(),
            commit: true,
        }));
    let ports = registry_with_transactional_counting(registration);
    let mut live_ctx = transactional_invocation_context_with_mode(
        live_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
        &binding,
    );
    live_ctx.effect_boundary = Some(Arc::new(ForeignExecutionBoundary {
        foreign_calls: foreign_calls.clone(),
    }));
    let mut live = EffectsCore::new(live_ctx);

    let live_err = live
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect_err("a foreign execution report must fail closed");

    assert!(matches!(live_err, EffectError::EffectProvenanceMismatch(_)));
    assert_eq!(transactional_calls.load(Ordering::SeqCst), 0);
    assert_eq!(foreign_calls.load(Ordering::SeqCst), 1);
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
    let records = effect_records(&live_journal);
    assert_eq!(records.len(), 1);
    assert!(matches!(
        records[0].outcome,
        EffectOutcomePayload::Failed { .. }
    ));

    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let history = Arc::new(
        EffectHistory::from_records(
            records[0].cursor.recorded_flow_id.as_str().to_string(),
            records,
        )
        .expect("recorded provenance failure indexes"),
    );
    let mut replay = EffectsCore::new(transactional_invocation_context_with_mode(
        replay_journal,
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
        &binding,
    ));

    let replay_err = replay
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls,
            binding: binding.invocation(),
        })
        .await
        .expect_err("strict replay returns the recorded provenance failure");
    assert!(matches!(replay_err, EffectError::RecordedFailure { .. }));
    assert_eq!(foreign_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn transactional_boundary_abort_records_failure_and_replays() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let transactional_calls = Arc::new(AtomicUsize::new(0));
    let (binding, registration) =
        transactional_counting_binding(Arc::new(TransactionalCountingPort {
            calls: transactional_calls.clone(),
            commit: true,
        }));
    let ports = registry_with_transactional_counting(registration);
    let mut live_ctx = transactional_invocation_context_with_mode(
        live_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
        &binding,
    );
    live_ctx.effect_boundary = Some(Arc::new(AbortingBoundary));
    let mut live = EffectsCore::new(live_ctx);

    let live_err = live
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect_err("boundary abort must reject the transactional effect");

    assert!(matches!(live_err, EffectError::BoundaryRejected { .. }));
    assert_eq!(
        transactional_calls.load(Ordering::SeqCst),
        0,
        "admission runs before execute_and_commit (H5)"
    );
    let records = effect_records(&live_journal);
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].cursor.effect_ordinal, 0);
    assert!(matches!(
        records[0].outcome,
        EffectOutcomePayload::Failed { .. }
    ));

    // Strict replay reproduces the recorded rejection without consulting the
    // port or the boundary.
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let history = Arc::new(
        EffectHistory::from_records(
            records[0].cursor.recorded_flow_id.as_str().to_string(),
            records,
        )
        .expect("recorded rejection indexes"),
    );
    let mut replay = EffectsCore::new(transactional_invocation_context_with_mode(
        replay_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
        &binding,
    ));

    let replay_err = replay
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
            binding: binding.invocation(),
        })
        .await
        .expect_err("strict replay returns the recorded rejection");

    assert!(
        matches!(&replay_err, EffectError::RecordedFailure { .. }),
        "expected RecordedFailure, got {replay_err:?}"
    );
}

#[tokio::test]
async fn transactional_boundary_abort_restores_output_ordinal() {
    // An aborted transactional effect reserves an output ordinal the abort
    // never consumes (failure records key off the cursor). The reservation
    // must roll back, or facts authored after it would carry a different
    // deterministic identity live than under replay reconstruction.
    let stage_id = StageId::new();
    let writer_id = WriterId::from(stage_id);
    let flow_id = FlowId::new();
    let parent = parent_envelope(writer_id);
    let (binding, registration) =
        transactional_counting_binding(Arc::new(TransactionalCountingPort {
            calls: Arc::new(AtomicUsize::new(0)),
            commit: true,
        }));

    let make_ctx = |journal: Arc<MemoryJournal<ChainEvent>>,
                    boundary: Option<Arc<dyn EffectBoundary>>,
                    ports: EffectPortRegistry| EffectInvocationContext {
        flow_id,
        stage_id,
        stage_key: "effect_stage".to_string(),
        writer_id,
        input_seq: StageInputPosition(1),
        lineage: obzenflow_core::config::LineagePolicy::default(),
        stage_logic_version: "test-v1".to_string(),
        data_journal: journal,
        flow_context: None,
        observers: None,
        system_journal: None,
        instrumentation: None,
        heartbeat_state: None,
        parent: parent.clone(),
        effect_history: None,
        runtime_execution: crate::execution::RuntimeExecution::new(
            crate::execution::RuntimeMode::Live,
            None,
        ),
        effect_ports: ports,
        effect_declarations: vec![
            EffectDeclaration::of::<CountingEffect>(),
            EffectDeclaration::transactional(&binding),
        ],
        output_contract: StageOutputContract::empty(),
        backpressure_writer: BackpressureWriter::disabled(),
        emit_enabled: false,
        effect_boundary: boundary,
    };

    // Run A: aborted transactional effect, then a counting effect.
    let journal_a = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let ports_a = registry_with_transactional_counting(registration);
    let mut effects_a = EffectsCore::new(make_ctx(
        journal_a.clone(),
        Some(Arc::new(TransactionalOnlyAbortBoundary)),
        ports_a,
    ));
    effects_a
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: Arc::new(AtomicUsize::new(0)),
            binding: binding.invocation(),
        })
        .await
        .expect_err("boundary aborts the transactional effect");
    effects_a
        .perform(CountingEffect {
            value: 41,
            label: "same",
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .await
        .expect("counting effect succeeds after the abort");

    // Run B: only the counting effect, same identity coordinates.
    let journal_b = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut effects_b =
        EffectsCore::new(make_ctx(journal_b.clone(), None, EffectPortRegistry::new()));
    effects_b
        .perform(CountingEffect {
            value: 41,
            label: "same",
            calls: Arc::new(AtomicUsize::new(0)),
        })
        .await
        .expect("counting effect succeeds");

    let fact_id = |journal: &MemoryJournal<ChainEvent>| {
        journal
            .events()
            .into_iter()
            .map(|envelope| envelope.event)
            .find(|event| event.event_type().starts_with("test.counting_output"))
            .expect("counting fact recorded")
            .id
    };
    assert_eq!(
        fact_id(&journal_a),
        fact_id(&journal_b),
        "an aborted transactional effect must not shift the output ordinals of later facts"
    );
}

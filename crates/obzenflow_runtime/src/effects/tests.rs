// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::{EventEnvelope, JournalEvent};
use obzenflow_core::journal::{JournalError, JournalReader};
use obzenflow_core::{JournalId, JournalOwner, JournalWriterId, TypedPayload};
use obzenflow_topology::TypeHintInfo;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

struct MemoryJournal<T: JournalEvent> {
    id: JournalId,
    owner: Option<JournalOwner>,
    events: Mutex<Vec<EventEnvelope<T>>>,
}

impl<T: JournalEvent> MemoryJournal<T> {
    fn new(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            events: Mutex::new(Vec::new()),
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

    async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
        let remaining = self.events.len().saturating_sub(self.position);
        let skipped = remaining.min(n as usize);
        self.position += skipped;
        Ok(skipped as u64)
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

    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(self.events())
    }

    async fn read_causally_after(
        &self,
        _after_event_id: &EventId,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(Vec::new())
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

    async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(MemoryJournalReader {
            events: self.events(),
            position: 0,
        }))
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

    type Outcome = CountingOutput;

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
struct FailingEffect {
    label: &'static str,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for FailingEffect {
    const EFFECT_TYPE: &'static str = "test.failing";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = CountingOutput;

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
}

#[async_trait]
impl Effect for TransactionalCountingEffect {
    const EFFECT_TYPE: &'static str = "test.transactional_counting";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Transactional;

    type Outcome = CountingOutput;

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

#[derive(Clone, Debug)]
struct MultiFactEffect {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for MultiFactEffect {
    const EFFECT_TYPE: &'static str = "test.multi_fact";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = MultiFactOutcome;

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

/// A carrier with a non-empty synthesized set, standing in for a `Guarded`
/// lifted carrier without depending on the adapters crate (FLOWIP-120m
/// coordination tests).
#[derive(Clone, Debug)]
struct LiftedProbeOutcome {
    value: CountingOutput,
}

impl TypedFactSet for LiftedProbeOutcome {
    fn fact_types() -> Vec<TypedFactType> {
        vec![
            TypedFactType::of::<CountingOutput>(),
            TypedFactType::of::<FirstOutput>(),
        ]
    }

    fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
        Ok(vec![TypedFact::from_payload(self.value)?])
    }

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
        Ok(Self {
            value: obzenflow_core::event::schema::decode_member_fact::<CountingOutput>(facts)?,
        })
    }

    fn synthesized_fact_types() -> Vec<TypedFactType> {
        vec![TypedFactType::of::<FirstOutput>()]
    }
}

#[derive(Clone, Debug)]
struct LiftedProbeEffect;

#[async_trait]
impl Effect for LiftedProbeEffect {
    const EFFECT_TYPE: &'static str = "test.lifted_probe";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = LiftedProbeOutcome;

    fn label(&self) -> &str {
        "lifted_probe"
    }

    fn canonical_input(&self) -> Value {
        json!({})
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        Ok(LiftedProbeOutcome {
            value: CountingOutput { value: 0 },
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
            EffectDeclaration::of::<FailingEffect>(),
            EffectDeclaration::of::<MultiFactEffect>(),
            EffectDeclaration::of::<KeylessEffect>(),
            EffectDeclaration::transactional_effect::<TransactionalCountingEffect>("tx"),
        ],
        synthesized_outcomes: Vec::new(),
        output_contract: StageOutputContract::empty(),
        backpressure_writer: BackpressureWriter::disabled(),
        emit_enabled: false,
        effect_boundary: None,
        boundary_control_events: Arc::new(Mutex::new(Vec::new())),
    }
}

#[tokio::test]
async fn emit_rejects_contexts_without_output_emission_support() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut effects = Effects::new(invocation_context(
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
    let mut effects = Effects::new(ctx);

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
    let mut effects = Effects::new(ctx);

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
    let mut effects = Effects::new(ctx);

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
    let mut effects = Effects::new(invocation_context(
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
    let mut live_effects = Effects::new(live_ctx);

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
    let mut replay_effects = Effects::new(replay_ctx);

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
    let mut live_effects = Effects::new(live_ctx);

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
    let mut replay_effects = Effects::new(replay_ctx);

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
    let mut replay_of_replay_effects = Effects::new(replay_of_replay_ctx);

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
    let mut effects = Effects::new(ctx);

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
    let mut live = Effects::new(invocation_context(live_journal.clone(), live_parent, None));
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
    let mut replay = Effects::new(ctx);

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
    let mut effects = Effects::new(ctx);

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
    let mut live = Effects::new(invocation_context(journal.clone(), live_parent, None));
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
    let mut replay = Effects::new(invocation_context(
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
    let mut effects = Effects::new(invocation_context_with_mode(
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
    let mut live = Effects::new(invocation_context(
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
    let mut replay = Effects::new(invocation_context(
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
    let mut replay_of_replay = Effects::new(invocation_context(
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
    let mut effects = Effects::new(invocation_context_with_mode(
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
    let mut live = Effects::new(invocation_context(
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
    let mut resume = Effects::new(invocation_context_with_mode(
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
    let mut ports = EffectPortRegistry::new();
    ports.insert::<dyn TransactionalEffectPort<TransactionalCountingEffect>>(
        "tx",
        Arc::new(TransactionalCountingPort {
            calls: transactional_calls.clone(),
            commit: true,
        }),
    );
    let mut effects = Effects::new(invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
    ));

    let output = effects
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
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
    let mut ports = EffectPortRegistry::new();
    ports.insert::<dyn TransactionalEffectPort<TransactionalCountingEffect>>(
        "tx",
        Arc::new(DivergentTransactionalPort {
            calls: port_calls.clone(),
        }),
    );
    let mut live = Effects::new(invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
    ));

    let live_output = live
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
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
    let mut replay = Effects::new(invocation_context_with_mode(
        Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
    ));

    let replay_output = replay
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
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
    let mut ports = EffectPortRegistry::new();
    ports.insert::<dyn TransactionalEffectPort<TransactionalCountingEffect>>(
        "tx",
        Arc::new(TransactionalCountingPort {
            calls: transactional_calls,
            commit: true,
        }),
    );
    let mut live = Effects::new(invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
    ));
    live.perform(TransactionalCountingEffect {
        value: 7,
        normal_calls: normal_calls.clone(),
    })
    .await
    .expect("live transactional effect should commit");

    let records = effect_records(&journal);
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("history should index"),
    );
    let mut replay = Effects::new(invocation_context_with_mode(
        Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        EffectPortRegistry::new(),
    ));

    let output = replay
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
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
    let mut ports = EffectPortRegistry::new();
    ports.insert::<dyn TransactionalEffectPort<TransactionalCountingEffect>>(
        "tx",
        Arc::new(TransactionalCountingPort {
            calls: transactional_calls.clone(),
            commit: false,
        }),
    );
    let mut effects = Effects::new(invocation_context_with_mode(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
    ));

    let err = effects
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
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
    let mut effects = Effects::new(invocation_context(
        journal,
        parent_envelope(WriterId::from(stage_id)),
        None,
    ));

    let err = effects
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
        })
        .await
        .expect_err("missing transactional port must fail before execution");

    assert!(matches!(err, EffectError::MissingEffectPort { .. }));
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn replay_fails_on_descriptor_hash_mismatch() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut live = Effects::new(invocation_context(
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
    let mut replay = Effects::new(invocation_context(
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
    let mut live = Effects::new(invocation_context(
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
    let mut live = Effects::new(invocation_context(
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
    let mut live = Effects::new(invocation_context(
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

    // A middleware-synthesized marker survives the same round trip.
    let mut event = journal.events()[0].event.clone();
    event
        .effect_provenance
        .as_mut()
        .expect("effect event should carry provenance")
        .origin = Some(EffectFactOrigin::MiddlewareSynthesized {
        label: "test_breaker".to_string(),
    });
    let record = effect_record_from_event(&event)
        .expect("provenance-stamped event should decode")
        .expect("data event should produce a record");
    assert_eq!(
        record.origin,
        Some(EffectFactOrigin::MiddlewareSynthesized {
            label: "test_breaker".to_string(),
        })
    );
}

#[tokio::test]
async fn replayed_group_prefers_recorded_origin_over_derivation() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut live = Effects::new(invocation_context(
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

    // Simulate a middleware-synthesized group: the replay context has no
    // registrations, so derivation would say Effect; the recorded origin must
    // win.
    let records: Vec<EffectRecord> = effect_records(&journal)
        .into_iter()
        .map(|mut record| {
            record.origin = Some(EffectFactOrigin::MiddlewareSynthesized {
                label: "recorded_breaker".to_string(),
            });
            record
        })
        .collect();
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("history should index"),
    );
    let replay_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
    let mut replay = Effects::new(invocation_context(
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
        Some(EffectFactOrigin::MiddlewareSynthesized {
            label: "recorded_breaker".to_string(),
        }),
        "the recorded origin must win over registration-based derivation"
    );
}

#[tokio::test]
async fn replayed_group_without_recorded_origin_falls_back_to_derivation() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut live = Effects::new(invocation_context(
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
    let mut replay = Effects::new(invocation_context(
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
        "a pre-120h record falls back to registration-based derivation"
    );
}

#[tokio::test]
async fn mixed_origin_group_is_rejected_as_provenance_mismatch() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut live = Effects::new(invocation_context(
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
    records[1].origin = Some(EffectFactOrigin::MiddlewareSynthesized {
        label: "tampered".to_string(),
    });

    let record_refs: Vec<&EffectRecord> = records.iter().collect();
    let err = effect_record_group_materialization(&record_refs)
        .expect_err("a group disagreeing on origin must fail loud");

    assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
}

trait DemoPort: Send + Sync {
    fn value(&self) -> u64;
}

struct DemoPortImpl;

impl DemoPort for DemoPortImpl {
    fn value(&self) -> u64 {
        42
    }
}

#[test]
fn effect_context_resolves_typed_trait_object_ports() {
    let mut ports = EffectPortRegistry::new();
    ports.insert::<dyn DemoPort>("primary", Arc::new(DemoPortImpl));
    let ctx = EffectContext {
        is_replaying: false,
        flow_id: FlowId::new(),
        stage_key: "effect_stage".to_string(),
        input_seq: StageInputPosition(3),
        ports,
    };

    let port = ctx
        .port::<dyn DemoPort>("primary")
        .expect("registered port should resolve");
    assert_eq!(port.value(), 42);
    assert!(matches!(
        ctx.port::<dyn DemoPort>("missing"),
        Err(EffectError::MissingEffectPort { .. })
    ));
}

#[tokio::test]
async fn capture_replays_recorded_value_without_using_live_value() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut live = Effects::new(invocation_context(
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
    let mut replay = Effects::new(invocation_context(
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
    let mut replay_of_replay = Effects::new(invocation_context(
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

#[async_trait]
impl EffectBoundary for AbortingBoundary {
    async fn around_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        _execute: EffectExecution,
    ) -> EffectBoundaryReport {
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Aborted(EffectAbortReason {
                cause: EffectFailureCause {
                    source: "circuit_breaker".into(),
                    code: "rejected_circuit_open".into(),
                },
                message: "circuit breaker rejected effect execution".to_string(),
                retry: RetryDisposition::Retryable,
            }),
            control_events: Vec::new(),
        }
    }
}

struct EmptySkipBoundary;

#[async_trait]
impl EffectBoundary for EmptySkipBoundary {
    async fn around_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        _execute: EffectExecution,
    ) -> EffectBoundaryReport {
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Skipped {
                results: Vec::new(),
                source: None,
            },
            control_events: Vec::new(),
        }
    }
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
    let mut live = Effects::new(live_ctx);

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
            assert_eq!(code.as_str(), "rejected_circuit_open");
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
            assert_eq!(cause.code, "rejected_circuit_open");
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
    let mut replay = Effects::new(invocation_context(
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
            assert_eq!(cause.code, "rejected_circuit_open");
        }
        other => panic!("expected RecordedFailure on replay, got {other:?}"),
    }
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(effect_records(&replay_journal), live_records);
}

#[tokio::test]
async fn perform_rejects_unguarded_effect_on_typed_outcome_stage() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.synthesized_outcomes = vec![SynthesizedOutcomeRegistration {
        effect_type: None,
        fact_types: vec![TypedFactType::of::<CountingOutput>()],
        source_label: "circuit_breaker".to_string(),
        kind: SynthesizedOutcomeKind::BranchShaped,
    }];
    let mut effects = Effects::new(ctx);

    let err = effects
        .perform(CountingEffect {
            value: 1,
            label: "unguarded",
            calls: calls.clone(),
        })
        .await
        .expect_err("plain perform on a typed-outcome stage must fail before any I/O");

    assert!(
        matches!(&err, EffectError::TypedOutcomeCoordination { .. }),
        "expected TypedOutcomeCoordination, got {err:?}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0, "no I/O before the check");
    assert!(
        journal.events().is_empty(),
        "the coordination check runs before any record is appended"
    );
}

#[tokio::test]
async fn plain_perform_is_allowed_on_outcome_shaped_registration() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.synthesized_outcomes = vec![SynthesizedOutcomeRegistration {
        effect_type: Some(CountingEffect::EFFECT_TYPE.to_string()),
        fact_types: vec![TypedFactType::of::<CountingOutput>()],
        source_label: "circuit_breaker".to_string(),
        kind: SynthesizedOutcomeKind::OutcomeShaped,
    }];
    let mut effects = Effects::new(ctx);

    let output = effects
        .perform(CountingEffect {
            value: 1,
            label: "plain",
            calls: calls.clone(),
        })
        .await
        .expect("outcome-shaped registrations use the plain perform");

    assert_eq!(output, CountingOutput { value: 2 });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn guarded_perform_is_rejected_on_outcome_shaped_registration() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.effect_declarations
        .push(EffectDeclaration::of::<LiftedProbeEffect>());
    ctx.synthesized_outcomes = vec![SynthesizedOutcomeRegistration {
        effect_type: Some(LiftedProbeEffect::EFFECT_TYPE.to_string()),
        fact_types: vec![TypedFactType::of::<CountingOutput>()],
        source_label: "circuit_breaker".to_string(),
        kind: SynthesizedOutcomeKind::OutcomeShaped,
    }];
    let mut effects = Effects::new(ctx);

    let err = effects
        .perform(LiftedProbeEffect)
        .await
        .expect_err("a lifted carrier on an outcome-shaped stage must fail before any I/O");

    assert!(
        matches!(&err, EffectError::TypedOutcomeCoordination { .. }),
        "expected TypedOutcomeCoordination, got {err:?}"
    );
    assert!(journal.events().is_empty());
}

#[tokio::test]
async fn transactional_perform_is_rejected_on_outcome_shaped_registration() {
    let stage_id = StageId::new();
    let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let mut ctx = invocation_context(
        journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    ctx.synthesized_outcomes = vec![SynthesizedOutcomeRegistration {
        effect_type: Some(TransactionalCountingEffect::EFFECT_TYPE.to_string()),
        fact_types: vec![TypedFactType::of::<CountingOutput>()],
        source_label: "circuit_breaker".to_string(),
        kind: SynthesizedOutcomeKind::OutcomeShaped,
    }];
    let mut effects = Effects::new(ctx);

    let err = effects
        .perform(TransactionalCountingEffect {
            value: 1,
            normal_calls: normal_calls.clone(),
        })
        .await
        .expect_err("the transactional path runs before the boundary; it cannot be protected");

    assert!(
        matches!(&err, EffectError::TypedOutcomeCoordination { .. }),
        "expected TypedOutcomeCoordination, got {err:?}"
    );
    assert_eq!(normal_calls.load(Ordering::SeqCst), 0);
    assert!(journal.events().is_empty());
}

#[tokio::test]
async fn boundary_empty_skip_records_failure_instead_of_unrecorded_error() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let live_calls = Arc::new(AtomicUsize::new(0));
    let mut live_ctx = invocation_context(
        live_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
    );
    live_ctx.effect_boundary = Some(Arc::new(EmptySkipBoundary));
    let mut live = Effects::new(live_ctx);

    let live_err = live
        .perform(CountingEffect {
            value: 1,
            label: "guarded",
            calls: live_calls.clone(),
        })
        .await
        .expect_err("empty boundary skip should fail the perform");

    assert!(matches!(
        &live_err,
        EffectError::BoundaryRejected { code, .. } if code.as_str() == "skip_without_facts"
    ));
    assert_eq!(live_calls.load(Ordering::SeqCst), 0);

    let live_records = effect_records(&live_journal);
    assert_eq!(
        live_records.len(),
        1,
        "empty skip must leave a recorded outcome, not an unrecorded error"
    );
    assert!(matches!(
        &live_records[0].outcome,
        EffectOutcomePayload::Failed { cause: Some(cause), .. }
            if cause.source.as_str() == "effect_boundary"
                && cause.code.as_str() == "skip_without_facts"
    ));

    // Strict replay must not fail with MissingRecordedEffect.
    let replay_history = Arc::new(
        EffectHistory::from_records("replay_archive_flow".to_string(), live_records)
            .expect("history from live records"),
    );
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let mut replay = Effects::new(invocation_context(
        Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id))),
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

    assert!(
        matches!(&replay_err, EffectError::RecordedFailure { .. }),
        "expected RecordedFailure, got {replay_err:?}"
    );
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
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

    type Outcome = CountingOutput;

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

struct CountingBoundary {
    consults: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectBoundary for CountingBoundary {
    async fn around_effect(
        &self,
        _identity: &EffectIdentity,
        _event: &ChainEvent,
        execute: EffectExecution,
    ) -> EffectBoundaryReport {
        self.consults.fetch_add(1, Ordering::SeqCst);
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(execute.await),
            control_events: Vec::new(),
        }
    }
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
    let mut effects = Effects::new(ctx);

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
    let mut effects = Effects::new(invocation_context_with_mode(
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
    let mut effects = Effects::new(invocation_context_with_mode(
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
    async fn around_effect(
        &self,
        identity: &EffectIdentity,
        _event: &ChainEvent,
        execute: EffectExecution,
    ) -> EffectBoundaryReport {
        if matches!(identity.safety, EffectSafety::Transactional) {
            return EffectBoundaryReport {
                outcome: EffectBoundaryOutcome::Aborted(EffectAbortReason {
                    cause: EffectFailureCause {
                        source: "circuit_breaker".into(),
                        code: "rejected_circuit_open".into(),
                    },
                    message: "circuit breaker rejected transactional effect".to_string(),
                    retry: RetryDisposition::Retryable,
                }),
                control_events: Vec::new(),
            };
        }
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(execute.await),
            control_events: Vec::new(),
        }
    }
}

#[tokio::test]
async fn transactional_boundary_abort_records_failure_and_replays() {
    let stage_id = StageId::new();
    let live_journal = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let normal_calls = Arc::new(AtomicUsize::new(0));
    let transactional_calls = Arc::new(AtomicUsize::new(0));
    let mut ports = EffectPortRegistry::new();
    ports.insert::<dyn TransactionalEffectPort<TransactionalCountingEffect>>(
        "tx",
        Arc::new(TransactionalCountingPort {
            calls: transactional_calls.clone(),
            commit: true,
        }),
    );
    let mut live_ctx = invocation_context_with_mode(
        live_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        None,
        EffectRuntimeMode::Live,
        ports,
    );
    live_ctx.effect_boundary = Some(Arc::new(AbortingBoundary));
    let mut live = Effects::new(live_ctx);

    let live_err = live
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
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
    let mut replay_ports = EffectPortRegistry::new();
    let replay_port_calls = Arc::new(AtomicUsize::new(0));
    replay_ports.insert::<dyn TransactionalEffectPort<TransactionalCountingEffect>>(
        "tx",
        Arc::new(TransactionalCountingPort {
            calls: replay_port_calls.clone(),
            commit: true,
        }),
    );
    let mut replay = Effects::new(invocation_context_with_mode(
        replay_journal.clone(),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
        EffectRuntimeMode::ReplayStrict,
        replay_ports,
    ));

    let replay_err = replay
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: normal_calls.clone(),
        })
        .await
        .expect_err("strict replay returns the recorded rejection");

    assert!(
        matches!(&replay_err, EffectError::RecordedFailure { .. }),
        "expected RecordedFailure, got {replay_err:?}"
    );
    assert_eq!(replay_port_calls.load(Ordering::SeqCst), 0);
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

    let make_ctx = |journal: Arc<MemoryJournal<ChainEvent>>,
                    boundary: Option<Arc<dyn EffectBoundary>>,
                    ports: EffectPortRegistry| EffectInvocationContext {
        flow_id,
        stage_id,
        stage_key: "effect_stage".to_string(),
        writer_id,
        input_seq: StageInputPosition(1),
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
            EffectDeclaration::transactional_effect::<TransactionalCountingEffect>("tx"),
        ],
        synthesized_outcomes: Vec::new(),
        output_contract: StageOutputContract::empty(),
        backpressure_writer: BackpressureWriter::disabled(),
        emit_enabled: false,
        effect_boundary: boundary,
        boundary_control_events: Arc::new(Mutex::new(Vec::new())),
    };

    // Run A: aborted transactional effect, then a counting effect.
    let journal_a = Arc::new(MemoryJournal::new(JournalOwner::stage(stage_id)));
    let mut ports_a = EffectPortRegistry::new();
    ports_a.insert::<dyn TransactionalEffectPort<TransactionalCountingEffect>>(
        "tx",
        Arc::new(TransactionalCountingPort {
            calls: Arc::new(AtomicUsize::new(0)),
            commit: true,
        }),
    );
    let mut effects_a = Effects::new(make_ctx(
        journal_a.clone(),
        Some(Arc::new(TransactionalOnlyAbortBoundary)),
        ports_a,
    ));
    effects_a
        .perform(TransactionalCountingEffect {
            value: 7,
            normal_calls: Arc::new(AtomicUsize::new(0)),
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
    let mut effects_b = Effects::new(make_ctx(journal_b.clone(), None, EffectPortRegistry::new()));
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

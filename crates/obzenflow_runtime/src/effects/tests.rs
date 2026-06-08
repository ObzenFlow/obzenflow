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

    type Output = CountingOutput;

    fn label(&self) -> &str {
        self.label
    }

    fn canonical_input(&self) -> Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(CountingOutput {
            value: self.value + 1,
        })
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

    type Output = CountingOutput;

    fn label(&self) -> &str {
        "transactional"
    }

    fn canonical_input(&self) -> Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct MultiFactOutcome {
    first: FirstOutput,
    second: SecondOutput,
}

impl TypedFactSet for MultiFactOutcome {
    fn fact_types() -> Vec<obzenflow_core::event::schema::TypedFactType> {
        vec![
            obzenflow_core::event::schema::TypedFactType::of::<FirstOutput>(),
            obzenflow_core::event::schema::TypedFactType::of::<SecondOutput>(),
        ]
    }

    fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
        Ok(vec![
            TypedFact::from_payload(self.first)?,
            TypedFact::from_payload(self.second)?,
        ])
    }

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
        Ok(Self {
            first: <FirstOutput as TypedFactSet>::try_from_facts(facts)?,
            second: <SecondOutput as TypedFactSet>::try_from_facts(facts)?,
        })
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

    type Output = MultiFactOutcome;

    fn label(&self) -> &str {
        "multi"
    }

    fn canonical_input(&self) -> Value {
        json!({ "kind": "multi" })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
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
        system_journal: None,
        instrumentation: None,
        heartbeat_state: None,
        parent,
        effect_history,
        effect_runtime_mode,
        effect_ports,
        effect_declarations: vec![
            EffectDeclaration::of::<CountingEffect>(),
            EffectDeclaration::of::<MultiFactEffect>(),
            EffectDeclaration::transactional_effect::<TransactionalCountingEffect>("tx"),
        ],
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
        EffectHistory::from_records(live_flow_id, records).expect("grouped history loads"),
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
    assert!(replay_journal.events().is_empty());
}

#[test]
fn effect_history_rejects_partial_multi_fact_outcome_group() {
    let cursor = EffectCursor {
        recorded_flow_id: "flow".to_string(),
        stage_key: "effect_stage".to_string(),
        input_seq: 1,
        effect_ordinal: 0,
    };
    let descriptor = EffectDescriptor {
        effect_type: MultiFactEffect::EFFECT_TYPE.to_string(),
        label: "multi".to_string(),
        schema_version: MultiFactEffect::SCHEMA_VERSION,
        stage_logic_version: "test-v1".to_string(),
        canonical_input_hash: "input".to_string(),
    };
    let records = vec![
        EffectRecord {
            cursor: cursor.clone(),
            descriptor_hash: "hash".into(),
            descriptor: descriptor.clone(),
            outcome: EffectOutcomePayload::SucceededFact {
                event_type: FirstOutput::versioned_event_type(),
                output: json!({ "value": 10 }),
                outcome_fact_ordinal: OutcomeFactOrdinal::new(0),
            },
        },
        EffectRecord {
            cursor,
            descriptor_hash: "hash".into(),
            descriptor,
            outcome: EffectOutcomePayload::SucceededFact {
                event_type: SecondOutput::versioned_event_type(),
                output: json!({ "value": "twenty" }),
                outcome_fact_ordinal: OutcomeFactOrdinal::new(2),
            },
        },
    ];

    let err = EffectHistory::from_records("flow".to_string(), records)
        .expect_err("missing ordinal 1 must fail loud");

    assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
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
        .effect_ordinal = 99;

    let err = effect_record_from_event(&event)
        .expect_err("payload/provenance cursor mismatch must fail loud");

    assert!(matches!(err, EffectError::EffectProvenanceMismatch(_)));
}

#[test]
fn effect_record_decode_rejects_reserved_event_without_provenance() {
    let stage_id = StageId::new();
    let cursor = EffectCursor {
        recorded_flow_id: "flow".to_string(),
        stage_key: "effect_stage".to_string(),
        input_seq: 1,
        effect_ordinal: 0,
    };
    let record = EffectRecord {
        cursor,
        descriptor_hash: "hash".into(),
        descriptor: EffectDescriptor {
            effect_type: CountingEffect::EFFECT_TYPE.to_string(),
            label: "same".to_string(),
            schema_version: CountingEffect::SCHEMA_VERSION,
            stage_logic_version: "test-v1".to_string(),
            canonical_input_hash: "input".to_string(),
        },
        outcome: EffectOutcomePayload::Succeeded {
            output: json!(2_u64),
        },
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
    let history = Arc::new(
        EffectHistory::from_records(records[0].cursor.recorded_flow_id.clone(), records)
            .expect("history should index"),
    );
    let mut replay = Effects::new(invocation_context(
        Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new()))),
        parent_envelope(WriterId::from(stage_id)),
        Some(history),
    ));

    let replayed: u64 = replay
        .capture("side_value", 999)
        .await
        .expect("capture should replay");

    assert_eq!(replayed, 7);
}

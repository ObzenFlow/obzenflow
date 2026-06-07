// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_adapters::middleware::control::circuit_breaker::{CircuitBreakerBuilder, OpenPolicy};
use obzenflow_adapters::middleware::RateLimiterBuilder;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    event::payloads::flow_control_payload::FlowControlPayload,
    event::payloads::observability_payload::{
        CircuitBreakerEvent, MiddlewareLifecycle, ObservabilityPayload, RateLimiterEvent,
    },
    event::schema::{TypedFact, TypedFactSet, TypedFactSetError, TypedFactType},
    event::{ChainEventContent, SystemEvent, SystemEventType},
    id::{StageId, SystemId},
    journal::{journal_owner::JournalOwner, Journal},
    TypedPayload, WriterId,
};
use obzenflow_dsl::{
    effectful_sink, effectful_stateful, effectful_transform, flow, sink, source, transform,
    FlowDefinition,
};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    is_framework_effect_event_type, Effect, EffectCommitHandle, EffectContext, EffectCursor,
    EffectError, EffectPortRegistry, EffectPortRequirement, EffectRecord, EffectSafety, Effects,
    IdempotencyKey, TransactionalEffectPort, EFFECT_RECORD_EVENT_TYPE,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulSinkHandler, EffectfulStatefulHandler, EffectfulTransformHandler, FiniteSourceHandler,
    SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

static EFFECT_REPLAY_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn effect_replay_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    EFFECT_REPLAY_TEST_LOCK.lock().await
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReplayInput {
    value: u64,
}

impl TypedPayload for ReplayInput {
    const EVENT_TYPE: &'static str = "effect_replay.input";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReplayOutput {
    value: u64,
    effect_value: u64,
}

impl TypedPayload for ReplayOutput {
    const EVENT_TYPE: &'static str = "effect_replay.output";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReplayEffectValue {
    effect_value: u64,
}

impl TypedPayload for ReplayEffectValue {
    const EVENT_TYPE: &'static str = "effect_replay.effect_value";
}

#[derive(Clone, Debug)]
struct ReplaySource {
    next_value: u64,
    writer_id: WriterId,
}

impl ReplaySource {
    fn new() -> Self {
        Self {
            next_value: 1,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for ReplaySource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next_value > 3 {
            return Ok(None);
        }

        let value = self.next_value;
        self.next_value += 1;

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            ReplayInput::EVENT_TYPE,
            json!(ReplayInput { value }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct SingleReplaySource {
    emitted: bool,
    writer_id: WriterId,
}

impl SingleReplaySource {
    fn new() -> Self {
        Self {
            emitted: false,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for SingleReplaySource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            ReplayInput::EVENT_TYPE,
            json!(ReplayInput { value: 1 }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct FanOutTransform {
    writer_id: WriterId,
}

impl FanOutTransform {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl TransformHandler for FanOutTransform {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let Some(input) = ReplayInput::from_event(&event) else {
            return Ok(Vec::new());
        };

        Ok(vec![
            ChainEventFactory::derived_data_event(
                self.writer_id,
                &event,
                ReplayInput::EVENT_TYPE,
                json!(ReplayInput {
                    value: input.value * 10 + 1
                }),
            ),
            ChainEventFactory::derived_data_event(
                self.writer_id,
                &event,
                ReplayInput::EVENT_TYPE,
                json!(ReplayInput {
                    value: input.value * 10 + 2
                }),
            ),
        ])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CountingEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for CountingEffect {
    const EFFECT_TYPE: &'static str = "effect_replay.counting";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Output = ReplayEffectValue;

    fn label(&self) -> &str {
        "counting"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ReplayEffectValue {
            effect_value: self.value + 100,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("counting:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct BlockingEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
}

#[async_trait]
impl Effect for BlockingEffect {
    const EFFECT_TYPE: &'static str = "effect_replay.blocking";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Output = ReplayEffectValue;

    fn label(&self) -> &str {
        "blocking"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let permit = self
            .release
            .acquire()
            .await
            .map_err(|_| EffectError::Execution("blocking gate closed".to_string()))?;
        drop(permit);
        Ok(ReplayEffectValue {
            effect_value: self.value + 100,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("blocking:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct ReplayTransform {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for ReplayTransform {
    type Input = ReplayInput;

    async fn process(&self, input: ReplayInput, fx: &mut Effects) -> Result<(), HandlerError> {
        let effect_value = fx
            .perform(CountingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;

        fx.emit(ReplayOutput {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-v1"
    }
}

#[derive(Clone, Debug)]
struct BlockingTransform {
    calls: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
}

#[async_trait]
impl EffectfulTransformHandler for BlockingTransform {
    type Input = ReplayInput;

    async fn process(&self, input: ReplayInput, fx: &mut Effects) -> Result<(), HandlerError> {
        let effect_value = fx
            .perform(BlockingEffect {
                value: input.value,
                calls: self.calls.clone(),
                release: self.release.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;

        fx.emit(ReplayOutput {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-blocking-v1"
    }
}

#[derive(Clone, Debug)]
struct AlwaysFailingEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for AlwaysFailingEffect {
    const EFFECT_TYPE: &'static str = "effect_replay.always_failing";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Output = ReplayEffectValue;

    fn label(&self) -> &str {
        "always_failing"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(EffectError::Execution("simulated_gateway_down".to_string()))
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("always-failing:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct FallbackTransform {
    calls: Arc<AtomicUsize>,
}

#[derive(Clone, Debug, Default)]
struct ReplayStatefulState {
    outputs: Vec<ReplayOutput>,
}

#[derive(Clone, Debug)]
struct ReplayStateful {
    calls: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
enum ReplayStatefulFact {
    EffectValue(ReplayEffectValue),
    Output(ReplayOutput),
}

impl TypedFactSet for ReplayStatefulFact {
    fn fact_types() -> Vec<TypedFactType> {
        vec![
            TypedFactType::of::<ReplayEffectValue>(),
            TypedFactType::of::<ReplayOutput>(),
        ]
    }

    fn into_facts(self) -> Result<Vec<TypedFact>, TypedFactSetError> {
        match self {
            ReplayStatefulFact::EffectValue(value) => Ok(vec![TypedFact::from_payload(value)?]),
            ReplayStatefulFact::Output(output) => Ok(vec![TypedFact::from_payload(output)?]),
        }
    }

    fn try_from_facts(facts: &[TypedFact]) -> Result<Self, TypedFactSetError> {
        if let Ok(value) = <ReplayEffectValue as TypedFactSet>::try_from_facts(facts) {
            return Ok(Self::EffectValue(value));
        }
        if let Ok(output) = <ReplayOutput as TypedFactSet>::try_from_facts(facts) {
            return Ok(Self::Output(output));
        }
        Err(TypedFactSetError::MissingFact {
            event_type: format!(
                "{} or {}",
                ReplayEffectValue::versioned_event_type(),
                ReplayOutput::versioned_event_type()
            ),
        })
    }
}

#[async_trait]
impl EffectfulStatefulHandler for ReplayStateful {
    type State = ReplayStatefulState;
    type Input = ReplayInput;
    type Fact = ReplayStatefulFact;

    fn initial_state(&self) -> Self::State {
        ReplayStatefulState::default()
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &ReplayInput,
        fx: &mut Effects,
    ) -> Result<(), HandlerError> {
        let effect = fx
            .perform(CountingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        fx.emit(ReplayOutput {
            value: input.value,
            effect_value: effect.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn apply(&mut self, state: &mut Self::State, fact: Self::Fact) -> Result<(), HandlerError> {
        if let ReplayStatefulFact::Output(output) = fact {
            state.outputs.push(output);
        }
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-stateful-v1"
    }
}

#[async_trait]
impl EffectfulTransformHandler for FallbackTransform {
    type Input = ReplayInput;

    async fn process(&self, input: ReplayInput, fx: &mut Effects) -> Result<(), HandlerError> {
        let effect_value = fx
            .perform(AlwaysFailingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Timeout(e.to_string()))?;

        fx.emit(ReplayOutput {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-fallback-v1"
    }
}

#[derive(Clone, Debug)]
struct CollectSink {
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
}

#[async_trait]
impl SinkHandler for CollectSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if let Some(output) = ReplayOutput::from_event(&event) {
            self.outputs
                .lock()
                .expect("outputs lock poisoned")
                .push(output);
        }

        Ok(DeliveryPayload::success(
            "collector",
            DeliveryMethod::Custom("Memory".to_string()),
            None,
        ))
    }
}

#[derive(Clone, Debug)]
struct EffectfulCollectSink {
    calls: Arc<AtomicUsize>,
    receipts: Arc<Mutex<Vec<ReplayOutput>>>,
}

#[async_trait]
impl EffectfulSinkHandler for EffectfulCollectSink {
    type Input = ReplayInput;

    async fn consume(
        &mut self,
        input: ReplayInput,
        fx: &mut Effects,
    ) -> Result<DeliveryPayload, HandlerError> {
        let effect_value = fx
            .perform(CountingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;

        self.receipts
            .lock()
            .expect("receipts lock poisoned")
            .push(ReplayOutput {
                value: input.value,
                effect_value: effect_value.effect_value,
            });

        Ok(DeliveryPayload::success(
            "effectful_collector",
            DeliveryMethod::Custom("Memory".to_string()),
            None,
        ))
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-sink-v1"
    }
}

fn build_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_suppression",
        journals: disk_journals(journal_base),
        middleware: [],
        effect_ports: obzenflow_runtime::effects::EffectPortRegistry::new(),

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => ReplayTransform { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

fn build_flow_level_limiter_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_flow_limiter",
        journals: disk_journals(journal_base),
        middleware: [
            RateLimiterBuilder::new(1.0).build()
        ],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => ReplayTransform { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

/// Same shape as `build_flow_level_limiter_flow` but with a high admission rate, so
/// the limiter consumes tokens and moves admission state without blocking a worker
/// thread for seconds. The resume regression only needs token consumption at the
/// effect boundary versus suppression at the sink, not real backpressure delay, so
/// this keeps that test fast and avoids starving the shared runtime under parallel
/// test execution.
fn build_fast_limiter_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_fast_limiter",
        journals: disk_journals(journal_base),
        middleware: [
            RateLimiterBuilder::new(1000.0).build()
        ],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => ReplayTransform { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

fn build_blocking_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_blocking",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => SingleReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => BlockingTransform { calls, release },
                effects: [BlockingEffect],
                middleware: []
            );
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

fn build_fan_out_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_fan_out",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => SingleReplaySource::new());
            fan_out = transform!(ReplayInput -> ReplayInput => FanOutTransform::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => ReplayTransform { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> fan_out;
            fan_out |> effectful;
            effectful |> collector;
        }
    }
}

fn build_effectful_sink_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    receipts: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_sink",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            collector = effectful_sink!(
                ReplayInput => EffectfulCollectSink { calls, receipts },
                effects: [CountingEffect],
                middleware: []
            );
        },

        topology: {
            inputs |> collector;
        }
    }
}

fn build_breaker_fallback_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_breaker_fallback",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => FallbackTransform { calls },
                effects: [AlwaysFailingEffect],
                middleware: [
                CircuitBreakerBuilder::new(1)
                    .open_policy(OpenPolicy::EmitFallback)
                    .with_typed_fallback::<ReplayInput, ReplayEffectValue, _>(|input| ReplayEffectValue {
                        effect_value: input.value + 900,
                    })
                    .build()
            ]);
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

fn build_stateful_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_stateful",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_stateful!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => ReplayStateful { calls },
                effects: [CountingEffect],
                middleware: []
            );
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let flows_dir = base.join("flows");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&flows_dir)
        .expect("flows directory should exist")
        .map(|entry| entry.expect("flow dir entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    entries.sort();
    entries
        .pop()
        .expect("live run should have produced a replay archive")
}

fn archive_manifest(run_dir: &Path) -> serde_json::Value {
    let manifest_path = run_dir.join("run_manifest.json");
    serde_json::from_str(
        &std::fs::read_to_string(&manifest_path).expect("run_manifest.json should be readable"),
    )
    .expect("run_manifest.json should parse")
}

fn mark_archive_incomplete(run_dir: &Path) {
    let manifest = archive_manifest(run_dir);
    let system_journal = manifest["system_journal_file"]
        .as_str()
        .expect("manifest should contain system journal file");
    std::fs::write(run_dir.join(system_journal), "")
        .expect("system journal should be writable for test mutation");
}

fn remove_effect_results_for_stage(run_dir: &Path, stage_key: &str) {
    let manifest = archive_manifest(run_dir);
    let stage_journal = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .expect("manifest should contain stage data journal file");
    let journal_path = run_dir.join(stage_journal);
    let original =
        std::fs::read_to_string(&journal_path).expect("stage journal should be readable");
    // FLOWIP-120b: successful effect outcomes are domain `Data` facts carrying
    // non-framework effect provenance. Failure/capture compatibility rows remain
    // framework-owned reserved event types. Drop both forms so resume sees the
    // effect outcomes as missing and re-executes; ordinary domain outputs on the
    // same journal are retained.
    let effect_outcome_type = ReplayEffectValue::versioned_event_type();
    let retained = original
        .lines()
        .filter(|line| {
            !line.contains(EFFECT_RECORD_EVENT_TYPE) && !line.contains(&effect_outcome_type)
        })
        .collect::<Vec<_>>()
        .join("\n");
    let retained = if retained.is_empty() {
        retained
    } else {
        format!("{retained}\n")
    };
    std::fs::write(&journal_path, retained).expect("stage journal should be writable");
}

async fn read_stage_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
    let manifest = archive_manifest(run_dir);
    let stage_journal = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .expect("manifest should contain stage data journal file");
    let journal: obzenflow_infra::journal::DiskJournal<ChainEvent> =
        obzenflow_infra::journal::DiskJournal::with_owner(
            run_dir.join(stage_journal),
            JournalOwner::stage(StageId::new()),
        )
        .expect("stage journal should open");

    journal
        .read_causally_ordered()
        .await
        .expect("stage journal should read")
        .into_iter()
        .map(|envelope| envelope.event)
        .collect()
}

/// True when an event is a framework-owned effect fact. Successful typed effect
/// outcomes are no longer framework-owned; they are normal domain `Data` facts
/// tagged with non-framework effect provenance. Reserved framework rows remain
/// for capture and failure compatibility.
fn is_framework_effect_fact(event: &ChainEvent) -> bool {
    matches!(
        &event.content,
        ChainEventContent::Data { event_type, .. }
            if event
                .effect_provenance
                .as_ref()
                .is_some_and(|provenance| provenance.framework_owned)
                && is_framework_effect_event_type(event_type)
    )
}

fn is_domain_effect_outcome_fact(event: &ChainEvent) -> bool {
    matches!(
        &event.content,
        ChainEventContent::Data { event_type, .. }
            if event_type == &ReplayEffectValue::versioned_event_type()
                && event
                    .effect_provenance
                    .as_ref()
                    .is_some_and(|provenance| !provenance.framework_owned)
    )
}

/// Decode the `EffectRecord` carried by a framework-owned effect-record `Data`
/// fact, mirroring the runtime's replay-history reader. Returns `None` for any
/// other event, including capture facts.
fn framework_effect_record(event: &ChainEvent) -> Option<EffectRecord> {
    match &event.content {
        ChainEventContent::Data {
            event_type,
            payload,
        } if event
            .effect_provenance
            .as_ref()
            .is_some_and(|provenance| provenance.framework_owned)
            && event_type == EFFECT_RECORD_EVENT_TYPE =>
        {
            serde_json::from_value(payload.clone()).ok()
        }
        _ => None,
    }
}

fn recorded_effect_cursor(event: &ChainEvent) -> Option<EffectCursor> {
    if let Some(record) = framework_effect_record(event) {
        return Some(record.cursor);
    }
    if matches!(event.content, ChainEventContent::Data { .. }) {
        return event
            .effect_provenance
            .as_ref()
            .filter(|provenance| !provenance.framework_owned)
            .map(|provenance| provenance.cursor.clone());
    }
    None
}

async fn rate_limiter_delayed_events_in_stage(run_dir: &Path, stage_key: &str) -> usize {
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter(|event| {
            matches!(
                event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::RateLimiter(RateLimiterEvent::Delayed { .. })
                ))
            )
        })
        .count()
}

async fn circuit_breaker_events_in_stage(run_dir: &Path, stage_key: &str) -> usize {
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter(|event| {
            matches!(
                event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::CircuitBreaker(_)
                ))
            )
        })
        .count()
}

async fn mirrored_circuit_breaker_events_in_system(run_dir: &Path, stage_name: &str) -> usize {
    let manifest = archive_manifest(run_dir);
    let system_journal = manifest["system_journal_file"]
        .as_str()
        .expect("manifest should contain system journal file");
    let journal: obzenflow_infra::journal::DiskJournal<SystemEvent> =
        obzenflow_infra::journal::DiskJournal::with_owner(
            run_dir.join(system_journal),
            JournalOwner::system(SystemId::new()),
        )
        .expect("system journal should open");

    journal
        .read_causally_ordered()
        .await
        .expect("system journal should read")
        .into_iter()
        .filter(|envelope| {
            matches!(
                &envelope.event.event,
                SystemEventType::MiddlewareLifecycle {
                    stage_name: Some(name),
                    middleware:
                        MiddlewareLifecycle::CircuitBreaker(
                            CircuitBreakerEvent::Opened { .. }
                                | CircuitBreakerEvent::Closed { .. }
                                | CircuitBreakerEvent::HalfOpen { .. }
                                | CircuitBreakerEvent::Summary { .. }
                        ),
                    ..
                } if name == stage_name
            )
        })
        .count()
}

/// Count rate-limiter observability events of any variant (Delayed, ActivityPulse,
/// ModeChange) in a stage. Used to assert the limiter was never invoked during
/// replay: an invoked limiter under the test workload leaves a journal trace, so
/// zero events of any kind means it consumed no tokens and moved no admission state.
async fn rate_limiter_events_in_stage(run_dir: &Path, stage_key: &str) -> usize {
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter(|event| {
            matches!(
                event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::RateLimiter(_)
                ))
            )
        })
        .count()
}

/// Maximum rate-limiter runtime counters embedded in a stage's events'
/// `RuntimeContext`: `(rl_events_total, rl_delayed_total, rl_delay_seconds_total)`.
///
/// FLOWIP-120a: a limiter that ran during replay consumes tokens (`rl_events_total`),
/// delays events (`rl_delayed_total`), and accrues wall-clock delay
/// (`rl_delay_seconds_total`). The instrumentation snapshotter embeds these into
/// the stage's wide events, so they are observable even where the limiter's
/// `Delayed` lifecycle records are not journaled (sinks do not persist control
/// events, which is exactly why the journal-only assertion below is blind to the
/// admission-middleware replay bug). During strict replay all three must stay zero.
async fn rate_limiter_runtime_activity_in_stage(
    run_dir: &Path,
    stage_key: &str,
) -> (u64, u64, f64) {
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter_map(|event| event.runtime_context)
        .fold((0u64, 0u64, 0f64), |(ev, dl, ds), rc| {
            (
                ev.max(rc.rl_events_total),
                dl.max(rc.rl_delayed_total),
                ds.max(rc.rl_delay_seconds_total),
            )
        })
}

#[tokio::test]
async fn effectful_sink_fails_closed_at_materialisation() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let calls = Arc::new(AtomicUsize::new(0));
    let receipts = Arc::new(Mutex::new(Vec::new()));
    let err = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_effectful_sink_flow(
            journal_base,
            calls.clone(),
            receipts.clone(),
        ))
        .await
        .expect_err("effectful sinks are retired under FLOWIP-120b");

    let message = err.to_string();
    assert!(
        message.contains("effectful sink") && message.contains("retired by FLOWIP-120b"),
        "materialisation error should explain effectful sink retirement, got: {message}"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(receipts.lock().expect("receipts lock poisoned").is_empty());
}

#[tokio::test]
async fn effectful_transform_replay_suppresses_effect_execution() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        live_domain_outputs,
        vec![
            ReplayOutput {
                value: 1,
                effect_value: 101
            },
            ReplayOutput {
                value: 2,
                effect_value: 102
            },
            ReplayOutput {
                value: 3,
                effect_value: 103
            },
        ]
    );

    let archive_dir = latest_run_dir(&journal_base);
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base,
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "replay must return recorded effect results without executing"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs,
        "replay should emit the same domain payloads"
    );
}

#[tokio::test]
async fn fan_out_sibling_effects_use_distinct_cursors_and_replay_suppresses_execution() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_fan_out_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live fan-out flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 2);
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        live_domain_outputs,
        vec![
            ReplayOutput {
                value: 11,
                effect_value: 111
            },
            ReplayOutput {
                value: 12,
                effect_value: 112
            },
        ]
    );

    let archive_dir = latest_run_dir(&journal_base);
    let effectful_events = read_stage_events(&archive_dir, "effectful").await;
    let effect_cursors: Vec<_> = effectful_events
        .iter()
        .filter_map(recorded_effect_cursor)
        .collect();
    assert_eq!(effect_cursors.len(), 2);
    assert_eq!(effect_cursors[0].input_seq, 1);
    assert_eq!(effect_cursors[0].effect_ordinal, 0);
    assert_eq!(effect_cursors[1].input_seq, 2);
    assert_eq!(effect_cursors[1].effect_ordinal, 0);
    assert_ne!(effect_cursors[0], effect_cursors[1]);

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_fan_out_flow(
            journal_base,
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay fan-out flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "fan-out replay must suppress every sibling effect"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs
    );
}

#[tokio::test]
async fn eof_writer_seq_counts_transport_data_not_effect_results() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    assert_eq!(live_outputs.lock().expect("outputs lock poisoned").len(), 3);

    let archive_dir = latest_run_dir(&journal_base);
    let effectful_events = read_stage_events(&archive_dir, "effectful").await;
    let framework_effect_facts = effectful_events
        .iter()
        .filter(|event| is_framework_effect_fact(event))
        .count();
    let domain_effect_outcome_facts = effectful_events
        .iter()
        .filter(|event| is_domain_effect_outcome_fact(event))
        .count();
    let selected_replay_outputs = effectful_events
        .iter()
        .filter(|event| {
            matches!(
                &event.content,
                ChainEventContent::Data { event_type, .. }
                    if event_type == &ReplayOutput::versioned_event_type()
            )
        })
        .count();
    let eof_writer_seq = effectful_events
        .iter()
        .find_map(|event| match &event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof { writer_seq, .. }) => {
                writer_seq.map(|seq| seq.0)
            }
            _ => None,
        });

    assert_eq!(framework_effect_facts, 0);
    assert_eq!(domain_effect_outcome_facts, 3);
    assert_eq!(selected_replay_outputs, 3);
    assert_eq!(
        eof_writer_seq,
        Some(3),
        "EOF writer_seq for the ReplayOutput feed must not be inflated by \
         other authored fact types"
    );

    let collector_events = read_stage_events(&archive_dir, "collector").await;
    assert!(
        collector_events
            .iter()
            .all(|event| !is_framework_effect_fact(event) && !is_domain_effect_outcome_fact(event)),
        "downstream transport should only receive the selected ReplayOutput feed"
    );
}

#[tokio::test]
async fn drain_waits_for_in_flight_effect_before_outputs_and_eof_complete() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Semaphore::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));

    let run_calls = calls.clone();
    let run_release = release.clone();
    let run_outputs = outputs.clone();
    let run = tokio::spawn(async move {
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(build_blocking_flow(
                journal_base,
                run_calls,
                run_release,
                run_outputs,
            ))
            .await
    });

    tokio::time::timeout(Duration::from_secs(10), async {
        while calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("blocking effect should start");

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !run.is_finished(),
        "flow must not drain while the effect is still pending"
    );
    assert!(
        outputs.lock().expect("outputs lock poisoned").is_empty(),
        "pending effect should not produce output before it commits"
    );

    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("flow should complete after effect release")
        .expect("flow task should not panic")
        .expect("flow should complete successfully");

    assert_eq!(
        outputs.lock().expect("outputs lock poisoned").clone(),
        vec![ReplayOutput {
            value: 1,
            effect_value: 101
        }]
    );
}

#[tokio::test]
async fn flow_level_admission_limiter_replay_suppresses_delay_events_and_effects() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow_level_limiter_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live flow-level limiter flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();

    let archive_dir = latest_run_dir(&journal_base);
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_flow_level_limiter_flow(
            journal_base.clone(),
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay flow-level limiter flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "replay must suppress live effect execution under flow-level middleware"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs
    );

    let replay_archive = latest_run_dir(&journal_base);

    // Directly assert the flow-level admission limiter consumed no tokens and moved no
    // admission state during replay. The burst capacity is 1.0, so an invoked limiter
    // must delay inputs 2 and 3 and emit Delayed events; the live run proves this,
    // making the limiter's complete silence during replay evidence that it was never
    // invoked (zero rate-limiter events of any variant), so no tokens were consumed.
    let live_delayed = rate_limiter_delayed_events_in_stage(&archive_dir, "inputs").await
        + rate_limiter_delayed_events_in_stage(&archive_dir, "effectful").await
        + rate_limiter_delayed_events_in_stage(&archive_dir, "collector").await;
    assert!(
        live_delayed > 0,
        "live run must exercise the limiter (3 inputs at burst capacity 1.0), otherwise \
         the replay-silence assertion below is vacuous"
    );

    let replay_rate_limiter_events = rate_limiter_events_in_stage(&replay_archive, "inputs").await
        + rate_limiter_events_in_stage(&replay_archive, "effectful").await
        + rate_limiter_events_in_stage(&replay_archive, "collector").await;
    assert_eq!(
        replay_rate_limiter_events, 0,
        "strict replay must not invoke the flow-level admission limiter, so it emits no \
         rate-limiter events of any kind and consumes no tokens or admission state"
    );

    // FLOWIP-120a regression. The journal-only assertion above is blind to the
    // admission-middleware replay bug: a downstream sink runs the limiter's
    // `pre_handle` on every replayed event but never journals its `Delayed`
    // lifecycle records, so the limiter can pace the whole replay while the journal
    // stays silent. The runtime counters embedded in wide events do capture it.
    //
    // The limiter embeds tokens-consumed (`rl_events_total`), delayed-event
    // (`rl_delayed_total`), and delay-seconds (`rl_delay_seconds_total`) totals via
    // the instrumentation snapshotter. Strict replay must leave every one at zero on
    // every stage. This assertion fails on the pre-fix code (`saw 3` tokens at the
    // sink) and is deterministic, so it does not depend on wall-clock timing under
    // parallel test load. The live run above proves the limiter is active
    // (`live_delayed > 0`), so the zero-on-replay assertion is not vacuous.
    for stage in ["inputs", "effectful", "collector"] {
        let (events_total, delayed_total, delay_seconds) =
            rate_limiter_runtime_activity_in_stage(&replay_archive, stage).await;
        assert_eq!(
            events_total, 0,
            "stage '{stage}': strict replay must consume zero rate-limit tokens, saw {events_total}"
        );
        assert_eq!(
            delayed_total, 0,
            "stage '{stage}': strict replay must delay zero events, saw {delayed_total}"
        );
        assert_eq!(
            delay_seconds, 0.0,
            "stage '{stage}': strict replay must accrue zero delay seconds, saw {delay_seconds}"
        );
    }
}

#[tokio::test]
async fn effect_boundary_breaker_fallback_replays_recorded_fallback_without_execute() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_breaker_fallback_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live flow should complete");

    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        1,
        "only the first live effect should execute before the breaker opens"
    );
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        live_domain_outputs,
        vec![
            ReplayOutput {
                value: 2,
                effect_value: 902
            },
            ReplayOutput {
                value: 3,
                effect_value: 903
            },
        ],
        "open-breaker fallbacks should be normal recorded domain outputs"
    );

    let archive_dir = latest_run_dir(&journal_base);
    let live_effectful_breaker_events =
        circuit_breaker_events_in_stage(&archive_dir, "effectful").await;
    assert!(
        live_effectful_breaker_events > 0,
        "live effect-boundary circuit-breaker state changes must be returned as wired \
         middleware events in the effectful stage journal"
    );
    let live_mirrored_effectful_breaker_events =
        mirrored_circuit_breaker_events_in_system(&archive_dir, "effectful").await;
    assert!(
        live_mirrored_effectful_breaker_events > 0,
        "live effect-boundary circuit-breaker lifecycle must be mirrored into system.log \
         for the effectful stage"
    );

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_breaker_fallback_flow(
            journal_base.clone(),
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "replay must not execute effects or move breaker state"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs,
        "replay should use recorded fallback outcomes"
    );

    let replay_archive = latest_run_dir(&journal_base);
    let replay_effectful_breaker_events =
        circuit_breaker_events_in_stage(&replay_archive, "effectful").await;
    assert_eq!(
        replay_effectful_breaker_events, 0,
        "strict replay must not execute effect-boundary middleware or emit new \
         effectful-stage circuit-breaker events"
    );
    let replay_mirrored_effectful_breaker_events =
        mirrored_circuit_breaker_events_in_system(&replay_archive, "effectful").await;
    assert_eq!(
        replay_mirrored_effectful_breaker_events, 0,
        "strict replay must not mirror new effect-boundary circuit-breaker lifecycle \
         events for the effectful stage"
    );
}

#[tokio::test]
async fn strict_effectful_stateful_replay_suppresses_effect_execution() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_stateful_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live stateful flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        live_domain_outputs,
        vec![
            ReplayOutput {
                value: 1,
                effect_value: 101
            },
            ReplayOutput {
                value: 2,
                effect_value: 102
            },
            ReplayOutput {
                value: 3,
                effect_value: 103
            },
        ]
    );

    let archive_dir = latest_run_dir(&journal_base);
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_stateful_flow(
            journal_base,
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay stateful flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "stateful replay must use recorded effects"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs
    );
}

#[tokio::test]
async fn resume_incomplete_archive_reexecutes_missing_effect_records_with_archived_cursor() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();

    let archive_dir = latest_run_dir(&journal_base);
    remove_effect_results_for_stage(&archive_dir, "effectful");
    mark_archive_incomplete(&archive_dir);

    let resume_calls = Arc::new(AtomicUsize::new(0));
    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(build_flow(
            journal_base.clone(),
            resume_calls.clone(),
            resume_outputs.clone(),
        ))
        .await
        .expect("resume flow should complete from incomplete archive");

    assert_eq!(
        resume_calls.load(Ordering::SeqCst),
        3,
        "missing committed effect records must execute live during incomplete resume"
    );
    assert_eq!(
        resume_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs
    );

    let resume_archive = latest_run_dir(&journal_base);
    let effectful_journal = archive_manifest(&resume_archive)["stages"]["effectful"]
        ["data_journal_file"]
        .as_str()
        .expect("manifest should contain effectful journal")
        .to_string();
    let resume_journal =
        std::fs::read_to_string(resume_archive.join(effectful_journal)).expect("journal readable");
    let original_flow_id = archive_manifest(&archive_dir)["flow_id"]
        .as_str()
        .expect("manifest should contain original flow id")
        .to_string();
    assert!(
        resume_journal.contains(&original_flow_id),
        "resume should append effect records using the archived recorded_flow_id cursor"
    );
}

/// FLOWIP-120a: incomplete-archive resume must suppress handler-level admission
/// middleware (the replayed inputs carry replay provenance) while STILL protecting
/// effects that execute live at the effect boundary. A blanket
/// `event.replay_context.is_some()` bypass would wrongly disable the live effect's
/// protection; the scoped fix keeps the effect boundary under `LiveEffectBoundary`
/// (never suppressed) and only suppresses the reconstructed handler shell.
///
/// With every effect record removed, all three effects re-execute live during
/// resume, so the effectful stage's effect-boundary limiter must run and consume
/// tokens, while the downstream sink's handler-level admission limiter must stay
/// silent.
#[tokio::test]
async fn resume_incomplete_runs_effect_boundary_limiter_but_suppresses_downstream_admission() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_fast_limiter_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live fast-limiter flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();

    // Drop every committed effect record so each effect must re-execute live.
    let archive_dir = latest_run_dir(&journal_base);
    remove_effect_results_for_stage(&archive_dir, "effectful");
    mark_archive_incomplete(&archive_dir);

    let resume_calls = Arc::new(AtomicUsize::new(0));
    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(build_fast_limiter_flow(
            journal_base.clone(),
            resume_calls.clone(),
            resume_outputs.clone(),
        ))
        .await
        .expect("resume flow should complete from incomplete archive");

    assert_eq!(
        resume_calls.load(Ordering::SeqCst),
        3,
        "every missing effect record must execute live during incomplete resume"
    );
    assert_eq!(
        resume_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs
    );

    let resume_archive = latest_run_dir(&journal_base);

    // The effect boundary is consulted only for effects that execute live, so its
    // limiter must run and consume a token per live effect. This is the invariant a
    // naive replay-provenance bypass would break: protection must NOT be skipped just
    // because the input carries replay provenance.
    let (effectful_tokens, _, _) =
        rate_limiter_runtime_activity_in_stage(&resume_archive, "effectful").await;
    assert!(
        effectful_tokens > 0,
        "resume must keep the effect-boundary limiter live for effects that execute \
         live, but it consumed {effectful_tokens} tokens"
    );

    // The downstream sink reconstructs deterministically from those committed-or-live
    // outcomes; its handler-level admission limiter performs no live external work and
    // must stay fully suppressed.
    let (collector_tokens, collector_delayed, collector_delay_seconds) =
        rate_limiter_runtime_activity_in_stage(&resume_archive, "collector").await;
    assert_eq!(
        collector_tokens, 0,
        "resume must suppress the downstream sink's handler-level admission limiter"
    );
    assert_eq!(
        collector_delayed, 0,
        "resume sink admission limiter must delay nothing"
    );
    assert_eq!(
        collector_delay_seconds, 0.0,
        "resume sink admission limiter must accrue no delay seconds"
    );
}

#[tokio::test]
async fn resume_incomplete_archive_suppresses_committed_effect_records() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();

    let archive_dir = latest_run_dir(&journal_base);
    mark_archive_incomplete(&archive_dir);

    let resume_calls = Arc::new(AtomicUsize::new(0));
    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(build_flow(
            journal_base,
            resume_calls.clone(),
            resume_outputs.clone(),
        ))
        .await
        .expect("resume flow should complete from incomplete archive");

    assert_eq!(
        resume_calls.load(Ordering::SeqCst),
        0,
        "committed effect records must suppress live execution during incomplete resume"
    );
    assert_eq!(
        resume_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs
    );
}

// ============================================================================
// FLOWIP-120g Decision 2: end-to-end effect-port provisioning through `flow!`.
//
// FLOWIP-120a implemented typed and transactional effect ports in the runtime
// but left the authoring-surface supply path unexercised end to end. These tests
// drive a populated `EffectPortRegistry` through the `flow!` `effect_ports:`
// clause into `build_typed_flow!` -> `with_effect_ports`, covering
// materialisation-time `required_ports` validation and live `EffectContext::port`
// access for both an ordinary ported effect and a transactional
// execute-and-record port.
// ============================================================================

/// A host-provided port. The ordinary ported effect resolves it through
/// `EffectContext::port` during live execution.
trait GreetingPort: Send + Sync {
    fn greet(&self, value: u64) -> u64;
}

struct DoublingGreetingPort;

impl GreetingPort for DoublingGreetingPort {
    fn greet(&self, value: u64) -> u64 {
        value * 2
    }
}

/// An idempotent effect that declares a required typed port and uses it.
#[derive(Clone, Debug)]
struct PortedEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for PortedEffect {
    const EFFECT_TYPE: &'static str = "effect_port_supply.ported";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Output = ReplayEffectValue;

    fn label(&self) -> &str {
        "ported"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let port = ctx.port::<dyn GreetingPort>("primary")?;
        Ok(ReplayEffectValue {
            effect_value: port.greet(self.value),
        })
    }

    fn required_ports() -> Vec<EffectPortRequirement> {
        vec![EffectPortRequirement::of::<dyn GreetingPort>("primary")]
    }
}

#[derive(Clone, Debug)]
struct PortedTransform {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for PortedTransform {
    type Input = ReplayInput;

    async fn process(&self, input: ReplayInput, fx: &mut Effects) -> Result<(), HandlerError> {
        let effect_value = fx
            .perform(PortedEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;

        fx.emit(ReplayOutput {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-port-supply-v1"
    }
}

fn build_ported_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
    ports: EffectPortRegistry,
) -> FlowDefinition {
    flow! {
        name: "effect_port_supply",
        journals: disk_journals(journal_base),
        middleware: [],
        effect_ports: ports,

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            ported = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => PortedTransform { calls },
                effects: [PortedEffect],
                middleware: []
            );
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> ported;
            ported |> collector;
        }
    }
}

#[tokio::test]
async fn flow_supplied_registry_satisfies_required_effect_port() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));

    let mut ports = EffectPortRegistry::new();
    ports.insert::<dyn GreetingPort>("primary", Arc::new(DoublingGreetingPort));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_ported_flow(
            journal_base,
            calls.clone(),
            outputs.clone(),
            ports,
        ))
        .await
        .expect("a flow that supplies the required effect port should materialise and run");

    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let domain = outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        domain,
        vec![
            ReplayOutput {
                value: 1,
                effect_value: 2
            },
            ReplayOutput {
                value: 2,
                effect_value: 4
            },
            ReplayOutput {
                value: 3,
                effect_value: 6
            },
        ]
    );
}

#[tokio::test]
async fn flow_without_required_effect_port_fails_closed_at_materialisation() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));

    // No port is registered, so the descriptor's declared `required_ports`
    // cannot be satisfied and materialisation must reject the flow.
    let err = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_ported_flow(
            journal_base,
            calls.clone(),
            outputs.clone(),
            EffectPortRegistry::new(),
        ))
        .await
        .expect_err("a flow missing the required effect port must fail closed at materialisation");

    let message = err.to_string();
    assert!(
        message.contains("requires effect port") && message.contains("primary"),
        "materialisation error should name the missing port, got: {message}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "the effect must not execute when its required port is unregistered"
    );
}

// --- Transactional execute-and-record port supplied through `flow!` ---------

/// A transactional effect whose commit runs through a flow-supplied
/// `TransactionalEffectPort`. The macro `transactional(..)` entry adds the port
/// requirement, and the runtime reads the executor name from the descriptor.
#[derive(Clone, Debug)]
struct LedgerEffect {
    value: u64,
}

#[async_trait]
impl Effect for LedgerEffect {
    const EFFECT_TYPE: &'static str = "effect_port_supply.ledger";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Transactional;

    type Output = ReplayEffectValue;

    fn label(&self) -> &str {
        "ledger"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        // Transactional dispatch runs through the port's execute_and_commit, so
        // this body is not exercised on the transactional path.
        Ok(ReplayEffectValue {
            effect_value: self.value,
        })
    }
}

struct LedgerPort {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl TransactionalEffectPort<LedgerEffect> for LedgerPort {
    async fn execute_and_commit(
        &self,
        effect: LedgerEffect,
        _ctx: &mut EffectContext,
        commit: EffectCommitHandle<ReplayEffectValue>,
    ) -> Result<ReplayEffectValue, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let output = ReplayEffectValue {
            effect_value: effect.value + 1_000,
        };
        commit.commit_success(&output).await?;
        Ok(output)
    }
}

#[derive(Clone, Debug)]
struct LedgerTransform;

#[async_trait]
impl EffectfulTransformHandler for LedgerTransform {
    type Input = ReplayInput;

    async fn process(&self, input: ReplayInput, fx: &mut Effects) -> Result<(), HandlerError> {
        let committed = fx
            .perform(LedgerEffect { value: input.value })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;

        fx.emit(ReplayOutput {
            value: input.value,
            effect_value: committed.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-port-supply-ledger-v1"
    }
}

fn build_transactional_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
    ports: EffectPortRegistry,
) -> FlowDefinition {
    flow! {
        name: "effect_port_supply_transactional",
        journals: disk_journals(journal_base),
        middleware: [],
        effect_ports: ports,

        stages: {
            inputs = source!(ReplayInput => SingleReplaySource::new());
            ledger = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => LedgerTransform,
                effects: [transactional(LedgerEffect, "ledger_tx")],
                middleware: []
            );
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> ledger;
            ledger |> collector;
        }
    }
}

#[tokio::test]
async fn flow_supplied_registry_dispatches_transactional_effect_through_port() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let port_calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));

    let mut ports = EffectPortRegistry::new();
    ports.insert::<dyn TransactionalEffectPort<LedgerEffect>>(
        "ledger_tx",
        Arc::new(LedgerPort {
            calls: port_calls.clone(),
        }),
    );

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_transactional_flow(
            journal_base,
            outputs.clone(),
            ports,
        ))
        .await
        .expect("a transactional flow with a supplied executor port should materialise and run");

    assert_eq!(
        port_calls.load(Ordering::SeqCst),
        1,
        "the transactional executor port should run exactly once"
    );
    let domain = outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        domain,
        vec![ReplayOutput {
            value: 1,
            effect_value: 1_001
        }]
    );
}

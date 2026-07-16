// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115b/115h: a named composition proof for the *built-in* circuit
//! breaker across all three live-I/O surfaces, with effect-bound recovery.
//!
//! A single flow places the built-in breaker on a source poll, on a declared
//! effect, and on a sink delivery, with a fan-out transform in between so one
//! source input fans out to several effect attempts and several sink deliveries
//! (fan-out) that converge on one sink stage (fan-in). Every derived effect
//! cursor fails once with a typed timeout, then succeeds on its retry.
//!
//! The proof:
//!   - the built-in breaker binds and composes on source, effect, and sink in
//!     one flow (it would fail to build otherwise);
//!   - all six logical effect cursors keep independent retry state and produce
//!     one scheduled and one succeeded evidence row apiece;
//!   - fan-in delivers exactly six outputs in deterministic order;
//!   - strict replay of the same archive reproduces the identical domain outputs
//!     without re-executing effects and without emitting a single fresh breaker
//!     or retry row on the source, effect, OR sink stage.

use async_trait::async_trait;
use obzenflow_adapters::middleware::circuit_breaker;
use obzenflow_adapters::middleware::control::circuit_breaker::{CircuitBreaker, OpenPolicy, Retry};
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    event::payloads::observability_payload::{
        CircuitBreakerEvent, CircuitBreakerHealthClassification, MiddlewareLifecycle,
        ObservabilityPayload,
    },
    event::ChainEventContent,
    id::StageId,
    journal::{journal_owner::JournalOwner, Journal},
    TypedPayload, WriterId,
};
use obzenflow_dsl::{effectful_transform, flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectCursor, EffectError, EffectSafety, Effects, IdempotencyKey,
    SinkDeliverySafety,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CompInput {
    value: u64,
}

impl TypedPayload for CompInput {
    const EVENT_TYPE: &'static str = "cb_composition.input";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CompOutput {
    value: u64,
    effect_value: u64,
}

impl TypedPayload for CompOutput {
    const EVENT_TYPE: &'static str = "cb_composition.output";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CompEffectValue {
    effect_value: u64,
}

impl TypedPayload for CompEffectValue {
    const EVENT_TYPE: &'static str = "cb_composition.effect_value";
}

/// Finite source emitting values 1..=3, one event per poll.
#[derive(Clone, Debug)]
struct CompSource {
    next_value: u64,
    writer_id: WriterId,
}

impl CompSource {
    fn new() -> Self {
        Self {
            next_value: 1,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for CompSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next_value > 3 {
            return Ok(None);
        }
        let value = self.next_value;
        self.next_value += 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            CompInput::EVENT_TYPE,
            json!(CompInput { value }),
        )]))
    }
}

/// One input fans out to two derived inputs, so each source event produces two
/// downstream effect attempts and two sink deliveries.
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
        let Some(input) = CompInput::from_event(&event) else {
            return Ok(Vec::new());
        };
        Ok(vec![
            ChainEventFactory::derived_data_event(
                self.writer_id,
                &event,
                CompInput::EVENT_TYPE,
                json!(CompInput {
                    value: input.value * 10 + 1
                }),
                obzenflow_core::config::LineagePolicy::default(),
            ),
            ChainEventFactory::derived_data_event(
                self.writer_id,
                &event,
                CompInput::EVENT_TYPE,
                json!(CompInput {
                    value: input.value * 10 + 2
                }),
                obzenflow_core::config::LineagePolicy::default(),
            ),
        ])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Each derived logical effect fails once with a typed, retryable timeout and
/// then succeeds. Calls are counted by canonical input so the proof verifies
/// that every fan-out sibling owns an independent recovery session.
#[derive(Clone, Debug)]
struct RetryOnceEffect {
    value: u64,
    calls: Arc<Mutex<BTreeMap<u64, usize>>>,
}

#[async_trait]
impl Effect for RetryOnceEffect {
    const EFFECT_TYPE: &'static str = "cb_composition.retry_once";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Outcome = CompEffectValue;

    fn label(&self) -> &str {
        "retry_once"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        let call = {
            let mut calls = self.calls.lock().expect("calls lock poisoned");
            let call = calls.entry(self.value).or_default();
            *call += 1;
            *call
        };
        if call == 1 {
            Err(EffectError::Timeout(format!(
                "simulated timeout for {}",
                self.value
            )))
        } else {
            Ok(CompEffectValue {
                effect_value: self.value + 900,
            })
        }
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("retry-once:{}", self.value)))
    }
}

/// Performs the retry-once effect and emits the recovered outcome.
#[derive(Clone, Debug)]
struct RetryTransform {
    calls: Arc<Mutex<BTreeMap<u64, usize>>>,
}

#[async_trait]
impl EffectfulTransformHandler for RetryTransform {
    type Input = CompInput;
    type Output = obzenflow_core::stage_fact_set![CompOutput, CompEffectValue];
    type AllowedEffects = obzenflow_runtime::effect_set![RetryOnceEffect];

    async fn process(
        &self,
        input: CompInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let effect_value = fx
            .perform(RetryOnceEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Timeout(e.to_string()))?;

        fx.emit(CompOutput {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "cb-composition-retry-v1"
    }
}

/// Original FLOWIP-115b composition fixture: a typed-ineligible failure opens
/// the breaker and exercises its fallback path independently of retry.
#[derive(Clone, Debug)]
struct AlwaysFailingEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for AlwaysFailingEffect {
    const EFFECT_TYPE: &'static str = "cb_composition.always_failing";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Outcome = CompEffectValue;

    fn label(&self) -> &str {
        "always_failing"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(EffectError::Execution(
            "simulated_dependency_down".to_string(),
        ))
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("always-failing:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct FallbackTransform {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for FallbackTransform {
    type Input = CompInput;
    type Output = obzenflow_core::stage_fact_set![CompOutput, CompEffectValue];
    type AllowedEffects = obzenflow_runtime::effect_set![AlwaysFailingEffect];

    async fn process(
        &self,
        input: CompInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let effect_value = fx
            .perform(AlwaysFailingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Timeout(e.to_string()))?;

        fx.emit(CompOutput {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "cb-composition-fallback-v1"
    }
}

#[derive(Clone, Debug)]
struct CollectSink {
    outputs: Arc<Mutex<Vec<CompOutput>>>,
}

#[async_trait]
impl SinkHandler for CollectSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if let Some(output) = CompOutput::from_event(&event) {
            self.outputs
                .lock()
                .expect("outputs lock poisoned")
                .push(output);
        }
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Memory".to_string()),
            None,
        ))
    }

    // In-memory collector: re-delivery under either archive verb is safe.
    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

fn build_retry_flow(
    journal_base: PathBuf,
    calls: Arc<Mutex<BTreeMap<u64, usize>>>,
    outputs: Arc<Mutex<Vec<CompOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "circuit_breaker_composition",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            // Source breaker (stays closed; the scripted source never fails) proves
            // the breaker binds onto the source-poll surface and is replay-safe.
            inputs = source!(CompInput => CompSource::new(), [ circuit_breaker(5) ]);
            // Fan-out: one source event becomes two downstream inputs.
            fan_out = transform!(CompInput -> CompInput => FanOutTransform::new());
            // Every derived cursor times out once and then recovers. Intermediate
            // failures remain private to the recovery session, so the shared
            // breaker settles one terminal success per logical invocation.
            effectful = effectful_transform!(
                CompInput -> { CompOutput, CompEffectValue } => RetryTransform { calls },
                effects: [RetryOnceEffect],
                middleware: [
                    CircuitBreaker::opens_after(1)
                        .retry(Retry::fixed(Duration::ZERO).attempts(2))
                        .build()
                ]
            );
            // Sink breaker (stays closed; deliveries succeed) proves the breaker
            // binds onto the sink-delivery boundary and is replay-safe.
            collector = sink!(CompOutput => CollectSink { outputs }, middleware: [circuit_breaker(5)]);
        },

        topology: {
            inputs |> fan_out;
            fan_out |> effectful;
            effectful |> collector;
        }
    }
}

fn build_fallback_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<CompOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "circuit_breaker_composition",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(CompInput => CompSource::new(), [ circuit_breaker(5) ]);
            fan_out = transform!(CompInput -> CompInput => FanOutTransform::new());
            effectful = effectful_transform!(
                CompInput -> { CompOutput, CompEffectValue } => FallbackTransform { calls },
                effects: [AlwaysFailingEffect],
                middleware: [
                    CircuitBreaker::opens_after(1)
                        .when_open(OpenPolicy::EmitFallback)
                        .transitional_fallback_fact(|input: &CompInput| CompEffectValue {
                            effect_value: input.value + 900,
                        })
                        .build()
                ]
            );
            collector = sink!(CompOutput => CollectSink { outputs }, middleware: [circuit_breaker(5)]);
        },

        topology: {
            inputs |> fan_out;
            fan_out |> effectful;
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
        .expect("a run should have produced an archive")
}

async fn read_stage_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json"))
            .expect("run_manifest.json should be readable"),
    )
    .expect("run_manifest.json should parse");
    let stage_journal = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest should contain data journal for '{stage_key}'"));
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

/// Count circuit-breaker observability rows (any variant) in a stage journal.
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

fn data_event_count(events: &[ChainEvent], event_type: &str) -> usize {
    events
        .iter()
        .filter(|event| {
            matches!(
                &event.content,
                ChainEventContent::Data {
                    event_type: actual,
                    ..
                } if actual == event_type
            )
        })
        .count()
}

fn expected_outputs() -> Vec<CompOutput> {
    [11, 12, 21, 22, 31, 32]
        .into_iter()
        .map(|value| CompOutput {
            value,
            effect_value: value + 900,
        })
        .collect()
}

fn expected_calls() -> BTreeMap<u64, usize> {
    [11, 12, 21, 22, 31, 32]
        .into_iter()
        .map(|value| (value, 2))
        .collect()
}

fn sorted(mut outputs: Vec<CompOutput>) -> Vec<CompOutput> {
    outputs.sort_by_key(|output| (output.value, output.effect_value));
    outputs
}

fn assert_retry_evidence_per_cursor(events: &[ChainEvent]) {
    let mut cursors: HashMap<EffectCursor, (usize, usize)> = HashMap::new();
    let mut retry_rows = 0;

    for event in events {
        let ChainEventContent::Observability(ObservabilityPayload::Middleware(
            MiddlewareLifecycle::CircuitBreaker(retry_event),
        )) = &event.content
        else {
            continue;
        };
        match retry_event {
            CircuitBreakerEvent::RetryScheduled {
                cursor,
                next_attempt,
                delay_ms,
            } => {
                retry_rows += 1;
                assert_eq!(*next_attempt, 2);
                assert_eq!(*delay_ms, 0);
                cursors.entry(cursor.clone()).or_default().0 += 1;
            }
            CircuitBreakerEvent::RetrySucceeded {
                cursor,
                total_attempts,
                terminal_classification,
            } => {
                retry_rows += 1;
                assert_eq!(*total_attempts, 2);
                assert!(matches!(
                    terminal_classification,
                    CircuitBreakerHealthClassification::Success
                ));
                cursors.entry(cursor.clone()).or_default().1 += 1;
            }
            CircuitBreakerEvent::RetryExhausted { .. }
            | CircuitBreakerEvent::RetryStoppedNonRetryable { .. } => {
                panic!("every derived cursor should recover on attempt two")
            }
            _ => {}
        }
    }

    assert_eq!(
        retry_rows, 12,
        "six cursors should emit two retry rows each"
    );
    assert_eq!(cursors.len(), 6, "fan-out should create six effect cursors");
    assert!(
        cursors.values().all(|counts| *counts == (1, 1)),
        "each cursor should have exactly one scheduled and one succeeded row: {cursors:?}"
    );
}

#[tokio::test]
async fn built_in_breaker_composes_source_effect_sink_with_replay_suppression() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![OsString::from("obzenflow")])
        .run_async(build_fallback_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live composition flow should complete");

    let live_domain_outputs = sorted(live_outputs.lock().expect("outputs lock poisoned").clone());
    assert!(
        !live_domain_outputs.is_empty(),
        "the live run should deliver at least one fallback output through the composed breakers"
    );

    let live_run = latest_run_dir(&journal_base);
    let live_effect_breaker_events = circuit_breaker_events_in_stage(&live_run, "effectful").await;
    assert!(
        live_effect_breaker_events > 0,
        "the effect breaker must trip and journal lifecycle rows on the live path"
    );

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_run.as_os_str().to_os_string(),
        ])
        .run_async(build_fallback_flow(
            journal_base.clone(),
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("strict replay should complete");

    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        sorted(
            replay_outputs
                .lock()
                .expect("outputs lock poisoned")
                .clone()
        ),
        live_domain_outputs
    );

    let replay_run = latest_run_dir(&journal_base);
    for stage in ["inputs", "effectful", "collector"] {
        assert_eq!(circuit_breaker_events_in_stage(&replay_run, stage).await, 0);
    }
}

#[tokio::test]
async fn retrying_breaker_composes_real_fan_out_fan_in_with_strict_replay() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    // --- Live run ---------------------------------------------------------
    let live_calls = Arc::new(Mutex::new(BTreeMap::new()));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![OsString::from("obzenflow")])
        .run_async(build_retry_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live composition flow should complete");

    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        live_domain_outputs,
        expected_outputs(),
        "fan-in should deliver all six recovered outputs in deterministic order"
    );
    assert_eq!(
        *live_calls.lock().expect("calls lock poisoned"),
        expected_calls(),
        "every derived logical invocation should make exactly two physical calls"
    );

    let live_run = latest_run_dir(&journal_base);
    assert_eq!(
        data_event_count(
            &read_stage_events(&live_run, "inputs").await,
            CompInput::EVENT_TYPE
        ),
        3,
        "the finite source should journal exactly three inputs"
    );
    assert_eq!(
        data_event_count(
            &read_stage_events(&live_run, "fan_out").await,
            CompInput::EVENT_TYPE,
        ),
        6,
        "fan-out should journal exactly two derived siblings per source input"
    );
    let live_effect_events = read_stage_events(&live_run, "effectful").await;
    assert_retry_evidence_per_cursor(&live_effect_events);

    // --- Strict replay of the same archive --------------------------------
    let replay_calls = Arc::new(Mutex::new(BTreeMap::new()));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_run.as_os_str().to_os_string(),
            OsString::from("--verify"),
        ])
        .run_async(build_retry_flow(
            journal_base.clone(),
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("strict replay should complete");

    // Effects are not re-executed; domain outputs are reproduced from the archive.
    assert!(
        replay_calls.lock().expect("calls lock poisoned").is_empty(),
        "strict replay must not execute effects or move breaker state"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs,
        "strict replay must reproduce the identical output order through the composed breakers"
    );

    // FLOWIP-115b AC48: no fresh breaker row on ANY delivered surface during
    // strict replay. The sink-delivery boundary in particular previously ran on
    // replay; the dispatch-scope gate now suppresses it like source and effect.
    let replay_run = latest_run_dir(&journal_base);
    for stage in ["inputs", "effectful", "collector"] {
        let replay_breaker_events = circuit_breaker_events_in_stage(&replay_run, stage).await;
        assert_eq!(
            replay_breaker_events, 0,
            "strict replay must not emit fresh circuit-breaker rows on the '{stage}' stage"
        );
    }
}

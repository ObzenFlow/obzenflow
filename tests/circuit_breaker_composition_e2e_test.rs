// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115h: strict-replay proof for retry-enabled circuit breakers across
//! all three live-I/O surfaces at once.
//!
//! A single flow places the built-in breaker on a source poll, on a declared
//! effect, and on a sink delivery, with a fan-out transform in between so one
//! source input fans out to several effect attempts and several sink deliveries
//! (fan-out) that converge on one sink stage (fan-in). The effect breaker is
//! tripped so the effectful stage emits real breaker lifecycle rows live.
//!
//! The proof:
//!   - retry-enabled breakers bind and compose on source, effect, and sink in
//!     one flow (it would fail to build otherwise);
//!   - a live run completes and the effect stage journals retry and breaker rows;
//!   - strict replay of the same archive reproduces the identical domain outputs
//!     without polling the source, re-executing effects, or emitting fresh retry
//!     or breaker rows on any surface;
//!   - the archive gate admits an explicit duplicate-delivery override for one
//!     reconstruction consume per archived input, and rejects the same sink with
//!     zero consumes when the override is absent.

use async_trait::async_trait;
use obzenflow_adapters::middleware::control::circuit_breaker::{CircuitBreakerBuilder, OpenPolicy};
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    event::payloads::observability_payload::{MiddlewareLifecycle, ObservabilityPayload},
    event::ChainEventContent,
    id::StageId,
    journal::{journal_owner::JournalOwner, Journal},
    TypedPayload, WriterId,
};
use obzenflow_dsl::{async_source, effectful_transform, flow, sink, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey, SinkDeliverySafety,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, EffectfulTransformHandler, SinkHandler, SourcePollRetryOwnership,
    SourcePollRetrySafety, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
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
    calls: Arc<AtomicUsize>,
    writer_id: WriterId,
}

impl CompSource {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self {
            next_value: 1,
            calls,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for CompSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
    }

    fn poll_retry_safety(&self) -> Option<SourcePollRetrySafety> {
        Some(SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation)
    }

    fn poll_retry_ownership(&self) -> SourcePollRetryOwnership {
        SourcePollRetryOwnership::NoNestedRetry
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
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

/// Effect that always fails, so the effect breaker trips and emits lifecycle
/// rows on the live path.
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
        Err(EffectError::TransientExecution(
            "simulated_dependency_down".to_string(),
        ))
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("always-failing:{}", self.value)))
    }
}

/// Performs the failing effect; on the breaker's emit-fallback path the
/// synthesized outcome resumes the handler, so a fallback output is emitted.
#[derive(Clone, Debug)]
struct FallbackTransform {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for FallbackTransform {
    type Input = CompInput;

    async fn process(&self, input: CompInput, fx: &mut Effects) -> Result<(), HandlerError> {
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
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "cb-composition-fallback-v1"
    }
}

#[derive(Clone, Debug)]
struct CollectSink {
    consumes: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<CompOutput>>>,
}

#[async_trait]
impl SinkHandler for CollectSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        self.consumes.fetch_add(1, Ordering::SeqCst);
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

#[derive(Clone, Copy, Debug)]
enum GateSinkSafety {
    Idempotent,
    NonIdempotent,
}

#[derive(Clone, Debug)]
struct CountingGateSink {
    consumes: Arc<AtomicUsize>,
    safety: GateSinkSafety,
}

#[async_trait]
impl SinkHandler for CountingGateSink {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        self.consumes.fetch_add(1, Ordering::SeqCst);
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Memory".to_string()),
            None,
        ))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(match self.safety {
            GateSinkSafety::Idempotent => SinkDeliverySafety::IdempotentProjection,
            GateSinkSafety::NonIdempotent => SinkDeliverySafety::NonIdempotentExternal,
        })
    }
}

fn build_flow(
    journal_base: PathBuf,
    source_calls: Arc<AtomicUsize>,
    effect_calls: Arc<AtomicUsize>,
    sink_consumes: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<CompOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "circuit_breaker_composition",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            // Source breaker (stays closed; the scripted source never fails) proves
            // the breaker binds onto the source-poll surface and is replay-safe.
            inputs = async_source!(CompInput => CompSource::new(source_calls), [
                CircuitBreakerBuilder::new(5)
                    .with_retry_fixed(Duration::ZERO, 2)
                    .build()
            ]);
            // Fan-out: one source event becomes two downstream inputs.
            fan_out = transform!(CompInput -> CompInput => FanOutTransform::new());
            // Effect breaker, threshold 5 + emit-fallback: the first logical
            // invocations exercise retry, then the breaker opens and later
            // inputs take the fallback path.
            effectful = effectful_transform!(
                CompInput -> { CompOutput, CompEffectValue } => FallbackTransform { calls: effect_calls },
                effects: [AlwaysFailingEffect with [
                    CircuitBreakerBuilder::new(5)
                        .with_retry_fixed(Duration::ZERO, 2)
                        .open_policy(OpenPolicy::EmitFallback)
                        .with_fallback_fact::<CompInput, CompEffectValue, _>(|input| CompEffectValue {
                            effect_value: input.value + 900,
                        })
                        .build()
                ]],
                middleware: []
            );
            // Sink breaker (stays closed; deliveries succeed) proves the breaker
            // binds onto the sink-delivery boundary and is replay-safe.
            collector = sink!(CompOutput => CollectSink { consumes: sink_consumes, outputs }, middleware: [
                CircuitBreakerBuilder::new(5)
                    .with_retry_fixed(Duration::ZERO, 2)
                    .build()
            ]);
        },

        topology: {
            inputs |> fan_out;
            fan_out |> effectful;
            effectful |> collector;
        }
    }
}

fn build_archive_gate_flow(
    journal_base: PathBuf,
    source_calls: Arc<AtomicUsize>,
    sink_consumes: Arc<AtomicUsize>,
    sink_safety: GateSinkSafety,
) -> FlowDefinition {
    flow! {
        name: "circuit_breaker_archive_gate",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = async_source!(CompInput => CompSource::new(source_calls));
            collector = sink!(CompInput => CountingGateSink {
                consumes: sink_consumes,
                safety: sink_safety,
            });
        },

        topology: {
            inputs |> collector;
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

/// Count retry lifecycle rows in a stage journal.
async fn retry_events_in_stage(run_dir: &Path, stage_key: &str) -> usize {
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter(|event| {
            matches!(
                event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::Retry(_)
                ))
            )
        })
        .count()
}

fn sorted(mut outputs: Vec<CompOutput>) -> Vec<CompOutput> {
    outputs.sort_by_key(|o| (o.value, o.effect_value));
    outputs
}

#[tokio::test]
async fn built_in_breaker_composes_source_effect_sink_with_replay_suppression() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    // --- Live run ---------------------------------------------------------
    let live_source_calls = Arc::new(AtomicUsize::new(0));
    let live_effect_calls = Arc::new(AtomicUsize::new(0));
    let live_sink_consumes = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![OsString::from("obzenflow")])
        .run_async(build_flow(
            journal_base.clone(),
            live_source_calls.clone(),
            live_effect_calls.clone(),
            live_sink_consumes.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live composition flow should complete");

    let live_domain_outputs = sorted(live_outputs.lock().expect("outputs lock poisoned").clone());
    assert!(
        !live_domain_outputs.is_empty(),
        "the live run should deliver at least one fallback output through the composed breakers"
    );
    assert_eq!(
        live_source_calls.load(Ordering::SeqCst),
        4,
        "the live source should execute three data polls and one EOF poll"
    );
    assert!(
        live_effect_calls.load(Ordering::SeqCst) > 0,
        "the live effect executor must run so replay suppression is non-vacuous"
    );
    assert!(live_sink_consumes.load(Ordering::SeqCst) > 0);

    let live_run = latest_run_dir(&journal_base);
    let live_effect_breaker_events = circuit_breaker_events_in_stage(&live_run, "effectful").await;
    assert!(
        live_effect_breaker_events > 0,
        "the effect breaker must trip and journal lifecycle rows on the live path, \
         so the replay-suppression assertion below is non-vacuous"
    );
    assert!(
        retry_events_in_stage(&live_run, "effectful").await > 0,
        "retry must be enabled and emit durable lifecycle evidence on the live effect surface"
    );

    // --- Strict replay of the same archive --------------------------------
    let replay_source_calls = Arc::new(AtomicUsize::new(0));
    let replay_effect_calls = Arc::new(AtomicUsize::new(0));
    let replay_sink_consumes = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_run.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base.clone(),
            replay_source_calls.clone(),
            replay_effect_calls.clone(),
            replay_sink_consumes.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("strict replay should complete");

    // Source and effect executors are not re-entered; sink reconstruction still
    // consumes each archive-admitted input exactly once outside its live retry onion.
    assert_eq!(
        replay_source_calls.load(Ordering::SeqCst),
        0,
        "strict replay must not poll the live source executor"
    );
    assert_eq!(
        replay_effect_calls.load(Ordering::SeqCst),
        0,
        "strict replay must not execute effects or move breaker state"
    );
    assert_eq!(
        replay_sink_consumes.load(Ordering::SeqCst),
        live_sink_consumes.load(Ordering::SeqCst),
        "the archive-admitted sink must consume every recorded sink input exactly once"
    );
    assert_eq!(
        sorted(
            replay_outputs
                .lock()
                .expect("outputs lock poisoned")
                .clone()
        ),
        live_domain_outputs,
        "strict replay must reproduce the identical domain outputs through the composed breakers"
    );

    // No live breaker or retry policy onion runs on any surface. The sink
    // executor remains the one reconstruction exception above.
    let replay_run = latest_run_dir(&journal_base);
    for stage in ["inputs", "effectful", "collector"] {
        let replay_breaker_events = circuit_breaker_events_in_stage(&replay_run, stage).await;
        assert_eq!(
            replay_breaker_events, 0,
            "strict replay must not emit fresh circuit-breaker rows on the '{stage}' stage"
        );
        assert_eq!(
            retry_events_in_stage(&replay_run, stage).await,
            0,
            "strict replay must not emit fresh retry lifecycle rows on the '{stage}' stage"
        );
    }

    // Exercise the archive gate itself with the same production application
    // path. Retry-enabled sink middleware cannot honestly be attached to a
    // non-idempotent sink (the binder rejects that unsafe composition), so the
    // override and rejection cases use a minimal source-to-sink archive while
    // the three-surface flow above proves retry-boundary bypass.
    let gate_temp = tempfile::tempdir().expect("archive gate tempdir");
    let gate_journal_base = gate_temp.path().join("journals");
    let gate_live_source_calls = Arc::new(AtomicUsize::new(0));
    let gate_live_sink_consumes = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(vec![OsString::from("obzenflow")])
        .run_async(build_archive_gate_flow(
            gate_journal_base.clone(),
            gate_live_source_calls.clone(),
            gate_live_sink_consumes.clone(),
            GateSinkSafety::Idempotent,
        ))
        .await
        .expect("the idempotent live flow should produce an archive");
    assert_eq!(gate_live_source_calls.load(Ordering::SeqCst), 4);
    assert_eq!(
        gate_live_sink_consumes.load(Ordering::SeqCst),
        4,
        "the live sink should receive three data inputs and one terminal EOF input"
    );
    let gate_live_run = latest_run_dir(&gate_journal_base);

    let override_source_calls = Arc::new(AtomicUsize::new(0));
    let override_sink_consumes = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            gate_live_run.as_os_str().to_os_string(),
            OsString::from("--allow-duplicate-sink-delivery"),
        ])
        .run_async(build_archive_gate_flow(
            gate_journal_base.clone(),
            override_source_calls.clone(),
            override_sink_consumes.clone(),
            GateSinkSafety::NonIdempotent,
        ))
        .await
        .expect("the explicit duplicate-delivery override should admit replay");
    assert_eq!(
        override_source_calls.load(Ordering::SeqCst),
        0,
        "override-admitted replay must still bypass the live source executor"
    );
    assert_eq!(
        override_sink_consumes.load(Ordering::SeqCst),
        gate_live_sink_consumes.load(Ordering::SeqCst),
        "an override-admitted sink must consume every archived sink input exactly once"
    );

    let rejected_source_calls = Arc::new(AtomicUsize::new(0));
    let rejected_sink_consumes = Arc::new(AtomicUsize::new(0));
    let rejection = FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            gate_live_run.as_os_str().to_os_string(),
        ])
        .run_async(build_archive_gate_flow(
            gate_journal_base,
            rejected_source_calls.clone(),
            rejected_sink_consumes.clone(),
            GateSinkSafety::NonIdempotent,
        ))
        .await
        .expect_err("a non-idempotent sink without the override must be rejected");
    assert!(
        rejection.to_string().contains("--replay-from refused"),
        "the failure must come from the archive sink gate: {rejection}"
    );
    assert_eq!(rejected_source_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        rejected_sink_consumes.load(Ordering::SeqCst),
        0,
        "a gate-rejected sink must not consume any archived input"
    );
}

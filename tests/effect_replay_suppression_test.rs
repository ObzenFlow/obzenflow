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
        MiddlewareLifecycle, ObservabilityPayload, RateLimiterEvent,
    },
    event::ChainEventContent,
    id::StageId,
    journal::{journal_owner::JournalOwner, Journal},
    TypedPayload, WriterId,
};
use obzenflow_dsl::{
    effectful_transform, effectful_sink, effectful_stateful, flow, sink, source, transform,
    FlowDefinition,
};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulSinkHandler, EffectfulTransformHandler, EffectfulStatefulHandler,
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

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

    type Output = ReplayEffectValue;

    fn label(&self) -> Cow<'static, str> {
        Cow::Borrowed("counting")
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

    fn safety(&self) -> EffectSafety {
        EffectSafety::NonIdempotentRequiresKey
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

    type Output = ReplayEffectValue;

    fn label(&self) -> Cow<'static, str> {
        Cow::Borrowed("blocking")
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

    fn safety(&self) -> EffectSafety {
        EffectSafety::NonIdempotentRequiresKey
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
    type Output = ReplayOutput;

    async fn process(
        &self,
        input: ReplayInput,
        fx: &mut Effects,
    ) -> Result<ReplayOutput, HandlerError> {
        let effect_value = fx
            .perform(CountingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;

        Ok(ReplayOutput {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
    }

    fn stage_logic_version(&self) -> Cow<'static, str> {
        Cow::Borrowed("effect-replay-v1")
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
    type Output = ReplayOutput;

    async fn process(
        &self,
        input: ReplayInput,
        fx: &mut Effects,
    ) -> Result<ReplayOutput, HandlerError> {
        let effect_value = fx
            .perform(BlockingEffect {
                value: input.value,
                calls: self.calls.clone(),
                release: self.release.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;

        Ok(ReplayOutput {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
    }

    fn stage_logic_version(&self) -> Cow<'static, str> {
        Cow::Borrowed("effect-replay-blocking-v1")
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

    type Output = ReplayEffectValue;

    fn label(&self) -> Cow<'static, str> {
        Cow::Borrowed("always_failing")
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(EffectError::Execution("simulated_gateway_down".to_string()))
    }

    fn safety(&self) -> EffectSafety {
        EffectSafety::NonIdempotentRequiresKey
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

#[async_trait]
impl EffectfulStatefulHandler for ReplayStateful {
    type State = ReplayStatefulState;
    type Input = ReplayInput;
    type Output = ReplayOutput;
    type Transition = ReplayEffectValue;

    fn initial_state(&self) -> Self::State {
        ReplayStatefulState::default()
    }

    async fn transition(
        &mut self,
        _state: &Self::State,
        input: &ReplayInput,
        fx: &mut Effects,
    ) -> Result<Self::Transition, HandlerError> {
        fx.perform(CountingEffect {
            value: input.value,
            calls: self.calls.clone(),
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))
    }

    fn apply(
        &mut self,
        state: &mut Self::State,
        input: ReplayInput,
        transition: Self::Transition,
    ) -> Result<(), HandlerError> {
        state.outputs.push(ReplayOutput {
            value: input.value,
            effect_value: transition.effect_value,
        });
        Ok(())
    }

    fn create_outputs(&self, state: &Self::State) -> Result<Vec<ReplayOutput>, HandlerError> {
        Ok(state.outputs.clone())
    }

    fn stage_logic_version(&self) -> Cow<'static, str> {
        Cow::Borrowed("effect-replay-stateful-v1")
    }
}

#[async_trait]
impl EffectfulTransformHandler for FallbackTransform {
    type Input = ReplayInput;
    type Output = ReplayOutput;

    async fn process(
        &self,
        input: ReplayInput,
        fx: &mut Effects,
    ) -> Result<ReplayOutput, HandlerError> {
        let effect_value = fx
            .perform(AlwaysFailingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Timeout(e.to_string()))?;

        Ok(ReplayOutput {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
    }

    fn stage_logic_version(&self) -> Cow<'static, str> {
        Cow::Borrowed("effect-replay-fallback-v1")
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

    fn stage_logic_version(&self) -> Cow<'static, str> {
        Cow::Borrowed("effect-replay-sink-v1")
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

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(ReplayInput -> ReplayOutput => ReplayTransform { calls });
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
            effectful = effectful_transform!(ReplayInput -> ReplayOutput => ReplayTransform { calls });
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
            effectful = effectful_transform!(ReplayInput -> ReplayOutput => BlockingTransform { calls, release });
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
            effectful = effectful_transform!(ReplayInput -> ReplayOutput => ReplayTransform { calls });
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
            collector = effectful_sink!(ReplayInput => EffectfulCollectSink { calls, receipts });
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
            effectful = effectful_transform!(ReplayInput -> ReplayOutput => FallbackTransform { calls }, [
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
            effectful = effectful_stateful!(ReplayInput -> ReplayOutput => ReplayStateful { calls });
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
    let retained = original
        .lines()
        .filter(|line| !line.contains("\"effect_result\""))
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

#[tokio::test]
async fn effectful_sink_replay_suppresses_effect_execution() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_receipts = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_effectful_sink_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_receipts.clone(),
        ))
        .await
        .expect("live sink flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    let live_domain_receipts = live_receipts
        .lock()
        .expect("receipts lock poisoned")
        .clone();
    assert_eq!(
        live_domain_receipts,
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
    let replay_receipts = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_effectful_sink_flow(
            journal_base,
            replay_calls.clone(),
            replay_receipts.clone(),
        ))
        .await
        .expect("replay sink flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "sink replay must return recorded effect results without executing"
    );
    assert_eq!(
        replay_receipts
            .lock()
            .expect("receipts lock poisoned")
            .clone(),
        live_domain_receipts,
        "sink replay should reconstruct equivalent consume receipts"
    );
}

#[tokio::test]
async fn effectful_transform_replay_suppresses_effect_execution() {
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
    let effect_records: Vec<_> = effectful_events
        .iter()
        .filter_map(|event| match &event.content {
            ChainEventContent::EffectResult(record) => Some(record),
            _ => None,
        })
        .collect();
    assert_eq!(effect_records.len(), 2);
    assert_eq!(effect_records[0].cursor.input_seq, 1);
    assert_eq!(effect_records[0].cursor.effect_ordinal, 0);
    assert_eq!(effect_records[1].cursor.input_seq, 2);
    assert_eq!(effect_records[1].cursor.effect_ordinal, 0);
    assert_ne!(effect_records[0].cursor, effect_records[1].cursor);

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
    let effect_results = effectful_events
        .iter()
        .filter(|event| matches!(event.content, ChainEventContent::EffectResult(_)))
        .count();
    let data_outputs = effectful_events
        .iter()
        .filter(|event| matches!(event.content, ChainEventContent::Data { .. }))
        .count();
    let eof_writer_seq = effectful_events
        .iter()
        .find_map(|event| match &event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof { writer_seq, .. }) => {
                writer_seq.map(|seq| seq.0)
            }
            _ => None,
        });

    assert_eq!(effect_results, 3);
    assert_eq!(data_outputs, 3);
    assert_eq!(
        eof_writer_seq,
        Some(3),
        "EffectResult records must not inflate EOF writer_seq"
    );

    let collector_events = read_stage_events(&archive_dir, "collector").await;
    assert!(
        collector_events
            .iter()
            .all(|event| !matches!(event.content, ChainEventContent::EffectResult(_))),
        "downstream transport should never receive private EffectResult records"
    );
}

#[tokio::test]
async fn drain_waits_for_in_flight_effect_before_outputs_and_eof_complete() {
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
}

#[tokio::test]
async fn effect_boundary_breaker_fallback_replays_recorded_fallback_without_execute() {
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
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_breaker_fallback_flow(
            journal_base,
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
}

#[tokio::test]
async fn strict_effectful_stateful_replay_suppresses_effect_execution() {
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

#[tokio::test]
async fn resume_incomplete_archive_suppresses_committed_effect_records() {
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

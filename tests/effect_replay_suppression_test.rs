// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_adapters::effects::{CircuitBreakerOutcome, GuardedEffectExt};
use obzenflow_adapters::middleware::control::circuit_breaker::{CircuitBreaker, OpenPolicy, Retry};
use obzenflow_adapters::middleware::RateLimiterBuilder;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    event::payloads::flow_control_payload::FlowControlPayload,
    event::payloads::observability_payload::{
        CircuitBreakerEvent, MiddlewareLifecycle, ObservabilityPayload,
    },
    event::{ChainEventContent, SystemEvent, SystemEventType},
    id::{StageId, SystemId},
    journal::{journal_owner::JournalOwner, Journal},
    TypedPayload, WriterId,
};
use obzenflow_dsl::{
    effectful_stateful, effectful_transform, flow, sink, source, transform, FlowDefinition,
};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    is_framework_effect_event_type, Effect, EffectCommitHandle, EffectContext, EffectCursor,
    EffectError, EffectPortRegistry, EffectPortRequirement, EffectRecord, EffectSafety, Effects,
    IdempotencyKey, SinkDeliverySafety, TransactionalEffectPort, EFFECT_RECORD_EVENT_TYPE,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulStatefulHandler, EffectfulTransformHandler, FiniteSourceHandler, SinkHandler,
    TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
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
                obzenflow_core::config::LineagePolicy::default(),
            ),
            ChainEventFactory::derived_data_event(
                self.writer_id,
                &event,
                ReplayInput::EVENT_TYPE,
                json!(ReplayInput {
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

    type Outcome = ReplayEffectValue;

    fn label(&self) -> &str {
        "counting"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
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
    cancelled_futures: Arc<AtomicUsize>,
    invocations: Arc<Mutex<Vec<BlockingInvocation>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct BlockingInvocation {
    runtime_flow_id: String,
    stage_key: String,
    input_seq: u64,
    idempotency_key: String,
}

struct PendingEffectGuard {
    cancelled_futures: Arc<AtomicUsize>,
    completed: bool,
}

impl Drop for PendingEffectGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.cancelled_futures.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[async_trait]
impl Effect for BlockingEffect {
    const EFFECT_TYPE: &'static str = "effect_replay.blocking";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Outcome = ReplayEffectValue;

    fn label(&self) -> &str {
        "blocking"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.invocations
            .lock()
            .expect("blocking invocation lock poisoned")
            .push(BlockingInvocation {
                runtime_flow_id: ctx.flow_id().to_string(),
                stage_key: ctx.stage_key().to_string(),
                input_seq: ctx.now(),
                idempotency_key: format!("blocking:{}", self.value),
            });
        let mut pending = PendingEffectGuard {
            cancelled_futures: self.cancelled_futures.clone(),
            completed: false,
        };
        let permit = self.release.acquire().await;
        pending.completed = true;
        let permit =
            permit.map_err(|_| EffectError::Execution("blocking gate closed".to_string()))?;
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
    cancelled_futures: Arc<AtomicUsize>,
    invocations: Arc<Mutex<Vec<BlockingInvocation>>>,
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
                cancelled_futures: self.cancelled_futures.clone(),
                invocations: self.invocations.clone(),
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

    type Outcome = ReplayEffectValue;

    fn label(&self) -> &str {
        "always_failing"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
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

/// Stateful `Fact` enum derived per FLOWIP-120m: the same sum shape an
/// effect outcome carrier uses also serves the per-fact Mealy fold, decoding
/// each committed fact into its matching variant.
#[derive(Clone, Debug, obzenflow_core::EffectOutcomeFacts)]
enum ReplayStatefulFact {
    EffectValue(ReplayEffectValue),
    Output(ReplayOutput),
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

// ---------------------------------------------------------------------------
// FLOWIP-120m: product carriers fold per fact on the stateful surface
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProductFirst {
    value: u64,
}

impl TypedPayload for ProductFirst {
    const EVENT_TYPE: &'static str = "effect_replay.product_first";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProductSecond {
    value: u64,
}

impl TypedPayload for ProductSecond {
    const EVENT_TYPE: &'static str = "effect_replay.product_second";
}

/// Product outcome: both facts are recorded together under one effect
/// cursor, in field order.
#[derive(Clone, Debug, PartialEq, Eq, obzenflow_core::EffectOutcomeFacts)]
struct ProductOutcome {
    first: ProductFirst,
    second: ProductSecond,
}

#[derive(Clone, Debug)]
struct ProductEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for ProductEffect {
    const EFFECT_TYPE: &'static str = "effect_replay.product";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;

    type Outcome = ProductOutcome;

    fn label(&self) -> &str {
        "product"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ProductOutcome {
            first: ProductFirst {
                value: self.value + 100,
            },
            second: ProductSecond {
                value: self.value + 200,
            },
        })
    }
}

/// The stage's per-fact `Fact` enum: the carrier never reaches `apply`;
/// each committed member fact folds individually, in ordinal order.
#[derive(Clone, Debug, obzenflow_core::EffectOutcomeFacts)]
enum ProductStatefulFact {
    First(ProductFirst),
    Second(ProductSecond),
    Output(ReplayOutput),
}

#[derive(Clone, Debug, Default)]
struct ProductStatefulState;

#[derive(Clone, Debug)]
struct ProductStateful {
    calls: Arc<AtomicUsize>,
    applied: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EffectfulStatefulHandler for ProductStateful {
    type State = ProductStatefulState;
    type Input = ReplayInput;
    type Fact = ProductStatefulFact;

    fn initial_state(&self) -> Self::State {
        ProductStatefulState
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &ReplayInput,
        fx: &mut Effects,
    ) -> Result<(), HandlerError> {
        let outcome = fx
            .perform(ProductEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        fx.emit(ReplayOutput {
            value: input.value,
            effect_value: outcome.first.value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn apply(&mut self, _state: &mut Self::State, fact: Self::Fact) -> Result<(), HandlerError> {
        let entry = match &fact {
            ProductStatefulFact::First(first) => format!("first:{}", first.value),
            ProductStatefulFact::Second(second) => format!("second:{}", second.value),
            ProductStatefulFact::Output(output) => format!("output:{}", output.value),
        };
        self.applied
            .lock()
            .expect("applied lock poisoned")
            .push(entry);
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-product-stateful-v1"
    }
}

// ---------------------------------------------------------------------------
// FLOWIP-120m: outcome-shaped circuit-breaker fallback
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct FailingProductEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for FailingProductEffect {
    const EFFECT_TYPE: &'static str = "effect_replay.failing_product";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Outcome = ProductOutcome;

    fn label(&self) -> &str {
        "failing_product"
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
        Some(IdempotencyKey(format!("failing-product:{}", self.value)))
    }
}

/// Outcome-shaped mode: the handler performs the plain effect, no `Guarded`
/// wrapper, and every branch resumes with the effect's own carrier.
#[derive(Clone, Debug)]
struct OutcomeFallbackTransform {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for OutcomeFallbackTransform {
    type Input = ReplayInput;

    async fn process(&self, input: ReplayInput, fx: &mut Effects) -> Result<(), HandlerError> {
        let outcome = fx
            .perform(FailingProductEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await;

        let output = match outcome {
            Ok(product) => ReplayOutput {
                value: input.value,
                effect_value: product.first.value,
            },
            Err(_) => ReplayOutput {
                value: input.value,
                effect_value: GUARDED_FAILED_MARKER,
            },
        };

        fx.emit(output)
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-outcome-fallback-v1"
    }
}

/// Sum carrier for the outcome-shaped fallback round trip.
#[derive(Clone, Debug, obzenflow_core::EffectOutcomeFacts)]
enum EitherOutcome {
    First(ProductFirst),
    Second(ProductSecond),
}

#[derive(Clone, Debug)]
struct FailingEitherEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for FailingEitherEffect {
    const EFFECT_TYPE: &'static str = "effect_replay.failing_either";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Outcome = EitherOutcome;

    fn label(&self) -> &str {
        "failing_either"
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
        Some(IdempotencyKey(format!("failing-either:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct EitherFallbackTransform {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for EitherFallbackTransform {
    type Input = ReplayInput;

    async fn process(&self, input: ReplayInput, fx: &mut Effects) -> Result<(), HandlerError> {
        let outcome = fx
            .perform(FailingEitherEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await;

        let output = match outcome {
            Ok(EitherOutcome::First(first)) => ReplayOutput {
                value: input.value,
                effect_value: first.value,
            },
            Ok(EitherOutcome::Second(second)) => ReplayOutput {
                value: input.value,
                effect_value: second.value,
            },
            Err(_) => ReplayOutput {
                value: input.value,
                effect_value: GUARDED_FAILED_MARKER,
            },
        };

        fx.emit(output)
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-either-fallback-v1"
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
            DeliveryMethod::Custom("Memory".to_string()),
            None,
        ))
    }

    // In-memory collector: re-delivery under either archive verb is safe.
    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
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

fn build_source_limiter_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_source_limiter",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new(), [
                RateLimiterBuilder::new(1.0).build()
            ]);
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

/// Same shape as `build_flow` but with a high effect-boundary admission rate, so
/// the limiter consumes tokens and moves admission state without blocking a worker
/// thread for seconds. The resume regression only needs token consumption at the
/// effect boundary, not real backpressure delay, so this keeps that test fast and
/// avoids starving the shared runtime under parallel test execution.
fn build_fast_limiter_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_fast_limiter",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => ReplayTransform { calls },
                effects: [CountingEffect with [
                    RateLimiterBuilder::new(1000.0).build()
                ]],
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
    cancelled_futures: Arc<AtomicUsize>,
    invocations: Arc<Mutex<Vec<BlockingInvocation>>>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_blocking",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => SingleReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => BlockingTransform {
                    calls,
                    release,
                    cancelled_futures,
                    invocations
                },
                effects: [BlockingEffect with [
                    CircuitBreaker::opens_after(1)
                        .retry(Retry::fixed(Duration::ZERO).attempts(2))
                        .build()
                ]],
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
                CircuitBreaker::opens_after(1)
                    .when_open(OpenPolicy::EmitFallback)
                    .transitional_fallback_fact(|input: &ReplayInput| ReplayEffectValue {
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

fn build_skip_policy_breaker_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_skip_policy_rejected",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => FallbackTransform { calls },
                effects: [AlwaysFailingEffect],
                middleware: [
                CircuitBreaker::opens_after(1)
                    .when_open(OpenPolicy::Skip)
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

#[tokio::test]
async fn open_policy_skip_is_rejected_on_effectful_stages() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));

    let err = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_skip_policy_breaker_flow(journal_base, calls, outputs))
        .await
        .expect_err("OpenPolicy::Skip on an effectful stage must fail the build");

    let message = err.to_string();
    assert!(
        message.contains("silently drops events at the effect boundary"),
        "expected the FLOWIP-120h truncation rejection, got: {message}"
    );
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

fn build_product_stateful_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    applied: Arc<Mutex<Vec<String>>>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_product_stateful",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_stateful!(
                ReplayInput -> { ReplayOutput, ProductFirst, ProductSecond } => ProductStateful { calls, applied },
                effects: [ProductEffect],
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

fn build_outcome_fallback_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_outcome_fallback",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ProductFirst, ProductSecond } => OutcomeFallbackTransform { calls },
                effects: [FailingProductEffect with [
                CircuitBreaker::opens_after(1)
                    .when_open(OpenPolicy::EmitFallback)
                    .outcome_fallback::<FailingProductEffect, _, _>(|input: &ReplayInput| ProductOutcome {
                        first: ProductFirst {
                            value: input.value + 900,
                        },
                        second: ProductSecond {
                            value: input.value + 1900,
                        },
                    })
                    .build()
            ]],
                middleware: []);
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

fn build_either_fallback_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_either_fallback",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ProductFirst, ProductSecond } => EitherFallbackTransform { calls },
                effects: [FailingEitherEffect with [
                CircuitBreaker::opens_after(1)
                    .when_open(OpenPolicy::EmitFallback)
                    .outcome_fallback::<FailingEitherEffect, _, _>(|input: &ReplayInput| {
                        EitherOutcome::Second(ProductSecond {
                            value: input.value + 900,
                        })
                    })
                    .build()
            ]],
                middleware: []);
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

// Mixing branch-shaped and outcome-shaped producers on one breaker is
// unrepresentable under the typestate builder (see
// breaker_builder_compile_fail_tests); the `MixedFallbackShapesOnStage`
// validator stays as defence for future non-breaker type-shaping middleware.

fn build_undeclared_outcome_target_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_undeclared_outcome_target",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => FallbackTransform { calls },
                effects: [AlwaysFailingEffect with [
                CircuitBreaker::opens_after(1)
                    .when_open(OpenPolicy::EmitFallback)
                    .outcome_fallback::<CountingEffect, _, _>(|input: &ReplayInput| ReplayEffectValue {
                        effect_value: input.value + 900,
                    })
                    .build()
            ]],
                middleware: []);
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

/// Project the product-outcome rows of a stage journal (FLOWIP-120m): per
/// row, the outcome group id, the fact ordinal, whether the origin is
/// middleware-synthesized, and the event type, in causal order.
async fn product_outcome_rows(run_dir: &Path, stage_key: &str) -> Vec<(String, u32, bool, String)> {
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
        .iter()
        .filter_map(|envelope| {
            let event_type = envelope.event.event_type().to_string();
            if !event_type.starts_with("effect_replay.product") {
                return None;
            }
            let provenance = envelope
                .event
                .effect_provenance
                .as_ref()
                .expect("product outcome rows must carry effect provenance");
            Some((
                format!(
                    "{:?}",
                    provenance
                        .group_id
                        .as_ref()
                        .expect("outcome rows must carry a group id")
                ),
                provenance
                    .outcome_fact_ordinal
                    .expect("outcome rows must carry an ordinal")
                    .get(),
                matches!(
                    provenance.origin,
                    Some(
                        obzenflow_runtime::effects::EffectFactOrigin::MiddlewareSynthesized { .. }
                    )
                ),
                event_type,
            ))
        })
        .collect()
}

/// Project a stage journal's effect-provenance origins (FLOWIP-120m):
/// one `(event_type, origin)` pair per data row carrying provenance, in
/// causal order. Reads through the run manifest, the same convention as the
/// shared replay testkit, locally so this suite stays free of the
/// `test-support` feature gate.
async fn effect_fact_origins(
    run_dir: &Path,
    stage_key: &str,
) -> Vec<(String, Option<obzenflow_runtime::effects::EffectFactOrigin>)> {
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
        .iter()
        .filter_map(|envelope| {
            envelope.event.effect_provenance.as_ref().map(|provenance| {
                (
                    envelope.event.event_type().to_string(),
                    provenance.origin.clone(),
                )
            })
        })
        .collect()
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
                .is_some_and(|provenance| provenance.fact_owner.is_framework())
                && is_framework_effect_event_type(event_type)
    )
}

fn is_domain_effect_outcome_fact(event: &ChainEvent) -> bool {
    matches!(
        &event.content,
        ChainEventContent::Data { event_type, .. }
            if event_type == ReplayEffectValue::versioned_event_type().as_str()
                && event
                    .effect_provenance
                    .as_ref()
                    .is_some_and(|provenance| provenance.fact_owner.is_user())
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
            .is_some_and(|provenance| provenance.fact_owner.is_framework())
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
            .filter(|provenance| provenance.fact_owner.is_user())
            .map(|provenance| provenance.cursor.clone());
    }
    None
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

async fn circuit_breaker_retry_events_in_stage(run_dir: &Path, stage_key: &str) -> usize {
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter(|event| {
            matches!(
                event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::CircuitBreaker(
                        CircuitBreakerEvent::RetryScheduled { .. }
                            | CircuitBreakerEvent::RetrySucceeded { .. }
                            | CircuitBreakerEvent::RetryExhausted { .. }
                            | CircuitBreakerEvent::RetryStoppedNonRetryable { .. }
                    )
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

/// Count rate-limiter observability events of any variant in a stage journal.
/// Source admission gate observability is deferred under FLOWIP-115a, so runtime
/// counters are the authoritative replay-silence assertion for source policies.
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

/// Per-effect limiter activity from the wide-event snapshot (FLOWIP-120c G9):
/// per-effect policy instances snapshot under their effect type, so boundary
/// limiter activity lives in `effect_rate_limiters`, not the stage-level
/// `rl_*` fields.
async fn effect_rate_limiter_runtime_activity_in_stage(
    run_dir: &Path,
    stage_key: &str,
) -> (u64, u64, f64) {
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter_map(|event| event.runtime_context)
        .fold((0u64, 0u64, 0f64), |(ev, dl, ds), rc| {
            let (sum_ev, sum_dl, sum_ds) =
                rc.effect_rate_limiters
                    .iter()
                    .fold((0u64, 0u64, 0f64), |(ev, dl, ds), entry| {
                        (
                            ev + entry.rl_events_total,
                            dl + entry.rl_delayed_total,
                            ds + entry.rl_delay_seconds_total,
                        )
                    });
            (ev.max(sum_ev), dl.max(sum_dl), ds.max(sum_ds))
        })
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
                    if event_type == ReplayOutput::versioned_event_type().as_str()
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
    let cancelled_futures = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(Mutex::new(Vec::new()));
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
                cancelled_futures,
                invocations,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn graceful_timeout_aborts_pending_recovery_and_resume_reuses_effect_identity() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let cancelled_calls = Arc::new(AtomicUsize::new(0));
    let cancelled_release = Arc::new(Semaphore::new(0));
    let cancelled_futures = Arc::new(AtomicUsize::new(0));
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let cancelled_outputs = Arc::new(Mutex::new(Vec::new()));
    let flow_handle = build_blocking_flow(
        journal_base.clone(),
        cancelled_calls.clone(),
        cancelled_release,
        cancelled_futures.clone(),
        invocations.clone(),
        cancelled_outputs.clone(),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .expect("blocking flow should build");

    tokio::time::timeout(Duration::from_secs(10), async {
        while cancelled_calls.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("blocking effect should start");

    let graceful_bound = Duration::from_millis(100);
    let stop_started = tokio::time::Instant::now();
    flow_handle
        .stop_graceful(graceful_bound)
        .await
        .expect("graceful stop request should be accepted");

    tokio::time::timeout(graceful_bound + Duration::from_millis(900), async {
        while cancelled_futures.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the pending effect must be aborted at the original graceful deadline");
    let stop_elapsed = stop_started.elapsed();
    assert!(
        stop_elapsed <= graceful_bound + Duration::from_millis(900),
        "the effect future should be dropped promptly at the original graceful deadline; elapsed {stop_elapsed:?}"
    );
    // Metrics teardown has its own pre-existing bounded drain. The stage future
    // above is the deadline authority under test;
    // wait for the rest of the pipeline only after proving its prompt abort.
    tokio::time::timeout(Duration::from_secs(5), flow_handle.wait_for_completion())
        .await
        .expect("cancelled flow should finish its bounded ancillary cleanup")
        .expect("cancelled flow should resolve");
    assert_eq!(cancelled_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        cancelled_futures.load(Ordering::SeqCst),
        1,
        "timeout escalation must drop the pending physical effect future"
    );
    assert!(
        cancelled_outputs
            .lock()
            .expect("outputs lock poisoned")
            .is_empty(),
        "a cancelled invocation must not emit an output"
    );

    let cancelled_archive = latest_run_dir(&journal_base);
    let original_flow_id = archive_manifest(&cancelled_archive)["flow_id"]
        .as_str()
        .expect("cancelled manifest should contain its flow id")
        .to_string();
    let cancelled_stage_events = read_stage_events(&cancelled_archive, "effectful").await;
    assert_eq!(
        cancelled_stage_events
            .iter()
            .filter(|event| {
                framework_effect_record(event).is_some() || is_domain_effect_outcome_fact(event)
            })
            .count(),
        0,
        "the aborted invocation must commit no terminal effect record or outcome fact"
    );
    assert_eq!(
        circuit_breaker_retry_events_in_stage(&cancelled_archive, "effectful").await,
        0,
        "the aborted recovery session must commit no retry evidence"
    );

    let cancelled_invocation = invocations
        .lock()
        .expect("blocking invocation lock poisoned")
        .first()
        .cloned()
        .expect("cancelled physical call should have recorded its identity");
    assert_eq!(cancelled_invocation.runtime_flow_id, original_flow_id);

    // Force the cancelled run through the same explicitly incomplete archive
    // path used by the other resume tests in this suite.
    mark_archive_incomplete(&cancelled_archive);

    let resume_calls = Arc::new(AtomicUsize::new(0));
    let resume_release = Arc::new(Semaphore::new(1));
    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            cancelled_archive.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(build_blocking_flow(
            journal_base.clone(),
            resume_calls.clone(),
            resume_release,
            cancelled_futures.clone(),
            invocations.clone(),
            resume_outputs.clone(),
        ))
        .await
        .expect("incomplete resume should re-execute the missing effect");

    assert_eq!(resume_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        cancelled_futures.load(Ordering::SeqCst),
        1,
        "the resumed effect should complete instead of being cancelled"
    );
    assert_eq!(
        resume_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        vec![ReplayOutput {
            value: 1,
            effect_value: 101,
        }],
        "resume must commit exactly one domain output"
    );

    let observed_invocations = invocations
        .lock()
        .expect("blocking invocation lock poisoned")
        .clone();
    assert_eq!(observed_invocations.len(), 2);
    let resumed_invocation = &observed_invocations[1];
    assert_eq!(
        resumed_invocation.stage_key, cancelled_invocation.stage_key,
        "resume must re-enter the same effectful stage"
    );
    assert_eq!(
        resumed_invocation.input_seq, cancelled_invocation.input_seq,
        "resume must re-enter the same archived input position"
    );
    assert_eq!(
        resumed_invocation.idempotency_key, cancelled_invocation.idempotency_key,
        "resume must reuse the same stable provider idempotency key"
    );

    let resume_archive = latest_run_dir(&journal_base);
    let resumed_stage_events = read_stage_events(&resume_archive, "effectful").await;
    let outcome_events: Vec<&ChainEvent> = resumed_stage_events
        .iter()
        .filter(|event| is_domain_effect_outcome_fact(event))
        .collect();
    assert_eq!(
        outcome_events.len(),
        1,
        "resume must commit exactly one terminal effect outcome fact"
    );
    assert_eq!(
        resumed_stage_events
            .iter()
            .filter(|event| event.event_type() == ReplayOutput::versioned_event_type())
            .count(),
        1,
        "resume must journal exactly one emitted output"
    );

    let resumed_cursor = recorded_effect_cursor(outcome_events[0])
        .expect("the resumed effect outcome should carry its archived cursor");
    assert_eq!(
        resumed_cursor,
        EffectCursor::new(
            original_flow_id,
            cancelled_invocation.stage_key,
            cancelled_invocation.input_seq,
            0u32,
        ),
        "resume must commit under the cursor derived by the cancelled invocation"
    );
    assert_eq!(
        circuit_breaker_retry_events_in_stage(&resume_archive, "effectful").await,
        0,
        "a first-attempt resume success should not fabricate retry evidence"
    );
}

#[tokio::test]
async fn source_admission_limiter_replay_suppresses_delay_events_and_effects() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_source_limiter_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live source-limiter flow should complete");

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
        .run_async(build_source_limiter_flow(
            journal_base.clone(),
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay source-limiter flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "replay must suppress live effect execution under source middleware"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs
    );

    let replay_archive = latest_run_dir(&journal_base);

    // Directly assert the source admission limiter consumed no tokens and moved no
    // admission state during replay. The burst capacity is 1.0, so an invoked limiter
    // must delay inputs 2 and 3. Under FLOWIP-115a source gate observability is no
    // longer journaled as Delayed events; the live runtime counter proves this test
    // workload still exercised the limiter.
    let (_live_events_total, live_delayed, _live_delay_seconds) =
        rate_limiter_runtime_activity_in_stage(&archive_dir, "inputs").await;
    assert!(
        live_delayed > 0,
        "live run must exercise the limiter (3 inputs at burst capacity 1.0), otherwise \
         the replay-silence assertion below is vacuous"
    );

    let replay_rate_limiter_events = rate_limiter_events_in_stage(&replay_archive, "inputs").await;
    assert_eq!(
        replay_rate_limiter_events, 0,
        "strict replay must not invoke the source admission limiter, so it emits no \
         rate-limiter events of any kind and consumes no tokens or admission state"
    );

    // FLOWIP-120a regression. The journal-only assertion above is blind to the
    // admission-middleware replay bug: a replayed deterministic shell runs the limiter's
    // `pre_handle` on every replayed event but never journals its `Delayed`
    // lifecycle records, so the limiter can pace the whole replay while the journal
    // stays silent. The runtime counters embedded in wide events do capture it.
    //
    // The limiter embeds tokens-consumed (`rl_events_total`), delayed-event
    // (`rl_delayed_total`), and delay-seconds (`rl_delay_seconds_total`) totals via
    // the instrumentation snapshotter. Strict replay must leave every one at zero on
    // every stage. The live run above proves the source limiter is active
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

        // Per-effect boundary instances (FLOWIP-120c) must be equally quiet:
        // strict replay returns recorded outcomes before the boundary is
        // consulted, so no per-effect limiter moves.
        let (effect_events, effect_delayed, effect_delay_seconds) =
            effect_rate_limiter_runtime_activity_in_stage(&replay_archive, stage).await;
        assert_eq!(
            effect_events, 0,
            "stage '{stage}': strict replay must consume zero per-effect tokens, saw {effect_events}"
        );
        assert_eq!(effect_delayed, 0);
        assert_eq!(effect_delay_seconds, 0.0);
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

/// FLOWIP-120m stateful doctrine: a product carrier is `perform`-return
/// machinery only. The committed member facts fold individually through
/// `apply`, in ordinal (field) order, live and replay alike.
#[tokio::test]
async fn effectful_stateful_folds_multi_fact_outcome_per_fact() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_applied = Arc::new(Mutex::new(Vec::new()));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_product_stateful_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_applied.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live product stateful flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    let expected_applied: Vec<String> = (1..=3)
        .flat_map(|value| {
            vec![
                format!("first:{}", value + 100),
                format!("second:{}", value + 200),
                format!("output:{value}"),
            ]
        })
        .collect();
    let live_applied_entries = live_applied.lock().expect("applied lock poisoned").clone();
    assert_eq!(
        live_applied_entries, expected_applied,
        "apply must fold each member fact individually, in ordinal order"
    );

    let archive_dir = latest_run_dir(&journal_base);
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_applied = Arc::new(Mutex::new(Vec::new()));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_product_stateful_flow(
            journal_base,
            replay_calls.clone(),
            replay_applied.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay product stateful flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "strict replay must not execute the product effect"
    );
    assert_eq!(
        replay_applied
            .lock()
            .expect("applied lock poisoned")
            .clone(),
        live_applied_entries,
        "replay must fold the same member facts in the same order"
    );
}

/// FLOWIP-120m: while the breaker is open, the outcome-shaped fallback
/// synthesizes the protected effect's own product carrier. The two facts
/// commit as one recorded group (shared group id, ordinals 0 and 1,
/// middleware-synthesized origin), the handler resumes from the plain
/// `perform` with the carrier, and strict replay reconstructs the same
/// group with zero executions.
#[tokio::test]
async fn outcome_shaped_fallback_synthesizes_multi_fact_group_and_replays_strictly() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_outcome_fallback_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live outcome-fallback flow should complete");

    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        1,
        "only the first input executes; the breaker opens on its failure"
    );
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        live_domain_outputs,
        vec![
            ReplayOutput {
                value: 1,
                effect_value: GUARDED_FAILED_MARKER
            },
            ReplayOutput {
                value: 2,
                effect_value: 902
            },
            ReplayOutput {
                value: 3,
                effect_value: 903
            },
        ],
        "fallback inputs must resume the handler with the synthesized carrier"
    );

    let archive_dir = latest_run_dir(&journal_base);
    let live_rows = product_outcome_rows(&archive_dir, "effectful").await;
    assert_eq!(
        live_rows.len(),
        4,
        "two fallback inputs, two product facts each"
    );
    for pair in live_rows.chunks(2) {
        let (first_group, first_ordinal, first_synthesized, first_type) = &pair[0];
        let (second_group, second_ordinal, second_synthesized, second_type) = &pair[1];
        assert_eq!(
            first_group, second_group,
            "a product outcome commits as one group"
        );
        assert_eq!((*first_ordinal, *second_ordinal), (0, 1));
        assert!(
            *first_synthesized && *second_synthesized,
            "synthesized groups must carry the middleware origin"
        );
        assert_eq!(first_type, "effect_replay.product_first.v1");
        assert_eq!(second_type, "effect_replay.product_second.v1");
    }
    assert_eq!(
        live_rows
            .chunks(2)
            .map(|pair| &pair[0].0)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        2,
        "each input gets its own outcome group"
    );

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_outcome_fallback_flow(
            journal_base.clone(),
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay outcome-fallback flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "strict replay must not execute the effect"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs,
        "strict replay must reconstruct the same carrier for every input"
    );

    let replay_run = latest_run_dir(&journal_base);
    let replay_rows = product_outcome_rows(&replay_run, "effectful").await;
    assert_eq!(
        replay_rows, live_rows,
        "replay must reproduce group ids, ordinals, and origins row for row"
    );
}

/// FLOWIP-120m: a sum carrier rides the same outcome-shaped path; the
/// fallback returns one variant and the handler matches it.
#[tokio::test]
async fn outcome_shaped_fallback_with_sum_carrier_resumes_handler_with_variant() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_either_fallback_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live either-fallback flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 1);
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        live_domain_outputs,
        vec![
            ReplayOutput {
                value: 1,
                effect_value: GUARDED_FAILED_MARKER
            },
            ReplayOutput {
                value: 2,
                effect_value: 902
            },
            ReplayOutput {
                value: 3,
                effect_value: 903
            },
        ],
        "the handler must resume with the synthesized variant"
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
        .run_async(build_either_fallback_flow(
            journal_base,
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay either-fallback flow should complete");

    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs
    );
}

/// FLOWIP-120m: an outcome-shaped fallback must target the stage's declared
/// effect; producing another effect's facts is rejected at build time.
#[tokio::test]
async fn outcome_shaped_fallback_for_undeclared_effect_is_rejected_at_build() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let err = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_undeclared_outcome_target_flow(
            journal_base,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(Vec::new())),
        ))
        .await
        .expect_err("an outcome-shaped fallback for an undeclared effect must fail the build");

    let message = err.to_string();
    assert!(
        message.contains("attach it to the effect it guards"),
        "expected the FLOWIP-120c H7 target-mismatch rejection, got: {message}"
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

/// FLOWIP-120a/120c: incomplete-archive resume must STILL protect effects that
/// execute live at the effect boundary. A blanket `event.replay_context.is_some()`
/// bypass would wrongly disable the live effect's protection; the scoped fix keeps
/// the effect boundary under `LiveEffectBoundary` (never suppressed) while
/// FLOWIP-120c makes downstream handler-level policy structurally unrepresentable.
///
/// With every effect record removed, all three effects re-execute live during
/// resume, so the effectful stage's effect-boundary limiter must run and consume
/// tokens.
#[tokio::test]
async fn resume_incomplete_runs_effect_boundary_limiter_for_missing_effect_records() {
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
    // because the input carries replay provenance. Under the FLOWIP-120c placement
    // split the boundary limiter is a per-effect instance, so its activity rides the
    // per-effect snapshot section.
    let (effectful_tokens, _, _) =
        effect_rate_limiter_runtime_activity_in_stage(&resume_archive, "effectful").await;
    assert!(
        effectful_tokens > 0,
        "resume must keep the effect-boundary limiter live for effects that execute \
         live, but it consumed {effectful_tokens} tokens"
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

    type Outcome = ReplayEffectValue;

    fn label(&self) -> &str {
        "ported"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
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

    type Outcome = ReplayEffectValue;

    fn label(&self) -> &str {
        "ledger"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
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

// ---------------------------------------------------------------------------
// FLOWIP-120h: typed circuit-breaker outcomes through the guarded wrapper
// ---------------------------------------------------------------------------

/// Marker outputs so sink-level equality proves which branch each input took,
/// live and replay alike.
const GUARDED_REJECTED_MARKER: u64 = 7_777;
const GUARDED_FAILED_MARKER: u64 = 9_999;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReplayFallback {
    effect_value: u64,
}

impl TypedPayload for ReplayFallback {
    const EVENT_TYPE: &'static str = "effect_replay.fallback";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReplayRejected {
    value: u64,
    reason: String,
}

impl TypedPayload for ReplayRejected {
    const EVENT_TYPE: &'static str = "effect_replay.rejected";
}

#[derive(Clone, Debug)]
struct GuardedTransform {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for GuardedTransform {
    type Input = ReplayInput;

    async fn process(&self, input: ReplayInput, fx: &mut Effects) -> Result<(), HandlerError> {
        let outcome = fx
            .perform(
                AlwaysFailingEffect {
                    value: input.value,
                    calls: self.calls.clone(),
                }
                .guarded::<ReplayFallback, ReplayRejected>(),
            )
            .await;

        let output = match outcome {
            Ok(CircuitBreakerOutcome::Primary(value)) => ReplayOutput {
                value: input.value,
                effect_value: value.effect_value,
            },
            Ok(CircuitBreakerOutcome::Fallback(fallback)) => ReplayOutput {
                value: input.value,
                effect_value: fallback.effect_value,
            },
            Ok(CircuitBreakerOutcome::Rejected(rejected)) => ReplayOutput {
                value: rejected.value,
                effect_value: GUARDED_REJECTED_MARKER,
            },
            Err(_) => ReplayOutput {
                value: input.value,
                effect_value: GUARDED_FAILED_MARKER,
            },
        };

        fx.emit(output)
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-guarded-v1"
    }
}

fn build_typed_rejection_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_typed_rejection",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue, ReplayFallback, ReplayRejected } => GuardedTransform { calls },
                effects: [AlwaysFailingEffect with [
                CircuitBreaker::opens_after(1)
                    .when_open(OpenPolicy::FailFast)
                    .fallback_fact(|input: &ReplayInput| ReplayFallback {
                        effect_value: input.value + 900,
                    })
                    .rejection_fact(|input, reason| ReplayRejected {
                        value: input.value,
                        reason: format!("{reason:?}"),
                    })
                    .build()
            ]],
                middleware: []);
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

fn build_multi_effect_typed_outcome_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_multi_effect_typed",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue, ReplayFallback, ReplayRejected } => GuardedTransform { calls },
                effects: [AlwaysFailingEffect with [
                CircuitBreaker::opens_after(1)
                    .when_open(OpenPolicy::FailFast)
                    .fallback_fact(|input: &ReplayInput| ReplayFallback {
                        effect_value: input.value + 900,
                    })
                    .rejection_fact(|input, reason| ReplayRejected {
                        value: input.value,
                        reason: format!("{reason:?}"),
                    })
                    .build()
            ], CountingEffect],
                middleware: []);
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

/// FLOWIP-120h two-regime rule, typed side: with a typed-outcome breaker, a
/// FailFast rejection synthesizes the author-named rejection fact, the guarded
/// carrier decodes it as `Ok(Rejected)`, the input completes, and strict
/// replay reconstructs the same branch per input with zero effect executions.
/// The first input's genuine effect failure stays an `Err` recorded under the
/// cursor, replayed deterministically.
#[tokio::test]
async fn typed_rejection_branch_completes_inputs_and_replays_without_execution() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_typed_rejection_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live flow should complete");

    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        1,
        "only the first effect should execute before the breaker opens"
    );
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        live_domain_outputs,
        vec![
            ReplayOutput {
                value: 1,
                effect_value: GUARDED_FAILED_MARKER
            },
            ReplayOutput {
                value: 2,
                effect_value: GUARDED_REJECTED_MARKER
            },
            ReplayOutput {
                value: 3,
                effect_value: GUARDED_REJECTED_MARKER
            },
        ],
        "rejected inputs must complete through the typed Rejected branch"
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
        .run_async(build_typed_rejection_flow(
            journal_base.clone(),
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "strict replay must not execute effects"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs,
        "strict replay must reconstruct the same branch for every input"
    );

    // FLOWIP-120m: branch origin reconstructs from the recorded provenance,
    // row for row, never from registration fact-type membership.
    let replay_run = latest_run_dir(&journal_base);
    let live_origins = effect_fact_origins(&archive_dir, "effectful").await;
    let replay_origins = effect_fact_origins(&replay_run, "effectful").await;
    assert!(
        !live_origins.is_empty(),
        "the typed-rejection run should record origin-stamped effect facts"
    );
    assert_eq!(
        live_origins, replay_origins,
        "strict replay must reconstruct the same per-row fact origin"
    );
}

/// FLOWIP-120c H7 lifted the single-effect restriction: a typed-outcome
/// policy attaches inline to the effect it guards (`Effect with [...]`), so
/// a multi-effect stage builds and the breaker guards only its own effect.
#[tokio::test]
async fn typed_outcome_middleware_builds_on_multi_effect_stages() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_multi_effect_typed_outcome_flow(
            journal_base.clone(),
            calls.clone(),
            outputs,
        ))
        .await
        .expect("a multi-effect stage with a per-effect typed policy must build and run");

    // FailFast with threshold 1: the first input executes (and fails), the
    // breaker opens, and later inputs complete through the typed rejection
    // branch without executing.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "only the first input should reach the failing effect"
    );

    // The per-effect instance snapshots under its effect type (FLOWIP-120c
    // G9): the breaker that opened belongs to the guarded effect, keyed by
    // its declared type, never conflated across the stage's other effects.
    let run_dir = latest_run_dir(&journal_base);
    let effect_breakers: Vec<(String, u64)> = read_stage_events(&run_dir, "effectful")
        .await
        .into_iter()
        .filter_map(|event| event.runtime_context)
        .flat_map(|rc| {
            rc.effect_circuit_breakers
                .into_iter()
                .map(|cb| (cb.effect_type, cb.cb_opened_total))
        })
        .fold(std::collections::HashMap::new(), |mut acc, (k, v)| {
            let entry: &mut u64 = acc.entry(k).or_default();
            *entry = (*entry).max(v);
            acc
        })
        .into_iter()
        .collect();
    let opened_for_failing = effect_breakers
        .iter()
        .find(|(effect, _)| effect == AlwaysFailingEffect::EFFECT_TYPE)
        .map(|(_, opened)| *opened)
        .unwrap_or(0);
    assert!(
        opened_for_failing > 0,
        "the guarded effect's breaker must open under its own effect key, got {effect_breakers:?}"
    );
    assert!(
        !effect_breakers
            .iter()
            .any(|(effect, _)| effect == CountingEffect::EFFECT_TYPE),
        "the unguarded effect must carry no breaker instance, got {effect_breakers:?}"
    );
}

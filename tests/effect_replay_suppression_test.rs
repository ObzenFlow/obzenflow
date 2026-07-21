// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_adapters::middleware::{CircuitBreaker, EffectResilience, RateLimiterBuilder, Retry};
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    event::payloads::flow_control_payload::FlowControlPayload,
    event::payloads::observability_payload::{
        CircuitBreakerEvent, MiddlewareLifecycle, ObservabilityPayload,
    },
    event::{ChainEventContent, StageLifecycleEvent, SystemEvent, SystemEventType},
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
    type Output = obzenflow_core::stage_fact_set![ReplayOutput, ReplayEffectValue];
    type AllowedEffects = obzenflow_runtime::effect_set![CountingEffect];

    async fn process(
        &self,
        input: ReplayInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
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
        Ok(fx.complete()?)
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
    type Output = obzenflow_core::stage_fact_set![ReplayOutput, ReplayEffectValue];
    type AllowedEffects = obzenflow_runtime::effect_set![BlockingEffect];

    async fn process(
        &self,
        input: ReplayInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
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
        Ok(fx.complete()?)
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

#[derive(Clone, Debug, Default)]
struct ReplayStatefulState {
    outputs: Vec<ReplayOutput>,
}

#[derive(Clone, Debug)]
struct ReplayStateful {
    calls: Arc<AtomicUsize>,
}

/// Stateful `Output` enum derived per FLOWIP-120m: the same sum shape an
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
    type Output = ReplayStatefulFact;
    type AllowedEffects = obzenflow_runtime::effect_set![CountingEffect];

    fn initial_state(&self) -> Self::State {
        ReplayStatefulState::default()
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &ReplayInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
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
        Ok(fx.complete()?)
    }

    fn apply(&mut self, state: &mut Self::State, fact: Self::Output) -> Result<(), HandlerError> {
        if let ReplayStatefulFact::Output(output) = fact {
            state.outputs.push(output);
        }
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-stateful-v1"
    }
}

/// B1 regression carrier: the product shape cannot reconstruct from one fact,
/// but the open marker permits a trusted manual assertion. The integration
/// test below proves that a false assertion fails the stage after the authored
/// facts become durable.
#[derive(Clone, Debug, obzenflow_core::StageOutputFacts)]
struct DishonestReplayStatefulOutput {
    effect_value: ReplayEffectValue,
    output: ReplayOutput,
}

impl obzenflow_core::OneFactStageOutput for DishonestReplayStatefulOutput {}

#[derive(Clone, Debug)]
struct DishonestOneFactStateful {
    calls: Arc<AtomicUsize>,
    decide_inputs: Arc<Mutex<Vec<u64>>>,
    apply_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulStatefulHandler for DishonestOneFactStateful {
    type State = ReplayStatefulState;
    type Input = ReplayInput;
    type Output = DishonestReplayStatefulOutput;
    type AllowedEffects = obzenflow_runtime::effect_set![CountingEffect];

    fn initial_state(&self) -> Self::State {
        ReplayStatefulState::default()
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        self.decide_inputs
            .lock()
            .expect("decide inputs lock poisoned")
            .push(input.value);

        let effect = fx
            .perform(CountingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await?;
        fx.emit(ReplayOutput {
            value: input.value,
            effect_value: effect.effect_value,
        })
        .await?;
        Ok(fx.complete()?)
    }

    fn apply(&mut self, state: &mut Self::State, fact: Self::Output) -> Result<(), HandlerError> {
        self.apply_calls.fetch_add(1, Ordering::SeqCst);
        state.outputs.push(fact.output);
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        // The sealed fixture and dishonest replay have identical durable
        // behaviour. Only the non-durable Rust carrier differs.
        "effect-replay-stateful-v1"
    }
}

#[derive(Clone, Debug, Default)]
struct ErrorAfterCommitState {
    applied: Vec<String>,
}

#[derive(Clone, Debug)]
struct ErrorAfterCommitStateful {
    calls: Arc<AtomicUsize>,
    observed_fact_counts: Arc<Mutex<Vec<usize>>>,
    applied: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl EffectfulStatefulHandler for ErrorAfterCommitStateful {
    type State = ErrorAfterCommitState;
    type Input = ReplayInput;
    type Output = ReplayStatefulFact;
    type AllowedEffects = obzenflow_runtime::effect_set![CountingEffect];

    fn initial_state(&self) -> Self::State {
        ErrorAfterCommitState::default()
    }

    async fn decide(
        &mut self,
        state: &Self::State,
        input: &ReplayInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        self.observed_fact_counts
            .lock()
            .expect("observed fact counts lock poisoned")
            .push(state.applied.len());

        let effect = fx
            .perform(CountingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await?;
        fx.emit(ReplayOutput {
            value: input.value,
            effect_value: effect.effect_value,
        })
        .await?;

        if input.value == 1 {
            return Err(HandlerError::Domain(
                "deterministic failure after completed commits".to_string(),
            ));
        }

        Ok(fx.complete()?)
    }

    fn apply(&mut self, state: &mut Self::State, fact: Self::Output) -> Result<(), HandlerError> {
        let entry = match fact {
            ReplayStatefulFact::EffectValue(value) => {
                format!("effect:{}", value.effect_value)
            }
            ReplayStatefulFact::Output(output) => format!("output:{}", output.value),
        };
        state.applied.push(entry.clone());
        self.applied
            .lock()
            .expect("applied lock poisoned")
            .push(entry);
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-error-after-commit-stateful-v1"
    }
}

#[derive(Clone, Debug, Default)]
struct ApplyRejectionState {
    applied: Vec<String>,
}

/// B2 proof handler. The live failure and strict replay use the same handler
/// surface and logic version; the completed replay fixture disables only the
/// deliberate `apply` rejection so it can provide a sealed effect history.
#[derive(Clone, Debug)]
struct ApplyRejectingStateful {
    calls: Arc<AtomicUsize>,
    decide_inputs: Arc<Mutex<Vec<u64>>>,
    apply_attempts: Arc<Mutex<Vec<String>>>,
    reject_apply: bool,
}

#[async_trait]
impl EffectfulStatefulHandler for ApplyRejectingStateful {
    type State = ApplyRejectionState;
    type Input = ReplayInput;
    type Output = ReplayStatefulFact;
    type AllowedEffects = obzenflow_runtime::effect_set![CountingEffect];

    fn initial_state(&self) -> Self::State {
        ApplyRejectionState::default()
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        self.decide_inputs
            .lock()
            .expect("decide inputs lock poisoned")
            .push(input.value);

        let effect = fx
            .perform(CountingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await?;
        fx.emit(ReplayOutput {
            value: input.value,
            effect_value: effect.effect_value,
        })
        .await?;
        Ok(fx.complete()?)
    }

    fn apply(&mut self, state: &mut Self::State, fact: Self::Output) -> Result<(), HandlerError> {
        let (entry, reject) = match fact {
            ReplayStatefulFact::EffectValue(value) => {
                (format!("effect:{}", value.effect_value), false)
            }
            ReplayStatefulFact::Output(output) => {
                (format!("output:{}", output.value), self.reject_apply)
            }
        };
        self.apply_attempts
            .lock()
            .expect("apply attempts lock poisoned")
            .push(entry.clone());

        if reject {
            return Err(HandlerError::Domain(
                "simulated rejection of an already-committed output fact".to_string(),
            ));
        }

        state.applied.push(entry);
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-apply-rejection-v1"
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

/// The stage's per-fact `Output` enum: the carrier never reaches `apply`;
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
    type Output = ProductStatefulFact;
    type AllowedEffects = obzenflow_runtime::effect_set![ProductEffect];

    fn initial_state(&self) -> Self::State {
        ProductStatefulState
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &ReplayInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
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
        Ok(fx.complete()?)
    }

    fn apply(&mut self, _state: &mut Self::State, fact: Self::Output) -> Result<(), HandlerError> {
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
    let resilience = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(1)
            .build()
            .expect("blocking breaker configuration"),
    )
    .retry(Retry::fixed(Duration::from_millis(1)).max_attempts(2))
    .build()
    .expect("blocking resilience configuration");

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
                effects: [BlockingEffect with [resilience]],
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

/// Match `build_stateful_flow` at every durable replay boundary while
/// replacing only the handler-side output carrier with a deliberately false
/// `OneFactStageOutput` implementation.
fn build_dishonest_one_fact_stateful_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    decide_inputs: Arc<Mutex<Vec<u64>>>,
    apply_calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_stateful",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_stateful!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => DishonestOneFactStateful {
                    calls,
                    decide_inputs,
                    apply_calls
                },
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

fn build_error_after_commit_stateful_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    observed_fact_counts: Arc<Mutex<Vec<usize>>>,
    applied: Arc<Mutex<Vec<String>>>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_error_after_commit_stateful",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_stateful!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => ErrorAfterCommitStateful {
                    calls,
                    observed_fact_counts,
                    applied
                },
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

fn build_apply_rejection_stateful_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    decide_inputs: Arc<Mutex<Vec<u64>>>,
    apply_attempts: Arc<Mutex<Vec<String>>>,
    reject_apply: bool,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_apply_rejection_stateful",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_stateful!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => ApplyRejectingStateful {
                    calls,
                    decide_inputs,
                    apply_attempts,
                    reject_apply
                },
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

async fn read_stage_error_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
    let manifest = archive_manifest(run_dir);
    let stage_journal = manifest["stages"][stage_key]["error_journal_file"]
        .as_str()
        .expect("manifest should contain stage error journal file");
    let journal: obzenflow_infra::journal::DiskJournal<ChainEvent> =
        obzenflow_infra::journal::DiskJournal::with_owner(
            run_dir.join(stage_journal),
            JournalOwner::stage(StageId::new()),
        )
        .expect("stage error journal should open");

    journal
        .read_causally_ordered()
        .await
        .expect("stage error journal should read")
        .into_iter()
        .map(|envelope| envelope.event)
        .collect()
}

async fn read_system_events(run_dir: &Path) -> Vec<SystemEvent> {
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
        .map(|envelope| envelope.event)
        .collect()
}

fn stage_id_from_manifest(run_dir: &Path, stage_key: &str) -> StageId {
    archive_manifest(run_dir)["stages"][stage_key]["stage_id"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest should contain stage id for '{stage_key}'"))
        .parse()
        .unwrap_or_else(|error| panic!("stage id for '{stage_key}' should parse: {error}"))
}

fn replay_stateful_fact_identities(events: &[ChainEvent]) -> Vec<(String, String)> {
    let effect_type = ReplayEffectValue::versioned_event_type();
    let output_type = ReplayOutput::versioned_event_type();
    events
        .iter()
        .filter(|event| {
            let event_type = event.event_type();
            event_type == effect_type || event_type == output_type
        })
        .map(|event| (event.id.to_string(), event.event_type().to_string()))
        .collect()
}

/// Assert the externally visible fail-stop boundary for a stateful contract
/// violation. The committed fact rows remain on the Data lane, but the
/// offending input is neither converted into an ordinary error-lane row nor
/// followed by EOF or a successful stage lifecycle.
async fn assert_replay_stateful_contract_failure_archive(
    run_dir: &Path,
    offending_fact_index: usize,
    expected_error_fragments: &[&str],
) -> Vec<(String, String)> {
    let stage_events = read_stage_events(run_dir, "effectful").await;
    assert!(
        !stage_events.iter().any(|event| matches!(
            event.content,
            ChainEventContent::FlowControl(FlowControlPayload::Eof { .. })
        )),
        "a stateful contract violation must prevent the effectful stage from committing EOF"
    );

    let fact_identities = replay_stateful_fact_identities(&stage_events);
    assert_eq!(
        fact_identities
            .iter()
            .map(|(_, event_type)| event_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            ReplayEffectValue::versioned_event_type(),
            ReplayOutput::versioned_event_type(),
        ],
        "both first-input facts must remain durable in their authored order"
    );

    assert!(
        read_stage_error_events(run_dir, "effectful")
            .await
            .is_empty(),
        "a committed-fact contract violation must not be downgraded to an ordinary error row"
    );

    let stage_id = stage_id_from_manifest(run_dir, "effectful");
    let system_events = read_system_events(run_dir).await;
    let mut failure_count = 0;
    for system_event in system_events {
        let SystemEventType::StageLifecycle {
            stage_id: lifecycle_stage_id,
            event,
        } = system_event.event
        else {
            continue;
        };
        if lifecycle_stage_id != stage_id {
            continue;
        }

        match event {
            StageLifecycleEvent::Failed { error, metrics, .. } => {
                failure_count += 1;
                let (offending_fact_id, offending_fact_type) = fact_identities
                    .get(offending_fact_index)
                    .unwrap_or_else(|| {
                        panic!(
                            "offending fact index {offending_fact_index} must exist in {fact_identities:?}"
                        )
                    });
                assert!(
                    error.contains(offending_fact_id.as_str())
                        && error.contains(offending_fact_type.as_str())
                        && expected_error_fragments
                            .iter()
                            .all(|fragment| error.contains(*fragment)),
                    "fatal lifecycle evidence must retain the fact identity, contract boundary, and cause: {error}"
                );
                if let Some(metrics) = metrics {
                    assert_eq!(
                        metrics.events_processed_total, 0,
                        "the failed input must not be counted as successfully processed"
                    );
                    assert_eq!(
                        metrics.events_accumulated_total, 0,
                        "the failed input must not be counted as accumulated"
                    );
                }
            }
            StageLifecycleEvent::Completed { .. } | StageLifecycleEvent::Drained => {
                panic!("a stateful contract violation must not reach a successful terminal state")
            }
            StageLifecycleEvent::Running
            | StageLifecycleEvent::Draining { .. }
            | StageLifecycleEvent::Cancelled { .. } => {}
        }
    }
    assert_eq!(
        failure_count, 1,
        "the effectful stage must publish exactly one fatal lifecycle event"
    );

    fact_identities
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

async fn circuit_breaker_recovery_completed_events_in_stage(
    run_dir: &Path,
    stage_key: &str,
) -> usize {
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter(|event| {
            matches!(
                event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::CircuitBreaker(
                        CircuitBreakerEvent::RecoveryCompleted { .. }
                    )
                ))
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
    let replay_output_count = effectful_events
        .iter()
        .filter(|event| event.event_type() == ReplayOutput::versioned_event_type())
        .count();
    let effect_value_count = effectful_events
        .iter()
        .filter(|event| event.event_type() == ReplayEffectValue::versioned_event_type())
        .count();
    assert_eq!(replay_output_count, 2);
    assert_eq!(effect_value_count, 2);

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
    assert_eq!(
        circuit_breaker_recovery_completed_events_in_stage(&cancelled_archive, "effectful").await,
        0,
        "the aborted recovery session must commit no terminal clock evidence"
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
    assert_eq!(
        circuit_breaker_recovery_completed_events_in_stage(&resume_archive, "effectful").await,
        1,
        "the resumed live invocation should commit one terminal clock row"
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
async fn effectful_stateful_reconciles_completed_facts_before_decide_error_live_and_replay() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_observed = Arc::new(Mutex::new(Vec::new()));
    let live_applied = Arc::new(Mutex::new(Vec::new()));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_error_after_commit_stateful_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_observed.clone(),
            live_applied.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live error-after-commit stateful flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        live_observed
            .lock()
            .expect("live observed lock poisoned")
            .as_slice(),
        &[0, 2, 4],
        "the next input must observe both facts committed before the prior decide error"
    );
    let expected_applied = vec![
        "effect:101".to_string(),
        "output:1".to_string(),
        "effect:102".to_string(),
        "output:2".to_string(),
        "effect:103".to_string(),
        "output:3".to_string(),
    ];
    let live_applied_entries = live_applied
        .lock()
        .expect("live applied lock poisoned")
        .clone();
    assert_eq!(live_applied_entries, expected_applied);
    let live_domain_outputs = live_outputs
        .lock()
        .expect("live outputs lock poisoned")
        .clone();
    assert_eq!(
        live_domain_outputs,
        vec![
            ReplayOutput {
                value: 1,
                effect_value: 101,
            },
            ReplayOutput {
                value: 2,
                effect_value: 102,
            },
            ReplayOutput {
                value: 3,
                effect_value: 103,
            },
        ],
        "the direct fact committed before the first decide error must remain visible"
    );

    let archive_dir = latest_run_dir(&journal_base);
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_observed = Arc::new(Mutex::new(Vec::new()));
    let replay_applied = Arc::new(Mutex::new(Vec::new()));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_error_after_commit_stateful_flow(
            journal_base,
            replay_calls.clone(),
            replay_observed.clone(),
            replay_applied.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay error-after-commit stateful flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "strict replay must use the recorded effect outcome"
    );
    assert_eq!(
        replay_observed
            .lock()
            .expect("replay observed lock poisoned")
            .as_slice(),
        &[0, 2, 4]
    );
    assert_eq!(
        replay_applied
            .lock()
            .expect("replay applied lock poisoned")
            .as_slice(),
        live_applied_entries.as_slice()
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("replay outputs lock poisoned")
            .as_slice(),
        live_domain_outputs.as_slice()
    );
}

/// FLOWIP-120z B1: a manual `OneFactStageOutput` implementation is a trusted
/// law, not a structural proof. A dishonest product keeps its already-authored
/// facts durable, then fails the stage when the adapter cannot reconstruct the
/// first fact as the product. Strict replay uses a lawful completed fixture
/// with the same durable contract and must reach the same boundary without
/// executing the effect again.
#[tokio::test]
async fn effectful_stateful_false_one_fact_marker_is_fatal_live_and_under_strict_replay() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");

    let falsely_marked_facts =
        obzenflow_core::TypedFactSet::into_facts(DishonestReplayStatefulOutput {
            effect_value: ReplayEffectValue { effect_value: 100 },
            output: ReplayOutput {
                value: 0,
                effect_value: 100,
            },
        })
        .expect("the dishonest product should lower as an ordinary two-fact carrier");
    assert_eq!(
        falsely_marked_facts
            .iter()
            .map(|fact| fact.event_type.as_str())
            .collect::<Vec<_>>(),
        vec![
            ReplayEffectValue::versioned_event_type(),
            ReplayOutput::versioned_event_type(),
        ],
        "the manual marker must be demonstrably false before exercising its runtime backstop"
    );
    let missing_output_type = ReplayOutput::versioned_event_type();

    let live_journal_base = temp.path().join("live_failure_journals");
    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_decide_inputs = Arc::new(Mutex::new(Vec::new()));
    let live_apply_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    let live_result = tokio::time::timeout(
        Duration::from_secs(15),
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(build_dishonest_one_fact_stateful_flow(
                live_journal_base.clone(),
                live_calls.clone(),
                live_decide_inputs.clone(),
                live_apply_calls.clone(),
                live_outputs,
            )),
    )
    .await
    .expect("live false one-fact assertion should terminate the flow promptly");
    live_result.expect_err("a false one-fact assertion must fail the live flow");
    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        1,
        "only the offending input may execute its effect"
    );
    assert_eq!(
        live_decide_inputs
            .lock()
            .expect("live decide inputs lock poisoned")
            .as_slice(),
        &[1],
        "the stage must not decide a later input after the carrier contradiction"
    );
    assert_eq!(
        live_apply_calls.load(Ordering::SeqCst),
        0,
        "singleton reconstruction must fail before apply"
    );

    let live_run = latest_run_dir(&live_journal_base);
    let live_fact_identities = assert_replay_stateful_contract_failure_archive(
        &live_run,
        0,
        &[
            "one_fact_stage_output",
            "could not be reconstructed",
            "missing typed fact",
            missing_output_type.as_str(),
        ],
    )
    .await;

    // A failed archive is not a strict replay source. Use the lawful unary
    // carrier to seal the same flat arrow, effect manifest, topology, and
    // stage logic version, then replay that history through the dishonest
    // handler-side carrier.
    let replay_journal_base = temp.path().join("strict_replay_journals");
    let fixture_calls = Arc::new(AtomicUsize::new(0));
    let fixture_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_stateful_flow(
            replay_journal_base.clone(),
            fixture_calls.clone(),
            fixture_outputs,
        ))
        .await
        .expect("lawful fixture flow should seal the strict replay archive");
    assert_eq!(
        fixture_calls.load(Ordering::SeqCst),
        3,
        "the lawful fixture must execute one effect for each source input"
    );

    let replay_source = latest_run_dir(&replay_journal_base);
    let replay_source_facts =
        replay_stateful_fact_identities(&read_stage_events(&replay_source, "effectful").await);
    assert_eq!(
        replay_source_facts.len(),
        6,
        "the completed fixture should contain two ordered facts for each input"
    );

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_decide_inputs = Arc::new(Mutex::new(Vec::new()));
    let replay_apply_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    let replay_result = tokio::time::timeout(
        Duration::from_secs(15),
        FlowApplication::builder()
            .with_cli_args(vec![
                OsString::from("obzenflow"),
                OsString::from("--replay-from"),
                replay_source.as_os_str().to_os_string(),
            ])
            .run_async(build_dishonest_one_fact_stateful_flow(
                replay_journal_base.clone(),
                replay_calls.clone(),
                replay_decide_inputs.clone(),
                replay_apply_calls.clone(),
                replay_outputs,
            )),
    )
    .await
    .expect("strict replay false one-fact assertion should terminate the flow promptly");
    replay_result.expect_err("strict replay must reach the false one-fact assertion");
    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "strict replay must reconstruct the committed effect outcome without live I/O"
    );
    assert_eq!(
        replay_decide_inputs
            .lock()
            .expect("replay decide inputs lock poisoned")
            .as_slice(),
        &[1],
        "strict replay must stop before the next archived input"
    );
    assert_eq!(
        replay_apply_calls.load(Ordering::SeqCst),
        0,
        "strict replay must reproduce the singleton reconstruction failure before apply"
    );

    let replay_run = latest_run_dir(&replay_journal_base);
    assert_ne!(
        replay_run, replay_source,
        "the failed replay must produce its own evidence archive"
    );
    let replay_fact_identities = assert_replay_stateful_contract_failure_archive(
        &replay_run,
        0,
        &[
            "one_fact_stage_output",
            "could not be reconstructed",
            "missing typed fact",
            missing_output_type.as_str(),
        ],
    )
    .await;
    assert_eq!(
        replay_fact_identities,
        replay_source_facts[..2],
        "strict replay must preserve the exact first-input fact identities and order"
    );
    assert_eq!(
        live_fact_identities
            .iter()
            .map(|(_, event_type)| event_type)
            .collect::<Vec<_>>(),
        replay_fact_identities
            .iter()
            .map(|(_, event_type)| event_type)
            .collect::<Vec<_>>(),
        "live and replay must stop after the same ordered fact types"
    );
}

/// FLOWIP-120z B2: a stateful fold cannot skip an already-durable fact. An
/// `apply` rejection keeps the facts durable, but publishes stage-fatal
/// lifecycle evidence before later input or successful EOF/drain. A completed
/// fixture archive lets the second half exercise true
/// strict replay while the rejecting handler proves the same fail-stop boundary
/// with zero live effect calls and the exact recorded fact identities.
#[tokio::test]
async fn effectful_stateful_apply_rejection_is_fatal_live_and_under_strict_replay() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");

    let live_journal_base = temp.path().join("live_failure_journals");
    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_decide_inputs = Arc::new(Mutex::new(Vec::new()));
    let live_apply_attempts = Arc::new(Mutex::new(Vec::new()));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    let live_result = tokio::time::timeout(
        Duration::from_secs(15),
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(build_apply_rejection_stateful_flow(
                live_journal_base.clone(),
                live_calls.clone(),
                live_decide_inputs.clone(),
                live_apply_attempts.clone(),
                true,
                live_outputs,
            )),
    )
    .await
    .expect("live apply rejection should terminate the flow promptly");
    live_result.expect_err("live apply rejection must fail the flow");
    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        1,
        "only the offending input may execute its effect"
    );
    assert_eq!(
        live_decide_inputs
            .lock()
            .expect("live decide inputs lock poisoned")
            .as_slice(),
        &[1],
        "the stage must not process input after the rejected fold"
    );
    assert_eq!(
        live_apply_attempts
            .lock()
            .expect("live apply attempts lock poisoned")
            .as_slice(),
        &["effect:101", "output:1"],
        "the first fact folds before the second committed fact is rejected"
    );
    let live_run = latest_run_dir(&live_journal_base);
    let live_fact_identities = assert_replay_stateful_contract_failure_archive(
        &live_run,
        1,
        &[
            "effectful_stateful_apply",
            "simulated rejection of an already-committed output fact",
        ],
    )
    .await;

    // A failed archive cannot be a strict replay source. Record the identical
    // handler/effect contract with its test-only rejection switch disabled to
    // obtain a sealed history, then replay that history through the rejecting
    // fold. This avoids `--allow-incomplete-archive` and its lenient effect-miss
    // semantics.
    let replay_journal_base = temp.path().join("strict_replay_journals");
    let fixture_calls = Arc::new(AtomicUsize::new(0));
    let fixture_decide_inputs = Arc::new(Mutex::new(Vec::new()));
    let fixture_apply_attempts = Arc::new(Mutex::new(Vec::new()));
    let fixture_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_apply_rejection_stateful_flow(
            replay_journal_base.clone(),
            fixture_calls.clone(),
            fixture_decide_inputs,
            fixture_apply_attempts,
            false,
            fixture_outputs,
        ))
        .await
        .expect("completed fixture flow should seal the strict replay archive");
    assert_eq!(fixture_calls.load(Ordering::SeqCst), 3);

    let replay_source = latest_run_dir(&replay_journal_base);
    let replay_source_facts =
        replay_stateful_fact_identities(&read_stage_events(&replay_source, "effectful").await);
    assert_eq!(
        replay_source_facts.len(),
        6,
        "the completed fixture should contain two ordered facts for each input"
    );

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_decide_inputs = Arc::new(Mutex::new(Vec::new()));
    let replay_apply_attempts = Arc::new(Mutex::new(Vec::new()));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    let replay_result = tokio::time::timeout(
        Duration::from_secs(15),
        FlowApplication::builder()
            .with_cli_args(vec![
                OsString::from("obzenflow"),
                OsString::from("--replay-from"),
                replay_source.as_os_str().to_os_string(),
            ])
            .run_async(build_apply_rejection_stateful_flow(
                replay_journal_base.clone(),
                replay_calls.clone(),
                replay_decide_inputs.clone(),
                replay_apply_attempts.clone(),
                true,
                replay_outputs,
            )),
    )
    .await
    .expect("strict replay apply rejection should terminate the flow promptly");
    replay_result.expect_err("strict replay must reach the same fatal apply rejection");
    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "strict replay must reconstruct the committed effect outcome without live I/O"
    );
    assert_eq!(
        replay_decide_inputs
            .lock()
            .expect("replay decide inputs lock poisoned")
            .as_slice(),
        &[1],
        "strict replay must stop before later archived input"
    );
    assert_eq!(
        replay_apply_attempts
            .lock()
            .expect("replay apply attempts lock poisoned")
            .as_slice(),
        &["effect:101", "output:1"],
        "strict replay must reproduce the same fold boundary"
    );

    let replay_run = latest_run_dir(&replay_journal_base);
    assert_ne!(
        replay_run, replay_source,
        "the failed replay must produce its own evidence archive"
    );
    let replay_fact_identities = assert_replay_stateful_contract_failure_archive(
        &replay_run,
        1,
        &[
            "effectful_stateful_apply",
            "simulated rejection of an already-committed output fact",
        ],
    )
    .await;
    assert_eq!(
        replay_fact_identities,
        replay_source_facts[..2],
        "strict replay must preserve the exact first-input fact identities and order"
    );
    assert_eq!(
        live_fact_identities
            .iter()
            .map(|(_, event_type)| event_type)
            .collect::<Vec<_>>(),
        replay_fact_identities
            .iter()
            .map(|(_, event_type)| event_type)
            .collect::<Vec<_>>(),
        "live and replay must stop after the same ordered fact types"
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

/// Incomplete resume re-executes missing physical calls while preserving the
/// archived logical cursor.
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
    type Output = obzenflow_core::stage_fact_set![ReplayOutput, ReplayEffectValue];
    type AllowedEffects = obzenflow_runtime::effect_set![PortedEffect];

    async fn process(
        &self,
        input: ReplayInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
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
        Ok(fx.complete()?)
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
    type Output = obzenflow_core::stage_fact_set![ReplayOutput, ReplayEffectValue];
    type AllowedEffects = obzenflow_runtime::effect_set![LedgerEffect];

    async fn process(
        &self,
        input: ReplayInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
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
        Ok(fx.complete()?)
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
// Plain fail-fast circuit-breaker rejections: a prevented call surfaces as a
// recorded rejection error, never a synthesized fact (FLOWIP-120z step 1).
// ---------------------------------------------------------------------------

/// Marker outputs so sink-level equality proves how each input ended,
/// live and replay alike.
const BREAKER_REJECTED_MARKER: u64 = 7_777;
const EFFECT_FAILED_MARKER: u64 = 9_999;

/// Live `BoundaryRejected` and the `RecordedFailure` it rehydrates to on
/// replay project to the same semantic reason (FLOWIP-120i), so one
/// predicate distinguishes breaker refusal from genuine effect failure in
/// both runs.
fn is_breaker_rejection(err: &EffectError) -> bool {
    err.semantic_reason()
        .starts_with("circuit breaker rejected effect execution")
}

#[derive(Clone, Debug)]
struct FailFastTransform {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for FailFastTransform {
    type Input = ReplayInput;
    type Output = obzenflow_core::stage_fact_set![ReplayOutput, ReplayEffectValue];
    type AllowedEffects = obzenflow_runtime::effect_set![AlwaysFailingEffect];

    async fn process(
        &self,
        input: ReplayInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let outcome = fx
            .perform(AlwaysFailingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await;

        let output = match outcome {
            Ok(value) => ReplayOutput {
                value: input.value,
                effect_value: value.effect_value,
            },
            Err(err) if is_breaker_rejection(&err) => ReplayOutput {
                value: input.value,
                effect_value: BREAKER_REJECTED_MARKER,
            },
            Err(_) => ReplayOutput {
                value: input.value,
                effect_value: EFFECT_FAILED_MARKER,
            },
        };

        fx.emit(output)
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-fail-fast-v1"
    }
}

/// Same authoring behaviour with the wider manifest used by the
/// per-effect-policy binding test. A distinct handler type keeps the static
/// capability set honest for each stage declaration.
#[derive(Clone, Debug)]
struct MultiEffectFailFastTransform {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for MultiEffectFailFastTransform {
    type Input = ReplayInput;
    type Output = obzenflow_core::stage_fact_set![ReplayOutput, ReplayEffectValue];
    type AllowedEffects = obzenflow_runtime::effect_set![AlwaysFailingEffect, CountingEffect];

    async fn process(
        &self,
        input: ReplayInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let outcome = fx
            .perform(AlwaysFailingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await;

        let output = match outcome {
            Ok(value) => ReplayOutput {
                value: input.value,
                effect_value: value.effect_value,
            },
            Err(err) if is_breaker_rejection(&err) => ReplayOutput {
                value: input.value,
                effect_value: BREAKER_REJECTED_MARKER,
            },
            Err(_) => ReplayOutput {
                value: input.value,
                effect_value: EFFECT_FAILED_MARKER,
            },
        };

        fx.emit(output).await?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-fail-fast-v1"
    }
}

fn build_fail_fast_rejection_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    let resilience = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(1)
            .build()
            .expect("fail-fast breaker configuration"),
    )
    .build()
    .expect("fail-fast resilience configuration");

    flow! {
        name: "effect_replay_fail_fast_rejection",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => FailFastTransform { calls },
                effects: [AlwaysFailingEffect with [resilience]],
                middleware: []);
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

fn build_multi_effect_per_effect_breaker_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    let resilience = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(1)
            .build()
            .expect("per-effect breaker configuration"),
    )
    .build()
    .expect("per-effect resilience configuration");
    flow! {
        name: "effect_replay_multi_effect_breaker",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_transform!(
                ReplayInput -> { ReplayOutput, ReplayEffectValue } => MultiEffectFailFastTransform { calls },
                effects: [AlwaysFailingEffect with [resilience], CountingEffect],
                middleware: []);
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

/// Plain fail-fast rejection: the breaker prevents the call, `perform`
/// returns a recorded rejection error, the input still completes, and strict
/// replay reconstructs the same rejection per input with zero effect
/// executions. The first input's genuine effect failure stays an `Err`
/// recorded under the cursor, replayed deterministically; the marker outputs
/// prove the handler distinguished the two failure kinds identically in both
/// runs.
#[tokio::test]
async fn fail_fast_rejection_completes_inputs_and_replays_without_execution() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_fail_fast_rejection_flow(
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
                effect_value: EFFECT_FAILED_MARKER
            },
            ReplayOutput {
                value: 2,
                effect_value: BREAKER_REJECTED_MARKER
            },
            ReplayOutput {
                value: 3,
                effect_value: BREAKER_REJECTED_MARKER
            },
        ],
        "rejected inputs must complete through the recorded rejection error"
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
        .run_async(build_fail_fast_rejection_flow(
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
        "strict replay must reconstruct the same rejection for every input"
    );
}

/// FLOWIP-120c H7 lifted the single-effect restriction: a breaker policy
/// attaches inline to the effect it guards (`Effect with [...]`), so a
/// multi-effect stage builds and the breaker guards only its own effect.
#[tokio::test]
async fn per_effect_breaker_builds_on_multi_effect_stages() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_multi_effect_per_effect_breaker_flow(
            journal_base.clone(),
            calls.clone(),
            outputs,
        ))
        .await
        .expect("a multi-effect stage with a per-effect breaker policy must build and run");

    // FailFast with threshold 1: the first input executes (and fails), the
    // breaker opens, and later inputs complete through the recorded
    // rejection error without executing.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "only the first input should reach the failing effect"
    );

    // The per-effect instance snapshots under its effect type (FLOWIP-120c
    // G9): the breaker that opened belongs to the effect it protects, keyed
    // by its declared type, never conflated across the stage's other effects.
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
        "the protected effect's breaker must open under its own effect key, got {effect_breakers:?}"
    );
    assert!(
        !effect_breakers
            .iter()
            .any(|(effect, _)| effect == CountingEffect::EFFECT_TYPE),
        "the unprotected effect must carry no breaker instance, got {effect_breakers:?}"
    );
}

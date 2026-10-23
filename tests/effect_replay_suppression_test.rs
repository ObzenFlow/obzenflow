// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "test_support/exported_jsonl.rs"]
mod exported_jsonl;
mod replay_testkit;

use async_trait::async_trait;
use obzenflow_adapters::middleware::{circuit_breaker, rate_limit, retry, MiddlewareFactory};
use obzenflow_core::{
    event::chain_event::ChainEvent,
    event::payloads::delivery_payload::DeliveryMethod,
    event::payloads::execution_payload::{ExecutionPayload, RecoveryFact},
    event::payloads::flow_control_payload::FlowControlPayload,
    event::{payloads::execution_payload::StageLifecycleFact, ChainPayload},
    id::StageId,
    StageOutputs, TypedPayload,
};
use obzenflow_dsl::{
    effectful_stateful, effectful_transform, flow, sink, source, transform, FlowDefinition,
};
use obzenflow_infra::application::{ApplicationError, FlowApplication};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Call, CallRequest, EffectCommitHandle, EffectContext, EffectCursor, EffectError,
    EffectOutcomePayload, EffectRecord, Effects, IdempotencyKey, Idempotent, Keyed, RequiresKey,
    SinkRedeliverySafety, System, Transactional,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulStatefulHandler, EffectfulTransformHandler, InlineSink, SinkDescription,
    SinkWriteFailure, TypedFiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
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
    calls: Option<Arc<AtomicUsize>>,
}

impl ReplaySource {
    fn new() -> Self {
        Self {
            next_value: 1,
            calls: None,
        }
    }

    fn counted(calls: Arc<AtomicUsize>) -> Self {
        Self {
            next_value: 1,
            calls: Some(calls),
        }
    }
}

impl TypedFiniteSourceHandler for ReplaySource {
    type Output = ReplayInput;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if let Some(calls) = &self.calls {
            calls.fetch_add(1, Ordering::SeqCst);
        }
        if self.next_value > 3 {
            return Ok(None);
        }

        let value = self.next_value;
        self.next_value += 1;

        Ok(Some(vec![ReplayInput { value }]))
    }
}

#[derive(Clone, Debug)]
struct SingleReplaySource {
    emitted: bool,
}

impl SingleReplaySource {
    fn new() -> Self {
        Self { emitted: false }
    }
}

impl TypedFiniteSourceHandler for SingleReplaySource {
    type Output = ReplayInput;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ReplayInput { value: 1 }]))
    }
}

#[derive(Clone, Debug)]
struct FanOutTransform;

impl FanOutTransform {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for FanOutTransform {
    type Input = ReplayInput;
    type Output = StageOutputs<ReplayInput>;

    fn process(&self, input: ReplayInput) -> Result<StageOutputs<ReplayInput>, HandlerError> {
        Ok(StageOutputs::many([
            ReplayInput {
                value: input.value * 10 + 1,
            },
            ReplayInput {
                value: input.value * 10 + 2,
            },
        ]))
    }
}

/// The in-process system the counting, failing, reserving and product
/// requests call. The test keeps its own `Arc` to the count and reads it.
#[obzenflow::port]
struct CallCounter {
    calls: Arc<AtomicUsize>,
    offset: u64,
}

fn call_counter(calls: Arc<AtomicUsize>) -> System<CallCounter> {
    System::eager("call-counter", Arc::new(CallCounter { calls, offset: 100 }))
        .expect("a valid system name")
}

#[derive(Clone, Debug, Serialize)]
struct Counting {
    value: u64,
}

#[async_trait]
impl CallRequest for Counting {
    const EFFECT_TYPE: &'static str = "effect_replay.counting";
    type Safety = RequiresKey;
    type Port = CallCounter;
    type Outcome = ReplayEffectValue;

    async fn call(
        &self,
        counter: &CallCounter,
        _ctx: &mut EffectContext,
    ) -> Result<ReplayEffectValue, EffectError> {
        counter.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ReplayEffectValue {
            effect_value: self.value + counter.offset,
        })
    }
}

impl Keyed for Counting {
    fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey(format!("counting:{}", self.value))
    }
}

type CountingEffect = Call<Counting>;

#[obzenflow::port]
struct BlockingGate {
    calls: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
    cancelled_futures: Arc<AtomicUsize>,
    invocations: Arc<Mutex<Vec<BlockingInvocation>>>,
}

#[derive(Clone, Debug, Serialize)]
struct Blocking {
    value: u64,
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
impl CallRequest for Blocking {
    const EFFECT_TYPE: &'static str = "effect_replay.blocking";
    type Safety = RequiresKey;
    type Port = BlockingGate;
    type Outcome = ReplayEffectValue;

    async fn call(
        &self,
        gate: &BlockingGate,
        ctx: &mut EffectContext,
    ) -> Result<ReplayEffectValue, EffectError> {
        gate.calls.fetch_add(1, Ordering::SeqCst);
        gate.invocations
            .lock()
            .expect("blocking invocation lock poisoned")
            .push(BlockingInvocation {
                runtime_flow_id: ctx.flow_id().to_string(),
                stage_key: ctx.stage_key().to_string(),
                input_seq: ctx.now(),
                idempotency_key: format!("blocking:{}", self.value),
            });
        let mut pending = PendingEffectGuard {
            cancelled_futures: gate.cancelled_futures.clone(),
            completed: false,
        };
        let permit = gate.release.acquire().await;
        pending.completed = true;
        let permit =
            permit.map_err(|_| EffectError::Execution("blocking gate closed".to_string()))?;
        drop(permit);
        Ok(ReplayEffectValue {
            effect_value: self.value + 100,
        })
    }
}

impl Keyed for Blocking {
    fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey(format!("blocking:{}", self.value))
    }
}

type BlockingEffect = Call<Blocking>;

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct ReplayTransform {
    #[system]
    counter: System<CallCounter>,
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
            .call(Counting { value: input.value })
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

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct BlockingTransform {
    #[system]
    gate: System<BlockingGate>,
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
            .call(Blocking { value: input.value })
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

/// Completes the physical effect and then deliberately keeps the handler alive.
/// `perform()` returning is the synchronization point: terminal effect evidence
/// must already be durable before cancellation can reach this second gate.
#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct PostPerformBlockingTransform {
    #[system]
    counter: System<CallCounter>,
    performed: Arc<Semaphore>,
    handler_release: Arc<Semaphore>,
}

#[async_trait]
impl EffectfulTransformHandler for PostPerformBlockingTransform {
    type Input = ReplayInput;
    type Output = obzenflow_core::stage_fact_set![ReplayOutput, ReplayEffectValue];
    type AllowedEffects = obzenflow_runtime::effect_set![CountingEffect];

    async fn process(
        &self,
        input: ReplayInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let effect_value = fx
            .call(Counting { value: input.value })
            .await
            .map_err(|error| HandlerError::Other(error.to_string()))?;

        self.performed.add_permits(1);
        let permit = self
            .handler_release
            .acquire()
            .await
            .map_err(|_| HandlerError::Other("post-perform handler gate closed".to_string()))?;
        drop(permit);

        fx.emit(ReplayOutput {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|error| HandlerError::Other(error.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-post-perform-blocking-v1"
    }
}

#[derive(Clone, Debug, Serialize)]
struct AlwaysFailing {
    value: u64,
}

#[async_trait]
impl CallRequest for AlwaysFailing {
    const EFFECT_TYPE: &'static str = "effect_replay.always_failing";
    type Safety = RequiresKey;
    type Port = CallCounter;
    type Outcome = ReplayEffectValue;

    async fn call(
        &self,
        counter: &CallCounter,
        _ctx: &mut EffectContext,
    ) -> Result<ReplayEffectValue, EffectError> {
        counter.calls.fetch_add(1, Ordering::SeqCst);
        Err(EffectError::Execution("simulated_gateway_down".to_string()))
    }
}

impl Keyed for AlwaysFailing {
    fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey(format!("always-failing:{}", self.value))
    }
}

type AlwaysFailingEffect = Call<AlwaysFailing>;

#[derive(Clone, Debug, Default)]
struct ReplayStatefulState {
    outputs: Vec<ReplayOutput>,
}

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct ReplayStateful {
    #[system]
    counter: System<CallCounter>,
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
            .call(Counting { value: input.value })
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct PolicyReserved {
    request_id: u64,
}

impl TypedPayload for PolicyReserved {
    const EVENT_TYPE: &'static str = "effect_replay.policy_reserved";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReservationFailed {
    request_id: u64,
}

impl TypedPayload for ReservationFailed {
    const EVENT_TYPE: &'static str = "effect_replay.reservation_failed";
}

#[derive(Clone, Debug, Serialize)]
struct SlowReserve {
    request_id: u64,
}

#[async_trait]
impl CallRequest for SlowReserve {
    const EFFECT_TYPE: &'static str = "effect_replay.slow_reserve";
    type Safety = Idempotent;
    type Port = CallCounter;
    type Outcome = PolicyReserved;

    async fn call(
        &self,
        counter: &CallCounter,
        _ctx: &mut EffectContext,
    ) -> Result<PolicyReserved, EffectError> {
        counter.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(PolicyReserved {
            request_id: self.request_id,
        })
    }
}

type SlowReserveEffect = Call<SlowReserve>;

#[derive(Clone, Debug, Default)]
struct PolicyStatefulState {
    allocated: Vec<u64>,
}

#[derive(Clone, Debug, obzenflow_core::StageOutputFacts)]
enum PolicyStatefulFact {
    Reserved(PolicyReserved),
    Failed(ReservationFailed),
}

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct PolicyStateful {
    #[system]
    counter: System<CallCounter>,
}

fn is_circuit_breaker_rejection(error: &EffectError) -> bool {
    error.failure_cause().is_some_and(|cause| {
        cause.source.as_str() == "circuit_breaker"
            && matches!(cause.code.as_str(), "circuit_open" | "probe_in_progress")
    })
}

#[async_trait]
impl EffectfulStatefulHandler for PolicyStateful {
    type State = PolicyStatefulState;
    type Input = ReplayInput;
    type Output = PolicyStatefulFact;
    type AllowedEffects = obzenflow_runtime::effect_set![SlowReserveEffect];

    fn initial_state(&self) -> Self::State {
        PolicyStatefulState::default()
    }

    async fn decide(
        &mut self,
        _state: &Self::State,
        input: &ReplayInput,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        match fx
            .call(SlowReserve {
                request_id: input.value,
            })
            .await
        {
            Ok(_) => Ok(fx.complete()?),
            Err(error) if is_circuit_breaker_rejection(&error) => {
                fx.emit(ReservationFailed {
                    request_id: input.value,
                })
                .await?;
                Ok(fx.complete()?)
            }
            Err(error) => Err(error.into()),
        }
    }

    fn apply(&mut self, state: &mut Self::State, fact: Self::Output) -> Result<(), HandlerError> {
        if let PolicyStatefulFact::Reserved(reserved) = fact {
            state.allocated.push(reserved.request_id);
        }
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "effect-replay-policy-stateful-v1"
    }
}

#[derive(Clone, Debug)]
struct ReservationFailureSink {
    failures: Arc<Mutex<Vec<ReservationFailed>>>,
}

#[async_trait]
impl InlineSink for ReservationFailureSink {
    type Input = ReservationFailed;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("Memory".to_string()))
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, failure: ReservationFailed) -> Result<(), SinkWriteFailure> {
        self.failures
            .lock()
            .expect("reservation failures lock poisoned")
            .push(failure);
        Ok(())
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

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct DishonestOneFactStateful {
    #[system]
    counter: System<CallCounter>,
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

        let effect = fx.call(Counting { value: input.value }).await?;
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

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct ErrorAfterCommitStateful {
    #[system]
    counter: System<CallCounter>,
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

        let effect = fx.call(Counting { value: input.value }).await?;
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
#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct ApplyRejectingStateful {
    #[system]
    counter: System<CallCounter>,
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

        let effect = fx.call(Counting { value: input.value }).await?;
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

#[derive(Clone, Debug, Serialize)]
struct Product {
    value: u64,
}

#[async_trait]
impl CallRequest for Product {
    const EFFECT_TYPE: &'static str = "effect_replay.product";
    type Safety = Idempotent;
    type Port = CallCounter;
    type Outcome = ProductOutcome;

    async fn call(
        &self,
        counter: &CallCounter,
        _ctx: &mut EffectContext,
    ) -> Result<ProductOutcome, EffectError> {
        counter.calls.fetch_add(1, Ordering::SeqCst);
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

type ProductEffect = Call<Product>;

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

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct ProductStateful {
    #[system]
    counter: System<CallCounter>,
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
            .call(Product { value: input.value })
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
impl InlineSink for CollectSink {
    type Input = ReplayOutput;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("Memory".to_string()))
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, output: ReplayOutput) -> Result<(), SinkWriteFailure> {
        self.outputs
            .lock()
            .expect("outputs lock poisoned")
            .push(output);
        Ok(())
    }
}

fn build_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let inputs_handler = ReplaySource::new();
        let effectful_handler = ReplayTransform {
            counter: call_counter(calls),
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_suppression",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                effectful = effectful_transform!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
}

fn build_source_limiter_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let inputs_handler = ReplaySource::new();
        let effectful_handler = ReplayTransform {
            counter: call_counter(calls),
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_source_limiter",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput with {
                    rate_limit(1.0)
                } => inputs_handler);
                effectful = effectful_transform!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
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
    FlowDefinition::materialize(move |_runtime_config| {
        let inputs_handler = ReplaySource::new();
        let effectful_handler = ReplayTransform {
            counter: call_counter(calls),
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_fast_limiter",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                effectful = effectful_transform!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } with rate_limit(1000.0) => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
}

fn build_blocking_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
    cancelled_futures: Arc<AtomicUsize>,
    invocations: Arc<Mutex<Vec<BlockingInvocation>>>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let resilience = (
            circuit_breaker().consecutive_failures(1),
            retry()
                .fixed_delay(Duration::from_millis(1))
                .max_attempts(2),
        );
        let inputs_handler = SingleReplaySource::new();
        let gate = BlockingGate {
            calls,
            release,
            cancelled_futures,
            invocations,
        };
        let effectful_handler = BlockingTransform {
            gate: System::eager("blocking-gate", Arc::new(gate)).expect("a valid system name"),
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_blocking",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                effectful = effectful_transform!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } with { resilience.0, resilience.1 } => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
}

fn build_post_perform_blocking_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    performed: Arc<Semaphore>,
    handler_release: Arc<Semaphore>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let resilience = circuit_breaker().consecutive_failures(2);
        let inputs_handler = SingleReplaySource::new();
        let effectful_handler = PostPerformBlockingTransform {
            counter: call_counter(calls),
            performed,
            handler_release,
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_post_perform_blocking",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                effectful = effectful_transform!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } with resilience => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
}

fn build_fan_out_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let resilience = circuit_breaker().consecutive_failures(2);
        let inputs_handler = SingleReplaySource::new();
        let fan_out_handler = FanOutTransform::new();
        let effectful_handler = ReplayTransform {
            counter: call_counter(calls),
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_fan_out",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                fan_out = transform!(ReplayInput -> ReplayInput => fan_out_handler);
                effectful = effectful_transform!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } with resilience => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> fan_out;
                fan_out |> effectful;
                effectful |> collector;
            }
        })
    })
}

fn build_stateful_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let inputs_handler = ReplaySource::new();
        let effectful_handler = ReplayStateful {
            counter: call_counter(calls),
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_stateful",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                effectful = effectful_stateful!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
}

#[derive(Clone, Copy, Debug)]
enum SiblingStages {
    Transform,
    Stateful,
}

struct SiblingSystems {
    eager_calls: Arc<AtomicUsize>,
    deferred_calls: Arc<AtomicUsize>,
    deferred_resolutions: Arc<AtomicUsize>,
    eager_outputs: Arc<Mutex<Vec<ReplayOutput>>>,
    deferred_outputs: Arc<Mutex<Vec<ReplayOutput>>>,
}

impl SiblingSystems {
    fn new() -> Self {
        Self {
            eager_calls: Arc::new(AtomicUsize::new(0)),
            deferred_calls: Arc::new(AtomicUsize::new(0)),
            deferred_resolutions: Arc::new(AtomicUsize::new(0)),
            eager_outputs: Arc::new(Mutex::new(Vec::new())),
            deferred_outputs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn eager(&self) -> System<CallCounter> {
        System::eager(
            "eager-counter",
            Arc::new(CallCounter {
                calls: self.eager_calls.clone(),
                offset: 100,
            }),
        )
        .expect("a valid system name")
    }

    fn deferred(&self) -> System<CallCounter> {
        let calls = self.deferred_calls.clone();
        let resolutions = self.deferred_resolutions.clone();
        System::deferred("deferred-counter", move || {
            resolutions.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(CallCounter {
                calls: calls.clone(),
                offset: 200,
            }))
        })
        .expect("a valid system name")
    }
}

fn build_sibling_systems_flow(
    journal_base: PathBuf,
    stages: SiblingStages,
    systems: &SiblingSystems,
) -> FlowDefinition {
    let eager = systems.eager();
    let deferred = systems.deferred();
    let eager_outputs = systems.eager_outputs.clone();
    let deferred_outputs = systems.deferred_outputs.clone();
    FlowDefinition::materialize(move |_runtime_config| {
        let inputs_handler = ReplaySource::new();
        let eager_collector_handler = CollectSink {
            outputs: eager_outputs,
        };
        let deferred_collector_handler = CollectSink {
            outputs: deferred_outputs,
        };

        match stages {
            SiblingStages::Transform => {
                let eager_handler = ReplayTransform { counter: eager };
                let deferred_handler = ReplayTransform { counter: deferred };
                Ok(flow! {
                    name: "effect_replay_sibling_transforms",
                    journals: disk_journals(journal_base),

                    stages: {
                        inputs = source!(ReplayInput => inputs_handler);
                        eager_effectful = effectful_transform!(
                            ReplayInput -> { ReplayOutput, ReplayEffectValue } => eager_handler);
                        deferred_effectful = effectful_transform!(
                            ReplayInput -> { ReplayOutput, ReplayEffectValue } => deferred_handler);
                        eager_collector = sink!(ReplayOutput => eager_collector_handler);
                        deferred_collector = sink!(ReplayOutput => deferred_collector_handler);
                    },

                    topology: {
                        inputs |> eager_effectful;
                        inputs |> deferred_effectful;
                        eager_effectful |> eager_collector;
                        deferred_effectful |> deferred_collector;
                    }
                })
            }
            SiblingStages::Stateful => {
                let eager_handler = ReplayStateful { counter: eager };
                let deferred_handler = ReplayStateful { counter: deferred };
                Ok(flow! {
                    name: "effect_replay_sibling_stateful",
                    journals: disk_journals(journal_base),

                    stages: {
                        inputs = source!(ReplayInput => inputs_handler);
                        eager_effectful = effectful_stateful!(
                            ReplayInput -> { ReplayOutput, ReplayEffectValue } => eager_handler);
                        deferred_effectful = effectful_stateful!(
                            ReplayInput -> { ReplayOutput, ReplayEffectValue } => deferred_handler);
                        eager_collector = sink!(ReplayOutput => eager_collector_handler);
                        deferred_collector = sink!(ReplayOutput => deferred_collector_handler);
                    },

                    topology: {
                        inputs |> eager_effectful;
                        inputs |> deferred_effectful;
                        eager_effectful |> eager_collector;
                        deferred_effectful |> deferred_collector;
                    }
                })
            }
        }
    })
}

fn build_policy_stateful_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    failures: Arc<Mutex<Vec<ReservationFailed>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let reserve_resilience = circuit_breaker()
            .count_window(1)
            .minimum_calls(1)
            .slow_call_duration(Duration::from_millis(1))
            .slow_call_rate_threshold(1.0)
            .open_for(Duration::from_secs(60))
            .probes(1);
        let inputs_handler = ReplaySource::new();
        let allocator = PolicyStateful {
            counter: call_counter(calls),
        };
        let failure_sink = ReservationFailureSink { failures };

        Ok(flow! {
            name: "effect_replay_policy_stateful",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                allocate = effectful_stateful!(
                    ReplayInput -> { PolicyReserved, ReservationFailed }
                    with reserve_resilience
                    => allocator);
                reservation_failures = sink!(ReservationFailed => failure_sink.idempotent());
            },

            topology: {
                inputs |> allocate;
                allocate |> reservation_failures;
            }
        })
    })
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
    FlowDefinition::materialize(move |_runtime_config| {
        let inputs_handler = ReplaySource::new();
        let effectful_handler = DishonestOneFactStateful {
            counter: call_counter(calls),
            decide_inputs,
            apply_calls,
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_stateful",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                effectful = effectful_stateful!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
}

fn build_error_after_commit_stateful_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    observed_fact_counts: Arc<Mutex<Vec<usize>>>,
    applied: Arc<Mutex<Vec<String>>>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let inputs_handler = ReplaySource::new();
        let effectful_handler = ErrorAfterCommitStateful {
            counter: call_counter(calls),
            observed_fact_counts,
            applied,
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_error_after_commit_stateful",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                effectful = effectful_stateful!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
}

fn build_apply_rejection_stateful_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    decide_inputs: Arc<Mutex<Vec<u64>>>,
    apply_attempts: Arc<Mutex<Vec<String>>>,
    reject_apply: bool,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let inputs_handler = ReplaySource::new();
        let effectful_handler = ApplyRejectingStateful {
            counter: call_counter(calls),
            decide_inputs,
            apply_attempts,
            reject_apply,
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_apply_rejection_stateful",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                effectful = effectful_stateful!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
}

fn build_product_stateful_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    applied: Arc<Mutex<Vec<String>>>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let inputs_handler = ReplaySource::new();
        let effectful_handler = ProductStateful {
            counter: call_counter(calls),
            applied,
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_product_stateful",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                effectful = effectful_stateful!(
                    ReplayInput -> { ReplayOutput, ProductFirst, ProductSecond } => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
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
    // Stage frames may reference definitions carried by the system journal.
    // Re-encode the retained archive before removing its lifecycle records.
    obzenflow_infra::testing::journal::retain_archive_frames(run_dir, |path, _| {
        path.file_name().and_then(|name| name.to_str()) != Some(system_journal)
    })
    .expect("incomplete fixture must retain readable stage journals");
}

fn truncate_stage_before_effect_results(run_dir: &Path, stage_key: &str) {
    let manifest = archive_manifest(run_dir);
    let stage_journal = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .expect("manifest should contain stage data journal file");
    // FLOWIP-120b: successful effect outcomes are domain `Data` facts carrying
    // non-framework effect provenance. Failure/capture compatibility rows remain
    // framework-owned reserved event types. An interrupted journal is a physical
    // prefix: stop before the first outcome frame, including every later output.
    // Removing outcomes alone would leave holes in the committed causal chain.
    let effect_outcome_type = ReplayEffectValue::event_type_name();
    let reached_outcome = std::cell::Cell::new(false);
    obzenflow_infra::testing::journal::retain_archive_frames(run_dir, |path, records| {
        if path.file_name().and_then(|name| name.to_str()) != Some(stage_journal) {
            return true;
        }
        if records.iter().any(|record| {
            record
                .pointer("/envelope/provenance/event/event_type")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|event_type| {
                    obzenflow_core::event::payloads::effect_payload::is_framework_effect_event_type(
                        event_type,
                    ) || event_type == effect_outcome_type
                })
        }) {
            reached_outcome.set(true);
        }
        !reached_outcome.get()
    })
    .expect("missing-outcome fixture must retain readable complete frames");
    assert!(
        reached_outcome.get(),
        "fixture must contain effect outcomes"
    );
}

async fn read_stage_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
    let manifest = archive_manifest(run_dir);
    let stage_journal = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .expect("manifest should contain stage data journal file");
    replay_testkit::read_journal_envelopes::<ChainEvent>(&run_dir.join(stage_journal))
        .await
        .into_iter()
        .map(|envelope| envelope.authored())
        .collect()
}

async fn stage_binding_identities(
    run_dir: &Path,
    stage_key: &str,
) -> std::collections::HashSet<obzenflow_core::EffectBindingIdentity> {
    read_stage_events(run_dir, stage_key)
        .await
        .iter()
        .filter_map(|event| event.effect_provenance.as_ref())
        .filter(|provenance| provenance.fact_owner.is_user())
        .map(|provenance| provenance.descriptor.binding.clone())
        .collect()
}

fn effect_values(outputs: &Mutex<Vec<ReplayOutput>>) -> Vec<u64> {
    let mut values: Vec<u64> = outputs
        .lock()
        .expect("outputs lock poisoned")
        .iter()
        .map(|output| output.effect_value)
        .collect();
    values.sort_unstable();
    values
}

async fn read_stage_error_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
    let manifest = archive_manifest(run_dir);
    let stage_journal = manifest["stages"][stage_key]["error_journal_file"]
        .as_str()
        .expect("manifest should contain stage error journal file");
    replay_testkit::read_journal_envelopes::<ChainEvent>(&run_dir.join(stage_journal))
        .await
        .into_iter()
        .map(|envelope| envelope.authored())
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
    let effect_type = ReplayEffectValue::event_type_name();
    let output_type = ReplayOutput::event_type_name();
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
            event.payload,
            ChainPayload::FlowControl(FlowControlPayload::Eof { .. })
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
            ReplayEffectValue::event_type_name(),
            ReplayOutput::event_type_name(),
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
    let reports = replay_testkit::read_stage_envelopes_appended(run_dir, "effectful")
        .await
        .into_iter();
    let mut failure_count = 0;
    for report in reports {
        let ChainPayload::Execution(ExecutionPayload::StageLifecycle(event)) = &report.payload
        else {
            continue;
        };
        if event.stage_id() != stage_id {
            continue;
        }

        match event {
            StageLifecycleFact::Failed {
                error,
                accounting: metrics,
                ..
            } => {
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
            StageLifecycleFact::Completed { .. } | StageLifecycleFact::Drained { .. } => {
                panic!("a stateful contract violation must not reach a successful terminal state")
            }
            StageLifecycleFact::Running { .. }
            | StageLifecycleFact::Draining { .. }
            | StageLifecycleFact::Cancelled { .. } => {}
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
        &event.payload,
        ChainPayload::Execution(ExecutionPayload::EffectRecord(_))
    ) && event
        .effect_provenance
        .as_ref()
        .is_some_and(|provenance| provenance.fact_owner.is_framework())
}

fn is_domain_effect_outcome_fact(event: &ChainEvent) -> bool {
    event.is_fact()
        && event.event_type() == ReplayEffectValue::event_type_name()
        && event
            .effect_provenance
            .as_ref()
            .is_some_and(|provenance| provenance.fact_owner.is_user())
}

fn framework_effect_record(event: &ChainEvent) -> Option<EffectRecord> {
    match &event.payload {
        ChainPayload::Execution(ExecutionPayload::EffectRecord(record))
            if event.event_type() == record.event_type()
                && event
                    .effect_provenance
                    .as_ref()
                    .is_some_and(|provenance| provenance.fact_owner.is_framework()) =>
        {
            Some(record.clone())
        }
        _ => None,
    }
}

fn recorded_effect_cursor(event: &ChainEvent) -> Option<EffectCursor> {
    if let Some(record) = framework_effect_record(event) {
        return Some(record.cursor);
    }
    if event.consumes_data_credit() {
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
                event.payload,
                ChainPayload::Execution(ExecutionPayload::Recovery(
                    RecoveryFact::RetryScheduled { .. }
                        | RecoveryFact::RetrySucceeded { .. }
                        | RecoveryFact::RetryExhausted { .. }
                        | RecoveryFact::RetryStoppedNonRetryable { .. }
                ))
            )
        })
        .count()
}

async fn circuit_breaker_recovery_completed_events_in_stage(
    run_dir: &Path,
    stage_key: &str,
) -> usize {
    circuit_breaker_recovery_completions_in_stage(run_dir, stage_key)
        .await
        .len()
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RecoveryCompletionEvidence {
    cursor: EffectCursor,
    total_attempts: u32,
    backoff_elapsed_ms: u64,
    recovery_elapsed_ms: u64,
}

async fn circuit_breaker_recovery_completions_in_stage(
    run_dir: &Path,
    stage_key: &str,
) -> Vec<RecoveryCompletionEvidence> {
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter_map(|event| match event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(
                RecoveryFact::RecoveryCompleted {
                    cursor,
                    total_attempts,
                    backoff_elapsed_ms,
                    recovery_elapsed_ms,
                },
            )) => Some(RecoveryCompletionEvidence {
                cursor,
                total_attempts,
                backoff_elapsed_ms,
                recovery_elapsed_ms,
            }),
            _ => None,
        })
        .collect()
}

fn exported_chain_events(run_dir: &Path, output: &Path) -> Vec<ChainEvent> {
    obzenflow_infra::journal::disk::inspect::export_jsonl(run_dir, Some(output))
        .expect("run should export through the supported JSONL projection");
    exported_jsonl::chain_events(
        &std::fs::read_to_string(output).expect("JSONL projection should be readable"),
    )
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
                event.payload,
                ChainPayload::Execution(ExecutionPayload::RateLimiter(_))
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
    let manifest = archive_manifest(run_dir);
    let flow_id = manifest["flow_id"].as_str().expect("flow identity");
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter_map(|event| event.envelope.observability)
        // Archived samples keep their original identity during replay. Only
        // captures made by this execution describe new limiter activity.
        .filter(|packet| packet.capture.capture_scope.flow_id.to_string() == flow_id)
        .filter_map(|packet| packet.runtime)
        .filter_map(|runtime| runtime.rate_limiter)
        .fold((0u64, 0u64, 0f64), |(ev, dl, ds), rl| {
            (
                ev.max(rl.events_total),
                dl.max(rl.delayed_total),
                ds.max(rl.delay_seconds_total),
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
    let manifest = archive_manifest(run_dir);
    let flow_id = manifest["flow_id"].as_str().expect("flow identity");
    read_stage_events(run_dir, stage_key)
        .await
        .into_iter()
        .filter_map(|event| event.envelope.observability)
        // Archived samples keep their original identity during replay. Only
        // captures made by this execution describe new limiter activity.
        .filter(|packet| packet.capture.capture_scope.flow_id.to_string() == flow_id)
        .filter_map(|packet| packet.runtime)
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
    let completion_evidence =
        circuit_breaker_recovery_completions_in_stage(&archive_dir, "effectful").await;
    let completion_cursors: std::collections::HashSet<_> = completion_evidence
        .iter()
        .map(|completion| completion.cursor.clone())
        .collect();
    assert_eq!(completion_evidence.len(), 2);
    assert_eq!(
        completion_cursors,
        effect_cursors.iter().cloned().collect(),
        "each fan-out cursor must own one independently correlated terminal clock row"
    );
    assert!(completion_evidence.iter().all(|completion| {
        completion.total_attempts == 1
            && completion.backoff_elapsed_ms == 0
            && completion.recovery_elapsed_ms >= completion.backoff_elapsed_ms
    }));
    let replay_output_count = effectful_events
        .iter()
        .filter(|event| event.event_type() == ReplayOutput::event_type_name())
        .count();
    let effect_value_count = effectful_events
        .iter()
        .filter(|event| event.event_type() == ReplayEffectValue::event_type_name())
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
            journal_base.clone(),
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
    let replay_archive = latest_run_dir(&journal_base);
    assert_eq!(
        circuit_breaker_recovery_completions_in_stage(&replay_archive, "effectful").await,
        completion_evidence,
        "replay must rematerialise each cursor's archived terminal evidence unchanged"
    );
}

async fn assert_sibling_stages_call_their_own_systems(stages: SiblingStages) {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live = SiblingSystems::new();
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_sibling_systems_flow(
            journal_base.clone(),
            stages,
            &live,
        ))
        .await
        .expect("live sibling flow should complete");

    assert_eq!(
        (
            live.eager_calls.load(Ordering::SeqCst),
            live.deferred_calls.load(Ordering::SeqCst),
            live.deferred_resolutions.load(Ordering::SeqCst),
        ),
        (3, 3, 1),
        "each system answers its own stage's inputs, and the deferred one resolves once"
    );
    assert_eq!(effect_values(&live.eager_outputs), vec![101, 102, 103]);
    assert_eq!(effect_values(&live.deferred_outputs), vec![201, 202, 203]);

    let archive_dir = latest_run_dir(&journal_base);
    let eager_bindings = stage_binding_identities(&archive_dir, "eager_effectful").await;
    let deferred_bindings = stage_binding_identities(&archive_dir, "deferred_effectful").await;
    assert_eq!(
        (eager_bindings.len(), deferred_bindings.len()),
        (1, 1),
        "every call in a stage records the stage's one system"
    );
    assert_ne!(
        eager_bindings, deferred_bindings,
        "sibling stages record the systems their own handlers hold"
    );

    let replay = SiblingSystems::new();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_sibling_systems_flow(journal_base, stages, &replay))
        .await
        .expect("replay sibling flow should complete");

    assert_eq!(
        (
            replay.eager_calls.load(Ordering::SeqCst),
            replay.deferred_calls.load(Ordering::SeqCst),
            replay.deferred_resolutions.load(Ordering::SeqCst),
        ),
        (0, 0, 0),
        "replay must neither resolve nor call either system"
    );
    assert_eq!(effect_values(&replay.eager_outputs), vec![101, 102, 103]);
    assert_eq!(effect_values(&replay.deferred_outputs), vec![201, 202, 203]);
}

#[tokio::test]
async fn sibling_transform_stages_of_one_handler_type_call_their_own_systems() {
    assert_sibling_stages_call_their_own_systems(SiblingStages::Transform).await;
}

#[tokio::test]
async fn sibling_stateful_stages_of_one_handler_type_call_their_own_systems() {
    assert_sibling_stages_call_their_own_systems(SiblingStages::Stateful).await;
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
                &event.payload,
                ChainPayload::Fact(_) if event.event_type() == ReplayOutput::event_type_name()
            )
        })
        .count();
    let eof_writer_seq = effectful_events
        .iter()
        .find_map(|event| match &event.payload {
            ChainPayload::FlowControl(FlowControlPayload::Eof { writer_seq, .. }) => {
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
async fn cancellation_after_perform_preserves_atomic_terminal_evidence_and_resume_replays_it() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let performed = Arc::new(Semaphore::new(0));
    let handler_release = Arc::new(Semaphore::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    let flow_handle = build_post_perform_blocking_flow(
        journal_base.clone(),
        live_calls.clone(),
        performed.clone(),
        handler_release,
        live_outputs.clone(),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .expect("post-perform blocking flow should build");

    let performed_permit = tokio::time::timeout(Duration::from_secs(10), performed.acquire())
        .await
        .expect("perform should return before cancellation")
        .expect("performed gate should remain open");
    drop(performed_permit);
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);
    assert!(live_outputs
        .lock()
        .expect("outputs lock poisoned")
        .is_empty());

    flow_handle
        .stop_graceful(Duration::from_millis(100))
        .await
        .expect("graceful stop request should be accepted");
    tokio::time::timeout(Duration::from_secs(5), flow_handle.wait_for_completion())
        .await
        .expect("post-perform cancellation should finish bounded cleanup")
        .expect("cancelled flow should resolve");
    // The interrupted process ends, releasing its journal writers (B7).
    drop(flow_handle);

    let interrupted_archive = latest_run_dir(&journal_base);
    let interrupted_events = read_stage_events(&interrupted_archive, "effectful").await;
    let outcome_cursors: Vec<_> = interrupted_events
        .iter()
        .filter(|event| is_domain_effect_outcome_fact(event))
        .filter_map(recorded_effect_cursor)
        .collect();
    let completions =
        circuit_breaker_recovery_completions_in_stage(&interrupted_archive, "effectful").await;
    assert_eq!(outcome_cursors.len(), 1);
    assert_eq!(completions.len(), 1);
    assert_eq!(
        completions[0].cursor, outcome_cursors[0],
        "the terminal outcome and recovery clock row must be committed for the same cursor"
    );
    assert_eq!(completions[0].total_attempts, 1);

    mark_archive_incomplete(&interrupted_archive);

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_performed = Arc::new(Semaphore::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--resume-from"),
            interrupted_archive.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(build_post_perform_blocking_flow(
            journal_base.clone(),
            replay_calls.clone(),
            replay_performed,
            Arc::new(Semaphore::new(1)),
            replay_outputs.clone(),
        ))
        .await
        .expect("resume should reuse the atomically recorded effect outcome");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "resume must not repeat an effect whose terminal group committed"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        vec![ReplayOutput {
            value: 1,
            effect_value: 101,
        }]
    );
    let replay_archive = latest_run_dir(&journal_base);
    assert_eq!(
        read_stage_events(&replay_archive, "effectful")
            .await
            .iter()
            .filter(|event| is_domain_effect_outcome_fact(event))
            .count(),
        1
    );
    assert_eq!(
        circuit_breaker_recovery_completions_in_stage(&replay_archive, "effectful").await,
        completions,
        "resume must rematerialise the archived recovery completion unchanged"
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
    // The interrupted process ends, releasing its journal writers (B7).
    drop(flow_handle);
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
            OsString::from("--resume-from"),
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
            .filter(|event| event.event_type() == ReplayOutput::event_type_name())
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
    // old replay-admission bug: replay could run live limiter admission for
    // every recorded event without journalling its `Delayed` lifecycle rows,
    // pacing the whole replay while the journal stayed silent. The runtime
    // counters embedded in wide events do capture it.
    //
    // The limiter embeds tokens-consumed (`rl_events_total`), delayed-event
    // (`rl_delayed_total`), and delay-seconds (`rl_delay_seconds_total`) totals via
    // the instrumentation snapshotter. Strict replay must leave every one at zero on
    // every stage. Original attached captures are retained without being relabelled
    // as new activity. The live run above proves the source limiter is active
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

#[test]
fn stateful_domain_translation_classifies_live_and_replayed_policy_rejection_only() {
    let cause = obzenflow_core::event::EffectFailureCause {
        source: obzenflow_core::event::EffectFailureSource::new("circuit_breaker"),
        code: obzenflow_core::event::EffectFailureCode::new("circuit_open"),
    };
    let live = EffectError::BoundaryRejected {
        rejected_by: cause.source.clone(),
        code: cause.code.clone(),
        message: "open".to_string(),
        retry: obzenflow_core::event::RetryDisposition::Retryable,
    };
    let replayed = EffectError::RecordedFailure {
        error_type: "boundary_rejected".into(),
        error_message: "open".to_string(),
        retry: obzenflow_core::event::RetryDisposition::Retryable,
        cause: Some(cause),
        detail: None,
    };

    assert!(is_circuit_breaker_rejection(&live));
    assert!(is_circuit_breaker_rejection(&replayed));
    assert!(!is_circuit_breaker_rejection(&EffectError::Timeout(
        "warehouse timeout".to_string()
    )));
    assert!(!is_circuit_breaker_rejection(
        &EffectError::RecordedFailure {
            error_type: "timeout".into(),
            error_message: "warehouse timeout".to_string(),
            retry: obzenflow_core::event::RetryDisposition::Retryable,
            cause: None,
            detail: None,
        }
    ));
}

#[tokio::test]
async fn effectful_stateful_policy_rejection_authors_flat_domain_fact_live_and_replay() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_failures = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_policy_stateful_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_failures.clone(),
        ))
        .await
        .expect("live policy-protected stateful flow should complete");

    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        1,
        "the first slow success opens the breaker and later calls are rejected"
    );
    let live_domain_failures = live_failures
        .lock()
        .expect("live reservation failures lock poisoned")
        .clone();
    assert_eq!(
        live_domain_failures,
        vec![
            ReservationFailed { request_id: 2 },
            ReservationFailed { request_id: 3 },
        ]
    );

    let archive_dir = latest_run_dir(&journal_base);
    let allocation_events = read_stage_events(&archive_dir, "allocate").await;
    let failed_effect_records = allocation_events
        .iter()
        .filter_map(|event| match &event.payload {
            ChainPayload::Execution(ExecutionPayload::EffectRecord(record))
                if event.event_type() == "effect.execution_rejected" =>
            {
                Some(record.clone())
            }
            _ => None,
        })
        .filter(|record| {
            matches!(
                &record.outcome,
                EffectOutcomePayload::Failed {
                    cause: Some(cause),
                    ..
                } if cause.source.as_str() == "circuit_breaker"
                    && cause.code.as_str() == "circuit_open"
            )
        })
        .count();
    assert_eq!(failed_effect_records, 2);
    assert_eq!(
        allocation_events
            .iter()
            .filter(|event| event.event_type() == ReservationFailed::event_type_name())
            .count(),
        2,
        "policy rejection must be accompanied by one flat domain fact"
    );

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_failures = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_policy_stateful_flow(
            journal_base,
            replay_calls.clone(),
            replay_failures.clone(),
        ))
        .await
        .expect("strict replay should reconstruct policy-protected stateful decisions");

    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        replay_failures
            .lock()
            .expect("replay reservation failures lock poisoned")
            .clone(),
        live_domain_failures
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
            ReplayEffectValue::event_type_name(),
            ReplayOutput::event_type_name(),
        ],
        "the manual marker must be demonstrably false before exercising its runtime backstop"
    );
    let missing_output_type = ReplayOutput::event_type_name();

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
    truncate_stage_before_effect_results(&archive_dir, "effectful");
    mark_archive_incomplete(&archive_dir);

    let resume_calls = Arc::new(AtomicUsize::new(0));
    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--resume-from"),
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
    let original_flow_id = archive_manifest(&archive_dir)["flow_id"]
        .as_str()
        .expect("manifest should contain original flow id")
        .to_string();
    let cursors: Vec<_> = read_stage_events(&resume_archive, "effectful")
        .await
        .iter()
        .filter(|event| is_domain_effect_outcome_fact(event))
        .filter_map(recorded_effect_cursor)
        .collect();
    assert_eq!(cursors.len(), 3);
    assert!(
        cursors
            .iter()
            .all(|cursor| cursor.recorded_flow_id.to_string() == original_flow_id),
        "resume should append every effect outcome using the archived recorded_flow_id cursor"
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
    truncate_stage_before_effect_results(&archive_dir, "effectful");
    mark_archive_incomplete(&archive_dir);

    let resume_calls = Arc::new(AtomicUsize::new(0));
    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--resume-from"),
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
            OsString::from("--resume-from"),
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

/// FLOWIP-134i B13: a replay never executes an effect. A replay of an archive
/// torn inside an effect stage stops at the end of that stage's recording,
/// names the stage, the input and the run to resume, and journals the stop.
#[tokio::test]
async fn incomplete_replay_stops_at_the_end_of_its_recording() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_calls.clone(),
            Arc::new(Mutex::new(Vec::new())),
        ))
        .await
        .expect("live flow should complete");
    assert_eq!(live_calls.load(Ordering::SeqCst), 3);

    let archive_dir = latest_run_dir(&journal_base);
    truncate_stage_before_effect_results(&archive_dir, "effectful");
    mark_archive_incomplete(&archive_dir);

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    let error = FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(build_flow(
            journal_base.clone(),
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect_err("a replay stops at the end of its recording");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "a replay never executes an effect"
    );
    assert!(replay_outputs
        .lock()
        .expect("outputs lock poisoned")
        .is_empty());

    let replay_run = latest_run_dir(&journal_base);
    assert_ne!(replay_run, archive_dir);
    let ended: Vec<_> = read_stage_error_events(&replay_run, "effectful")
        .await
        .into_iter()
        .filter_map(|event| match event.payload {
            ChainPayload::Execution(ExecutionPayload::StageFatalRecorded(fatal))
                if fatal.reason == obzenflow_core::event::StageFatalReason::RecordingEnded =>
            {
                Some(fatal)
            }
            _ => None,
        })
        .collect();
    assert_eq!(ended.len(), 1, "the replay journals one end of recording");
    let input = ended[0]
        .input_position
        .expect("the stop names the input it reached");

    match &error {
        ApplicationError::ReplayRecordingEnded {
            replayed,
            stage_key,
            input_position,
            resume,
        } => {
            assert_eq!(replayed, &archive_dir);
            assert_eq!(stage_key, "effectful");
            assert_eq!(*input_position, Some(input));
            assert_eq!(
                resume,
                &std::fs::canonicalize(&archive_dir).expect("the archive resolves"),
                "the unclaimed archive is its own head"
            );
        }
        other => panic!("expected the end-of-recording refusal, got: {other}"),
    }
    let rendered = error.to_string();
    assert!(
        rendered.contains(&format!(
            "stage 'effectful' reached the end of its recording at input {input}"
        )),
        "{rendered}"
    );
    assert!(
        rendered.contains(&format!(
            "Resume {} to finish this run's work.",
            std::fs::canonicalize(&archive_dir)
                .expect("the archive resolves")
                .display()
        )),
        "{rendered}"
    );
}

// ============================================================================
// End-to-end requests over a host trait port, answered by an eager or a
// deferred system the handler holds in a `#[system]` field.
// ============================================================================

/// A host-provided port that the greeting request calls.
#[obzenflow::port]
trait GreetingPort: Send + Sync {
    fn greet(&self, request: &Greet) -> Result<ReplayEffectValue, EffectError>;
}

struct DoublingGreetingPort {
    calls: Arc<AtomicUsize>,
}

impl GreetingPort for DoublingGreetingPort {
    fn greet(&self, request: &Greet) -> Result<ReplayEffectValue, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(ReplayEffectValue {
            effect_value: request.value * 2,
        })
    }
}

/// An idempotent request answered through the greeting port.
#[derive(Clone, Debug, Serialize)]
struct Greet {
    value: u64,
}

#[async_trait]
impl CallRequest for Greet {
    const EFFECT_TYPE: &'static str = "effect_port_supply.ported";
    type Safety = Idempotent;
    type Port = dyn GreetingPort;
    type Outcome = ReplayEffectValue;

    async fn call(
        &self,
        greeter: &dyn GreetingPort,
        _ctx: &mut EffectContext,
    ) -> Result<ReplayEffectValue, EffectError> {
        greeter.greet(self)
    }
}

type PortedEffect = Call<Greet>;

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct PortedTransform {
    #[system]
    greeting: System<dyn GreetingPort>,
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
            .call(Greet { value: input.value })
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
    source_calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
    greeting: System<dyn GreetingPort>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let inputs_handler = ReplaySource::counted(source_calls);
        let ported_handler = PortedTransform { greeting };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_port_supply",
            journals: disk_journals(journal_base),
            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                ported = effectful_transform!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } => ported_handler
                );
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> ported;
                ported |> collector;
            }
        })
    })
}

fn greeting_system(calls: Arc<AtomicUsize>) -> System<dyn GreetingPort> {
    System::eager("greeting", Arc::new(DoublingGreetingPort { calls }))
        .expect("a valid system name")
}

fn deferred_greeting_system(
    resolver_calls: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
) -> System<dyn GreetingPort> {
    System::deferred("greeting", move || {
        resolver_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(DoublingGreetingPort {
            calls: calls.clone(),
        }))
    })
    .expect("a valid system name")
}

#[tokio::test]
async fn a_request_over_a_host_port_runs_live_and_strict_replays() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let calls = Arc::new(AtomicUsize::new(0));
    let source_calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_ported_flow(
            journal_base.clone(),
            source_calls.clone(),
            outputs.clone(),
            greeting_system(calls.clone()),
        ))
        .await
        .expect("a flow whose handler holds the greeting system should materialise and run");

    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let live_source_calls = source_calls.load(Ordering::SeqCst);
    assert!(live_source_calls > 0, "the live source must be polled");
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

    let archive = latest_run_dir(&journal_base);
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive.as_os_str().to_os_string(),
            OsString::from("--verify"),
        ])
        .run_async(build_ported_flow(
            journal_base,
            source_calls.clone(),
            replay_outputs.clone(),
            greeting_system(calls.clone()),
        ))
        .await
        .expect("strict replay must complete from the recorded outcomes");

    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "strict replay must not call the greeting port"
    );
    assert_eq!(
        source_calls.load(Ordering::SeqCst),
        live_source_calls,
        "strict replay must not poll the live source"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        domain,
        "strict replay must reproduce the live outputs"
    );
}

#[tokio::test]
async fn a_system_reused_by_a_second_flow_registers_its_port_again() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let resolver_calls = Arc::new(AtomicUsize::new(0));
    let effect_calls = Arc::new(AtomicUsize::new(0));
    let greeting = deferred_greeting_system(resolver_calls.clone(), effect_calls.clone());
    let reused_greeting = greeting.clone();
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_ported_flow(
            journal_base.clone(),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(Vec::new())),
            greeting,
        ))
        .await
        .expect("the first flow registers the system and runs");
    assert_eq!(resolver_calls.load(Ordering::SeqCst), 1);
    assert_eq!(effect_calls.load(Ordering::SeqCst), 3);

    let second_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_ported_flow(
            journal_base,
            Arc::new(AtomicUsize::new(0)),
            second_outputs.clone(),
            reused_greeting,
        ))
        .await
        .expect("a second flow registers the same system again and runs");
    assert_eq!(
        resolver_calls.load(Ordering::SeqCst),
        2,
        "each run resolves the deferred port once"
    );
    assert_eq!(effect_calls.load(Ordering::SeqCst), 6);
    assert_eq!(
        second_outputs.lock().expect("outputs lock poisoned").len(),
        3
    );
}

// --- Transactional request whose port commits through the handle it is given

/// The ledger a transactional request posts to. It commits the outcome
/// through the handle `ctx.transaction(self)` returns.
#[obzenflow::port]
#[async_trait]
trait LedgerPort: Send + Sync {
    async fn post(
        &self,
        value: u64,
        tx: EffectCommitHandle<ReplayEffectValue>,
    ) -> Result<ReplayEffectValue, EffectError>;
}

#[derive(Clone, Debug, Serialize)]
struct LedgerPost {
    value: u64,
}

#[async_trait]
impl CallRequest for LedgerPost {
    const EFFECT_TYPE: &'static str = "effect_port_supply.ledger";
    type Safety = Transactional;
    type Port = dyn LedgerPort;
    type Outcome = ReplayEffectValue;

    async fn call(
        &self,
        ledger: &dyn LedgerPort,
        ctx: &mut EffectContext,
    ) -> Result<ReplayEffectValue, EffectError> {
        let tx = ctx.transaction(self)?;
        ledger.post(self.value, tx).await
    }
}

type LedgerEffect = Call<LedgerPost>;

struct CountingLedger {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl LedgerPort for CountingLedger {
    async fn post(
        &self,
        value: u64,
        tx: EffectCommitHandle<ReplayEffectValue>,
    ) -> Result<ReplayEffectValue, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let output = ReplayEffectValue {
            effect_value: value + 1_000,
        };
        tx.commit_success(&output).await?;
        Ok(output)
    }
}

fn ledger_system(calls: Arc<AtomicUsize>) -> System<dyn LedgerPort> {
    System::eager("ledger_tx", Arc::new(CountingLedger { calls })).expect("a valid system name")
}

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct LedgerTransform {
    #[system]
    ledger: System<dyn LedgerPort>,
}

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
            .call(LedgerPost { value: input.value })
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
    ledger: System<dyn LedgerPort>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let resilience = circuit_breaker().consecutive_failures(2);
        let inputs_handler = SingleReplaySource::new();
        let ledger_handler = LedgerTransform { ledger };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_port_supply_transactional",
            journals: disk_journals(journal_base),
            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                ledger = effectful_transform!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } with resilience => ledger_handler.transactional::<LedgerEffect>()
                );
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> ledger;
                ledger |> collector;
            }
        })
    })
}

#[tokio::test]
async fn transactional_request_commits_through_its_port_and_replays_the_record() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let port_calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_transactional_flow(
            journal_base.clone(),
            outputs.clone(),
            ledger_system(port_calls.clone()),
        ))
        .await
        .expect("a transactional request whose port commits should materialise and run");

    assert_eq!(
        port_calls.load(Ordering::SeqCst),
        1,
        "the ledger port should run exactly once"
    );
    let domain = outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        domain,
        vec![ReplayOutput {
            value: 1,
            effect_value: 1_001
        }]
    );

    let live_archive = latest_run_dir(&journal_base);
    let ledger_events = read_stage_events(&live_archive, "ledger").await;
    let outcome_cursors: Vec<_> = ledger_events
        .iter()
        .filter(|event| is_domain_effect_outcome_fact(event))
        .filter_map(recorded_effect_cursor)
        .collect();
    let completions = circuit_breaker_recovery_completions_in_stage(&live_archive, "ledger").await;
    assert_eq!(outcome_cursors.len(), 1);
    assert_eq!(completions.len(), 1);
    assert_eq!(completions[0].cursor, outcome_cursors[0]);
    assert_eq!(completions[0].total_attempts, 1);
    let exported_transactional_completions: Vec<_> =
        exported_chain_events(&live_archive, &temp.path().join("transactional-live.jsonl"))
            .into_iter()
            .filter_map(|event| match event.payload {
                ChainPayload::Execution(ExecutionPayload::Recovery(
                    RecoveryFact::RecoveryCompleted {
                        cursor,
                        total_attempts,
                        backoff_elapsed_ms,
                        recovery_elapsed_ms,
                    },
                )) => Some(RecoveryCompletionEvidence {
                    cursor,
                    total_attempts,
                    backoff_elapsed_ms,
                    recovery_elapsed_ms,
                }),
                _ => None,
            })
            .collect();
    assert_eq!(exported_transactional_completions, completions);

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_transactional_flow(
            journal_base.clone(),
            replay_outputs.clone(),
            ledger_system(replay_calls.clone()),
        ))
        .await
        .expect("transactional replay should use the atomically recorded outcome");
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        domain
    );
    let replay_archive = latest_run_dir(&journal_base);
    assert_eq!(
        circuit_breaker_recovery_completions_in_stage(&replay_archive, "ledger").await,
        completions,
        "transactional replay must rematerialise archived terminal evidence unchanged"
    );
    let exported_replay_completions: Vec<_> = exported_chain_events(
        &replay_archive,
        &temp.path().join("transactional-replay.jsonl"),
    )
    .into_iter()
    .filter_map(|event| match event.payload {
        ChainPayload::Execution(ExecutionPayload::Recovery(RecoveryFact::RecoveryCompleted {
            cursor,
            total_attempts,
            backoff_elapsed_ms,
            recovery_elapsed_ms,
        })) => Some(RecoveryCompletionEvidence {
            cursor,
            total_attempts,
            backoff_elapsed_ms,
            recovery_elapsed_ms,
        }),
        _ => None,
    })
    .collect();
    assert_eq!(
        exported_replay_completions, completions,
        "the supported replay JSONL projection must preserve the archived terminal evidence"
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

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct FailFastTransform {
    #[system]
    counter: System<CallCounter>,
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
        let outcome = fx.call(AlwaysFailing { value: input.value }).await;

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
#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct MultiEffectFailFastTransform {
    #[system]
    counter: System<CallCounter>,
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
        let outcome = fx.call(AlwaysFailing { value: input.value }).await;

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
    FlowDefinition::materialize(move |_runtime_config| {
        let resilience = circuit_breaker().consecutive_failures(1);
        let inputs_handler = ReplaySource::new();
        let effectful_handler = FailFastTransform {
            counter: call_counter(calls),
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_fail_fast_rejection",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                effectful = effectful_transform!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue } with resilience => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
}

fn build_multi_effect_per_effect_breaker_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let resilience = circuit_breaker()
            .consecutive_failures(1)
            .for_effect::<AlwaysFailingEffect>();
        let inputs_handler = ReplaySource::new();
        let effectful_handler = MultiEffectFailFastTransform {
            counter: call_counter(calls),
        };
        let collector_handler = CollectSink { outputs };

        Ok(flow! {
            name: "effect_replay_multi_effect_breaker",
            journals: disk_journals(journal_base),

            stages: {
                inputs = source!(ReplayInput => inputs_handler);
                effectful = effectful_transform!(
                    ReplayInput -> { ReplayOutput, ReplayEffectValue }
                    with resilience
                    => effectful_handler);
                collector = sink!(ReplayOutput => collector_handler);
            },

            topology: {
                inputs |> effectful;
                effectful |> collector;
            }
        })
    })
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
/// attaches inline to the effect it guards (`Effect with policy`), so a
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
        .filter_map(|event| event.envelope.observability?.runtime)
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
        .find(|(effect, _)| effect == AlwaysFailing::EFFECT_TYPE)
        .map(|(_, opened)| *opened)
        .unwrap_or(0);
    assert!(
        opened_for_failing > 0,
        "the protected effect's breaker must open under its own effect key, got {effect_breakers:?}"
    );
    assert!(
        !effect_breakers
            .iter()
            .any(|(effect, _)| effect == Counting::EFFECT_TYPE),
        "the unprotected effect must carry no breaker instance, got {effect_breakers:?}"
    );
}

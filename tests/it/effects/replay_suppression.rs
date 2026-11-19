// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::replay_testkit;

use async_trait::async_trait;
use obzenflow::stages::stateful::fold;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_stateful, flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{perform, EffectError, Effects, IdempotencyKey, Keyed, Port};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulStatefulHandler, TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

type KeyLog = Arc<Mutex<Vec<IdempotencyKey>>>;

/// The in-process system the counting, failing, reserving and product
/// requests call. The test keeps its own `Arc` to the count and reads it.
struct CallCounter {
    calls: Arc<AtomicUsize>,
    keys: KeyLog,
    offset: u64,
}

#[obzenflow::port(system = "call-counter")]
impl CallCounter {
    #[effect(type = "effect_replay.counting", exactly_once)]
    fn count(
        &self,
        request: &Counting,
        key: &IdempotencyKey,
    ) -> Result<ReplayEffectValue, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.keys.lock().expect("key log").push(key.clone());
        Ok(ReplayEffectValue {
            effect_value: request.value + self.offset,
        })
    }

    #[effect(type = "effect_replay.always_failing", exactly_once)]
    fn fail(
        &self,
        _: &AlwaysFailing,
        key: &IdempotencyKey,
    ) -> Result<ReplayEffectValue, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.keys.lock().expect("key log").push(key.clone());
        Err(EffectError::Execution("simulated_gateway_down".to_string()))
    }

    #[effect(type = "effect_replay.slow_reserve", exactly_once)]
    async fn reserve(&self, request: &SlowReserve) -> Result<PolicyReserved, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(PolicyReserved {
            request_id: request.request_id,
        })
    }

    #[effect(type = "effect_replay.product", exactly_once)]
    fn product(
        &self,
        request: &Product,
    ) -> Result<obzenflow::schema::outcome![(ProductFirst, ProductSecond)], EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(obzenflow::schema::outcome((
            ProductFirst {
                value: request.value + 100,
            },
            ProductSecond {
                value: request.value + 200,
            },
        )))
    }
}

fn call_counter(calls: Arc<AtomicUsize>) -> Port<CallCounter> {
    recording_call_counter(calls, KeyLog::default())
}

fn recording_call_counter(calls: Arc<AtomicUsize>, keys: KeyLog) -> Port<CallCounter> {
    Port::to(CallCounter {
        calls,
        keys,
        offset: 100,
    })
}

#[derive(Clone, Debug, Serialize)]
struct Counting {
    value: u64,
}

impl Keyed for Counting {
    fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey(format!("counting:{}", self.value))
    }
}

#[derive(Clone, Debug, Serialize)]
struct AlwaysFailing {
    value: u64,
}

impl Keyed for AlwaysFailing {
    fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey(format!("always-failing:{}", self.value))
    }
}

#[derive(Clone, Debug, Default)]
struct ReplayStatefulState {
    outputs: Vec<ReplayOutput>,
}

#[obzenflow::command]
#[derive(Clone, Debug)]
struct ReplayStateful {
    #[port(Counting)]
    counter: Port<CallCounter>,
}

#[async_trait]
impl EffectfulStatefulHandler for ReplayStateful {
    type State = ReplayStatefulState;
    type Input = ReplayInput;
    type Output = obzenflow::schema::outcome![ReplayEffectValue | ReplayOutput];

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
        fold!(match fact {
            output @ ReplayOutput { .. } => {
                state.outputs.push(output)
            }
            _ => {}
        });
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

#[derive(Clone, Debug, Serialize)]
struct SlowReserve {
    request_id: u64,
}

fn is_circuit_breaker_rejection(error: &EffectError) -> bool {
    error.failure_cause().is_some_and(|cause| {
        cause.source.as_str() == "circuit_breaker"
            && matches!(cause.code.as_str(), "circuit_open" | "probe_in_progress")
    })
}

// ---------------------------------------------------------------------------
// FLOWIP-120m: product outcomes fold per fact on the stateful surface
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

#[derive(Clone, Debug, Serialize)]
struct Product {
    value: u64,
}

#[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
trait CollectOutputPort: Send + Sync {
    #[effect(type = "effect_replay.collect", at_least_once, records = reply)]
    fn collect(&self, output: &ReplayOutput) -> Result<(), EffectError>;
}

/// Keeps every output its sink is called with. Replay makes no call.
struct CollectSink {
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
}

#[obzenflow_runtime::effects::system("replay-output-collector", effects = obzenflow_runtime::effects)]
impl CollectOutputPort for CollectSink {
    fn collect(&self, output: &ReplayOutput) -> Result<(), EffectError> {
        self.outputs
            .lock()
            .expect("outputs lock poisoned")
            .push(output.clone());
        Ok(())
    }
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
        let collector_handler = perform(|output: &ReplayOutput| output.clone())
            .through(Port::to(CollectSink { outputs }));

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

#[tokio::test]
async fn strict_effectful_stateful_replay_suppresses_effect_execution() {
    let _guard = effect_replay_test_guard().await;
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    replay_testkit::unsynced_application()
        .with_cli_args(["obzenflow"])
        .run_async(build_stateful_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live stateful flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        *live_outputs.lock().expect("outputs lock poisoned"),
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
    replay_testkit::unsynced_application()
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
    assert!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .is_empty(),
        "replay calls no sink system"
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

// ============================================================================
// End-to-end requests over a host trait port, answered by an eager or a
// deferred system behind the port the command holds.
// ============================================================================

// --- Transactional request whose port commits through the handle it is given

// ---------------------------------------------------------------------------
// Plain fail-fast circuit-breaker rejections: a prevented call surfaces as a
// recorded rejection error, never a synthesized fact (FLOWIP-120z step 1).
// ---------------------------------------------------------------------------

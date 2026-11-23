// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use obzenflow_adapters::middleware::stateful_observer;
use obzenflow_adapters::sinks::RecordingSink;
use obzenflow_adapters::sources::ValuesSource;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, stateful, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handlers::{
    StatefulEmission, TypedFiniteSourceHandler, TypedStatefulHandler,
};
use obzenflow_runtime::stages::observer::{StatefulObserver, StatefulObserverContext};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NumberEvent {
    value: u64,
}

impl TypedPayload for NumberEvent {
    const EVENT_TYPE: &'static str = "stateful_patterns.number_event";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CountResult {
    total_count: u64,
}

impl TypedPayload for CountResult {
    const EVENT_TYPE: &'static str = "stateful_patterns.count_result";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CollectedValue {
    value: u64,
}

impl TypedPayload for CollectedValue {
    const EVENT_TYPE: &'static str = "stateful_patterns.collected_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SumResult {
    total_sum: u64,
}

impl TypedPayload for SumResult {
    const EVENT_TYPE: &'static str = "stateful_patterns.sum_result";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProgressUpdate {
    current_count: u64,
}

impl TypedPayload for ProgressUpdate {
    const EVENT_TYPE: &'static str = "stateful_patterns.progress_update";
}

fn numbers(max: u64) -> impl TypedFiniteSourceHandler<Output = NumberEvent> {
    ValuesSource::new(
        (1..=max)
            .map(|value| NumberEvent { value })
            .collect::<Vec<_>>(),
    )
}

#[derive(Clone, Debug, Default)]
struct CounterState {
    count: u64,
}

#[derive(Debug, Clone)]
struct CounterHandler;

impl CounterHandler {
    fn new() -> Self {
        Self
    }
}

impl TypedStatefulHandler for CounterHandler {
    type State = CounterState;
    type Input = NumberEvent;
    type Output = CountResult;

    fn accumulate(&self, state: &mut Self::State, _event: NumberEvent) {
        state.count += 1;
    }

    fn initial_state(&self) -> Self::State {
        CounterState::default()
    }

    fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output> {
        StatefulEmission::RetainEpoch {
            outputs: vec![CountResult {
                total_count: state.count,
            }],
        }
    }

    fn post_emit(&self, _: &mut Self::State) {}
}

#[derive(Debug, Clone)]
struct AccumulatorHandler;

impl AccumulatorHandler {
    fn new() -> Self {
        Self
    }
}

impl TypedStatefulHandler for AccumulatorHandler {
    type State = Vec<u64>;
    type Input = NumberEvent;
    type Output = CollectedValue;

    fn accumulate(&self, state: &mut Self::State, event: NumberEvent) {
        state.push(event.value);
    }

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output> {
        StatefulEmission::RetainEpoch {
            outputs: state
                .iter()
                .copied()
                .map(|value| CollectedValue { value })
                .collect(),
        }
    }

    fn post_emit(&self, _: &mut Self::State) {}
}

#[derive(Debug, Clone)]
struct SumHandler;

impl SumHandler {
    fn new() -> Self {
        Self
    }
}

impl TypedStatefulHandler for SumHandler {
    type State = u64;
    type Input = NumberEvent;
    type Output = SumResult;

    fn accumulate(&self, state: &mut Self::State, event: NumberEvent) {
        *state += event.value;
    }

    fn initial_state(&self) -> Self::State {
        0
    }

    fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output> {
        StatefulEmission::RetainEpoch {
            outputs: vec![SumResult { total_sum: *state }],
        }
    }

    fn post_emit(&self, _: &mut Self::State) {}
}

#[derive(Debug, Clone)]
struct ImmediateEmitter;

struct CountsNonEmptyStatefulEmits {
    calls: Arc<AtomicUsize>,
}

impl StatefulObserver for CountsNonEmptyStatefulEmits {
    fn after_state_emit(&self, _ctx: &StatefulObserverContext<'_>, outputs: &[ChainEvent]) {
        assert!(
            !outputs.is_empty(),
            "after_state_emit is defined only for non-empty emitted batches"
        );
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

impl ImmediateEmitter {
    fn new() -> Self {
        Self
    }
}

impl TypedStatefulHandler for ImmediateEmitter {
    type State = u64;
    type Input = NumberEvent;
    type Output = ProgressUpdate;

    fn accumulate(&self, state: &mut Self::State, _event: NumberEvent) {
        *state += 1;
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        *state > 0
    }

    fn emit(&self, state: &Self::State) -> StatefulEmission<Self::Output> {
        StatefulEmission::RetainEpoch {
            outputs: vec![ProgressUpdate {
                current_count: *state,
            }],
        }
    }

    fn post_emit(&self, _: &mut Self::State) {}

    fn initial_state(&self) -> Self::State {
        0
    }

    fn drain(&self, _state: &Self::State) -> Vec<Self::Output> {
        Vec::new()
    }
}

#[tokio::test]
async fn counter_emits_single_event_on_drain() {
    let (sink_handler, results) = RecordingSink::<CountResult>::new();

    crate::replay_testkit::unsynced_application()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let source_handler = numbers(5);
            let counter_handler = CounterHandler::new();

            Ok(flow! {
                name: "pattern_counter_test",
                journals: disk_journals(std::path::PathBuf::from("target/stateful_patterns_test_counter")),

                stages: {
                    src = source!(NumberEvent => source_handler);
                    counter = stateful!(NumberEvent -> CountResult => counter_handler);
                    sink = sink!(CountResult => sink_handler);
                },

                topology: {
                    src |> counter;
                    counter |> sink;
                }
            })
        }))
    .await
    .expect("flow should complete");
    let results = results.items();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].total_count, 5);
}

#[tokio::test]
async fn accumulator_emits_one_event_per_input_on_drain() {
    let (sink_handler, results) = RecordingSink::<CollectedValue>::new();

    crate::replay_testkit::unsynced_application()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let source_handler = numbers(5);
            let accumulator_handler = AccumulatorHandler::new();

            Ok(flow! {
                name: "pattern_accumulator_test",
                journals: disk_journals(std::path::PathBuf::from("target/stateful_patterns_test_accumulator")),

                stages: {
                    src = source!(NumberEvent => source_handler);
                    acc = stateful!(NumberEvent -> CollectedValue => accumulator_handler);
                    sink = sink!(CollectedValue => sink_handler);
                },

                topology: {
                    src |> acc;
                    acc |> sink;
                }
            })
        }))
    .await
    .expect("flow should complete");
    assert_eq!(results.len(), 5);
}

#[tokio::test]
async fn sum_handler_emits_aggregated_result_on_drain() {
    let (sink_handler, results) = RecordingSink::<SumResult>::new();

    crate::replay_testkit::unsynced_application()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let source_handler = numbers(10);
            let sum_handler = SumHandler::new();

            Ok(flow! {
                name: "pattern_sum_test",
                journals: disk_journals(std::path::PathBuf::from("target/stateful_patterns_test_sum")),

                stages: {
                    src = source!(NumberEvent => source_handler);
                    summer = stateful!(NumberEvent -> SumResult => sum_handler);
                    sink = sink!(SumResult => sink_handler);
                },

                topology: {
                    src |> summer;
                    summer |> sink;
                }
            })
        }))
        .await
        .expect("flow should complete");
    let results = results.items();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].total_sum, 55);
}

#[tokio::test]
async fn immediate_emitter_emits_during_accumulating() {
    let (sink_handler, results) = RecordingSink::<ProgressUpdate>::new();
    let observer_calls = Arc::new(AtomicUsize::new(0));
    let observer_calls_for_flow = observer_calls.clone();

    crate::replay_testkit::unsynced_application()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let source_handler = numbers(5);
            let emitter_handler = ImmediateEmitter::new();

            Ok(flow! {
                name: "pattern_immediate_test",
                journals: disk_journals(std::path::PathBuf::from("target/stateful_patterns_test_immediate")),

                stages: {
                    src = source!(NumberEvent => source_handler);
                    emitter = stateful!(NumberEvent -> ProgressUpdate with { stateful_observer(
                            "non-empty-stateful-emits",
                            CountsNonEmptyStatefulEmits {
                                calls: observer_calls_for_flow,
                            }
                        ) } => emitter_handler);
                    sink = sink!(ProgressUpdate => sink_handler);
                },

                topology: {
                    src |> emitter;
                    emitter |> sink;
                }
            })
        }))
    .await
    .expect("flow should complete");
    assert_eq!(results.len(), 5);
    assert_eq!(
        observer_calls.load(Ordering::SeqCst),
        results.len(),
        "the empty final drain must not manufacture an after_state_emit occurrence"
    );
}

#[tokio::test]
async fn empty_source_still_triggers_drain_for_stateful_handler() {
    let (sink_handler, results) = RecordingSink::<CountResult>::new();

    crate::replay_testkit::unsynced_application()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let source_handler = numbers(0);
            let counter_handler = CounterHandler::new();

            Ok(flow! {
                name: "pattern_empty_test",
                journals: disk_journals(std::path::PathBuf::from("target/stateful_patterns_test_empty")),

                stages: {
                    src = source!(NumberEvent => source_handler);
                    counter = stateful!(NumberEvent -> CountResult => counter_handler);
                    sink = sink!(CountResult => sink_handler);
                },

                topology: {
                    src |> counter;
                    counter |> sink;
                }
            })
        }))
    .await
    .expect("flow should complete");
    let results = results.items();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].total_count, 0);
}

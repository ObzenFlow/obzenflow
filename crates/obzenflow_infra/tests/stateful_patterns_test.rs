// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, stateful, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    StatefulEmission, TypedFiniteSourceHandler, TypedStatefulHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};

/// File-local payload for the stateful-patterns test. The JSON shape
/// matches what `NumberSource` emits; the type fingerprints the stage
/// contract per FLOWIP-114c.
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

#[derive(Clone, Debug)]
struct NumberSource {
    current: u64,
    max: u64,
}

impl NumberSource {
    fn new(max: u64) -> Self {
        Self { current: 1, max }
    }
}

impl TypedFiniteSourceHandler for NumberSource {
    type Output = NumberEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.current <= self.max {
            let num = self.current;
            self.current += 1;
            Ok(Some(vec![NumberEvent { value: num }]))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug)]
struct EmptySource;

impl EmptySource {
    fn new() -> Self {
        Self
    }
}

impl TypedFiniteSourceHandler for EmptySource {
    type Output = NumberEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }
}

#[derive(Debug)]
struct CollectingSink<T> {
    events: Arc<Mutex<Vec<ChainEvent>>>,
    _input: std::marker::PhantomData<fn() -> T>,
}

impl<T> Clone for CollectingSink<T> {
    fn clone(&self) -> Self {
        Self {
            events: Arc::clone(&self.events),
            _input: std::marker::PhantomData,
        }
    }
}

impl<T> CollectingSink<T> {
    fn new(events: Arc<Mutex<Vec<ChainEvent>>>) -> Self {
        Self {
            events,
            _input: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T> InlineSink for CollectingSink<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Input = T;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        input: T,
        _context: SinkWriteContext,
    ) -> std::result::Result<SinkWriteReport, HandlerError> {
        self.events
            .lock()
            .unwrap()
            .push(input.to_event(WriterId::from(StageId::new())));
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Custom(
                "collect".to_string(),
            ),
            None,
        )))
    }
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

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: state.clone(),
            outputs: vec![CountResult {
                total_count: state.count,
            }],
        })
    }
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

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: state.clone(),
            outputs: state
                .iter()
                .copied()
                .map(|value| CollectedValue { value })
                .collect(),
        })
    }
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

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: *state,
            outputs: vec![SumResult { total_sum: *state }],
        })
    }
}

#[derive(Debug, Clone)]
struct ImmediateEmitter;

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

    fn emit(
        &self,
        state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: *state,
            outputs: vec![ProgressUpdate {
                current_count: *state,
            }],
        })
    }

    fn initial_state(&self) -> Self::State {
        0
    }

    fn drain(&self, _state: &Self::State) -> Result<Vec<Self::Output>, HandlerError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
async fn counter_emits_single_event_on_drain() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_flow = events.clone();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let source_handler = NumberSource::new(5);
            let counter_handler = CounterHandler::new();
            let sink_handler = CollectingSink::<CountResult>::new(events_for_flow);

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
    let events = events.lock().unwrap();
    let results: Vec<_> = events
        .iter()
        .filter(|e| e.event_type() == CountResult::versioned_event_type())
        .collect();
    assert_eq!(results.len(), 1);
    let total = results[0].payload()["total_count"].as_u64().unwrap();
    assert_eq!(total, 5);
}

#[tokio::test]
async fn accumulator_emits_one_event_per_input_on_drain() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_flow = events.clone();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let source_handler = NumberSource::new(5);
            let accumulator_handler = AccumulatorHandler::new();
            let sink_handler = CollectingSink::<CollectedValue>::new(events_for_flow);

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
    let events = events.lock().unwrap();
    let results: Vec<_> = events
        .iter()
        .filter(|e| e.event_type() == CollectedValue::versioned_event_type())
        .collect();
    assert_eq!(results.len(), 5);
}

#[tokio::test]
async fn sum_handler_emits_aggregated_result_on_drain() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_flow = events.clone();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let source_handler = NumberSource::new(10);
            let sum_handler = SumHandler::new();
            let sink_handler = CollectingSink::<SumResult>::new(events_for_flow);

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
    let events = events.lock().unwrap();
    let results: Vec<_> = events
        .iter()
        .filter(|e| e.event_type() == SumResult::versioned_event_type())
        .collect();
    assert_eq!(results.len(), 1);
    let total = results[0].payload()["total_sum"].as_u64().unwrap();
    assert_eq!(total, 55);
}

#[tokio::test]
async fn immediate_emitter_emits_during_accumulating() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_flow = events.clone();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let source_handler = NumberSource::new(5);
            let emitter_handler = ImmediateEmitter::new();
            let sink_handler = CollectingSink::<ProgressUpdate>::new(events_for_flow);

            Ok(flow! {
                name: "pattern_immediate_test",
                journals: disk_journals(std::path::PathBuf::from("target/stateful_patterns_test_immediate")),

                stages: {
                    src = source!(NumberEvent => source_handler);
                    emitter = stateful!(NumberEvent -> ProgressUpdate => emitter_handler);
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
    let events = events.lock().unwrap();
    let results: Vec<_> = events
        .iter()
        .filter(|e| e.event_type() == ProgressUpdate::versioned_event_type())
        .collect();
    assert!(!results.is_empty());
}

#[tokio::test]
async fn empty_source_still_triggers_drain_for_stateful_handler() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_flow = events.clone();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let source_handler = EmptySource::new();
            let counter_handler = CounterHandler::new();
            let sink_handler = CollectingSink::<CountResult>::new(events_for_flow);

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
    let events = events.lock().unwrap();
    let results: Vec<_> = events
        .iter()
        .filter(|e| e.event_type() == CountResult::versioned_event_type())
        .collect();
    assert_eq!(results.len(), 1);
    let total = results[0].payload()["total_count"].as_u64().unwrap();
    assert_eq!(total, 0);
}

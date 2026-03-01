// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    WriterId,
};
use obzenflow_dsl::{flow, sink, source, stateful};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, StatefulHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde_json::json;

#[derive(Clone, Debug)]
struct NumberSource {
    current: u64,
    max: u64,
    writer_id: WriterId,
}

impl NumberSource {
    fn new(max: u64) -> Self {
        Self {
            current: 1,
            max,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for NumberSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.current <= self.max {
            let num = self.current;
            self.current += 1;
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                "number",
                json!({ "value": num }),
            )]))
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

impl FiniteSourceHandler for EmptySource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
struct CollectingSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
}

impl CollectingSink {
    fn new() -> (Self, Arc<Mutex<Vec<ChainEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                events: events.clone(),
            },
            events,
        )
    }
}

#[async_trait]
impl SinkHandler for CollectingSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<
        obzenflow_core::event::payloads::delivery_payload::DeliveryPayload,
        HandlerError,
    > {
        self.events.lock().unwrap().push(event);
        Ok(
            obzenflow_core::event::payloads::delivery_payload::DeliveryPayload::success(
                "test",
                obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Custom(
                    "collect".to_string(),
                ),
                None,
            ),
        )
    }
}

#[derive(Clone, Debug, Default)]
struct CounterState {
    count: u64,
}

#[derive(Debug, Clone)]
struct CounterHandler {
    writer_id: WriterId,
}

impl CounterHandler {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for CounterHandler {
    type State = CounterState;

    fn accumulate(&mut self, state: &mut Self::State, _event: ChainEvent) {
        state.count += 1;
    }

    fn initial_state(&self) -> Self::State {
        CounterState::default()
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            "count_result",
            json!({ "total_count": state.count }),
        )])
    }
}

#[derive(Debug, Clone)]
struct AccumulatorHandler {
    writer_id: WriterId,
}

impl AccumulatorHandler {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for AccumulatorHandler {
    type State = Vec<u64>;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        if let Some(value) = event.payload()["value"].as_u64() {
            state.push(value);
        }
    }

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(state
            .iter()
            .map(|&value| {
                ChainEventFactory::data_event(
                    self.writer_id,
                    "collected_value",
                    json!({ "value": value }),
                )
            })
            .collect())
    }
}

#[derive(Debug, Clone)]
struct SumHandler {
    writer_id: WriterId,
}

impl SumHandler {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for SumHandler {
    type State = u64;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let value = event.payload()["value"].as_u64().unwrap_or(0);
        *state += value;
    }

    fn initial_state(&self) -> Self::State {
        0
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            "sum_result",
            json!({ "total_sum": *state }),
        )])
    }
}

#[derive(Debug, Clone)]
struct ImmediateEmitter {
    writer_id: WriterId,
}

impl ImmediateEmitter {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for ImmediateEmitter {
    type State = u64;

    fn accumulate(&mut self, state: &mut Self::State, _event: ChainEvent) {
        *state += 1;
    }

    fn should_emit(&self, _state: &Self::State) -> bool {
        true
    }

    fn emit(&self, state: &mut Self::State) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            "progress_update",
            json!({ "current_count": *state }),
        )])
    }

    fn initial_state(&self) -> Self::State {
        0
    }

    fn create_events(
        &self,
        _state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![])
    }
}

#[tokio::test]
async fn counter_emits_single_event_on_drain() {
    let (sink, events) = CollectingSink::new();

    FlowApplication::run(flow! {
        name: "pattern_counter_test",
        journals: disk_journals(std::path::PathBuf::from("target/stateful_patterns_test_counter")),
        middleware: [],

        stages: {
            src = source!("numbers" => NumberSource::new(5));
            counter = stateful!("counter" => CounterHandler::new());
            sink = sink!("sink" => sink);
        },

        topology: {
            src |> counter;
            counter |> sink;
        }
    })
    .await
    .expect("flow should complete");
    let events = events.lock().unwrap();
    let results: Vec<_> = events
        .iter()
        .filter(|e| e.event_type() == "count_result")
        .collect();
    assert_eq!(results.len(), 1);
    let total = results[0].payload()["total_count"].as_u64().unwrap();
    assert_eq!(total, 5);
}

#[tokio::test]
async fn accumulator_emits_one_event_per_input_on_drain() {
    let (sink, events) = CollectingSink::new();

    FlowApplication::run(flow! {
        name: "pattern_accumulator_test",
        journals: disk_journals(std::path::PathBuf::from("target/stateful_patterns_test_accumulator")),
        middleware: [],

        stages: {
            src = source!("numbers" => NumberSource::new(5));
            acc = stateful!("accumulator" => AccumulatorHandler::new());
            sink = sink!("sink" => sink);
        },

        topology: {
            src |> acc;
            acc |> sink;
        }
    })
    .await
    .expect("flow should complete");
    let events = events.lock().unwrap();
    let results: Vec<_> = events
        .iter()
        .filter(|e| e.event_type() == "collected_value")
        .collect();
    assert_eq!(results.len(), 5);
}

#[tokio::test]
async fn sum_handler_emits_aggregated_result_on_drain() {
    let (sink, events) = CollectingSink::new();

    FlowApplication::run(flow! {
        name: "pattern_sum_test",
        journals: disk_journals(std::path::PathBuf::from("target/stateful_patterns_test_sum")),
        middleware: [],

        stages: {
            src = source!("numbers" => NumberSource::new(10));
            summer = stateful!("sum" => SumHandler::new());
            sink = sink!("sink" => sink);
        },

        topology: {
            src |> summer;
            summer |> sink;
        }
    })
    .await
    .expect("flow should complete");
    let events = events.lock().unwrap();
    let results: Vec<_> = events
        .iter()
        .filter(|e| e.event_type() == "sum_result")
        .collect();
    assert_eq!(results.len(), 1);
    let total = results[0].payload()["total_sum"].as_u64().unwrap();
    assert_eq!(total, 55);
}

#[tokio::test]
async fn immediate_emitter_emits_during_accumulating() {
    let (sink, events) = CollectingSink::new();

    FlowApplication::run(flow! {
        name: "pattern_immediate_test",
        journals: disk_journals(std::path::PathBuf::from("target/stateful_patterns_test_immediate")),
        middleware: [],

        stages: {
            src = source!("numbers" => NumberSource::new(5));
            emitter = stateful!("emitter" => ImmediateEmitter::new());
            sink = sink!("sink" => sink);
        },

        topology: {
            src |> emitter;
            emitter |> sink;
        }
    })
    .await
    .expect("flow should complete");
    let events = events.lock().unwrap();
    let results: Vec<_> = events
        .iter()
        .filter(|e| e.event_type() == "progress_update")
        .collect();
    assert!(!results.is_empty());
}

#[tokio::test]
async fn empty_source_still_triggers_drain_for_stateful_handler() {
    let (sink, events) = CollectingSink::new();

    FlowApplication::run(flow! {
        name: "pattern_empty_test",
        journals: disk_journals(std::path::PathBuf::from("target/stateful_patterns_test_empty")),
        middleware: [],

        stages: {
            src = source!("empty" => EmptySource::new());
            counter = stateful!("counter" => CounterHandler::new());
            sink = sink!("sink" => sink);
        },

        topology: {
            src |> counter;
            counter |> sink;
        }
    })
    .await
    .expect("flow should complete");
    let events = events.lock().unwrap();
    let results: Vec<_> = events
        .iter()
        .filter(|e| e.event_type() == "count_result")
        .collect();
    assert_eq!(results.len(), 1);
    let total = results[0].payload()["total_count"].as_u64().unwrap();
    assert_eq!(total, 0);
}

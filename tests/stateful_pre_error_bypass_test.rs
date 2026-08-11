// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115g regression: stateful supervisors, not handler wrappers, own
//! pre-error bypass.

use async_trait::async_trait;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, stateful, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkHandler, StatefulEmission, TypedFiniteSourceHandler, TypedStatefulHandler,
    TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input {
    value: u64,
}

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "stateful_pre_error.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Aggregate {
    count: usize,
}

impl TypedPayload for Aggregate {
    const EVENT_TYPE: &'static str = "stateful_pre_error.aggregate";
}

#[derive(Clone, Debug)]
struct ThreeRows {
    next: u64,
}

impl ThreeRows {
    fn new() -> Self {
        Self { next: 1 }
    }
}

impl TypedFiniteSourceHandler for ThreeRows {
    type Output = Input;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next > 3 {
            return Ok(None);
        }
        let value = self.next;
        self.next += 1;
        Ok(Some(vec![Input { value }]))
    }
}

#[derive(Clone, Debug)]
struct RejectTwo;

impl RejectTwo {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for RejectTwo {
    type Input = Input;
    type Output = Input;

    fn process(&self, input: Input) -> Result<Input, HandlerError> {
        if input.value == 2 {
            return Err(HandlerError::Domain("rejected value 2".to_string()));
        }
        Ok(input)
    }
}

#[derive(Clone, Debug)]
struct GuardedAccumulator {
    calls: Arc<AtomicUsize>,
}

impl TypedStatefulHandler for GuardedAccumulator {
    type State = usize;
    type Input = Input;
    type Output = Aggregate;

    fn accumulate(&self, state: &mut Self::State, _input: Input) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *state += 1;
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
            outputs: vec![Aggregate { count: *state }],
        })
    }
}

#[derive(Clone, Debug)]
struct CollectSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
}

#[async_trait]
impl SinkHandler for CollectSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        self.events.lock().expect("collector lock").push(event);
        Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

fn build_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    collected: Arc<Mutex<Vec<ChainEvent>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let input_handler = ThreeRows::new();
        let validate_handler = RejectTwo::new();
        let aggregate_handler = GuardedAccumulator { calls };
        let output_handler = CollectSink { events: collected };

        Ok(flow! {
            name: "stateful_pre_error_bypass",
            journals: disk_journals(journal_base),

            stages: {
                input = source!(Input => input_handler);
                validate = transform!(Input -> Input => validate_handler);
                aggregate = stateful!(Input -> Aggregate => aggregate_handler);
                output = sink!(Aggregate => output_handler);
            },

            topology: {
                input |> validate;
                validate |> aggregate;
                aggregate |> output;
            }
        })
    })
}

#[tokio::test]
async fn stateful_handler_never_receives_an_upstream_pre_error_row() {
    let temp = tempfile::tempdir().expect("tempdir");
    let calls = Arc::new(AtomicUsize::new(0));
    let collected = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            temp.path().join("journals"),
            calls.clone(),
            collected.clone(),
        ))
        .await
        .expect("stateful flow with one in-band business error must complete");

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "only the two successful rows reach stateful accumulation"
    );
    let aggregates: Vec<_> = collected
        .lock()
        .expect("collector lock")
        .iter()
        .filter_map(Aggregate::from_event)
        .collect();
    assert_eq!(aggregates.len(), 1);
    assert_eq!(aggregates[0].count, 2);
}

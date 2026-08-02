// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115g regression: stateful supervisors, not handler wrappers, own
//! pre-error bypass.

use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, stateful, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, StatefulHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
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
    writer_id: WriterId,
}

impl ThreeRows {
    fn new() -> Self {
        Self {
            next: 1,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for ThreeRows {
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next > 3 {
            return Ok(None);
        }
        let value = self.next;
        self.next += 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            Input::EVENT_TYPE,
            json!(Input { value }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct RejectTwo {
    writer_id: WriterId,
}

impl RejectTwo {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl TransformHandler for RejectTwo {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let input = Input::from_event(&event)
            .ok_or_else(|| HandlerError::Deserialization("missing Input payload".to_string()))?;
        if input.value == 2 {
            return Err(HandlerError::Domain("rejected value 2".to_string()));
        }
        Ok(vec![ChainEventFactory::derived_data_event(
            self.writer_id,
            &event,
            Input::EVENT_TYPE,
            json!(input),
            obzenflow_core::config::LineagePolicy::default(),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct GuardedAccumulator {
    calls: Arc<AtomicUsize>,
    unexpected_error_calls: Arc<AtomicUsize>,
    writer_id: WriterId,
}

#[async_trait]
impl StatefulHandler for GuardedAccumulator {
    type State = usize;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            self.unexpected_error_calls.fetch_add(1, Ordering::SeqCst);
        }
        *state += 1;
    }

    fn initial_state(&self) -> Self::State {
        0
    }

    fn create_events(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            Aggregate::EVENT_TYPE,
            json!(Aggregate { count: *state }),
        )])
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
    unexpected_error_calls: Arc<AtomicUsize>,
    collected: Arc<Mutex<Vec<ChainEvent>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let input_handler = ThreeRows::new();
        let validate_handler = RejectTwo::new();
        let aggregate_handler = GuardedAccumulator {
            calls,
            unexpected_error_calls,
            writer_id: WriterId::from(StageId::new()),
        };
        let output_handler = CollectSink { events: collected };

        Ok(flow! {
            name: "stateful_pre_error_bypass",
            journals: disk_journals(journal_base),
            middleware: [],

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
    let unexpected_error_calls = Arc::new(AtomicUsize::new(0));
    let collected = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            temp.path().join("journals"),
            calls.clone(),
            unexpected_error_calls.clone(),
            collected.clone(),
        ))
        .await
        .expect("stateful flow with one in-band business error must complete");

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "only the two successful rows reach stateful accumulation"
    );
    assert_eq!(
        unexpected_error_calls.load(Ordering::SeqCst),
        0,
        "the stateful supervisor must bypass the pre-error row"
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

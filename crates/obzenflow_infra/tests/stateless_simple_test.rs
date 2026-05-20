// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    id::StageId,
    WriterId,
};
use obzenflow_dsl::{flow, sink, source, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::transform::Map;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// File-local payload for the stateless-simple test. The JSON shape
/// matches what `SimpleSource` emits; the type fingerprints the stage
/// contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct StatelessSimpleEvent {
    value: u64,
}

impl TypedPayload for StatelessSimpleEvent {
    const EVENT_TYPE: &'static str = "stateless_simple.event";
}

/// Output of the `Map` doubler.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct DoubledEvent {
    original: u64,
    doubled: u64,
}

impl TypedPayload for DoubledEvent {
    const EVENT_TYPE: &'static str = "stateless_simple.doubled_event";
}

#[derive(Clone, Debug)]
struct SimpleSource {
    count: usize,
    writer_id: WriterId,
}

impl SimpleSource {
    fn new(count: usize) -> Self {
        Self {
            count,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for SimpleSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.count == 0 {
            Ok(None)
        } else {
            self.count -= 1;

            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                "number",
                json!({
                    "value": self.count + 1,
                }),
            )]))
        }
    }
}

#[derive(Clone, Debug)]
struct Printer;

#[async_trait]
impl SinkHandler for Printer {
    async fn consume(
        &mut self,
        _event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            "printer",
            DeliveryMethod::Custom("Print".to_string()),
            None,
        ))
    }
}

#[tokio::test]
async fn stateless_pipeline_runs_to_completion() {
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(flow! {
            name: "stateless_simple_test",
            journals: disk_journals(std::path::PathBuf::from("target/stateless_simple_test_logs")),
            middleware: [],

            stages: {
                numbers = source!(StatelessSimpleEvent => SimpleSource::new(5));
                doubler = transform!(StatelessSimpleEvent -> DoubledEvent => Map::new(|event| {
                    if let Some(value) = event.payload()["value"].as_u64() {
                        ChainEventFactory::data_event(
                            WriterId::from(StageId::new()),
                            "doubled",
                            json!({
                                "original": value,
                                "doubled": value * 2,
                            }),
                        )
                    } else {
                        event
                    }
                }));
                printer = sink!(DoubledEvent => Printer);
            },

            topology: {
                numbers |> doubler;
                doubler |> printer;
            }
        })
        .await
        .expect("flow should complete without stateful stages");
}

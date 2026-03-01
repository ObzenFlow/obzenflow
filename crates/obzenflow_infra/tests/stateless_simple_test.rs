// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
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
use serde_json::json;

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
    FlowApplication::run(flow! {
        name: "stateless_simple_test",
        journals: disk_journals(std::path::PathBuf::from("target/stateless_simple_test_logs")),
        middleware: [],

        stages: {
            numbers = source!("numbers" => SimpleSource::new(5));
            doubler = transform!("doubler" => Map::new(|event| {
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
            printer = sink!("printer" => Printer);
        },

        topology: {
            numbers |> doubler;
            doubler |> printer;
        }
    })
    .await
    .expect("flow should complete without stateful stages");
}

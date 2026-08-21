// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::transform::MapTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};

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

/// Output of the typed map doubler.
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
}

impl SimpleSource {
    fn new(count: usize) -> Self {
        Self { count }
    }
}

impl TypedFiniteSourceHandler for SimpleSource {
    type Output = StatelessSimpleEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.count == 0 {
            Ok(None)
        } else {
            self.count -= 1;

            Ok(Some(vec![StatelessSimpleEvent {
                value: (self.count + 1) as u64,
            }]))
        }
    }
}

#[derive(Clone, Debug)]
struct Printer;

#[async_trait]
impl InlineSink for Printer {
    type Input = DoubledEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _input: DoubledEvent,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Print".to_string()),
            None,
        )))
    }
}

#[tokio::test]
async fn stateless_pipeline_runs_to_completion() {
    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let simple_source = SimpleSource::new(5);
        let doubler = MapTyped::new(|event: StatelessSimpleEvent| DoubledEvent {
            original: event.value,
            doubled: event.value * 2,
        });
        let printer = Printer;

        Ok(flow! {
            name: "stateless_simple_test",
            journals: disk_journals(std::path::PathBuf::from("target/stateless_simple_test_logs")),

            stages: {
                numbers = source!(StatelessSimpleEvent => simple_source);
                doubler = transform!(StatelessSimpleEvent -> DoubledEvent => doubler);
                printer = sink!(DoubledEvent => printer);
            },

            topology: {
                numbers |> doubler;
                doubler |> printer;
            }
        })
    });

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(flow_definition)
        .await
        .expect("flow should complete without stateful stages");
}

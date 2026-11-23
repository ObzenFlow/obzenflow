// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::sources::ValuesSource;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::transform::MapTyped;
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

#[tokio::test]
async fn stateless_pipeline_runs_to_completion() {
    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let simple_source = ValuesSource::new(
            (1..=5)
                .rev()
                .map(|value| StatelessSimpleEvent { value })
                .collect::<Vec<_>>(),
        );
        let doubler = MapTyped::new(|event: StatelessSimpleEvent| DoubledEvent {
            original: event.value,
            doubled: event.value * 2,
        });
        let printer = obzenflow_adapters::sinks::DiscardSink::<DoubledEvent>::new();

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

    crate::replay_testkit::unsynced_application()
        .with_cli_args(["obzenflow"])
        .run_async(flow_definition)
        .await
        .expect("flow should complete without stateful stages");
}

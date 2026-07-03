// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, stateful, transform};
use obzenflow_infra::journal::disk_journals;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct InputEvent {
    n: u64,
}

impl TypedPayload for InputEvent {
    const EVENT_TYPE: &'static str = "placeholder.input";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IntermediateEvent {
    n: u64,
}

impl TypedPayload for IntermediateEvent {
    const EVENT_TYPE: &'static str = "placeholder.intermediate";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OutputEvent {
    n: u64,
}

impl TypedPayload for OutputEvent {
    const EVENT_TYPE: &'static str = "placeholder.output";
    const SCHEMA_VERSION: u32 = 1;
}

#[tokio::test]
async fn placeholder_handlers_survive_full_lifecycle() -> Result<()> {
    let handle = flow! {
        name: "placeholder_handlers_lifecycle_test",
        journals: disk_journals(PathBuf::from("target/placeholder_handlers_lifecycle")),
        middleware: [],

        stages: {
            input = source!(InputEvent => placeholder!());
            transform = transform!(InputEvent -> IntermediateEvent => placeholder!());
            stateful = stateful!(IntermediateEvent -> OutputEvent => placeholder!());
            sink = sink!(OutputEvent => placeholder!());
        },

        topology: {
            input |> transform;
            transform |> stateful;
            stateful |> sink;
        }
    }
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    handle.run().await?;
    Ok(())
}

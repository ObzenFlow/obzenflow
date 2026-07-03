// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-054j: Processing-time tumbling windows via `emit_within(...)`.
//!
//! This example exercises the canonical stateful API shape:
//!
//! - `typed_stateful::group_by(...).emit_within(duration)`
//! - more than one mid-stream window output per run
//! - a final partial window flushed during drain
//!
//! Run with: `cargo run -p obzenflow --example stateful_emit_within_tumbling`

use anyhow::Result;
use obzenflow::typed::{sources, stateful as typed_stateful};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, stateful};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ClickEvent {
    user_id: String,
    seq: u64,
}

impl TypedPayload for ClickEvent {
    const EVENT_TYPE: &'static str = "demo.click";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct UserCount {
    count: u64,
}

impl TypedPayload for UserCount {
    const EVENT_TYPE: &'static str = "demo.user_count";
    const SCHEMA_VERSION: u32 = 1;
}

/// GroupByTyped emits payloads shaped as `{ key, result }` but uses the state
/// type's event type. The wrapper output type mirrors that payload.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct UserCountUpdate {
    key: String,
    result: UserCount,
}

impl TypedPayload for UserCountUpdate {
    const EVENT_TYPE: &'static str = UserCount::EVENT_TYPE;
    const SCHEMA_VERSION: u32 = UserCount::SCHEMA_VERSION;
}

fn main() -> Result<()> {
    let window = Duration::from_millis(200);
    let per_event_delay = Duration::from_millis(100);
    let total = 5usize;

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_blocking(flow! {
            name: "stateful_emit_within_tumbling",
            journals: disk_journals(std::path::PathBuf::from("target/stateful_emit_within_tumbling")),
            middleware: [],

            stages: {
                clicks = source!(ClickEvent => sources::finite_from_fn(move |index| {
                    if index >= total {
                        return None;
                    }

                    // Ensure the run lasts long enough to produce multiple window emissions.
                    std::thread::sleep(per_event_delay);

                    let user_id = if index % 2 == 0 { "alice" } else { "bob" }.to_string();
                    Some(ClickEvent { user_id, seq: index as u64 })
                }));

                per_user = stateful!(
                    ClickEvent -> UserCountUpdate => typed_stateful::group_by(
                        |e: &ClickEvent| e.user_id.clone(),
                        |state: &mut UserCount, _e: &ClickEvent| {
                            state.count += 1;
                        },
                    )
                    .emit_within(window)
                );

                out = sink!(|update: UserCountUpdate| {
                    println!("window update: user={} count={}", update.key, update.result.count);
                }, delivery: idempotent);
            },

            topology: {
                clicks |> per_user;
                per_user |> out;
            }
        })?;

    Ok(())
}

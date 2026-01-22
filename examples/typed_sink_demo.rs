//! Typed Sink Demo - FLOWIP-081c
//!
//! Demonstrates typed sinks without `ChainEvent` payload extraction:
//! - `SinkTyped::new` (infallible)
//! - `FallibleSinkTyped::new` (fallible, returns `HandlerError`)
//! - Strict-by-default type checking (mismatches error unless `.allow_skip()` is set)
//!
//! Run with: `cargo run -p obzenflow --example typed_sink_demo`

use anyhow::Result;
use obzenflow_core::TypedPayload;
use obzenflow_dsl_infra::{flow, sink, source};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::sink::SinkTyped;
use obzenflow_runtime_services::stages::source::FiniteSourceTyped;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Foo {
    index: usize,
}

impl TypedPayload for Foo {
    const EVENT_TYPE: &'static str = "demo.foo";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Bar {
    index: usize,
}

impl TypedPayload for Bar {
    const EVENT_TYPE: &'static str = "demo.bar";
    const SCHEMA_VERSION: u32 = 1;
}

#[tokio::main]
async fn main() -> Result<()> {
    let journal_path = std::path::PathBuf::from("target/typed_sink_demo_journal");

    FlowApplication::run(flow! {
        name: "typed_sink_demo",
        journals: disk_journals(journal_path),
        middleware: [],

        stages: {
            foo_src = source!("foo_src" => {
                let items: Vec<Foo> = (0..5).map(|index| Foo { index }).collect();
                FiniteSourceTyped::new(items)
            });

            bar_src = source!("bar_src" => {
                let items: Vec<Bar> = (0..3).map(|index| Bar { index }).collect();
                FiniteSourceTyped::new(items)
            });

            ok_sink = sink!("ok_sink" => SinkTyped::new(|item: Foo| async move {
                println!("ok_sink saw Foo {{ index: {} }}", item.index);
            }).allow_skip());

            fallible_sink = sink!("fallible_sink" => SinkTyped::fallible(|item: Foo| async move {
                if item.index == 2 {
                    return Err(HandlerError::Remote("simulated remote failure".to_string()));
                }
                println!("fallible_sink saw Foo {{ index: {} }}", item.index);
                Ok(())
            }));
        },

        topology: {
            // Foo events are processed by both typed sinks.
            foo_src |> ok_sink;
            foo_src |> fallible_sink;

            // Bar events flow into ok_sink but are skipped (`.allow_skip()`).
            bar_src |> ok_sink;
        }
    })
    .await?;

    Ok(())
}

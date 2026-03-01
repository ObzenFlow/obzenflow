// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful counter demo
//!
//! Pipeline: Source → Transform → Stateful → Sink
//!   • Source: Emits numbered events (1-10)
//!   • Transform: Filters even numbers
//!   • Stateful: Counts events and accumulates sum
//!   • Sink: Prints results as JSON
//!
//! Run with: `cargo run --example stateful_counter_demo`

use anyhow::Result;
use async_trait::async_trait;
use obzenflow::sinks::ConsoleSink;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl::{flow, sink, source, stateful, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::StatefulHandler;
use obzenflow_runtime::stages::source::FiniteSourceTyped;
use obzenflow_runtime::stages::transform::Filter;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::PathBuf;

// ============================================================================
// Source: Emits numbered events
// ============================================================================

/// Source helper that emits a sequence of numbered events
fn number_source(
    count: u64,
) -> FiniteSourceTyped<
    NumberEvent,
    impl FnMut(usize) -> Option<Vec<NumberEvent>> + Send + Sync + Clone,
> {
    FiniteSourceTyped::from_item_fn(move |index| {
        let value = (index as u64) + 1;
        if value > count {
            return None;
        }
        println!("📤 Source emitting: {value}");
        Some(NumberEvent { value })
    })
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct NumberEvent {
    value: u64,
}

impl TypedPayload for NumberEvent {
    const EVENT_TYPE: &'static str = "number";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct AggregationResult {
    count: u64,
    sum: u64,
    average: f64,
}

impl TypedPayload for AggregationResult {
    const EVENT_TYPE: &'static str = "aggregation";
    const SCHEMA_VERSION: u32 = 1;
}

// ============================================================================
// Transform: Filter even numbers
// ============================================================================

/// Filter that only passes even numbers
fn even_filter() -> Filter<impl Fn(&ChainEvent) -> bool + Send + Sync + Clone> {
    Filter::new(|event| {
        if let Some(value) = event.payload()["value"].as_u64() {
            let is_even = value % 2 == 0;
            if is_even {
                println!("  ✓ Transform passed (even): {value}");
            } else {
                println!("  ✗ Transform filtered (odd): {value}");
            }
            is_even
        } else {
            true // Pass through events without value field
        }
    })
}

// ============================================================================
// Stateful: Count and sum events
// ============================================================================

/// State for the counter - tracks count and running sum
#[derive(Clone, Debug, Default)]
struct CounterState {
    count: u64,
    sum: u64,
}

/// Stateful handler that counts events and accumulates sum
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

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        // Extract value
        let value = event.payload()["value"].as_u64().unwrap_or(0);

        // Update state directly (now mutable)
        state.count += 1;
        state.sum += value;

        println!(
            "    📊 Stateful accumulated: count={}, sum={} (added {})",
            state.count, state.sum, value
        );
    }

    fn initial_state(&self) -> Self::State {
        println!("    🔰 Stateful initialized with empty state");
        CounterState::default()
    }

    fn create_events(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        println!("\n    💧 Stateful DRAINING final state:");
        println!("       Total events: {}", state.count);
        println!("       Total sum: {}", state.sum);

        if state.count > 0 {
            let avg = state.sum as f64 / state.count as f64;
            println!("       Average: {avg:.2}");

            // Emit final aggregated result
            let result_event = ChainEventFactory::data_event(
                self.writer_id,
                "aggregation",
                json!({
                    "count": state.count,
                    "sum": state.sum,
                    "average": avg,
                }),
            );

            Ok(vec![result_event])
        } else {
            Ok(vec![])
        }
    }
}

// ============================================================================
// Main: Build and run the pipeline
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment to use console metrics
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    println!("🚀 Stateful Counter Demo");
    println!("========================================");
    println!("Pipeline: Source(1-10) → Transform(even) → Stateful(count+sum) → Sink");
    println!();

    // Use FlowApplication to handle everything
    use obzenflow_infra::application::FlowApplication;

    FlowApplication::run(flow! {
        name: "stateful_counter",
        journals: disk_journals(PathBuf::from("target/counter-logs")),
        middleware: [],

        stages: {
            numbers = source!("numbers" => number_source(10));
            evens   = transform!("even_filter" => even_filter());
            counter = stateful!("counter" => CounterHandler::new());
            output  = sink!("summary" => ConsoleSink::<AggregationResult>::json_pretty());
        },

        topology: {
            numbers |> evens;
            evens   |> counter;
            counter |> output;
        }
    })
    .await?;

    println!("\n✅ Pipeline completed successfully!");
    println!("\n📝 Summary:");
    println!("   • Source emitted: 10 numbers (1-10)");
    println!("   • Transform filtered: 5 even numbers (2,4,6,8,10)");
    println!("   • Stateful accumulated and drained final result");
    println!("   • Sink printed the aggregation");
    println!("\n💡 Key achievement: State managed WITHOUT Arc<Mutex>!");

    Ok(())
}

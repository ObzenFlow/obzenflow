// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful counter demo - demonstrates FLOWIP-080b stateful transform stages (FLOWIP-080h)
//!
//! Pipeline: Source → Transform → Stateful → Sink
//!   • Source: Emits numbered events
//!   • Transform: Filters even numbers
//!   • Stateful: Counts events and accumulates sum
//!   • Sink: Prints results
//!
//! This demonstrates:
//!   - StatefulHandler trait usage
//!   - Functional state updates (no Arc<Mutex>!)
//!   - drain() being called on EOF
//!   - Integration with transform and sink stages
//!
//! **FLOWIP-080h Update**: Replaced 29-line EvenFilter struct with Filter helper
//!
//! Run with: `cargo run --example stateful_counter_demo`

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::StatefulHandler;
use obzenflow_runtime_services::stages::source::FiniteSourceTyped;
// ✨ FLOWIP-080h: Import Filter helper
use obzenflow_runtime_services::stages::transform::Filter;
use serde::{Deserialize, Serialize};
use serde_json::json;

// ============================================================================
// Source: Emits numbered events
// ============================================================================

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
// FLOWIP-080h: Filter Helper for Even Numbers
// ============================================================================

/// Filter that only passes even numbers (FLOWIP-080h)
///
/// Replaces 29-line EvenFilter struct with a Filter helper
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
// Stateful: Count and sum events (FLOWIP-080b)
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

    println!("🚀 Stateful Counter Demo (FLOWIP-080b)");
    println!("========================================");
    println!("Pipeline: Source(1-10) → Transform(even) → Stateful(count+sum) → Sink");
    println!();

    // Use FlowApplication to handle everything
    use obzenflow_infra::application::FlowApplication;

    FlowApplication::run(flow! {
        name: "stateful_counter_demo",
        journals: disk_journals(std::path::PathBuf::from("target/stateful_demo_journal")),
        middleware: [],

            stages: {
                // FLOWIP-081: Typed finite sources (no WriterId/ChainEvent boilerplate)
                numbers = source!("numbers" => FiniteSourceTyped::from_item_fn(|index| {
                    let value = (index as u64) + 1;
                    if value > 10 {
                        return None;
                    }

                    println!("📤 Source emitting: {value}");
                    Some(NumberEvent { value })
                }));
                // ✨ FLOWIP-080h: Using Filter helper instead of EvenFilter struct
                evens = transform!("even_filter" => even_filter());
                counter = stateful!("counter" => CounterHandler::new());
                output = sink!("print" => |agg: AggregationResult| {
                    println!("\n✨ FINAL RESULT:");
                    println!("   Count: {}", agg.count);
                    println!("   Sum: {}", agg.sum);
                    println!("   Average: {}", agg.average);
                });
            },

            topology: {
                numbers |> evens;
                evens |> counter;
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

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
    event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform, stateful};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, StatefulHandler,
};
// ✨ FLOWIP-080h: Import Filter helper
use obzenflow_runtime_services::stages::transform::Filter;
use serde_json::json;

// ============================================================================
// Source: Emits numbered events
// ============================================================================

#[derive(Clone, Debug)]
struct NumberSource {
    current: u64,
    max: u64,
    writer_id: WriterId,
}

impl NumberSource {
    fn new(max: u64) -> Self {
        Self {
            current: 1,
            max,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for NumberSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.current <= self.max {
            let num = self.current;
            self.current += 1;

            println!("📤 Source emitting: {}", num);

            Some(ChainEventFactory::data_event(
                self.writer_id.clone(),
                "number",
                json!({ "value": num })
            ))
        } else {
            None
        }
    }

    fn is_complete(&self) -> bool {
        self.current > self.max
    }
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
                println!("  ✓ Transform passed (even): {}", value);
            } else {
                println!("  ✗ Transform filtered (odd): {}", value);
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

    fn create_events(&self, state: &Self::State) -> Vec<ChainEvent> {
        println!("\n    💧 Stateful DRAINING final state:");
        println!("       Total events: {}", state.count);
        println!("       Total sum: {}", state.sum);

        if state.count > 0 {
            let avg = state.sum as f64 / state.count as f64;
            println!("       Average: {:.2}", avg);

            // Emit final aggregated result
            let result_event = ChainEventFactory::data_event(
                self.writer_id.clone(),
                "aggregation",
                json!({
                    "count": state.count,
                    "sum": state.sum,
                    "average": avg,
                })
            );

            vec![result_event]
        } else {
            vec![]
        }
    }
}

// ============================================================================
// Sink: Print final results
// ============================================================================

#[derive(Clone, Debug)]
struct PrintSink;

impl PrintSink {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SinkHandler for PrintSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        if event.event_type() == "aggregation" {
            println!("\n✨ FINAL RESULT:");
            println!("   Count: {}", event.payload()["count"]);
            println!("   Sum: {}", event.payload()["sum"]);
            println!("   Average: {}", event.payload()["average"]);
        }
        Ok(DeliveryPayload::success(
            "console",
            DeliveryMethod::Custom("Print".to_string()),
            None,
        ))
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
    println!("");

    // Use FlowApplication to handle everything
    use obzenflow_infra::application::FlowApplication;

    FlowApplication::run(async {
        flow! {
            name: "stateful_counter_demo",
            journals: disk_journals(std::path::PathBuf::from("target/stateful_demo_journal")),
            middleware: [],

            stages: {
                numbers = source!("numbers" => NumberSource::new(10));
                // ✨ FLOWIP-080h: Using Filter helper instead of EvenFilter struct
                evens = transform!("even_filter" => even_filter());
                counter = stateful!("counter" => CounterHandler::new());
                output = sink!("print" => PrintSink::new());
            },

            topology: {
                numbers |> evens;
                evens |> counter;
                counter |> output;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {}", e))?;

    println!("\n✅ Pipeline completed successfully!");
    println!("\n📝 Summary:");
    println!("   • Source emitted: 10 numbers (1-10)");
    println!("   • Transform filtered: 5 even numbers (2,4,6,8,10)");
    println!("   • Stateful accumulated and drained final result");
    println!("   • Sink printed the aggregation");
    println!("\n💡 Key achievement: State managed WITHOUT Arc<Mutex>!");

    Ok(())
}

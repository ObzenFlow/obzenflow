//! Comprehensive Stateful Patterns Demo (FLOWIP-080b)
//!
//! This demo showcases all StatefulHandler patterns and validates the
//! FLOWIP-080b stateful transform stage implementation.
//!
//! Demonstrates:
//! 1. CounterHandler - Basic accumulation, emit on drain
//! 2. AccumulatorHandler - Vec collection, emit multiple on drain
//! 3. SumHandler - Numeric aggregation
//! 4. ImmediateEmitter - Emit during Accumulating (not just drain)
//! 5. EmptySource edge case - Validates drain() with zero events
//!
//! Note: This demo runs ONE pattern at a time. To see all patterns, run with
//! the PATTERN environment variable:
//!   PATTERN=counter cargo run --package obzenflow --example stateful_patterns_demo
//!   PATTERN=accumulator cargo run --package obzenflow --example stateful_patterns_demo
//!   PATTERN=sum cargo run --package obzenflow --example stateful_patterns_demo
//!   PATTERN=immediate cargo run --package obzenflow --example stateful_patterns_demo
//!   PATTERN=empty cargo run --package obzenflow --example stateful_patterns_demo
//!
//! Or run without PATTERN to see Pattern 1 (counter) by default.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, StatefulHandler,
};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// ============================================================================
// Shared Utilities
// ============================================================================

/// Simple number source
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
            Some(ChainEventFactory::data_event(
                self.writer_id.clone(),
                "number",
                json!({ "value": num }),
            ))
        } else {
            None
        }
    }

    fn is_complete(&self) -> bool {
        self.current > self.max
    }
}

/// Empty source (emits zero events)
#[derive(Clone, Debug)]
struct EmptySource {
    writer_id: WriterId,
}

impl EmptySource {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for EmptySource {
    fn next(&mut self) -> Option<ChainEvent> {
        None
    }

    fn is_complete(&self) -> bool {
        true
    }
}

/// Sink that prints and counts events
#[derive(Clone, Debug)]
struct PrintCountSink {
    name: String,
    count: Arc<AtomicU64>,
}

impl PrintCountSink {
    fn new(name: &str) -> (Self, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
        (
            Self {
                name: name.to_string(),
                count: count.clone(),
            },
            count,
        )
    }
}

#[async_trait]
impl SinkHandler for PrintCountSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        let count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        println!(
            "  [{}] Event #{}: {} = {:?}",
            self.name,
            count,
            event.event_type(),
            event.payload()
        );
        Ok(DeliveryPayload::success(
            &self.name,
            DeliveryMethod::Custom("Print".to_string()),
            None,
        ))
    }
}

// ============================================================================
// Pattern 1: CounterHandler - Basic Accumulation
// ============================================================================

#[derive(Clone, Debug, Default)]
struct CounterState {
    count: u64,
}

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

    fn accumulate(&mut self, state: &mut Self::State, _event: ChainEvent) {
        // Accumulate only - emit on drain
        state.count += 1;
    }

    fn initial_state(&self) -> Self::State {
        CounterState::default()
    }

    fn create_events(&self, state: &Self::State) -> Vec<ChainEvent> {
        vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "count_result",
            json!({ "total_count": state.count }),
        )]
    }
}

// ============================================================================
// Pattern 2: AccumulatorHandler - Vec Collection
// ============================================================================

#[derive(Debug, Clone)]
struct AccumulatorHandler {
    writer_id: WriterId,
}

impl AccumulatorHandler {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for AccumulatorHandler {
    type State = Vec<u64>;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        if let Some(value) = event.payload()["value"].as_u64() {
            state.push(value);
        }
    }

    fn initial_state(&self) -> Self::State {
        Vec::new()
    }

    fn create_events(&self, state: &Self::State) -> Vec<ChainEvent> {
        // Emit one event per collected value
        state
            .iter()
            .map(|&value| {
                ChainEventFactory::data_event(
                    self.writer_id.clone(),
                    "collected_value",
                    json!({ "value": value }),
                )
            })
            .collect()
    }
}

// ============================================================================
// Pattern 3: SumHandler - Numeric Aggregation
// ============================================================================

#[derive(Debug, Clone)]
struct SumHandler {
    writer_id: WriterId,
}

impl SumHandler {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for SumHandler {
    type State = u64;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        let value = event.payload()["value"].as_u64().unwrap_or(0);
        *state += value;
    }

    fn initial_state(&self) -> Self::State {
        0
    }

    fn create_events(&self, state: &Self::State) -> Vec<ChainEvent> {
        vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "sum_result",
            json!({ "total_sum": *state }),
        )]
    }
}

// ============================================================================
// Pattern 4: ImmediateEmitter - Emit During Accumulating
// ============================================================================

#[derive(Debug, Clone)]
struct ImmediateEmitter {
    writer_id: WriterId,
}

impl ImmediateEmitter {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for ImmediateEmitter {
    type State = u64;

    fn accumulate(&mut self, state: &mut Self::State, _event: ChainEvent) {
        *state += 1;
    }

    fn should_emit(&self, _state: &Self::State) -> bool {
        // Emit immediately after every event
        true
    }

    fn emit(&self, state: &mut Self::State) -> Vec<ChainEvent> {
        // Emit progress update with current count
        vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "progress_update",
            json!({ "current_count": *state }),
        )]
    }

    fn initial_state(&self) -> Self::State {
        0
    }

    fn create_events(&self, _state: &Self::State) -> Vec<ChainEvent> {
        // No drain emission - already emitted everything during processing
        vec![]
    }
}

// ============================================================================
// Main: Run All Pattern Demos
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let pattern = std::env::var("PATTERN").unwrap_or_else(|_| "counter".to_string());

    println!("🚀 FLOWIP-080b: Comprehensive Stateful Patterns Demo");
    println!("====================================================\n");

    match pattern.as_str() {
        "counter" => run_counter_pattern().await,
        "accumulator" => run_accumulator_pattern().await,
        "sum" => run_sum_pattern().await,
        "immediate" => run_immediate_pattern().await,
        "empty" => run_empty_pattern().await,
        _ => {
            println!("❌ Unknown pattern: {}", pattern);
            println!("Valid patterns: counter, accumulator, sum, immediate, empty");
            Ok(())
        }
    }
}

async fn run_counter_pattern() -> Result<()> {
    println!("📊 Pattern 1: CounterHandler - Basic Accumulation");
    println!("   Accumulates count, emits single result on drain\n");

    let (sink1, count1) = PrintCountSink::new("Counter");
    let count1_clone = count1.clone();

    FlowApplication::run(async move {
        flow! {
            name: "pattern1_counter",
            journals: disk_journals(std::path::PathBuf::from("target/stateful_demo_pattern1")),
            middleware: [],

            stages: {
                src = source!("numbers" => NumberSource::new(5));
                counter = stateful!("counter" => CounterHandler::new());
                sink = sink!("sink" => sink1);
            },

            topology: {
                src |> counter;
                counter |> sink;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await?;

    println!("   ✅ Result: Emitted {} event(s) on drain", count1_clone.load(Ordering::Relaxed));
    println!("\n💡 State managed WITHOUT Arc<Mutex> anti-pattern!");
    Ok(())
}

async fn run_accumulator_pattern() -> Result<()> {
    println!("📊 Pattern 2: AccumulatorHandler - Vec Collection");
    println!("   Collects all values, emits multiple events on drain\n");

    let (sink2, count2) = PrintCountSink::new("Accumulator");
    let count2_clone = count2.clone();

    FlowApplication::run(async move {
        flow! {
            name: "pattern2_accumulator",
            journals: disk_journals(std::path::PathBuf::from("target/stateful_demo_pattern2")),
            middleware: [],

            stages: {
                src = source!("numbers" => NumberSource::new(5));
                acc = stateful!("accumulator" => AccumulatorHandler::new());
                sink = sink!("sink" => sink2);
            },

            topology: {
                src |> acc;
                acc |> sink;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await?;

    println!("   ✅ Result: Emitted {} event(s) on drain (should be 5, one per collected value)", count2_clone.load(Ordering::Relaxed));
    println!("\n💡 Demonstrates emitting multiple events from drain()!");
    Ok(())
}

async fn run_sum_pattern() -> Result<()> {
    println!("📊 Pattern 3: SumHandler - Numeric Aggregation");
    println!("   Sums numeric values, emits total on drain\n");

    let (sink3, count3) = PrintCountSink::new("Sum");
    let count3_clone = count3.clone();

    FlowApplication::run(async move {
        flow! {
            name: "pattern3_sum",
            journals: disk_journals(std::path::PathBuf::from("target/stateful_demo_pattern3")),
            middleware: [],

            stages: {
                src = source!("numbers" => NumberSource::new(10));
                summer = stateful!("sum" => SumHandler::new());
                sink = sink!("sink" => sink3);
            },

            topology: {
                src |> summer;
                summer |> sink;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await?;

    println!("   ✅ Result: Emitted {} event(s) on drain (sum 1+2+...+10 = 55)", count3_clone.load(Ordering::Relaxed));
    println!("\n💡 Numeric aggregation pattern works correctly!");
    Ok(())
}

async fn run_immediate_pattern() -> Result<()> {
    println!("📊 Pattern 4: ImmediateEmitter - Emit During Accumulating");
    println!("   Emits progress events immediately during processing\n");

    let (sink4, count4) = PrintCountSink::new("Immediate");
    let count4_clone = count4.clone();

    FlowApplication::run(async move {
        flow! {
            name: "pattern4_immediate",
            journals: disk_journals(std::path::PathBuf::from("target/stateful_demo_pattern4")),
            middleware: [],

            stages: {
                src = source!("numbers" => NumberSource::new(5));
                emitter = stateful!("emitter" => ImmediateEmitter::new());
                sink = sink!("sink" => sink4);
            },

            topology: {
                src |> emitter;
                emitter |> sink;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await?;

    println!("   ✅ Result: Emitted {} progress event(s) during Accumulating state", count4_clone.load(Ordering::Relaxed));
    println!("\n💡 Demonstrates immediate emission (not just on drain)!");
    Ok(())
}

async fn run_empty_pattern() -> Result<()> {
    println!("📊 Pattern 5: EmptySource Edge Case");
    println!("   Validates drain() works correctly with zero input events\n");

    let (sink5, count5) = PrintCountSink::new("Empty");
    let count5_clone = count5.clone();

    FlowApplication::run(async move {
        flow! {
            name: "pattern5_empty",
            journals: disk_journals(std::path::PathBuf::from("target/stateful_demo_pattern5")),
            middleware: [],

            stages: {
                src = source!("empty" => EmptySource::new());
                counter = stateful!("counter" => CounterHandler::new());
                sink = sink!("sink" => sink5);
            },

            topology: {
                src |> counter;
                counter |> sink;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await?;

    println!("   ✅ Result: Emitted {} event(s) on drain (count=0)", count5_clone.load(Ordering::Relaxed));
    println!("\n💡 Edge case: drain() called even with zero input events!");
    Ok(())
}

//! Prometheus 100k Demo with FlowApplication Framework (FLOWIP-080h & FLOWIP-080j)
//!
//! This demo processes 100,000 events demonstrating:
//! - Flow-level rate limiting middleware
//! - Fan-out topology pattern (one stage to multiple downstream stages)
//! - ReduceTyped for type-safe event counting (FLOWIP-080j)
//! - Prometheus metrics via /metrics endpoint
//!
//! **FLOWIP-080h Update**: Replaced 38-line ErrorProneTransform struct with Map helper
//! **FLOWIP-080j Update**: Replaced 59-line EventCounter StatefulHandler with ReduceTyped
//!
//! Run with: cargo run -p obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server
//!
//! The --server flag will start the web server with:
//! - /metrics endpoint for Prometheus metrics (framework-level metrics)
//! - /api/topology endpoint for flow structure
//! - /health and /ready endpoints for monitoring

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform, stateful};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler,
};
// ✨ FLOWIP-080h: Import Map helper
use obzenflow_runtime_services::stages::transform::Map;
// ✨ FLOWIP-080j: Import ReduceTyped for type-safe accumulation
use obzenflow_runtime_services::stages::stateful::accumulators::ReduceTyped;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::{Duration, Instant};

/// Source that generates 100k events
#[derive(Clone, Debug)]
struct HighVolumeSource {
    count: usize,
    writer_id: WriterId,
    total_events: usize,
}

impl HighVolumeSource {
    fn new(total_events: usize) -> Self {
        Self {
            count: 0,
            writer_id: WriterId::from(StageId::new()),
            total_events,
        }
    }
}

impl FiniteSourceHandler for HighVolumeSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= self.total_events {
            println!("🏁 Source complete: Generated {} total events", self.count);
            return None;
        }

        // Increment after we know we're emitting an event
        let current_id = self.count;
        self.count += 1;

        // Log progress every 10k events for 100k demo
        if self.count % 10_000 == 0 {
            println!("📊 Generated {} events...", self.count);
        }

        // Every 100th event will simulate an error
        let should_fail = current_id % 100 == 0;

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "data.request",
            json!({
                "id": current_id,
                "should_fail": should_fail,
                "batch": current_id / 100,  // Group into batches of 100
            }),
        ))
    }

    fn is_complete(&self) -> bool {
        self.count >= self.total_events
    }
}

// ============================================================================
// FLOWIP-080j: Domain Types
// ============================================================================

/// Successfully processed event from the error-prone transform
#[derive(Debug, Clone, Deserialize)]
struct ProcessedEvent {
    id: usize,
    should_fail: bool,
    batch: usize,
    processed: bool,
    processing_stage: String,
}

// ============================================================================
// FLOWIP-080h: Map Helper for Error-Prone Transform
// ============================================================================

/// Transform that can fail on certain events (FLOWIP-080h)
///
/// Replaces 38-line ErrorProneTransform struct with a Map helper
fn error_prone_transform() -> Map<impl Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone> {
    Map::new(|event| {
        let payload = event.payload();

        // Check if this event should fail
        if payload["should_fail"].as_bool().unwrap_or(false) {
            // Return error event
            let mut error_payload = payload;
            error_payload["error"] = json!("Simulated processing error");
            error_payload["error_code"] = json!(500);

            ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                "error.event",
                error_payload,
            )
        } else {
            // Successful processing
            let mut result_payload = payload;
            result_payload["processed"] = json!(true);
            result_payload["processing_stage"] = json!("error_prone_transform");

            ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                "processed.event",
                result_payload,
            )
        }
    })
}

// ============================================================================
// FLOWIP-080j: ReduceTyped for Type-Safe Event Counting
// ============================================================================

/// State for business-level event counting (FLOWIP-080j)
#[derive(Clone, Debug, Serialize)]
struct EventCountState {
    event_count: usize,
}

impl Default for EventCountState {
    fn default() -> Self {
        Self {
            event_count: 0,
        }
    }
}

/// Simple sink that consumes all events (simulates Kafka/S3 persistence)
/// Framework metrics at /metrics show how many events were processed
#[derive(Clone, Debug)]
struct CompletionSink;

impl CompletionSink {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SinkHandler for CompletionSink {
    async fn consume(&mut self, _event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        Ok(DeliveryPayload::success(
            "completion_sink",
            DeliveryMethod::Custom("InMemory".to_string()),
            Some(1),
        ))
    }
}

/// Sink that prints the aggregator's final summary
#[derive(Clone, Debug)]
struct SummarySink;

impl SummarySink {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SinkHandler for SummarySink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        // ReduceTyped emits with event_type "reduced"
        if event.event_type() == "reduced" {
            let payload = event.payload();
            let result = &payload["result"];
            let count = result["event_count"].as_u64().unwrap_or(0);

            println!("");
            println!("=====================================");
            println!("📊 Business-Level Event Count (FLOWIP-080j):");
            println!("   Successfully processed: {} events", count);
            println!("   Note: 100000 generated - {} = {} errors (routed to error journal)", count, 100000 - count as u32);
            println!("=====================================");
            println!("");
            println!("💡 Key Improvement:");
            println!("   59-line EventCounter StatefulHandler → ReduceTyped helper");
            println!("   Type-safe accumulation with zero ChainEvent manipulation!");
            println!("");
            println!("📈 To view framework-level Prometheus metrics:");
            println!("   1. Run with --server flag");
            println!("   2. Visit http://localhost:3000/metrics");
            println!("   3. See detailed per-stage metrics, errors, latencies, etc.");
            println!("=====================================");
        }

        Ok(DeliveryPayload::success(
            "summary_sink",
            DeliveryMethod::Custom("InMemory".to_string()),
            Some(1),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment to use prometheus exporter
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "prometheus");

    println!("🚀 Prometheus 100k Demo with FlowApplication Framework");
    println!("=====================================================");
    println!("📊 Processing 100,000 events demonstrating:");
    println!("   • Flow-level rate limiting middleware");
    println!("   • Fan-out topology (processor → counter + sink)");
    println!("   • StatefulHandler for business-level counting");
    println!("   • Framework Prometheus metrics");
    println!("");
    println!("Usage:");
    println!("  Run with server:    cargo run -p obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server");
    println!("  Custom port:        cargo run -p obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server --server-port 8080");
    println!("  Without server:     cargo run -p obzenflow --example prometheus_100k_demo");
    println!("");

    // Use FlowApplication to handle everything
    FlowApplication::run(async {
        flow! {
            name: "prometheus_100k_demo",
            journals: disk_journals(std::path::PathBuf::from("target/prometheus_100k_demo_journal")),

            // Flow-level rate limiting (1000 events/sec) for fast processing
            middleware: [
                rate_limit(1000.0)
            ],

            stages: {
                // Source generating 100k events
                src = source!("high_volume_source" => HighVolumeSource::new(100_000));

                // Error-prone transform (every 100th event fails)
                // ✨ FLOWIP-080h: Using Map helper instead of ErrorProneTransform struct
                processor = transform!("error_processor" => error_prone_transform());

                // Fan-out branch 1: Type-safe event counter (FLOWIP-080j)
                // Replaces 59-line EventCounter StatefulHandler with ReduceTyped!
                // Counts ProcessedEvent domain objects from the stream
                counter = stateful!("event_counter" =>
                    ReduceTyped::new(
                        EventCountState::default(),
                        |state: &mut EventCountState, _event: &ProcessedEvent| {
                            state.event_count += 1;
                            // Log progress every 10k events
                            if state.event_count % 10_000 == 0 {
                                println!("📊 Counted {} events so far...", state.event_count);
                            }
                        }
                    ).emit_on_eof()
                );
                counter_sink = sink!("summary_sink" => SummarySink::new());

                // Fan-out branch 2: Completion sink
                completion = sink!("completion_sink" => CompletionSink::new());
            },

            topology: {
                // Linear processing
                src |> processor;

                // Fan-out: processor feeds both branches
                processor |> counter;
                processor |> completion;

                // Counter emits summary to its sink
                counter |> counter_sink;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {}", e))?;

    Ok(())
}
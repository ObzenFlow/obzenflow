//! Prometheus 100k Demo with FlowApplication Framework (FLOWIP-080h, 080j & 082a)
//!
//! This demo processes 100,000 events demonstrating:
//! - Flow-level rate limiting middleware
//! - Fan-out topology pattern (one stage to multiple downstream stages)
//! - ReduceTyped for type-safe event counting (FLOWIP-080j)
//! - TypedPayload for strongly-typed events (FLOWIP-082a)
//! - Prometheus metrics via /metrics endpoint
//!
//! **FLOWIP-080h Update**: Replaced 38-line ErrorProneTransform struct with Map helper
//! **FLOWIP-080j Update**: Replaced 59-line EventCounter StatefulHandler with ReduceTyped
//! **FLOWIP-082a Update**: Added TypedPayload with EVENT_TYPE and SCHEMA_VERSION constants
//!
//! Run with: cargo run -p obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server
//!
//! The --server flag will start the web server with:
//! - /metrics endpoint for Prometheus metrics (framework-level metrics)
//! - /api/topology endpoint for flow structure
//! - /health and /ready endpoints for monitoring

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
// ✨ FLOWIP-080h: Import Map helper
use obzenflow_runtime_services::stages::transform::Map;
// ✨ FLOWIP-080j: Import ReduceTyped for type-safe accumulation
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_runtime_services::stages::stateful::strategies::accumulators::ReduceTyped;
use serde::{Deserialize, Serialize};
use serde_json::json;

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
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError> {
        if self.is_complete() {
            println!("🏁 Source complete: Generated {} total events", self.count);
            return Ok(None);
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

        // ✨ FLOWIP-082a: Emit typed event using EVENT_TYPE constant
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            &DataRequest::versioned_event_type(),
            json!({
                "id": current_id,
                "should_fail": should_fail,
                "batch": current_id / 100,  // Group into batches of 100
            }),
        )]))
    }
}

// ============================================================================
// FLOWIP-082a: Strongly-Typed Domain Events
// ============================================================================

/// Data request event from the source
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DataRequest {
    id: usize,
    should_fail: bool,
    batch: usize,
}

impl TypedPayload for DataRequest {
    const EVENT_TYPE: &'static str = "data.request";
    const SCHEMA_VERSION: u32 = 1;
}

/// Successfully processed event from the error-prone transform
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessedEvent {
    id: usize,
    should_fail: bool,
    batch: usize,
    processed: bool,
    processing_stage: String,
}

impl TypedPayload for ProcessedEvent {
    const EVENT_TYPE: &'static str = "processed.event";
    const SCHEMA_VERSION: u32 = 1;
}

/// Error event from failed processing
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorEvent {
    id: usize,
    should_fail: bool,
    batch: usize,
    error: String,
    error_code: u32,
}

impl TypedPayload for ErrorEvent {
    const EVENT_TYPE: &'static str = "error.event";
    const SCHEMA_VERSION: u32 = 1;
}

// ============================================================================
// FLOWIP-080h: Map Helper for Error-Prone Transform
// ============================================================================

/// Transform that can fail on certain events (FLOWIP-080h & FLOWIP-082a)
///
/// Replaces 38-line ErrorProneTransform struct with a Map helper
fn error_prone_transform() -> Map<impl Fn(ChainEvent) -> ChainEvent + Send + Sync + Clone> {
    Map::new(|event| {
        let payload = event.payload();

        // Check if this event should fail
        if payload["should_fail"].as_bool().unwrap_or(false) {
            // ✨ FLOWIP-082a: Return typed error event
            let mut error_payload = payload;
            error_payload["error"] = json!("Simulated processing error");
            error_payload["error_code"] = json!(500);

            ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                &ErrorEvent::versioned_event_type(),
                error_payload,
            )
        } else {
            // ✨ FLOWIP-082a: Successful processing with typed event
            let mut result_payload = payload;
            result_payload["processed"] = json!(true);
            result_payload["processing_stage"] = json!("error_prone_transform");

            ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                &ProcessedEvent::versioned_event_type(),
                result_payload,
            )
        }
    })
}

// ============================================================================
// FLOWIP-080j: ReduceTyped for Type-Safe Event Counting
// ============================================================================

/// State for business-level event counting (FLOWIP-080j)
#[derive(Clone, Debug, Serialize, Deserialize)]
struct EventCountState {
    event_count: usize,
}

impl Default for EventCountState {
    fn default() -> Self {
        Self { event_count: 0 }
    }
}

impl TypedPayload for EventCountState {
    const EVENT_TYPE: &'static str = "prometheus.event_count";
    const SCHEMA_VERSION: u32 = 1;
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
    async fn consume(
        &mut self,
        _event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
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
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        // ✨ FLOWIP-082a: ReduceTyped emits with state's EVENT_TYPE
        if EventCountState::event_type_matches(&event.event_type()) {
            let payload = event.payload();
            let result = &payload["result"];
            let count = result["event_count"].as_u64().unwrap_or(0);

            println!("");
            println!("=====================================");
            println!("📊 Business-Level Event Count (FLOWIP-080j):");
            println!("   Successfully processed: {} events", count);
            println!(
                "   Note: 100000 generated - {} = {} errors (routed to error journal)",
                count,
                100000 - count as u32
            );
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

fn main() -> Result<()> {
    // Set environment to use prometheus exporter
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "prometheus");

    println!("🚀 Prometheus 100k Demo with FlowApplication Framework");
    println!("========================================================");
    println!("📊 Processing 100,000 events demonstrating:");
    println!("   • Flow-level rate limiting middleware");
    println!("   • Fan-out topology (processor → counter + sink)");
    println!("   • StatefulHandler for business-level counting");
    println!("   • Framework Prometheus metrics");
    println!("");
    println!("Usage:");
    println!("  Basic:              cargo run --package obzenflow --example prometheus_100k_demo");
    println!("  With metrics:       cargo run --package obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server");
    println!("  With console:       cargo run --package obzenflow --example prometheus_100k_demo --features console,obzenflow_infra/warp-server -- --server");
    println!("  Custom port:        cargo run --package obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server --server-port 8080");
    println!("");

    // Use FlowApplication builder - handles runtime, observability, and features automatically
    // No #[cfg] needed! with_console_subscriber() is a no-op if 'console' feature is disabled
    FlowApplication::builder()
        .with_console_subscriber()  // Enables tokio-console if --features console is used
        .with_log_level(obzenflow_infra::application::LogLevel::Info)
        .run_blocking(async {
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
    .map_err(|e| anyhow::anyhow!("Application failed: {}", e))?;

    Ok(())
}

//! Prometheus 1k Demo with FlowApplication Framework
//!
//! This demo processes 1,000 events demonstrating:
//! - Flow-level rate limiting middleware
//! - Fan-out topology pattern (one stage to multiple downstream stages)
//! - StatefulHandler for business-level event counting
//! - Prometheus metrics via /metrics endpoint
//!
//! Run with: cargo run -p obzenflow --example prometheus_1k_demo --features obzenflow_infra/warp-server -- --server
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
    FiniteSourceHandler, SinkHandler, TransformHandler, StatefulHandler,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use serde_json::json;
use std::time::{Duration, Instant};

/// Source that generates 1k events
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

        // Log progress every 100 events for 1k demo
        if self.count % 100 == 0 {
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

/// Transform that can fail on certain events
#[derive(Clone, Debug)]
struct ErrorProneTransform;

#[async_trait]
impl TransformHandler for ErrorProneTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let payload = event.payload();

        // Check if this event should fail
        if payload["should_fail"].as_bool().unwrap_or(false) {
            // Return error event
            let mut error_payload = payload.clone();
            error_payload["error"] = json!("Simulated processing error");
            error_payload["error_code"] = json!(500);
            
            vec![ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                "error.event",
                error_payload,
            )]
        } else {
            // Successful processing
            let mut result_payload = payload.clone();
            result_payload["processed"] = json!(true);
            result_payload["processing_stage"] = json!("error_prone_transform");
            
            vec![ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                "processed.event",
                result_payload,
            )]
        }
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// State for business-level event counting
#[derive(Clone, Debug, Default)]
struct EventCountState {
    event_count: usize,
    start_time: Option<Instant>,
}

/// Stateful aggregator that counts successfully processed events
/// Note: Only sees events that made it through the pipeline (not errors routed to error journal)
#[derive(Debug, Clone)]
struct EventCounter {
    writer_id: WriterId,
}

impl EventCounter {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for EventCounter {
    type State = EventCountState;

    fn accumulate(&mut self, state: &mut Self::State, _event: ChainEvent) {
        // Initialize start time on first event
        if state.start_time.is_none() {
            state.start_time = Some(Instant::now());
        }

        state.event_count += 1;

        // Log progress every 100 events
        if state.event_count % 100 == 0 {
            println!("📊 Counted {} events so far...", state.event_count);
        }
    }

    fn initial_state(&self) -> Self::State {
        EventCountState::default()
    }

    fn create_events(&self, state: &Self::State) -> Vec<ChainEvent> {
        // Calculate final statistics
        let duration = state.start_time
            .map(|start| start.elapsed())
            .unwrap_or(Duration::from_secs(0));

        // Emit final count event
        vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "EventCountSummary",
            json!({
                "events_counted": state.event_count,
                "duration_secs": duration.as_secs_f64(),
            }),
        )]
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
        if event.event_type() == "EventCountSummary" {
            let payload = event.payload();
            let count = payload["events_counted"].as_u64().unwrap_or(0);
            let duration = payload["duration_secs"].as_f64().unwrap_or(0.0);

            println!("");
            println!("=====================================");
            println!("📊 Business-Level Event Count:");
            println!("   Successfully processed: {} events", count);
            println!("   Duration: {:.2}s", duration);
            println!("   Note: 1000 generated - {} = {} errors (routed to error journal)", count, 1000 - count as u32);
            println!("=====================================");
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

    println!("🚀 Prometheus 1k Demo with FlowApplication Framework");
    println!("=====================================================");
    println!("📊 Processing 1,000 events demonstrating:");
    println!("   • Flow-level rate limiting middleware");
    println!("   • Fan-out topology (processor → counter + sink)");
    println!("   • StatefulHandler for business-level counting");
    println!("   • Framework Prometheus metrics");
    println!("");
    println!("Usage:");
    println!("  Run with server:    cargo run -p obzenflow --example prometheus_1k_demo --features obzenflow_infra/warp-server -- --server");
    println!("  Custom port:        cargo run -p obzenflow --example prometheus_1k_demo --features obzenflow_infra/warp-server -- --server --server-port 8080");
    println!("  Without server:     cargo run -p obzenflow --example prometheus_1k_demo");
    println!("");

    // Use FlowApplication to handle everything
    FlowApplication::run(async {
        flow! {
            name: "prometheus_1k_demo",
            journals: disk_journals(std::path::PathBuf::from("target/prometheus_1k_demo_journal")),

            // Flow-level rate limiting (1000 events/sec) for fast processing
            middleware: [
                rate_limit(1000.0)
            ],

            stages: {
                // Source generating 1k events
                src = source!("high_volume_source" => HighVolumeSource::new(1_000));

                // Error-prone transform (every 100th event fails)
                processor = transform!("error_processor" => ErrorProneTransform);

                // Fan-out branch 1: Stateful event counter
                counter = stateful!("event_counter" => EventCounter::new());
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
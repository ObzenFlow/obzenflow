//! Prometheus 100k Demo with FlowApplication Framework
//!
//! This demo processes 100,000 events demonstrating:
//! - Flow-level rate limiting middleware
//! - Fan-out topology pattern (one stage to multiple downstream stages)
//! - StatefulHandler for business-level event counting
//! - Prometheus metrics via /metrics endpoint
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
    FiniteSourceHandler, SinkHandler, TransformHandler, StatefulHandler,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
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

    fn process(&self, state: &Self::State, _event: ChainEvent) -> (Self::State, Vec<ChainEvent>) {
        let mut new_state = state.clone();

        // Initialize start time on first event
        if new_state.start_time.is_none() {
            new_state.start_time = Some(Instant::now());
        }

        new_state.event_count += 1;

        // Log progress every 10k events
        if new_state.event_count % 10_000 == 0 {
            println!("📊 Counted {} events so far...", new_state.event_count);
        }

        // Accumulate only - emit summary on drain
        (new_state, vec![])
    }

    fn initial_state(&self) -> Self::State {
        EventCountState::default()
    }

    async fn drain(
        &mut self,
        state: &Self::State,
    ) -> Result<Vec<ChainEvent>, Box<dyn std::error::Error + Send + Sync>> {
        // Calculate final statistics
        let duration = state.start_time
            .map(|start| start.elapsed())
            .unwrap_or(Duration::from_secs(0));

        // Emit final count event
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "EventCountSummary",
            json!({
                "events_counted": state.event_count,
                "duration_secs": duration.as_secs_f64(),
            }),
        )])
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
            println!("   Note: 100000 generated - {} = {} errors (routed to error journal)", count, 100000 - count as u32);
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
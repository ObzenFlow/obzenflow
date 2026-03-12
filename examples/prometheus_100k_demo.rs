// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

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
use obzenflow::typed::{sources, stateful as typed_stateful};
use obzenflow_adapters::middleware::RateLimiterBuilder;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::status::processing_status::ErrorKind,
    TypedPayload,
};
use obzenflow_dsl::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::SinkHandler;
// ✨ FLOWIP-080h: Import Map helper
use obzenflow_runtime::stages::transform::Map;
// ✨ FLOWIP-080j: Import ReduceTyped for type-safe accumulation
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use serde::{Deserialize, Serialize};
use serde_json::json;

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
        let mut payload = event.payload();

        // Check if this event should fail
        if payload["should_fail"].as_bool().unwrap_or(false) {
            // ✨ FLOWIP-082a: Return typed error event
            payload["error"] = json!("Simulated processing error");
            payload["error_code"] = json!(500);

            event.derive_error_event(
                ErrorEvent::versioned_event_type(),
                payload,
                "Simulated processing error",
                ErrorKind::Domain,
            )
        } else {
            // ✨ FLOWIP-082a: Successful processing with typed event
            payload["processed"] = json!(true);
            payload["processing_stage"] = json!("error_prone_transform");

            ChainEventFactory::derived_data_event(
                event.writer_id,
                &event,
                ProcessedEvent::versioned_event_type(),
                payload,
            )
        }
    })
}

// ============================================================================
// FLOWIP-080j: ReduceTyped for Type-Safe Event Counting
// ============================================================================

/// State for business-level event counting (FLOWIP-080j)
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct EventCountState {
    event_count: usize,
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
    println!();
    println!("Usage:");
    println!("  Basic:              cargo run --package obzenflow --example prometheus_100k_demo");
    println!("  With metrics:       cargo run --package obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server");
    println!("  Custom port:        cargo run --package obzenflow --example prometheus_100k_demo --features obzenflow_infra/warp-server -- --server --server-port 8080");
    println!();

    // Use FlowApplication builder - handles runtime, observability, and features automatically.
    FlowApplication::builder()
        .with_log_level(obzenflow_infra::application::LogLevel::Info)
        .run_blocking(flow! {
            name: "prometheus_100k_demo",
            journals: disk_journals(std::path::PathBuf::from("target/prometheus_100k_demo_journal")),

            // Flow-level rate limiting (1000 events/sec) for fast processing
            middleware: [
                RateLimiterBuilder::new(1000.0).build()
            ],

            stages: {
                // Source generating 100k events
                high_volume_source = source!(DataRequest =>
                    sources::finite_from_fn(move |index| {
                        let total = 100_000usize;
                        if index >= total {
                            println!("🏁 Source complete: Generated {index} total events");
                            return None;
                        }

                        let current_id = index;
                        let next_count = index + 1;

                        if next_count.is_multiple_of(10_000) {
                            println!("📊 Generated {next_count} events...");
                        }

                        Some(DataRequest {
                            id: current_id,
                            should_fail: current_id % 100 == 0,
                            batch: current_id / 100,
                        })
                    })
                );

                // Error-prone transform (every 100th event fails)
                // ✨ FLOWIP-080h: Using Map helper instead of ErrorProneTransform struct
                error_processor = transform!(error_prone_transform());

                // Fan-out branch 1: Type-safe event counter (FLOWIP-080j)
                // Replaces 59-line EventCounter StatefulHandler with ReduceTyped!
                // Counts ProcessedEvent domain objects from the stream
                event_counter = stateful!(ProcessedEvent -> EventCountState =>
                    typed_stateful::reduce(
                        EventCountState::default(),
                        |state: &mut EventCountState, _event: &ProcessedEvent| {
                            state.event_count += 1;
                            // Log progress every 10k events
                            if state.event_count.is_multiple_of(10_000) {
                                println!("📊 Counted {} events so far...", state.event_count);
                            }
                        }
                    ).emit_on_eof()
                );
                summary_sink = sink!(|summary: EventCountState| {
                    let count = summary.event_count;
                    let errors = 100_000usize.saturating_sub(count);

                    println!();
                    println!("=====================================");
                    println!("📊 Business-Level Event Count (FLOWIP-080j):");
                    println!("   Successfully processed: {count} events");
                    println!(
                        "   Note: 100000 generated - {count} = {errors} errors (routed to error journal)"
                    );
                    println!("=====================================");
                    println!();
                    println!("💡 Key Improvement:");
                    println!("   59-line EventCounter StatefulHandler → ReduceTyped helper");
                    println!("   Type-safe accumulation with zero ChainEvent manipulation!");
                    println!();
                    println!("📈 To view framework-level Prometheus metrics:");
                    println!("   1. Run with --server flag");
                    println!("   2. Visit http://localhost:3000/metrics");
                    println!("   3. See detailed per-stage metrics, errors, latencies, etc.");
                    println!("=====================================");
                });

                // Fan-out branch 2: Completion sink
                completion_sink = sink!(CompletionSink::new());
            },

            topology: {
                // Linear processing
                high_volume_source |> error_processor;

                // Fan-out: processor feeds both branches
                error_processor |> event_counter;
                error_processor |> completion_sink;

                // Counter emits summary to its sink
                event_counter |> summary_sink;
            }
        })?;

    Ok(())
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Demo: Flow-Level vs Stage-Level Middleware Configuration
//!
//! This demonstrates middleware inheritance - a critical concept for production flows.
//!
//! **How to observe rate limiting in real-time:**
//!   1. Run with --server flag (requires warp-server feature):
//!      ```
//!      cargo run -p obzenflow --example flow_middleware_config --features obzenflow_infra/warp-server -- --server
//!      ```
//!   2. Query metrics while flow runs:
//!      ```
//!      curl http://localhost:9090/metrics | grep "events_processed_total"
//!      ```
//!   3. Observe rate differences:
//!      - fast_source: ~10 events/sec (stage override)
//!      - throttled_transform: ~1.0 events/sec (inherits flow-level)
//!      - counting_sink: ~1.0 events/sec (bottlenecked by transform)
//!
//! Key concepts demonstrated:
//! - Flow-level middleware applies to all stages by default
//! - Stage-level middleware overrides flow-level
//! - Stages without overrides inherit flow-level config
//! - Backpressure naturally flows upstream
//! - Production observability via /metrics endpoint

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{event::chain_event::ChainEvent, TypedPayload};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{SinkHandler, TransformHandler};
use obzenflow_runtime_services::stages::source::FiniteSourceTyped;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
struct CounterEvent {
    count: usize,
}

impl TypedPayload for CounterEvent {
    const EVENT_TYPE: &'static str = "counter.event";
    const SCHEMA_VERSION: u32 = 1;
}

/// Simple passthrough transform
#[derive(Debug, Clone)]
struct PassthroughTransform;

impl PassthroughTransform {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TransformHandler for PassthroughTransform {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Simple sink that counts events
#[derive(Debug, Clone)]
struct CountingSink {
    received: usize,
}

impl CountingSink {
    fn new() -> Self {
        Self { received: 0 }
    }
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        self.received += 1;

        // Log progress every 20 events
        if self.received.is_multiple_of(20) {
            let payload = event.payload();
            println!(
                "[SINK] Received {} events (current: #{})",
                self.received, payload["count"]
            );
        }

        Ok(DeliveryPayload::success(
            "counting_sink",
            DeliveryMethod::Custom("InMemory".to_string()),
            Some(1),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Use Prometheus exporter for /metrics endpoint
    // (defaults to "prometheus" if not set, but being explicit here)
    // Change to "console" if you want formatted summary instead
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "prometheus");

    println!("Middleware Configuration Demo");
    println!("============================\n");
    println!("Demonstrating:");
    println!("  • Flow-level rate limit: 1.0 events/sec (applies to all stages)");
    println!("  • Source override: 10.0 events/sec (stage-level override)");
    println!("  • Transform: inherits flow-level 1.0 events/sec");
    println!("  • Result: Source produces at 10/sec, transform throttles to 1.0/sec\n");

    println!("How to observe in real-time:");
    println!("  1. Run with --server flag (requires warp-server feature):");
    println!("     cargo run -p obzenflow --example flow_middleware_config \\");
    println!("       --features obzenflow_infra/warp-server -- --server");
    println!("  2. Query metrics while flow runs:");
    println!("     curl http://localhost:9090/metrics | grep events_processed_total");
    println!("  3. Watch the rate differences between stages!\n");

    println!("Running with 120 events (will take ~120 seconds due to 1.0/sec transform rate)...\n");

    let journal_path = std::path::PathBuf::from("target/flow_middleware_config_journal");

    FlowApplication::run(flow! {
        name: "middleware_config_demo",
        journals: disk_journals(journal_path.clone()),
        middleware: [
                // Flow-level rate limit: 1.0 events/sec
                // All stages inherit this unless they override
                rate_limit(1.0)
            ],

            stages: {
                // Source with stage-level override: 10 events/sec
                // This overrides the flow-level 1.0 events/sec
                src = source!("fast_source" => FiniteSourceTyped::from_item_fn(|index| {
                    if index >= 120 {
                        return None;
                    }

                    let count = index + 1;

                    // Log progress every 20 events
                    if count % 20 == 0 {
                        println!("[SOURCE] Generated {count} events");
                    }

                    Some(CounterEvent { count })
                }), [
                    rate_limit(10.0)
                ]);

                // Transform with NO override
                // Inherits flow-level rate limit of 1.0 events/sec
                trans = transform!("throttled_transform" => PassthroughTransform::new());

                // Sink
                snk = sink!("counting_sink" => CountingSink::new());
            },

            topology: {
                src |> trans;
                trans |> snk;
            }
    })
    .await?;

    println!("\n\nFlow completed successfully!");
    println!("\nKey observations:");
    println!("  • Source emitted at 10 events/sec (stage override worked)");
    println!("  • Transform processed at 1.0 events/sec (inherited flow limit)");
    println!("  • Sink received at 1.0 events/sec (bottlenecked by transform)");
    println!("  • Backpressure naturally flowed upstream");
    println!("\nThis proves:");
    println!("  1. Stage-level middleware overrides flow-level");
    println!("  2. Stages without overrides inherit flow-level config");
    println!("  3. Middleware inheritance works as designed!");
    println!("\nProduction tip:");
    println!("  Use --server flag + /metrics endpoint to observe rate limiting");
    println!("  in real-time. This is how you debug middleware in production.");

    Ok(())
}

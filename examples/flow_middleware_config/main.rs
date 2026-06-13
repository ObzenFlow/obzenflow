// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Demo: Targeted Middleware Configuration
//!
//! This demonstrates targeted policy middleware on a live source intake surface.
//!
//! **How to observe rate limiting in real-time:**
//!   1. Run the example (requires `warp-server` feature):
//!      ```
//!      cargo run -p obzenflow --example flow_middleware_config --features obzenflow_infra/warp-server
//!      ```
//!   2. Query metrics while flow runs:
//!      ```
//!      curl http://localhost:9090/metrics | grep "events_processed_total"
//!      ```
//!   3. Observe source intake pacing:
//!      - fast_source: ~10 events/sec (source policy)
//!      - throttled_transform: deterministic processing, no policy middleware
//!      - counting_sink: receives events at the source-paced rate
//!
//! Key concepts demonstrated:
//! - Policy middleware attaches to live I/O units under FLOWIP-120c H1
//! - Flow-level policy broadcast is intentionally not used
//! - Deterministic transforms stay free of rate-limit/circuit-breaker policy
//! - Backpressure naturally flows upstream
//! - Production observability via /metrics endpoint

use anyhow::Result;
use async_trait::async_trait;
use obzenflow::typed::sources;
use obzenflow_adapters::middleware::RateLimiterBuilder;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{event::chain_event::ChainEvent, TypedPayload};
use obzenflow_dsl::{flow, sink, source, transform};
use obzenflow_infra::application::{Banner, FlowApplication, Presentation};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{SinkHandler, TransformHandler};
use serde::{Deserialize, Serialize};
const CONFIG_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/flow_middleware_config/obzenflow.toml"
);

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

fn main() -> Result<()> {
    let journal_path = std::path::PathBuf::from("target/flow_middleware_config_journal");

    let presentation = Presentation::new(
        Banner::new("Middleware Configuration Demo")
            .description("Policy middleware attached to a live source surface.")
            .bullets(
                "Demonstrating",
                [
                    "Source rate limit: 10.0 events/sec",
                    "Transform: deterministic processing with no policy middleware",
                    "Flow-level policy broadcast: intentionally absent under FLOWIP-120c",
                    "Result: source intake paces the rest of the flow",
                ],
            )
            .section(
                "How to observe in real-time",
                "1. Run the example (requires warp-server feature):\n   cargo run -p obzenflow --example flow_middleware_config \\\n     --features obzenflow_infra/warp-server\n2. Query metrics while flow runs:\n   curl http://localhost:9090/metrics | grep events_processed_total\n3. Watch the rate differences between stages!",
            )
            .section(
                "Run duration",
                "Running with 120 events paced at the source intake boundary...",
            ),
    )
    .with_footer(|outcome| {
        outcome
            .into_footer()
            .bullets(
                "Key observations",
                [
                    "Source emitted at the configured intake rate",
                    "Transform processed deterministically without policy middleware",
                    "Sink received events at the source-paced rate",
                    "Backpressure naturally flowed upstream",
                ],
            )
            .bullets(
                "This proves",
                [
                    "Policy middleware targets the protected live I/O unit",
                    "Deterministic handler shells do not carry policy middleware",
                    "Observation/structural middleware can still be configured separately",
                ],
            )
            .paragraph(
                "Production tip: use the default config-backed /metrics endpoint to observe rate limiting in real time.",
            )
    });

    FlowApplication::builder()
        .with_config_file(CONFIG_FILE)
        .with_presentation(presentation)
        .run_blocking(flow! {
            name: "middleware_config_demo",
            journals: disk_journals(journal_path.clone()),
            middleware: [],

            stages: {
                // Source intake is a live I/O unit, so rate limiting belongs here.
                fast_source = source!(CounterEvent => sources::finite_from_fn(|index| {
                    if index >= 120 {
                        return None;
                    }

                    let count = index + 1;

                    // Log progress every 20 events
                    if count % 20 == 0 {
                        println!("[SOURCE] Generated {count} events");
                    }

                    Some(CounterEvent { count })
                }), [RateLimiterBuilder::new(10.0).build()]);

                // Deterministic transform with no policy middleware.
                throttled_transform = transform!(CounterEvent -> CounterEvent => PassthroughTransform::new());

                // Sink
                counting_sink = sink!(CounterEvent => CountingSink::new());
            },

            topology: {
                fast_source |> throttled_transform;
                throttled_transform |> counting_sink;
            }
        })?;

    Ok(())
}

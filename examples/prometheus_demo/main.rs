// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Prometheus Demo with FlowApplication Framework (FLOWIP-080h, 080j & 082a)
//!
//! Processes a configurable volume of events (default 100,000) demonstrating:
//! - Source-intake rate limiting middleware
//! - Fan-out topology pattern (one stage to multiple downstream stages)
//! - ReduceTyped for type-safe event counting (FLOWIP-080j)
//! - TypedPayload for strongly-typed events (FLOWIP-082a)
//! - Prometheus metrics via /metrics endpoint
//!
//! **FLOWIP-080h Update**: Replaced 38-line ErrorProneTransform struct with Map helper
//! **FLOWIP-080j Update**: Replaced 59-line EventCounter StatefulHandler with ReduceTyped
//! **FLOWIP-082a Update**: Added TypedPayload with EVENT_TYPE and SCHEMA_VERSION constants
//!
//! Run with: cargo run -p obzenflow --example prometheus_demo --features obzenflow_infra/warp-server
//!
//! Event volume is operator-tunable via `PROMETHEUS_EVENT_COUNT` (default
//! 100000), so varying the load needs no code change.
//!
//! The default startup config starts the web server with:
//! - /metrics endpoint for Prometheus metrics (framework-level metrics)
//! - /api/topology endpoint for flow structure
//! - /health and /ready endpoints for monitoring

use anyhow::Result;
use async_trait::async_trait;
use obzenflow::env::env_var_or;
use obzenflow::typed::{sources, stateful as typed_stateful, transforms as typed_transforms};
use obzenflow_adapters::middleware::RateLimiterBuilder;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{event::chain_event::ChainEvent, TypedPayload};
use obzenflow_dsl::{flow, sink, source, stateful, transform, FlowDefinition};
use obzenflow_infra::application::{Banner, FlowApplication, LogLevel, Presentation};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::SinkHandler;
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::transform::TryMapTyped;
use obzenflow_runtime::typing::SinkTyping;
use serde::{Deserialize, Serialize};
const CONFIG_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/prometheus_demo/obzenflow.toml"
);

/// Default event volume; override with `PROMETHEUS_EVENT_COUNT`.
const DEFAULT_EVENT_COUNT: usize = 100_000;

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

// ============================================================================
// FLOWIP-114b: Typed transform helper for the error-prone stage
// ============================================================================

/// Transform that can fail on certain events.
///
/// Returns `TryMapTyped<DataRequest, ProcessedEvent, String, _>`, which both
/// satisfies `TransformTyping<Input = DataRequest, Output = ProcessedEvent>`
/// for the typed `transform!` macro. The transform supervisor owns terminal
/// error marking and error-journal routing.
fn error_prone_transform() -> TryMapTyped<
    DataRequest,
    ProcessedEvent,
    String,
    impl Fn(DataRequest) -> Result<ProcessedEvent, String> + Send + Sync + Clone,
> {
    typed_transforms::try_map(|req: DataRequest| {
        if req.should_fail {
            Err("Simulated processing error".to_string())
        } else {
            Ok(ProcessedEvent {
                id: req.id,
                should_fail: req.should_fail,
                batch: req.batch,
                processed: true,
                processing_stage: "error_prone_transform".to_string(),
            })
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

impl SinkTyping for CompletionSink {
    type Input = ProcessedEvent;
}

#[async_trait]
impl SinkHandler for CompletionSink {
    async fn consume(
        &mut self,
        _event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("InMemory".to_string()),
            Some(1),
        ))
    }

    // Receipt-only completion probe: re-delivery under either archive verb is safe.
    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

fn main() -> Result<()> {
    // Operator-tunable event volume through the framework env helpers, so the
    // load varies without a code change (default 100k).
    let total_events = env_var_or::<usize>("PROMETHEUS_EVENT_COUNT", DEFAULT_EVENT_COUNT)?;

    let presentation = Presentation::new(
        Banner::new("Prometheus Demo")
            .description("Configurable event volume (default 100k) with rate limiting and fan-out.")
            .bullets(
                "Demonstrating",
                [
                    "Source-intake rate limiting middleware",
                    "Fan-out topology (processor -> counter + sink)",
                    "StatefulHandler for business-level counting",
                    "Framework Prometheus metrics",
                ],
            )
            .section(
                "Usage",
                "Default:     cargo run --package obzenflow --example prometheus_demo --features obzenflow_infra/warp-server\nVolume:      PROMETHEUS_EVENT_COUNT=1000 cargo run --package obzenflow --example prometheus_demo --features obzenflow_infra/warp-server\nCustom port: cargo run --package obzenflow --example prometheus_demo --features obzenflow_infra/warp-server -- --server-port 8080",
            ),
    )
    .with_footer(|outcome| {
        outcome
            .into_footer()
            .paragraph("Run with the default config and visit /metrics for Prometheus metrics.")
    });

    // Use FlowApplication builder - handles runtime, observability, and features automatically.
    FlowApplication::builder()
        .with_config_file(CONFIG_FILE)
        .with_log_level(LogLevel::Info)
        .with_presentation(presentation)
        .run_blocking(FlowDefinition::materialize(move |_runtime_config| {
            let high_volume_source_handler = sources::finite_from_fn(move |index| {
                if index >= total_events {
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
            });
            let error_processor_handler = error_prone_transform();
            let event_counter_handler = typed_stateful::reduce(
                EventCountState::default(),
                |state: &mut EventCountState, _event: &ProcessedEvent| {
                    state.event_count += 1;
                    if state.event_count.is_multiple_of(10_000) {
                        println!("📊 Counted {} events so far...", state.event_count);
                    }
                },
            )
            .emit_on_eof();
            let summary_sink_handler = SinkTyped::new(move |summary: EventCountState| async move {
                    let count = summary.event_count;
                    let errors = total_events.saturating_sub(count);

                    println!();
                    println!("=====================================");
                    println!("📊 Business-Level Event Count (FLOWIP-080j):");
                    println!("   Successfully processed: {count} events");
                    println!(
                        "   Note: {total_events} generated - {count} = {errors} errors (routed to error journal)"
                    );
                    println!("=====================================");
                    println!();
                    println!("💡 Key Improvement:");
                    println!("   59-line EventCounter StatefulHandler → ReduceTyped helper");
                    println!("   Type-safe accumulation with zero ChainEvent manipulation!");
                    println!();
                    println!("📈 To view framework-level Prometheus metrics:");
                    println!("   1. Run the example with the default config");
                    println!("   2. Visit http://localhost:9090/metrics");
                    println!("   3. See detailed per-stage metrics, errors, latencies, etc.");
                    println!("=====================================");
                })
                .idempotent();
            let completion_sink_handler = CompletionSink::new();

            Ok(flow! {
                name: "prometheus_demo",
                journals: disk_journals(std::path::PathBuf::from("target/prometheus_demo_journal")),

                stages: {
                    // Source intake is the live I/O boundary where rate limiting belongs.
                    high_volume_source = source!(DataRequest => high_volume_source_handler, [
                        RateLimiterBuilder::new(1000.0).build()
                    ]);
                    error_processor = transform!(DataRequest -> ProcessedEvent => error_processor_handler);
                    event_counter = stateful!(ProcessedEvent -> EventCountState => event_counter_handler);
                    summary_sink = sink!(EventCountState => summary_sink_handler);
                    completion_sink = sink!(ProcessedEvent => completion_sink_handler);
                },

                topology: {
                    high_volume_source |> error_processor;
                    error_processor |> event_counter;
                    error_processor |> completion_sink;
                    event_counter |> summary_sink;
                }
            })
        }))?;

    Ok(())
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Prometheus metrics demo
//!
//! Processes a configurable volume of events (default 100,000) demonstrating:
//! - Source-intake rate limiting middleware
//! - Circuit-breaker opening, failed probing, and recovery during source outages
//! - Enforced backpressure between stages
//! - Counting successfully processed events
//! - Strongly typed events
//! - Prometheus metrics via /metrics endpoint
//!
//! Run with: cargo run -p obzenflow --example prometheus_demo --features prometheus,web-host
//!
//! Event volume is operator-tunable through `[app] event_count` (default
//! 100000), so varying the load needs no code change.
//! With the `tokio-console` feature, the framework's `--tokio-console` flag enables
//! Tokio Console diagnostics. Diagnostics are disabled by default.
//!
//! This example explicitly opts into hosting and monitoring through its config:
//! - /metrics endpoint for Prometheus metrics (framework-level metrics)
//! - /api/topology endpoint for flow structure
//! - /health and /ready endpoints for monitoring

use anyhow::Result;
use obzenflow::application::{Banner, FlowApplication, LogLevel, Presentation};
use obzenflow::flow::backpressure::enforced;
use obzenflow::flow::{flow, sink, source, stateful, transform, FlowBuildError, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::{circuit_breaker, rate_limit};
use obzenflow::schema::TypedPayload;
use obzenflow::stages::sinks::ConsoleSink;
use obzenflow::stages::sources::TypedFiniteSourceHandler;
use obzenflow::stages::sources::{SourceDiagnosticReason, SourceError};
use obzenflow::stages::transforms::TryMapTyped;
use obzenflow::stages::{stateful, transforms};
use serde::{Deserialize, Serialize};
use std::time::Duration;
const CONFIG_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/prometheus_demo/obzenflow.toml"
);

/// Default event volume; override with `[app] event_count`.
const DEFAULT_EVENT_COUNT: usize = 100_000;
const SOURCE_OUTAGE_INTERVAL: usize = 20_000;
const SOURCE_BREAKER_COOLDOWN: Duration = Duration::from_secs(5);
const JOURNAL_ROOT: &str = "target/prometheus_demo_journal";

/// The demo's `[app]` settings (FLOWIP-010t).
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PrometheusDemoSettings {
    /// Inputs the source generates.
    #[serde(default = "default_event_count")]
    event_count: usize,
    /// Keeps the 1,000/s source limiter; false omits it for measurement.
    #[serde(default = "keep_source_rate_limit")]
    source_rate_limit: bool,
}

fn default_event_count() -> usize {
    DEFAULT_EVENT_COUNT
}

fn keep_source_rate_limit() -> bool {
    true
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

/// Simulates an input service that periodically becomes unavailable. A failed
/// poll does not consume an input: the breaker waits, probes once unsuccessfully,
/// then recovers on its next probe and emits the same pending input.
#[derive(Clone, Debug)]
struct HighVolumeSource {
    next_id: usize,
    total_events: usize,
    outage_interval: usize,
    outage_failures: u8,
}

impl TypedFiniteSourceHandler for HighVolumeSource {
    type Output = DataRequest;

    fn next(&mut self) -> Result<Option<Vec<DataRequest>>, SourceError> {
        if self.next_id >= self.total_events {
            println!(
                "🏁 Source complete: Generated {} total events",
                self.next_id
            );
            return Ok(None);
        }

        if self.next_id > 0
            && self.next_id.is_multiple_of(self.outage_interval)
            && self.outage_failures < 2
        {
            self.outage_failures += 1;
            tracing::warn!(
                next_id = self.next_id,
                attempt = self.outage_failures,
                "Simulated source outage: circuit breaker will wait five seconds before probing"
            );
            return Err(SourceError::Timeout(
                SourceDiagnosticReason::TimedOut.into(),
            ));
        }

        self.outage_failures = 0;
        let current_id = self.next_id;
        self.next_id += 1;
        if self.next_id.is_multiple_of(10_000) {
            println!("📊 Generated {} events...", self.next_id);
        }
        Ok(Some(vec![DataRequest {
            id: current_id,
            should_fail: current_id.is_multiple_of(100),
            batch: current_id / 100,
        }]))
    }
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
    transforms::try_map(|req: DataRequest| {
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
pub(crate) struct EventCountState {
    event_count: usize,
}

impl TypedPayload for EventCountState {
    const EVENT_TYPE: &'static str = "prometheus.event_count";
    const SCHEMA_VERSION: u32 = 1;
}

fn main() -> Result<()> {
    let presentation = Presentation::new(
        Banner::new("Prometheus Demo")
            .description("Configurable event volume (default 100k) with circuit breaking, rate limiting, and backpressure.")
            .bullets(
                "Demonstrating",
                [
                    "Source-intake rate limiting at 1,000/s, unless [app] source_rate_limit = false",
                    "Source outages with five-second circuit-breaker cooldowns",
                    "Enforced backpressure (64 events per edge)",
                    "Counting successfully processed events",
                    "Framework Prometheus metrics",
                ],
            )
            .section(
                "Usage",
                "Default:     cargo run --package obzenflow --example prometheus_demo --features prometheus,web-host\nVolume:      set event_count under [app] in a copy of the config, then pass the copy with --config\nCustom port: cargo run --package obzenflow --example prometheus_demo --features prometheus,web-host -- --server-port 8080",
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
        .run_blocking(demo_flow())?;

    Ok(())
}

/// The shipped demo, with its volume and source limiter read from `[app]`.
fn demo_flow() -> FlowDefinition {
    FlowDefinition::materialize_with(|_runtime_config, settings: PrometheusDemoSettings| {
        let journal_root = std::path::PathBuf::from(JOURNAL_ROOT);
        Ok(if settings.source_rate_limit {
            flow_definition(settings.event_count, journal_root)
        } else {
            flow_definition_with_options(
                settings.event_count,
                journal_root,
                SOURCE_OUTAGE_INTERVAL,
                SourceRateLimit::Disabled,
            )
        })
    })
}

/// The example's flow, also exercised by its journal integration test.
pub(crate) fn flow_definition(
    total_events: usize,
    journal_root: std::path::PathBuf,
) -> FlowDefinition {
    flow_definition_with_outage_interval(total_events, journal_root, SOURCE_OUTAGE_INTERVAL)
}

/// The same demo with a shorter outage interval for bounded integration proofs.
/// The breaker duration, middleware, backpressure, and business path stay intact.
pub(crate) fn flow_definition_with_outage_interval(
    total_events: usize,
    journal_root: std::path::PathBuf,
    outage_interval: usize,
) -> FlowDefinition {
    flow_definition_with_options(
        total_events,
        journal_root,
        outage_interval,
        SourceRateLimit::Enabled,
    )
}

#[derive(Clone, Copy)]
enum SourceRateLimit {
    Enabled,
    Disabled,
}

fn flow_definition_with_options(
    total_events: usize,
    journal_root: std::path::PathBuf,
    outage_interval: usize,
    source_rate_limit: SourceRateLimit,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        prometheus_flow(
            total_events,
            journal_root,
            outage_interval,
            source_rate_limit,
        )
    })
}

fn prometheus_flow(
    total_events: usize,
    journal_root: std::path::PathBuf,
    outage_interval: usize,
    source_rate_limit: SourceRateLimit,
) -> Result<FlowDefinition, FlowBuildError> {
    assert!(
        outage_interval > 0,
        "source outage interval must be positive"
    );
    let high_volume_source_handler = HighVolumeSource {
        next_id: 0,
        total_events,
        outage_interval,
        outage_failures: 0,
    };
    let error_processor_handler = error_prone_transform();
    let event_counter_handler = stateful::reduce(
        EventCountState::default(),
        |state: &mut EventCountState, _event: &ProcessedEvent| {
            state.event_count += 1;
            if state.event_count.is_multiple_of(10_000) {
                println!("📊 Counted {} events so far...", state.event_count);
            }
        },
    )
    .emit_on_eof();
    let summary_sink_handler = ConsoleSink::new(format_summary);

    Ok(flow! {
        name: "prometheus_demo",
        journals: disk_journals(journal_root),
        // Explicit demo defaults; operator configuration can override them.
        backpressure: enforced(64).stall_timeout_ms(30_000),

        stages: {
            // The simulated service outage belongs at the live source boundary.
            // Pure transform errors still follow their existing error-journal path.
            high_volume_source = match source_rate_limit {
                SourceRateLimit::Enabled => source!(DataRequest => high_volume_source_handler with {
                    circuit_breaker()
                        .consecutive_failures(1)
                        .open_for(SOURCE_BREAKER_COOLDOWN)
                        .probes(1),
                    rate_limit(1000.0)
                }),
                SourceRateLimit::Disabled => source!(DataRequest => high_volume_source_handler with {
                    circuit_breaker()
                        .consecutive_failures(1)
                        .open_for(SOURCE_BREAKER_COOLDOWN)
                        .probes(1)
                }),
            };
            error_processor = transform!(DataRequest -> ProcessedEvent => error_processor_handler);
            event_counter = stateful!(ProcessedEvent -> EventCountState => event_counter_handler);
            summary_sink = sink!(EventCountState => summary_sink_handler);
        },

        topology: {
            high_volume_source |> error_processor;
            error_processor |> event_counter;
            event_counter |> summary_sink;
        }
    })
}

pub(crate) fn format_summary(summary: &EventCountState) -> String {
    let count = summary.event_count;
    format!(
        concat!(
            "\n=====================================\n",
            "📊 Processing summary:\n",
            "   Successfully processed: {count} events\n",
            "====================================="
        ),
        count = count
    )
}

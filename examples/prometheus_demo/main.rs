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
//! With the `tokio-console` feature, the framework's `--tokio-console` flag enables
//! Tokio Console diagnostics. Diagnostics are disabled by default.
//!
//! This example explicitly opts into hosting and monitoring through its config:
//! - /metrics endpoint for Prometheus metrics (framework-level metrics)
//! - /api/topology endpoint for flow structure
//! - /health and /ready endpoints for monitoring

mod support;

use anyhow::Result;
use obzenflow::application::{Banner, FlowApplication, LogLevel, Presentation};
use obzenflow::flow::FlowDefinition;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use support::{flow_definition, flow_definition_with_options, SOURCE_OUTAGE_INTERVAL};

const CONFIG_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/examples/prometheus_demo/obzenflow.toml"
);

const DEFAULT_EVENT_COUNT: usize = 100_000;
const JOURNAL_ROOT: &str = "target/prometheus_demo_journal";

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PrometheusDemoSettings {
    #[serde(default = "default_event_count")]
    event_count: usize,
    #[serde(default = "keep_source_rate_limit")]
    source_rate_limit: bool,
}

fn default_event_count() -> usize {
    DEFAULT_EVENT_COUNT
}

fn keep_source_rate_limit() -> bool {
    true
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

fn demo_flow() -> FlowDefinition {
    FlowDefinition::materialize_with(|_runtime_config, settings: PrometheusDemoSettings| {
        let journal_root = PathBuf::from(JOURNAL_ROOT);
        Ok(if settings.source_rate_limit {
            flow_definition(settings.event_count, journal_root)
        } else {
            flow_definition_with_options(
                settings.event_count,
                journal_root,
                SOURCE_OUTAGE_INTERVAL,
                false,
            )
        })
    })
}

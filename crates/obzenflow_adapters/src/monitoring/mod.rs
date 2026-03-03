// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Monitoring Taxonomies for ObzenFlow
//!
//! Monitoring taxonomies are now documentation and view definitions.
//! Metrics are automatically collected by the MetricsAggregator from the event journal.
//!
//! ## Available Taxonomies
//!
//! - **RED**: Rate, Errors, Duration - ideal for request/response systems
//! - **USE**: Utilization, Saturation, Errors - ideal for resource monitoring  
//! - **Golden Signals**: Latency, Traffic, Errors, Saturation - Google SRE's approach
//! - **SAAFE**: Saturation, Amendments, Anomalies, Failures, Errors - for data pipelines
//!
//! ## How Metrics Work
//!
//! 1. Events flow through the journal with all context (wide events)
//! 2. MetricsAggregator subscribes to the journal and derives metrics
//! 3. MetricsEndpoint exposes metrics in Prometheus format
//! 4. Taxonomies provide Prometheus queries and Grafana dashboards

pub mod aggregator;
pub mod exporters;
pub mod metrics;
pub mod taxonomies;

pub use exporters::PrometheusExporter;
pub use taxonomies::{GoldenSignals, RED, SAAFE, USE};

/// Initialize the monitoring subsystem
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    // No longer needed - MetricsAggregator handles everything
    Ok(())
}

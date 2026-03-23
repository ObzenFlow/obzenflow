// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Monitoring support for ObzenFlow.
//!
//! After `FLOWIP-084h`, ObzenFlow's metrics model is intentionally split:
//!
//! 1. application metrics are derived from wide events and journals into
//!    `AppMetricsSnapshot`
//! 2. infrastructure metrics are observed directly into `InfraMetricsSnapshot`
//! 3. exporters render both into one Prometheus exposition surface
//!
//! This crate contains the runtime-facing collection and export plumbing for that
//! model. Dashboard organization and monitoring lenses live in static dashboard/docs
//! assets, not in the Rust API surface.

pub mod aggregator;
pub mod exporters;
pub mod metrics;

pub use exporters::PrometheusExporter;

/// Initialize the monitoring subsystem
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    // Legacy no-op retained for older setup code. Monitoring is configured through
    // the runtime/exporter wiring rather than an explicit init step.
    Ok(())
}

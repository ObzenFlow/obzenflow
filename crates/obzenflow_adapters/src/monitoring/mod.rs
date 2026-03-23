// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Monitoring support for ObzenFlow.
//!
//! This module contains two different kinds of monitoring material:
//!
//! - runtime collection and export plumbing
//! - taxonomy helpers such as RED, USE, Golden Signals, and SAAFE
//!
//! After `FLOWIP-084h`, ObzenFlow's metrics model is intentionally split:
//!
//! 1. application metrics are derived from wide events and journals into
//!    `AppMetricsSnapshot`
//! 2. infrastructure metrics are observed directly into `InfraMetricsSnapshot`
//! 3. exporters render both into one Prometheus exposition surface
//!
//! The taxonomy modules in this crate are query and dashboard helpers over that
//! exported metrics surface. They are not middleware, and they are not the source
//! of runtime metric truth.
//!
//! ## Available taxonomy helpers
//!
//! - **RED**: Rate, Errors, Duration
//! - **USE**: Utilization, Saturation, Errors
//! - **Golden Signals**: Latency, Traffic, Errors, Saturation
//! - **SAAFE**: Saturation, Amendments, Anomalies, Failures, Errors

pub mod aggregator;
pub mod exporters;
pub mod metrics;
pub mod taxonomies;

pub use exporters::PrometheusExporter;
pub use taxonomies::{GoldenSignals, RED, SAAFE, USE};

/// Initialize the monitoring subsystem
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    // Legacy no-op retained for older setup code. Monitoring is configured through
    // the runtime/exporter wiring rather than an explicit init step.
    Ok(())
}

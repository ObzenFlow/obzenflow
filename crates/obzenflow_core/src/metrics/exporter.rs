// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics exporter trait for exposing metrics in various formats
//!
//! This trait allows the runtime layer to work with metrics exporters
//! without knowing about concrete implementations in the adapters layer.

use crate::metrics::snapshots::{AppMetricsSnapshot, InfraMetricsSnapshot};
use std::error::Error;

/// Trait for exporting metrics in various formats
///
/// This trait is implemented by concrete exporters in the adapters layer
/// and used as a trait object in the runtime layer to maintain proper
/// architectural boundaries.
///
/// The dual collection pattern separates application metrics (derived from
/// the event stream) from infrastructure metrics (observed directly).
pub trait MetricsExporter: Send + Sync {
    /// Update application metrics derived from the event stream
    ///
    /// Called periodically with aggregated metrics from journal events.
    /// These metrics include event counts, processing times, and flow metrics.
    fn update_app_metrics(
        &self,
        snapshot: AppMetricsSnapshot,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Default implementation for backward compatibility
        let _ = snapshot;
        Ok(())
    }

    /// Update infrastructure metrics from direct observation
    ///
    /// Called periodically with metrics that cannot go through the journal
    /// such as journal write latency, memory usage, and queue depths.
    fn update_infra_metrics(
        &self,
        snapshot: InfraMetricsSnapshot,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Default implementation for backward compatibility
        let _ = snapshot;
        Ok(())
    }

    /// Render metrics in Prometheus exposition format
    ///
    /// Returns the metrics as a string ready to be served via HTTP
    /// or written to a file. Should merge both app and infra metrics.
    fn render_metrics(&self) -> Result<String, Box<dyn Error + Send + Sync>>;

    /// Get the number of metric families being tracked
    ///
    /// Useful for monitoring the metrics system itself
    fn metric_count(&self) -> usize {
        0 // Default implementation
    }
}

/// No-op implementation for when metrics are disabled
pub struct NoOpMetricsExporter;

impl MetricsExporter for NoOpMetricsExporter {
    fn render_metrics(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        Ok(String::from("# Metrics disabled\n"))
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::monitoring::metrics::core::Metric;

/// Low-level exporter interface for adapter-side helpers and tests.
///
/// This is not the main runtime metrics contract. The public runtime/export path uses
/// `obzenflow_core::metrics::MetricsExporter`.
pub(crate) trait MetricExporter<T> {
    /// Export a single metric to the target format
    fn export(&self, metric: &dyn Metric) -> Result<T, ExportError>;

    /// Export multiple metrics efficiently (can be overridden for batch optimizations)
    #[cfg_attr(not(test), allow(dead_code))]
    fn export_batch(&self, metrics: Vec<&dyn Metric>) -> Result<Vec<T>, ExportError> {
        metrics
            .into_iter()
            .map(|metric| self.export(metric))
            .collect()
    }
}

/// Errors that can occur during metric export
#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    #[error("Unsupported metric type: {metric_type} for exporter {exporter_name}")]
    UnsupportedMetricType {
        metric_type: String,
        exporter_name: String,
    },

    #[error("Export format error: {message}")]
    FormatError { message: String },

    #[error("Network error during export: {message}")]
    NetworkError { message: String },

    #[error("Authentication error: {message}")]
    AuthError { message: String },

    #[error("Configuration error: {message}")]
    ConfigError { message: String },
}

// Always available for testing
pub mod test;

// Supported adapter-side exporters
pub mod console_summary;
pub mod prometheus_exporter;

// Builder for creating exporters
pub mod builder;

// Re-exports
pub use self::builder::{ExporterType, MetricsExporterBuilder};
pub use self::console_summary::ConsoleSummaryExporter;
pub use self::prometheus_exporter::PrometheusExporter;
pub use self::test::TestExporter;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::metrics::core::{MetricSnapshot, MetricValue};
    use std::collections::HashMap;
    use std::time::Instant;
    use tokio::sync::broadcast;

    // Mock metric for testing
    struct MockMetric {
        name: String,
        value: MetricValue,
        tx: broadcast::Sender<crate::monitoring::metrics::core::MetricUpdate>,
    }

    impl MockMetric {
        fn new(name: String, value: MetricValue) -> Self {
            let (tx, _) = broadcast::channel(10);
            Self { name, value, tx }
        }
    }

    impl Metric for MockMetric {
        fn name(&self) -> &str {
            &self.name
        }

        fn snapshot(&self) -> MetricSnapshot {
            MetricSnapshot {
                name: self.name.clone(),
                metric_type: self.value.metric_type(),
                value: self.value.clone(),
                timestamp: Instant::now(),
                labels: HashMap::new(),
            }
        }

        fn update(&self, _value: MetricValue) {
            // Mock implementation
        }

        fn subscribe(&self) -> broadcast::Receiver<crate::monitoring::metrics::core::MetricUpdate> {
            self.tx.subscribe()
        }
    }

    // Mock exporter for testing
    struct MockExporter;

    impl MetricExporter<String> for MockExporter {
        fn export(&self, metric: &dyn Metric) -> Result<String, ExportError> {
            Ok(format!("exported:{}", metric.name()))
        }
    }

    #[test]
    fn test_mock_exporter() {
        let metric = MockMetric::new("test_counter".to_string(), MetricValue::Counter(42));

        let exporter = MockExporter;
        let result = exporter.export(&metric).unwrap();

        assert_eq!(result, "exported:test_counter");
    }

    #[test]
    fn test_batch_export() {
        let metrics = [
            MockMetric::new("counter1".to_string(), MetricValue::Counter(10)),
            MockMetric::new("counter2".to_string(), MetricValue::Counter(20)),
        ];

        let metric_refs: Vec<&dyn Metric> = metrics.iter().map(|m| m as &dyn Metric).collect();

        let exporter = MockExporter;
        let results = exporter.export_batch(metric_refs).unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "exported:counter1");
        assert_eq!(results[1], "exported:counter2");
    }
}

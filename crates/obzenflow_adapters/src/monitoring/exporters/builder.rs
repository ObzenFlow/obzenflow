//! Metrics exporter builder for creating different types of exporters
//!
//! This module provides a clean builder pattern for creating metrics exporters
//! based on configuration, avoiding the "block cosplaying as a factory" anti-pattern.

use super::{ConsoleSummaryExporter, PrometheusExporter};
use obzenflow_core::metrics::MetricsExporter;
use std::sync::Arc;

/// Builder for creating metrics exporters
pub struct MetricsExporterBuilder {
    exporter_type: ExporterType,
}

/// Types of metrics exporters available
pub enum ExporterType {
    /// Prometheus pull-based metrics
    Prometheus,
    /// StatsD push-based metrics
    #[cfg(feature = "metrics-statsd")]
    StatsD { host: String, port: u16 },
    /// OpenTelemetry metrics
    #[cfg(feature = "metrics-otel")]
    OpenTelemetry { endpoint: String },
    /// Console output for debugging
    Console,
    /// No-op exporter when metrics are disabled
    Noop,
}

impl MetricsExporterBuilder {
    /// Create a builder configured from environment variables
    pub fn from_env() -> Self {
        use std::env;

        // Check if metrics are enabled first
        let enabled = env::var("OBZENFLOW_METRICS_ENABLED")
            .ok()
            .and_then(|v| v.parse::<bool>().ok())
            .unwrap_or(true);

        if !enabled {
            return Self {
                exporter_type: ExporterType::Noop,
            };
        }

        // Determine exporter type from env var
        let exporter_type = match env::var("OBZENFLOW_METRICS_EXPORTER").as_deref() {
            Ok("prometheus") | Err(_) => ExporterType::Prometheus, // Default
            Ok("console") => ExporterType::Console,
            Ok("noop") => ExporterType::Noop,
            #[cfg(feature = "metrics-statsd")]
            Ok("statsd") => {
                let host =
                    env::var("OBZENFLOW_STATSD_HOST").unwrap_or_else(|_| "localhost".to_string());
                let port = env::var("OBZENFLOW_STATSD_PORT")
                    .ok()
                    .and_then(|p| p.parse().ok())
                    .unwrap_or(8125);
                ExporterType::StatsD { host, port }
            }
            #[cfg(feature = "metrics-otel")]
            Ok("opentelemetry") => {
                let endpoint = env::var("OBZENFLOW_OTEL_ENDPOINT")
                    .unwrap_or_else(|_| "http://localhost:4318".to_string());
                ExporterType::OpenTelemetry { endpoint }
            }
            Ok(other) => {
                tracing::warn!("Unknown metrics exporter type: {}, using prometheus", other);
                ExporterType::Prometheus
            }
        };

        Self { exporter_type }
    }

    /// Create a builder with Prometheus exporter
    pub fn prometheus() -> Self {
        Self {
            exporter_type: ExporterType::Prometheus,
        }
    }

    /// Create a builder with console summary exporter
    pub fn console() -> Self {
        Self {
            exporter_type: ExporterType::Console,
        }
    }

    /// Create a builder with no-op exporter
    pub fn noop() -> Self {
        Self {
            exporter_type: ExporterType::Noop,
        }
    }

    /// Set the exporter type
    pub fn with_type(mut self, exporter_type: ExporterType) -> Self {
        self.exporter_type = exporter_type;
        self
    }

    /// Build the metrics exporter
    pub fn build(self) -> Arc<dyn MetricsExporter> {
        match self.exporter_type {
            ExporterType::Prometheus => {
                tracing::info!("Creating Prometheus metrics exporter");
                Arc::new(PrometheusExporter::new())
            }
            ExporterType::Console => {
                tracing::info!("Creating Console summary metrics exporter");
                Arc::new(ConsoleSummaryExporter::new())
            }
            ExporterType::Noop => {
                tracing::info!("Creating No-op metrics exporter");
                Arc::new(NoopExporter)
            }
            #[cfg(feature = "metrics-statsd")]
            ExporterType::StatsD { host, port } => {
                tracing::info!("Creating StatsD metrics exporter: {}:{}", host, port);
                let target = format!("{host}:{port}");
                match super::StatsDExporter::new(target)
                    .map(|exporter| exporter.with_prefix("obzenflow."))
                {
                    Ok(exporter) => Arc::new(exporter),
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            "Failed to create StatsD exporter; falling back to Noop"
                        );
                        Arc::new(NoopExporter)
                    }
                }
            }
            #[cfg(feature = "metrics-otel")]
            ExporterType::OpenTelemetry { endpoint } => {
                tracing::info!("Creating OpenTelemetry metrics exporter: {}", endpoint);
                Arc::new(super::OtelExporter::new(endpoint))
            }
        }
    }
}

/// No-op metrics exporter that does nothing
struct NoopExporter;

impl MetricsExporter for NoopExporter {
    fn update_app_metrics(
        &self,
        _snapshot: obzenflow_core::metrics::AppMetricsSnapshot,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn update_infra_metrics(
        &self,
        _snapshot: obzenflow_core::metrics::InfraMetricsSnapshot,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    fn render_metrics(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_defaults() {
        let exporter = MetricsExporterBuilder::prometheus().build();
        assert!(exporter.render_metrics().is_ok());
    }

    #[test]
    fn test_noop_exporter() {
        let exporter = MetricsExporterBuilder::noop().build();
        let result = exporter.render_metrics().unwrap();
        assert_eq!(result, "");
    }
}

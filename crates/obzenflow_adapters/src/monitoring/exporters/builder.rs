// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics exporter builder for the supported exporter set.

use super::{ConsoleSummaryExporter, PrometheusExporter};
use obzenflow_core::metrics::MetricsExporter;
use std::sync::Arc;

/// Builder for creating metrics exporters
pub struct MetricsExporterBuilder {
    exporter_type: ExporterType,
}

/// Supported metrics exporters.
pub enum ExporterType {
    /// Prometheus pull-based metrics
    Prometheus,
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
    use std::env;
    use std::sync::Mutex;

    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn with_metrics_exporter_env(value: Option<&str>, test: impl FnOnce()) {
        let _guard = ENV_MUTEX.lock().unwrap();
        let saved_enabled = env::var("OBZENFLOW_METRICS_ENABLED").ok();
        let saved_exporter = env::var("OBZENFLOW_METRICS_EXPORTER").ok();

        env::remove_var("OBZENFLOW_METRICS_ENABLED");
        match value {
            Some(value) => env::set_var("OBZENFLOW_METRICS_EXPORTER", value),
            None => env::remove_var("OBZENFLOW_METRICS_EXPORTER"),
        }

        test();

        match saved_enabled {
            Some(value) => env::set_var("OBZENFLOW_METRICS_ENABLED", value),
            None => env::remove_var("OBZENFLOW_METRICS_ENABLED"),
        }
        match saved_exporter {
            Some(value) => env::set_var("OBZENFLOW_METRICS_EXPORTER", value),
            None => env::remove_var("OBZENFLOW_METRICS_EXPORTER"),
        }
    }

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

    #[test]
    fn test_from_env_supports_console() {
        with_metrics_exporter_env(Some("console"), || {
            let exporter = MetricsExporterBuilder::from_env().build();
            assert!(exporter.render_metrics().is_ok());
        });
    }

    #[test]
    fn test_from_env_supports_noop() {
        with_metrics_exporter_env(Some("noop"), || {
            let exporter = MetricsExporterBuilder::from_env().build();
            assert_eq!(exporter.render_metrics().unwrap(), "");
        });
    }

    #[test]
    fn test_from_env_unknown_exporter_falls_back_to_prometheus() {
        with_metrics_exporter_env(Some("legacy_exporter"), || {
            let exporter = MetricsExporterBuilder::from_env().build();
            let output = exporter.render_metrics().unwrap();
            assert!(output.contains("obzenflow_build_info"));
        });
    }
}

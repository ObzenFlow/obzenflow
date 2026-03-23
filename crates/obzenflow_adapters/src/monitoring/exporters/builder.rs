// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics exporter builder for the supported exporter set.

use super::{ConsoleSummaryExporter, PrometheusExporter};
use obzenflow_core::metrics::MetricsExporter;
#[cfg(test)]
use obzenflow_runtime::bootstrap::{install_bootstrap_config, BootstrapConfig, MetricsBootstrap};
use obzenflow_runtime::bootstrap::{metrics_bootstrap, MetricsExporterKind};
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
    /// Create a builder configured from the runtime bootstrap context.
    pub fn from_bootstrap() -> Self {
        let metrics = metrics_bootstrap();
        if !metrics.enabled {
            return Self::noop();
        }

        let exporter_type = match metrics.exporter {
            MetricsExporterKind::Prometheus => ExporterType::Prometheus,
            MetricsExporterKind::Console => ExporterType::Console,
            MetricsExporterKind::Noop => ExporterType::Noop,
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

    fn with_metrics_bootstrap(metrics: MetricsBootstrap, test: impl FnOnce()) {
        let _guard = install_bootstrap_config(BootstrapConfig {
            metrics,
            ..BootstrapConfig::default()
        });
        test();
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
    fn test_from_bootstrap_supports_console() {
        with_metrics_bootstrap(
            MetricsBootstrap {
                enabled: true,
                exporter: MetricsExporterKind::Console,
            },
            || {
                let exporter = MetricsExporterBuilder::from_bootstrap().build();
                assert!(exporter.render_metrics().is_ok());
            },
        );
    }

    #[test]
    fn test_from_bootstrap_supports_noop() {
        with_metrics_bootstrap(
            MetricsBootstrap {
                enabled: false,
                exporter: MetricsExporterKind::Prometheus,
            },
            || {
                let exporter = MetricsExporterBuilder::from_bootstrap().build();
                assert_eq!(exporter.render_metrics().unwrap(), "");
            },
        );
    }

    #[test]
    fn test_from_bootstrap_defaults_to_prometheus() {
        with_metrics_bootstrap(MetricsBootstrap::default(), || {
            let exporter = MetricsExporterBuilder::from_bootstrap().build();
            let output = exporter.render_metrics().unwrap();
            assert!(output.contains("obzenflow_build_info"));
        });
    }
}

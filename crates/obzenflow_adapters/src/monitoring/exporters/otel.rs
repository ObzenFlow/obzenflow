//! OpenTelemetry exporter placeholder.
//!
//! The `metrics-otel` feature currently provides a minimal `MetricsExporter` implementation
//! so `--all-features` builds cleanly. A full OTLP implementation can be added later.

use obzenflow_core::metrics::{AppMetricsSnapshot, InfraMetricsSnapshot, MetricsExporter};
use std::error::Error;
use std::sync::RwLock;

pub struct OtelExporter {
    endpoint: String,
    app_snapshot: RwLock<Option<AppMetricsSnapshot>>,
    infra_snapshot: RwLock<Option<InfraMetricsSnapshot>>,
}

impl OtelExporter {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            app_snapshot: RwLock::new(None),
            infra_snapshot: RwLock::new(None),
        }
    }
}

impl MetricsExporter for OtelExporter {
    fn update_app_metrics(
        &self,
        snapshot: AppMetricsSnapshot,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut guard = self
            .app_snapshot
            .write()
            .map_err(|_| "Failed to acquire app snapshot write lock")?;
        *guard = Some(snapshot);
        Ok(())
    }

    fn update_infra_metrics(
        &self,
        snapshot: InfraMetricsSnapshot,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut guard = self
            .infra_snapshot
            .write()
            .map_err(|_| "Failed to acquire infra snapshot write lock")?;
        *guard = Some(snapshot);
        Ok(())
    }

    fn render_metrics(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        Ok(format!(
            "# OpenTelemetry exporter configured\n# endpoint: {}\n# NOTE: OTEL export is not implemented yet\n",
            self.endpoint
        ))
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Per-run metrics reporting, collection ownership and scrape delivery (FLOWIP-140h).
use crate::application::config::{MetricsReporter, ResolvedMetricsConfig};
use obzenflow_adapters::monitoring::MetricsReadModel;
use obzenflow_core::metrics::MetricsSnapshotSink;
use std::sync::Arc;
use tokio::task::JoinHandle;

pub(crate) struct MetricsReporting {
    model: Option<Arc<MetricsReadModel>>,
    collector: Option<JoinHandle<()>>,
}

impl MetricsReporting {
    pub(crate) fn new(config: &ResolvedMetricsConfig) -> Self {
        let model = (cfg!(feature = "prometheus")
            && config.enabled
            && config.exporter == MetricsReporter::Prometheus)
            .then(|| Arc::new(MetricsReadModel::default()));
        Self {
            model,
            collector: None,
        }
    }

    pub(crate) fn sink(&self) -> Option<Arc<dyn MetricsSnapshotSink>> {
        self.model
            .as_ref()
            .map(|model| model.clone() as Arc<dyn MetricsSnapshotSink>)
    }

    pub(crate) fn start(&mut self, collector: JoinHandle<()>) {
        self.collector = Some(collector);
    }

    pub(crate) async fn finish(&mut self) {
        if let Some(collector) = self.collector.take() {
            collector.abort();
            let _ = collector.await;
        }
    }

    #[cfg(feature = "prometheus")]
    pub(crate) fn prometheus_endpoint(
        &self,
    ) -> Option<crate::web::endpoints::PrometheusMetricsEndpoint> {
        self.model
            .as_ref()
            .map(|model| crate::web::endpoints::PrometheusMetricsEndpoint::new(model.clone()))
    }
}

impl Drop for MetricsReporting {
    fn drop(&mut self) {
        if let Some(task) = &self.collector {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_enabled_compiled_prometheus_allocates_reporting() {
        for enabled in [false, true] {
            for exporter in [MetricsReporter::Prometheus, MetricsReporter::Noop] {
                let reporting = MetricsReporting::new(&ResolvedMetricsConfig { enabled, exporter });
                assert_eq!(
                    reporting.sink().is_some(),
                    cfg!(feature = "prometheus")
                        && enabled
                        && exporter == MetricsReporter::Prometheus
                );
                assert!(reporting.collector.is_none());
            }
        }
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Infra owns monitoring selection, delivery and shutdown (FLOWIP-140h).
mod console;
use crate::application::config::{MonitoringSelection, ResolvedMetricsConfig};
use obzenflow_adapters::monitoring::MetricsReadModel;
use obzenflow_core::metrics::MetricsSnapshotSink;
use obzenflow_core::{event::SystemEvent, journal::Journal};
use obzenflow_runtime::pipeline::FlowHandle;
use std::sync::{Arc, Weak};
use tokio::task::JoinHandle;
use tokio::time::Instant;

pub(crate) struct MonitoringBackend {
    model: Option<Arc<MetricsReadModel>>,
    selection: MonitoringSelection,
    console: Option<console::ConsoleOwner>,
    collector: Option<JoinHandle<()>>,
}

impl MonitoringBackend {
    pub(crate) fn new(config: &ResolvedMetricsConfig) -> Self {
        let model = (config.enabled && config.exporter != MonitoringSelection::Noop)
            .then(|| Arc::new(MetricsReadModel::default()));
        Self {
            model,
            selection: config.exporter,
            console: None,
            collector: None,
        }
    }
    pub(crate) fn sink(&self) -> Option<Arc<dyn MetricsSnapshotSink>> {
        self.model
            .as_ref()
            .map(|model| model.clone() as Arc<dyn MetricsSnapshotSink>)
    }
    pub(crate) fn is_console(&self) -> bool {
        self.model.is_some() && self.selection == MonitoringSelection::Console
    }
    pub(crate) fn start(
        &mut self,
        collector: JoinHandle<()>,
        flow: Option<Weak<FlowHandle>>,
        journal: Option<Arc<dyn Journal<SystemEvent>>>,
    ) {
        let abort_collector = collector.abort_handle();
        self.collector = Some(collector);
        if self.is_console() {
            self.console = Some(console::ConsoleOwner::start(
                self.model.as_ref().unwrap().clone(),
                flow,
                journal,
                Some(abort_collector),
            ));
        }
    }
    pub(crate) fn shutdown_deadline(&self, deadline: Instant) {
        if let Some(console) = &self.console {
            console.shutdown_deadline(deadline);
        }
    }
    pub(crate) async fn finish(&mut self, deadline: Instant, cleanup_complete: bool) {
        if let Some(collector) = self.collector.take() {
            collector.abort();
            let _ = collector.await;
        }
        if let Some(console) = self.console.take() {
            console.finish(deadline, cleanup_complete).await;
        }
    }
    #[cfg(feature = "prometheus")]
    pub(crate) fn prometheus_endpoint(&self) -> Option<crate::web::endpoints::MetricsHttpEndpoint> {
        if self.selection != MonitoringSelection::Prometheus {
            return None;
        }
        self.model
            .as_ref()
            .map(|model| crate::web::endpoints::MetricsHttpEndpoint::new(model.clone()))
    }
}

impl Drop for MonitoringBackend {
    fn drop(&mut self) {
        if let Some(task) = &self.collector {
            task.abort();
        }
    }
}

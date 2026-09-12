// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics aggregator implementation

pub mod builder;
pub mod constants;
pub mod fsm;
pub mod handle;
pub mod inputs;
pub mod instrumentation;
pub mod supervisor;
pub mod tail_read;

// Re-export commonly used types
// Note: MetricsAggregatorSupervisor is intentionally NOT exported - use MetricsAggregatorBuilder
pub use builder::MetricsAggregatorBuilder;
#[doc(hidden)]
pub use fsm::MetricsStore;
pub use fsm::{
    MetricsAggregatorAction, MetricsAggregatorContext, MetricsAggregatorEvent,
    MetricsAggregatorState, StageMetrics,
};
pub use handle::{MetricsHandle, MetricsHandleExt};
pub use inputs::MetricsInputs;

#[cfg(test)]
#[derive(Default)]
pub(crate) struct RecordingSnapshots {
    app: std::sync::Mutex<Option<obzenflow_core::metrics::AppMetricsSnapshot>>,
    infra: std::sync::Mutex<Option<obzenflow_core::metrics::InfraMetricsSnapshot>>,
}
#[cfg(test)]
impl obzenflow_core::metrics::MetricsSnapshotExporter for RecordingSnapshots {
    fn publish_app_snapshot(&self, value: obzenflow_core::metrics::AppMetricsSnapshot) {
        *self.app.lock().unwrap() = Some(value);
    }
    fn publish_infra_snapshot(&self, value: obzenflow_core::metrics::InfraMetricsSnapshot) {
        *self.infra.lock().unwrap() = Some(value);
    }
}

mod snapshot;
mod subscription;

#[cfg(feature = "test-support")]
pub(crate) mod tests;

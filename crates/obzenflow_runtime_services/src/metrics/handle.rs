//! Handle for controlling the metrics aggregator
//!
//! The metrics aggregator runs mostly autonomously, processing events from
//! the journal. This handle allows for basic control like triggering exports
//! or initiating shutdown.

use super::fsm::{MetricsAggregatorEvent, MetricsAggregatorState};
use crate::supervised_base::{HandleError, StandardHandle, SupervisorHandle};

/// Type alias for the metrics handle
///
/// Since metrics doesn't need custom error types or extra fields,
/// we can use StandardHandle directly.
pub type MetricsHandle = StandardHandle<MetricsAggregatorEvent, MetricsAggregatorState>;

/// Extension trait for metrics-specific convenience methods
pub trait MetricsHandleExt {
    /// Trigger an immediate metrics export
    fn export_metrics(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Start draining the metrics aggregator
    fn start_draining(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;
}

impl MetricsHandleExt for MetricsHandle {
    async fn export_metrics(&self) -> Result<(), HandleError> {
        self.send_event(MetricsAggregatorEvent::ExportMetrics).await
    }

    async fn start_draining(&self) -> Result<(), HandleError> {
        self.send_event(MetricsAggregatorEvent::StartDraining).await
    }
}

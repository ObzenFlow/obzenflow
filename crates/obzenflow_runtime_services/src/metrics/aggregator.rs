//! Metrics aggregator trait for deriving metrics from event streams
//!
//! This trait defines the interface for aggregating metrics from journal events,
//! allowing the runtime layer to work with aggregators without depending on
//! concrete implementations in the adapters layer.

use obzenflow_core::EventEnvelope;
use obzenflow_core::metrics::AppMetricsSnapshot;

/// Trait for aggregating metrics from the event stream
/// 
/// Implementations of this trait subscribe to the journal and derive metrics
/// from events according to the "metrics as queries over events" philosophy.
/// The aggregator processes events and periodically creates snapshots for export.
pub trait MetricsAggregator: Send + Sync {
    /// Process a single event envelope to update internal metrics
    /// 
    /// This method is called for each event received from the journal subscription.
    /// Implementations should:
    /// - Extract flow and stage information from event context
    /// - Update counters, histograms, and other metrics
    /// - Handle both data events and control events appropriately
    fn process_event(&mut self, envelope: &EventEnvelope);
    
    /// Create a snapshot of current metrics for export
    /// 
    /// This method is called periodically (typically every 10 seconds) to create
    /// a snapshot of all accumulated metrics. The snapshot is then pushed to the
    /// MetricsExporter for rendering in various formats.
    fn create_snapshot(&self) -> AppMetricsSnapshot;
    
    /// Reset all metrics to initial state
    /// 
    /// Optional method for clearing all accumulated metrics.
    /// Useful for testing or when metrics need to be reset.
    fn reset(&mut self) {
        // Default implementation: no-op
    }
}

/// Factory trait for creating MetricsAggregator instances
/// 
/// This trait allows the adapters layer to provide a factory that the runtime
/// can use to create aggregator instances without knowing the concrete type.
pub trait MetricsAggregatorFactory: Send + Sync {
    /// Create a new MetricsAggregator instance
    fn create(&self) -> Box<dyn MetricsAggregator>;
}
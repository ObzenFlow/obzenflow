use super::{NewMetric as Metric, NewMetricValue as MetricValue, NewMetricUpdate as MetricUpdate, NewMetricSnapshot as MetricSnapshot, NewMetricType as MetricType};
use super::core::{EventfulMetric};
use super::primitives::Counter;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::broadcast;

const DEFAULT_METRIC_CHANNEL_CAPACITY: usize = 1024;

/// Types of amendments that can occur during pipeline execution
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmendmentType {
    /// Lifecycle amendments
    Start,
    Stop,
    // Future: Pause, Resume
    
    // Future: Configuration amendments
    // SchemaChange { from: String, to: String },
    // ModelSwap { from: String, to: String },
    
    // Future: Scaling amendments
    // ScaleUp { instances: usize },
    // ScaleDown { instances: usize },
    
    // Future: Operational amendments
    // FailoverInitiated,
    // BackpressureApplied,
}


/// Amendment metric - wraps Counter primitive for amendment tracking
///
/// Simplified to wrap a Counter primitive. Amendment type categorization is handled 
/// by exporters using labels. This follows the FLOWIP-004 specification.
pub struct AmendmentMetric {
    name: String,
    counter: Counter,  // Simple counter for total amendments
    realtime_tx: broadcast::Sender<MetricUpdate>,
}

impl AmendmentMetric {
    pub fn new(name: impl Into<String>) -> Self {
        let (tx, _) = broadcast::channel(DEFAULT_METRIC_CHANNEL_CAPACITY);
        
        Self {
            name: name.into(),
            counter: Counter::new(),
            realtime_tx: tx,
        }
    }
    
    /// Record any amendment (delegated to Counter primitive)
    pub fn record_amendment(&self) {
        self.counter.increment();
        
        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Counter(self.counter.get()),
            timestamp: Instant::now(),
        });
    }
    
    /// Record that a stage has started (convenience method)
    pub fn record_start(&self) {
        self.record_amendment();
    }
    
    /// Record that a stage has stopped (convenience method)
    pub fn record_stop(&self) {
        self.record_amendment();
    }
    
    /// Record amendment with type information (for backward compatibility)
    /// 
    /// Note: Type categorization is now handled by exporters via labels.
    /// Multiple AmendmentMetric instances can be used for different types.
    pub fn record(&self, _amendment_type: AmendmentType) {
        self.record_amendment();
    }
    
    /// Get total number of amendments recorded
    pub fn total_amendments(&self) -> u64 {
        self.counter.get()
    }
    
    /// Reset amendment counter to zero
    pub fn reset(&self) {
        self.counter.reset();
        
        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Counter(0),
            timestamp: Instant::now(),
        });
    }
}

impl Metric for AmendmentMetric {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn snapshot(&self) -> MetricSnapshot {
        let labels = HashMap::new();
        
        // Labels provide dimensional metadata for metrics filtering and grouping.
        // Examples: {"service": "api", "amendment_type": "start", "region": "us-west-2"}
        // This enables Prometheus queries like: amendments_total{amendment_type="start"}
        // Amendment type categorization is now handled by exporters, not the metric itself.
        // For different amendment types, use separate AmendmentMetric instances.
        
        MetricSnapshot {
            name: self.name.clone(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(self.total_amendments()),
            timestamp: Instant::now(),
            labels,
        }
    }
    
    fn update(&self, value: MetricValue) {
        match value {
            MetricValue::Counter(count) => {
                // Record multiple amendments
                for _ in 0..count {
                    self.record_amendment();
                }
            }
            _ => {} // Amendments only accept counter values
        }
    }
    
    fn subscribe(&self) -> broadcast::Receiver<MetricUpdate> {
        self.realtime_tx.subscribe()
    }
}

// Mark AmendmentMetric as externally observable (can be tracked via lifecycle events)
impl EventfulMetric for AmendmentMetric {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_amendment_metric_basic_functionality() {
        let metric = AmendmentMetric::new("test_amendments");
        
        assert_eq!(metric.name(), "test_amendments");
        assert_eq!(metric.total_amendments(), 0);
        
        metric.record_start();
        assert_eq!(metric.total_amendments(), 1);
        
        metric.record_stop();
        assert_eq!(metric.total_amendments(), 2);
        
        metric.record_amendment();
        assert_eq!(metric.total_amendments(), 3);
    }

    #[tokio::test]
    async fn test_amendment_metric_counter_behavior() {
        let metric = AmendmentMetric::new("counter_test");
        
        // Test that amendments are counted
        metric.record_amendment();
        metric.record_amendment();
        metric.record_amendment();
        
        assert_eq!(metric.total_amendments(), 3);
    }

    #[tokio::test]
    async fn test_amendment_metric_snapshot() {
        let metric = AmendmentMetric::new("snapshot_test");
        metric.record_start();
        metric.record_stop();
        
        let snapshot = metric.snapshot();
        assert_eq!(snapshot.name, "snapshot_test");
        assert_eq!(snapshot.metric_type, MetricType::Counter);
        
        if let MetricValue::Counter(value) = snapshot.value {
            assert_eq!(value, 2);
        } else {
            panic!("Expected Counter value");
        }
        
        // Amendment type tracking is now handled by exporters via labels
        assert!(snapshot.labels.is_empty());
    }

    #[tokio::test]
    async fn test_amendment_metric_backward_compatibility() {
        let metric = AmendmentMetric::new("compat_test");
        
        // Old API still works but types are ignored
        metric.record(AmendmentType::Start);
        metric.record(AmendmentType::Stop);
        
        assert_eq!(metric.total_amendments(), 2);
    }

    #[tokio::test]
    async fn test_amendment_metric_reset() {
        let metric = AmendmentMetric::new("reset_test");
        
        metric.record_start();
        metric.record_stop();
        assert_eq!(metric.total_amendments(), 2);
        
        metric.reset();
        assert_eq!(metric.total_amendments(), 0);
    }

    #[tokio::test]
    async fn test_amendment_metric_realtime_updates() {
        let metric = AmendmentMetric::new("realtime_test");
        let mut receiver = metric.subscribe();
        
        metric.record_start();
        
        let update = receiver.recv().await.expect("Should receive update");
        assert_eq!(update.metric_name, "realtime_test");
        
        if let MetricValue::Counter(value) = update.value {
            assert_eq!(value, 1);
        } else {
            panic!("Expected Counter value in update");
        }
    }

    #[tokio::test]
    async fn test_amendment_metric_update_via_trait() {
        let metric = AmendmentMetric::new("trait_test");
        
        metric.update(MetricValue::Counter(3));
        assert_eq!(metric.total_amendments(), 3);
        
        // Non-counter values should be ignored
        metric.update(MetricValue::Gauge(3.14));
        assert_eq!(metric.total_amendments(), 3);
    }

    #[tokio::test]
    async fn test_amendment_type_categorization_pattern() {
        // Demonstrate how to handle different amendment types with separate metrics
        
        let start_amendments = AmendmentMetric::new("amendments_start");
        let stop_amendments = AmendmentMetric::new("amendments_stop");
        
        // Record different types with separate metrics
        start_amendments.record_start();
        start_amendments.record_start();
        
        stop_amendments.record_stop();
        
        assert_eq!(start_amendments.total_amendments(), 2);
        assert_eq!(stop_amendments.total_amendments(), 1);
        
        // Exporters can combine these with appropriate labels:
        // amendments_total{type="start"} 2
        // amendments_total{type="stop"} 1
    }
}
use super::{NewMetric as Metric, NewMetricValue as MetricValue, NewMetricUpdate as MetricUpdate, NewMetricSnapshot as MetricSnapshot, NewMetricType as MetricType};
use super::core::{EventfulMetric};
use super::primitives::Counter;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::broadcast;

const DEFAULT_METRIC_CHANNEL_CAPACITY: usize = 1024;

/// Types of failures that can occur
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailureType {
    /// Critical system failure
    Critical,
    /// Major service disruption  
    Major,
    /// Minor operational issue
    Minor,
    /// External dependency failure
    External,
    /// Configuration or setup issue
    Configuration,
}

impl FailureType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Critical => "critical",
            Self::Major => "major", 
            Self::Minor => "minor",
            Self::External => "external",
            Self::Configuration => "configuration",
        }
    }
}

/// Failure metric - tracks critical failures using a simple counter
/// 
/// Simplified to wrap a Counter primitive. Failure type categorization is handled by exporters using labels.
/// Failure metrics track catastrophic events that require immediate attention,
/// distinct from regular errors which might be expected/handled.
pub struct FailureMetric {
    name: String,
    counter: Counter,
    realtime_tx: broadcast::Sender<MetricUpdate>,
}

impl FailureMetric {
    pub fn new(name: impl Into<String>) -> Self {
        let (tx, _) = broadcast::channel(DEFAULT_METRIC_CHANNEL_CAPACITY);
        
        Self {
            name: name.into(),
            counter: Counter::new(),
            realtime_tx: tx,
        }
    }
    
    /// Record that a failure occurred
    pub fn record_failure(&self) {
        self.counter.increment();
        
        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Counter(self.counter.get()),
            timestamp: Instant::now(),
        });
    }
    
    /// Get the total number of failures recorded
    pub fn total_failures(&self) -> u64 {
        self.counter.get()
    }
    
    pub fn reset(&self) {
        self.counter.reset();
        
        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Counter(0),
            timestamp: Instant::now(),
        });
    }
}

impl Metric for FailureMetric {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn snapshot(&self) -> MetricSnapshot {
        MetricSnapshot {
            name: self.name.clone(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(self.total_failures()),
            timestamp: Instant::now(),
            // Labels provide dimensional metadata for metrics filtering and grouping.
            // Examples: {"service": "api", "failure_type": "critical", "region": "us-west-2"}
            // This enables Prometheus queries like: failures_total{failure_type="critical"}
            // For FailureMetric, we currently don't add specific labels since failure 
            // categorization is handled by exporters via separate metric instances.
            labels: HashMap::new(),
        }
    }
    
    fn update(&self, value: MetricValue) {
        match value {
            MetricValue::Counter(_) => {
                self.record_failure();
            }
            _ => {}
        }
    }
    
    fn subscribe(&self) -> broadcast::Receiver<MetricUpdate> {
        self.realtime_tx.subscribe()
    }
}

// Mark FailureMetric as externally observable
// (failures can often be detected by monitoring stage behavior)
impl EventfulMetric for FailureMetric {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_failure_metric_basic_functionality() {
        let metric = FailureMetric::new("test_failures");
        
        assert_eq!(metric.name(), "test_failures");
        assert_eq!(metric.total_failures(), 0);
        
        metric.record_failure();
        assert_eq!(metric.total_failures(), 1);
        
        metric.record_failure();
        assert_eq!(metric.total_failures(), 2);
        
        metric.record_failure();
        assert_eq!(metric.total_failures(), 3);
    }

    #[tokio::test]
    async fn test_failure_metric_counter_behavior() {
        let metric = FailureMetric::new("counter_test");
        
        // Test that failures are counted
        metric.record_failure();
        metric.record_failure();
        metric.record_failure();
        
        assert_eq!(metric.total_failures(), 3);
    }

    #[tokio::test]
    async fn test_failure_metric_snapshot() {
        let metric = FailureMetric::new("snapshot_test");
        metric.record_failure();
        metric.record_failure();
        
        let snapshot = metric.snapshot();
        assert_eq!(snapshot.name, "snapshot_test");
        assert_eq!(snapshot.metric_type, MetricType::Counter);
        
        if let MetricValue::Counter(value) = snapshot.value {
            assert_eq!(value, 2);
        } else {
            panic!("Expected Counter value");
        }
    }

    #[tokio::test]
    async fn test_failure_metric_reset() {
        let metric = FailureMetric::new("reset_test");
        
        metric.record_failure();
        metric.record_failure();
        assert_eq!(metric.total_failures(), 2);
        
        metric.reset();
        assert_eq!(metric.total_failures(), 0);
    }

    #[tokio::test]
    async fn test_failure_metric_realtime_updates() {
        let metric = FailureMetric::new("realtime_test");
        let mut receiver = metric.subscribe();
        
        metric.record_failure();
        
        let update = receiver.recv().await.expect("Should receive update");
        assert_eq!(update.metric_name, "realtime_test");
        
        if let MetricValue::Counter(value) = update.value {
            assert_eq!(value, 1);
        } else {
            panic!("Expected Counter value in update");
        }
    }

}
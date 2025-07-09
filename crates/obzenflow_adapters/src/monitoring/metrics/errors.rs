use super::core::{Metric, MetricValue, MetricUpdate, MetricSnapshot, MetricType};
use super::core::{EventfulMetric};
use obzenflow_core::metrics::primitives::Counter;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::broadcast;

const DEFAULT_METRIC_CHANNEL_CAPACITY: usize = 1024;

/// Error metric - tracks error counts using a simple counter
/// 
/// Simplified to wrap a Counter primitive. Error type categorization is handled by exporters using labels.
/// This follows the industry pattern from Prometheus, OpenTelemetry, etc.
pub struct ErrorMetric {
    name: String,
    counter: Counter,
    realtime_tx: broadcast::Sender<MetricUpdate>,
}

impl ErrorMetric {
    pub fn new(name: impl Into<String>) -> Self {
        let (tx, _) = broadcast::channel(DEFAULT_METRIC_CHANNEL_CAPACITY);
        
        Self {
            name: name.into(),
            counter: Counter::new(),
            realtime_tx: tx,
        }
    }
    
    /// Record that an error occurred
    pub fn record_error(&self) {
        self.counter.increment();
        
        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Counter(self.counter.get()),
            timestamp: Instant::now(),
        });
    }
    
    /// Get the total number of errors recorded
    pub fn total_errors(&self) -> u64 {
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

impl Metric for ErrorMetric {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn snapshot(&self) -> MetricSnapshot {
        MetricSnapshot {
            name: self.name.clone(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(self.counter.get()),
            timestamp: Instant::now(),
            labels: HashMap::new(),
        }
    }
    
    fn update(&self, value: MetricValue) {
        match value {
            MetricValue::Counter(count) => {
                // Record individual errors - no bulk operations
                for _ in 0..count {
                    self.record_error();
                }
            },
            _ => {}
        }
    }
    
    fn subscribe(&self) -> broadcast::Receiver<MetricUpdate> {
        self.realtime_tx.subscribe()
    }
}

// Mark ErrorMetric as externally observable
impl EventfulMetric for ErrorMetric {}

/// Support trait for stages that can provide error metrics
/// 
/// Error metrics count and categorize failures and can be updated
/// by external observation of processing outcomes.
pub trait ErrorSupport {
    /// Get the error metric instance for this stage
    fn error_metric(&self) -> &ErrorMetric;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_error_metric_basic_functionality() {
        let metric = ErrorMetric::new("test_errors");
        
        assert_eq!(metric.name(), "test_errors");
        assert_eq!(metric.total_errors(), 0);
        
        metric.record_error();
        assert_eq!(metric.total_errors(), 1);
        
        metric.record_error();
        assert_eq!(metric.total_errors(), 2);
        
        metric.record_error();
        assert_eq!(metric.total_errors(), 3);
    }

    #[tokio::test]
    async fn test_error_metric_snapshot() {
        let metric = ErrorMetric::new("snapshot_test");
        metric.record_error();
        
        let snapshot = metric.snapshot();
        assert_eq!(snapshot.name, "snapshot_test");
        assert_eq!(snapshot.metric_type, MetricType::Counter);
        
        if let MetricValue::Counter(value) = snapshot.value {
            assert_eq!(value, 1);
        } else {
            panic!("Expected Counter value");
        }
    }

    #[tokio::test]
    async fn test_error_metric_reset() {
        let metric = ErrorMetric::new("reset_test");
        
        metric.record_error();
        metric.record_error();
        assert_eq!(metric.total_errors(), 2);
        
        metric.reset();
        assert_eq!(metric.total_errors(), 0);
    }
}
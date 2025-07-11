use super::core::{Metric, MetricValue, MetricUpdate, MetricSnapshot, MetricType};
use super::core::{StatefulMetric};
use obzenflow_core::metrics::primitives::Counter;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::broadcast;

const DEFAULT_METRIC_CHANNEL_CAPACITY: usize = 1024;

/// Anomaly metric - wraps Counter primitive for anomaly detection tracking
///
/// Simplified to wrap a Counter primitive. Anomaly severity tracking and complex analysis 
/// is handled by exporters. This follows the FLOWIP-004 specification.
/// 
/// For severity tracking, use a separate Gauge-based metric or multiple Counter instances.
pub struct AnomalyMetric {
    name: String,
    counter: Counter,  // Simple counter for total anomalies detected
    realtime_tx: broadcast::Sender<MetricUpdate>,
}

impl AnomalyMetric {
    pub fn new(name: impl Into<String>) -> Self {
        let (tx, _) = broadcast::channel(DEFAULT_METRIC_CHANNEL_CAPACITY);
        
        Self {
            name: name.into(),
            counter: Counter::new(),
            realtime_tx: tx,
        }
    }
    
    /// Record that an anomaly was detected (delegated to Counter primitive)
    pub fn record_anomaly(&self) {
        self.counter.increment();
        
        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Counter(self.counter.get()),
            timestamp: Instant::now(),
        });
    }
    
    /// Record anomaly with severity (for backward compatibility)
    /// 
    /// Note: Severity tracking is now handled by exporters or separate metrics.
    /// This method ignores the severity and just counts the anomaly.
    pub fn record_anomaly_with_severity(&self, _severity: f64) {
        self.record_anomaly();
    }
    
    /// Get total number of anomalies detected
    pub fn anomaly_count(&self) -> u64 {
        self.counter.get()
    }
    
    /// Reset anomaly counter to zero
    pub fn reset(&self) {
        self.counter.reset();
        
        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Counter(0),
            timestamp: Instant::now(),
        });
    }
}

impl Metric for AnomalyMetric {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn snapshot(&self) -> MetricSnapshot {
        let labels = HashMap::new();
        
        // Labels provide dimensional metadata for metrics filtering and grouping.
        // Examples: {"service": "api", "anomaly_type": "latency_spike", "region": "us-west-2"}
        // This enables Prometheus queries like: anomalies_total{anomaly_type="latency_spike"}
        // Severity tracking and complex analysis is now handled by exporters, not the metric itself.
        // For severity tracking, use separate Gauge-based metrics or multiple Counter instances.
        
        MetricSnapshot {
            name: self.name.clone(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(self.anomaly_count()),
            timestamp: Instant::now(),
            labels,
        }
    }
    
    fn update(&self, value: MetricValue) {
        match value {
            MetricValue::Counter(count) => {
                // Record multiple anomalies
                for _ in 0..count {
                    self.record_anomaly();
                }
            }
            _ => {}
        }
    }
    
    fn subscribe(&self) -> broadcast::Receiver<MetricUpdate> {
        self.realtime_tx.subscribe()
    }
}

// Mark AnomalyMetric as requiring internal state reporting
// (stages must decide what constitutes an anomaly in their domain)
impl StatefulMetric for AnomalyMetric {}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_anomaly_metric_basic_functionality() {
        let metric = AnomalyMetric::new("test_anomalies");
        
        assert_eq!(metric.name(), "test_anomalies");
        assert_eq!(metric.anomaly_count(), 0);
        
        metric.record_anomaly();
        assert_eq!(metric.anomaly_count(), 1);
        
        metric.record_anomaly();
        assert_eq!(metric.anomaly_count(), 2);
        
        metric.record_anomaly();
        assert_eq!(metric.anomaly_count(), 3);
    }

    #[tokio::test]
    async fn test_anomaly_metric_counter_behavior() {
        let metric = AnomalyMetric::new("counter_test");
        
        // Test that anomalies are counted
        metric.record_anomaly();
        metric.record_anomaly();
        metric.record_anomaly();
        
        assert_eq!(metric.anomaly_count(), 3);
    }

    #[tokio::test]
    async fn test_anomaly_metric_snapshot() {
        let metric = AnomalyMetric::new("snapshot_test");
        metric.record_anomaly();
        metric.record_anomaly();
        
        let snapshot = metric.snapshot();
        assert_eq!(snapshot.name, "snapshot_test");
        assert_eq!(snapshot.metric_type, MetricType::Counter);
        
        if let MetricValue::Counter(value) = snapshot.value {
            assert_eq!(value, 2);
        } else {
            panic!("Expected Counter value");
        }
        
        // Severity tracking is now handled by exporters via labels or separate metrics
        assert!(snapshot.labels.is_empty());
    }

    #[tokio::test]
    async fn test_anomaly_metric_backward_compatibility() {
        let metric = AnomalyMetric::new("compat_test");
        
        // Old API still works but severity is ignored
        metric.record_anomaly_with_severity(0.8);
        metric.record_anomaly_with_severity(0.2);
        
        assert_eq!(metric.anomaly_count(), 2);
    }

    #[tokio::test]
    async fn test_anomaly_metric_reset() {
        let metric = AnomalyMetric::new("reset_test");
        
        metric.record_anomaly();
        metric.record_anomaly();
        assert_eq!(metric.anomaly_count(), 2);
        
        metric.reset();
        assert_eq!(metric.anomaly_count(), 0);
    }

    #[tokio::test]
    async fn test_anomaly_metric_realtime_updates() {
        let metric = AnomalyMetric::new("realtime_test");
        let mut receiver = metric.subscribe();
        
        metric.record_anomaly();
        
        let update = receiver.recv().await.expect("Should receive update");
        assert_eq!(update.metric_name, "realtime_test");
        
        if let MetricValue::Counter(value) = update.value {
            assert_eq!(value, 1);
        } else {
            panic!("Expected Counter value in update");
        }
    }

    #[tokio::test]
    async fn test_anomaly_metric_update_via_trait() {
        let metric = AnomalyMetric::new("trait_test");
        
        metric.update(MetricValue::Counter(3));
        assert_eq!(metric.anomaly_count(), 3);
        
        // Non-counter values should be ignored
        metric.update(MetricValue::Gauge(3.14));
        assert_eq!(metric.anomaly_count(), 3);
    }

    #[tokio::test]
    async fn test_anomaly_severity_tracking_pattern() {
        // Demonstrate how to handle severity tracking with separate metrics
        
        use obzenflow_core::metrics::primitives::Gauge;
        
        let anomaly_count = AnomalyMetric::new("anomalies_count");
        let anomaly_severity = Gauge::new(); // Separate gauge for severity tracking
        
        // Record anomalies with severity using separate metrics
        let severity1 = 0.8;
        anomaly_count.record_anomaly();
        anomaly_severity.set(severity1);
        
        let severity2 = 0.3;
        anomaly_count.record_anomaly();
        anomaly_severity.set(severity2); // Latest severity
        
        assert_eq!(anomaly_count.anomaly_count(), 2);
        assert!((anomaly_severity.get() - 0.3).abs() < 0.001);
        
        // Exporters can combine these metrics:
        // anomalies_total 2
        // anomaly_severity_current 0.3
        // 
        // Or use multiple counters for severity buckets:
        // anomalies_low_severity_total 1
        // anomalies_high_severity_total 1
    }

    #[tokio::test]
    async fn test_anomaly_type_categorization_pattern() {
        // Demonstrate how to handle different anomaly types with separate metrics
        
        let latency_anomalies = AnomalyMetric::new("anomalies_latency");
        let error_rate_anomalies = AnomalyMetric::new("anomalies_error_rate");
        let throughput_anomalies = AnomalyMetric::new("anomalies_throughput");
        
        // Record different types with separate metrics
        latency_anomalies.record_anomaly();
        latency_anomalies.record_anomaly();
        
        error_rate_anomalies.record_anomaly();
        
        throughput_anomalies.record_anomaly();
        throughput_anomalies.record_anomaly();
        throughput_anomalies.record_anomaly();
        
        assert_eq!(latency_anomalies.anomaly_count(), 2);
        assert_eq!(error_rate_anomalies.anomaly_count(), 1);
        assert_eq!(throughput_anomalies.anomaly_count(), 3);
        
        // Exporters can combine these with appropriate labels:
        // anomalies_total{type="latency"} 2
        // anomalies_total{type="error_rate"} 1
        // anomalies_total{type="throughput"} 3
    }
}
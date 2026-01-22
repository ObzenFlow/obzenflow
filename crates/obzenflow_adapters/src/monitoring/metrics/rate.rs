use super::core::{EventfulMetric, Metric, MetricSnapshot, MetricType, MetricUpdate, MetricValue};
use obzenflow_core::metrics::primitives::Counter;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::broadcast;

const DEFAULT_METRIC_CHANNEL_CAPACITY: usize = 1024;

/// Rate metric - tracks events using a simple counter
///
/// Simplified to wrap a Counter primitive. Rate calculation is handled by exporters.
/// This follows the industry pattern from Prometheus, OpenTelemetry, etc.
pub struct RateMetric {
    name: String,
    counter: Counter,
    realtime_tx: broadcast::Sender<MetricUpdate>,
}

impl RateMetric {
    pub fn new(name: impl Into<String>) -> Self {
        let (tx, rx) = broadcast::channel(DEFAULT_METRIC_CHANNEL_CAPACITY);
        drop(rx); // We only need the sender

        Self {
            name: name.into(),
            counter: Counter::new(),
            realtime_tx: tx,
        }
    }

    /// Record that an event occurred
    pub fn record_event(&self) {
        self.counter.increment();

        let result = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Counter(self.counter.get()),
            timestamp: Instant::now(),
        });

        if result.is_err() {
            // No subscribers, which is fine
        }
    }

    /// Get the total number of events recorded
    /// Note: Rate calculation is now handled by exporters, not internally
    pub fn total_events(&self) -> u64 {
        self.counter.get()
    }

    pub fn reset(&self) {
        self.counter.reset();

        let result = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Counter(0),
            timestamp: Instant::now(),
        });

        if result.is_err() {
            // No subscribers, which is fine
        }
    }
}

impl Metric for RateMetric {
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
                // Record individual events - no bulk operations
                for _ in 0..count {
                    self.record_event();
                }
            }
            MetricValue::Gauge(_) => {
                // Can't meaningfully update a counter with a gauge value
            }
            MetricValue::Histogram { .. } | MetricValue::Summary { .. } => {
                // Other metric types don't apply to counter-based metrics
            }
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<MetricUpdate> {
        self.realtime_tx.subscribe()
    }
}

// Mark RateMetric as externally observable
impl EventfulMetric for RateMetric {}

/// Support trait for stages that can provide rate metrics
///
/// Rate metrics count discrete events (requests, messages, operations)
/// and can be updated by external observation of event flow.
pub trait RateSupport {
    /// Get the rate metric instance for this stage
    fn rate_metric(&self) -> &RateMetric;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rate_metric_basic_functionality() {
        let metric = RateMetric::new("test_rate");

        assert_eq!(metric.name(), "test_rate");
        assert_eq!(metric.total_events(), 0);

        metric.record_event();
        assert_eq!(metric.total_events(), 1);

        metric.record_event();
        metric.record_event();
        assert_eq!(metric.total_events(), 3);
    }

    #[tokio::test]
    async fn test_rate_metric_snapshot() {
        let metric = RateMetric::new("snapshot_test");
        metric.record_event();

        let snapshot = metric.snapshot();
        assert_eq!(snapshot.name, "snapshot_test");
        assert_eq!(snapshot.metric_type, MetricType::Counter);

        if let MetricValue::Counter(count) = snapshot.value {
            assert_eq!(count, 1);
        } else {
            panic!("Expected Counter value");
        }
    }

    #[tokio::test]
    async fn test_rate_metric_update_via_trait() {
        let metric = RateMetric::new("trait_test");

        metric.update(MetricValue::Counter(3));
        assert_eq!(metric.total_events(), 3);

        // Non-counter values should be ignored
        metric.update(MetricValue::Gauge(std::f64::consts::PI));
        assert_eq!(metric.total_events(), 3);
    }

    #[tokio::test]
    async fn test_rate_metric_realtime_updates() {
        let metric = RateMetric::new("realtime_test");
        let mut receiver = metric.subscribe();

        metric.record_event();

        let update = receiver.recv().await.expect("Should receive update");
        assert_eq!(update.metric_name, "realtime_test");

        if let MetricValue::Counter(count) = update.value {
            assert_eq!(count, 1);
        } else {
            panic!("Expected Counter value in update");
        }
    }

    #[tokio::test]
    async fn test_rate_metric_reset() {
        let metric = RateMetric::new("reset_test");

        metric.record_event();
        metric.record_event();
        assert_eq!(metric.total_events(), 2);

        metric.reset();
        assert_eq!(metric.total_events(), 0);
    }
}

use super::core::StatefulMetric;
use super::core::{Metric, MetricSnapshot, MetricType, MetricUpdate, MetricValue};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::broadcast;

const DEFAULT_METRIC_CHANNEL_CAPACITY: usize = 1024;

/// Pure saturation metric - tracks resource capacity usage without export format coupling
///
/// This metric requires internal state reporting from stages since external observers
/// cannot see queue depths, connection pool states, or other internal capacity limits.
///
/// The stage decides:
/// - What constitutes saturation for their specific use case
/// - How to calculate their saturation ratio
/// - When to report state changes
///
/// FlowState just stores and reports the value as a ratio (0.0 to 1.0).
pub struct SaturationMetric {
    name: String,
    current_ratio: AtomicU64, // Store as fixed-point (multiply by 10000 for 4 decimal places)
    realtime_tx: broadcast::Sender<MetricUpdate>,
}

impl SaturationMetric {
    pub fn new(name: impl Into<String>) -> Self {
        let (tx, _) = broadcast::channel(DEFAULT_METRIC_CHANNEL_CAPACITY);

        Self {
            name: name.into(),
            current_ratio: AtomicU64::new(0),
            realtime_tx: tx,
        }
    }

    /// Set saturation as a ratio (0.0 to 1.0)
    pub fn set_ratio(&self, ratio: f64) {
        let clamped = ratio.clamp(0.0, 1.0);
        let fixed_point = (clamped * 10000.0) as u64;
        self.current_ratio.store(fixed_point, Ordering::Relaxed);

        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Gauge(clamped),
            timestamp: Instant::now(),
        });
    }

    /// Get current saturation ratio (0.0 to 1.0)
    pub fn current_ratio(&self) -> f64 {
        let fixed_point = self.current_ratio.load(Ordering::Relaxed);
        fixed_point as f64 / 10000.0
    }

    /// Reset to non-saturated state
    pub fn reset(&self) {
        self.set_ratio(0.0);
    }
}

impl Metric for SaturationMetric {
    fn name(&self) -> &str {
        &self.name
    }

    fn snapshot(&self) -> MetricSnapshot {
        let ratio = self.current_ratio();

        MetricSnapshot {
            name: self.name.clone(),
            metric_type: MetricType::Gauge,
            value: MetricValue::Gauge(ratio),
            timestamp: Instant::now(),
            labels: HashMap::new(),
        }
    }

    fn update(&self, value: MetricValue) {
        if let MetricValue::Gauge(v) = value {
            self.set_ratio(v);
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<MetricUpdate> {
        self.realtime_tx.subscribe()
    }
}

// Mark SaturationMetric as requiring internal state reporting
impl StatefulMetric for SaturationMetric {}

/// Support trait for stages that can provide saturation metrics
///
/// Saturation metrics track resource capacity usage and require internal
/// state reporting from the stage. The stage decides:
/// - What constitutes saturation for their specific use case
/// - How to calculate their saturation ratio
/// - What thresholds to use for warnings/alerts
/// - When to report state changes
///
/// FlowState just provides the reporting mechanism.
pub trait SaturationSupport {
    /// Get the saturation metric instance for this stage
    fn saturation_metric(&self) -> &SaturationMetric;

    /// Report saturation as a ratio (0.0 to 1.0)
    fn report_saturation(&self, ratio: f64) {
        self.saturation_metric().set_ratio(ratio);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_saturation_metric_basic_functionality() {
        let metric = SaturationMetric::new("test_saturation");

        assert_eq!(metric.name(), "test_saturation");
        assert_eq!(metric.current_ratio(), 0.0);

        metric.set_ratio(0.5);
        assert!((metric.current_ratio() - 0.5).abs() < 0.0001);

        metric.set_ratio(0.85);
        assert!((metric.current_ratio() - 0.85).abs() < 0.0001);

        metric.set_ratio(0.98);
        assert!((metric.current_ratio() - 0.98).abs() < 0.0001);
    }

    #[tokio::test]
    async fn test_saturation_metric_clamping() {
        let metric = SaturationMetric::new("clamp_test");

        // Test over 1.0
        metric.set_ratio(1.5);
        assert_eq!(metric.current_ratio(), 1.0);

        // Test under 0.0
        metric.set_ratio(-0.5);
        assert_eq!(metric.current_ratio(), 0.0);
    }

    #[tokio::test]
    async fn test_saturation_metric_snapshot() {
        let metric = SaturationMetric::new("snapshot_test");
        metric.set_ratio(0.75);

        let snapshot = metric.snapshot();
        assert_eq!(snapshot.name, "snapshot_test");
        assert_eq!(snapshot.metric_type, MetricType::Gauge);

        if let MetricValue::Gauge(value) = snapshot.value {
            assert!((value - 0.75).abs() < 0.0001);
        } else {
            panic!("Expected Gauge value");
        }
    }

    #[tokio::test]
    async fn test_saturation_metric_realtime_updates() {
        let metric = SaturationMetric::new("realtime_test");
        let mut receiver = metric.subscribe();

        metric.set_ratio(0.5);

        let update = receiver.recv().await.expect("Should receive update");
        assert_eq!(update.metric_name, "realtime_test");

        if let MetricValue::Gauge(value) = update.value {
            assert!((value - 0.5).abs() < 0.0001);
        } else {
            panic!("Expected Gauge value in update");
        }
    }

    #[tokio::test]
    async fn test_saturation_metric_reset() {
        let metric = SaturationMetric::new("reset_test");

        metric.set_ratio(0.9);
        assert!((metric.current_ratio() - 0.9).abs() < 0.0001);

        metric.reset();
        assert_eq!(metric.current_ratio(), 0.0);
    }

    #[tokio::test]
    async fn test_saturation_support_trait() {
        struct TestStage {
            saturation: SaturationMetric,
        }

        impl SaturationSupport for TestStage {
            fn saturation_metric(&self) -> &SaturationMetric {
                &self.saturation
            }
        }

        let stage = TestStage {
            saturation: SaturationMetric::new("stage_test"),
        };

        // Test trait methods
        stage.report_saturation(0.8);
        assert!((stage.saturation_metric().current_ratio() - 0.8).abs() < 0.0001);

        stage.report_saturation(0.25);
        assert!((stage.saturation_metric().current_ratio() - 0.25).abs() < 0.0001);
    }

    #[tokio::test]
    async fn test_saturation_example_simple_queue() {
        // Example: Simple queue with basic saturation logic
        struct SimpleQueue {
            saturation: SaturationMetric,
            queue: Vec<String>,
            max_size: usize,
        }

        impl SaturationSupport for SimpleQueue {
            fn saturation_metric(&self) -> &SaturationMetric {
                &self.saturation
            }
        }

        impl SimpleQueue {
            fn add_item(&mut self, item: String) {
                self.queue.push(item);

                // Simple saturation: current/max
                let ratio = self.queue.len() as f64 / self.max_size as f64;
                self.report_saturation(ratio);
            }
        }

        let mut queue = SimpleQueue {
            saturation: SaturationMetric::new("simple_queue"),
            queue: Vec::new(),
            max_size: 10,
        };

        for i in 0..8 {
            queue.add_item(format!("item{i}"));
        }

        assert!((queue.saturation_metric().current_ratio() - 0.8).abs() < 0.0001);
    }

    #[tokio::test]
    async fn test_saturation_example_complex_resource() {
        // Example: Complex resource with sophisticated saturation calculation
        struct DatabaseConnectionPool {
            saturation: SaturationMetric,
            active_connections: usize,
            max_connections: usize,
            pending_requests: usize,
            avg_wait_time_ms: f64,
            max_acceptable_wait_ms: f64,
        }

        impl SaturationSupport for DatabaseConnectionPool {
            fn saturation_metric(&self) -> &SaturationMetric {
                &self.saturation
            }
        }

        impl DatabaseConnectionPool {
            fn calculate_saturation(&mut self) {
                // Complex "death star" saturation logic
                let connection_ratio = self.active_connections as f64 / self.max_connections as f64;
                let pending_factor = (self.pending_requests as f64 / 10.0).min(1.0);
                let wait_factor = (self.avg_wait_time_ms / self.max_acceptable_wait_ms).min(1.0);

                // Weighted combination of factors
                let saturation =
                    (connection_ratio * 0.5) + (pending_factor * 0.3) + (wait_factor * 0.2);

                self.report_saturation(saturation);
            }
        }

        let mut pool = DatabaseConnectionPool {
            saturation: SaturationMetric::new("db_pool"),
            active_connections: 8,
            max_connections: 10,
            pending_requests: 5,
            avg_wait_time_ms: 150.0,
            max_acceptable_wait_ms: 200.0,
        };

        pool.calculate_saturation();

        // Complex calculation should yield: (0.8 * 0.5) + (0.5 * 0.3) + (0.75 * 0.2) = 0.7
        assert!((pool.saturation_metric().current_ratio() - 0.7).abs() < 0.0001);
    }
}

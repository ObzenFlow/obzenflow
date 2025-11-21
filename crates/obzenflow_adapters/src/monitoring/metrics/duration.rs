use super::core::EventfulMetric;
use super::core::{Metric, MetricSnapshot, MetricType, MetricUpdate, MetricValue};
use obzenflow_core::metrics::primitives::{Histogram, TimeUnit};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

const DEFAULT_METRIC_CHANNEL_CAPACITY: usize = 1024;

/// Predefined bucket configurations for different use cases
#[derive(Debug, Clone, Copy)]
pub enum DurationBuckets {
    /// For sub-second operations: 100μs to 1s
    Microseconds,
    /// For typical API calls: 1ms to 10s  
    Milliseconds,
    /// For batch operations: 100ms to 5min
    Seconds,
    /// For long-running tasks: 1s to 1hr
    Minutes,
    /// Custom buckets (in seconds)
    Custom(&'static [f64]),
}

impl DurationBuckets {
    pub fn as_slice(&self) -> &[f64] {
        match self {
            Self::Microseconds => &[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
            Self::Milliseconds => &[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0],
            Self::Seconds => &[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0],
            Self::Minutes => &[1.0, 5.0, 10.0, 30.0, 60.0, 300.0, 600.0, 1800.0, 3600.0],
            Self::Custom(buckets) => buckets,
        }
    }
}

impl Default for DurationBuckets {
    fn default() -> Self {
        Self::Milliseconds
    }
}

/// Duration metric - wraps Histogram primitive for timing measurements
///
/// Simplified to wrap a Histogram primitive. This follows the industry pattern
/// from Prometheus, OpenTelemetry, etc. and complies with FLOWIP-004 spec.
pub struct DurationMetric {
    name: String,
    histogram: Histogram,
    realtime_tx: broadcast::Sender<MetricUpdate>,
}

impl DurationMetric {
    pub fn new(name: impl Into<String>) -> Self {
        Self::with_buckets(name, DurationBuckets::default())
    }

    pub fn with_buckets(name: impl Into<String>, buckets: DurationBuckets) -> Self {
        let histogram = match buckets {
            DurationBuckets::Microseconds | DurationBuckets::Milliseconds => {
                // Use default OTel-style buckets for sub-second measurements
                Histogram::new()
            }
            DurationBuckets::Seconds | DurationBuckets::Minutes => {
                // Use Prometheus-style seconds buckets for longer measurements
                Histogram::for_seconds()
            }
            DurationBuckets::Custom(boundaries) => Histogram::with_buckets(boundaries.to_vec()),
        };

        let (tx, _) = broadcast::channel(DEFAULT_METRIC_CHANNEL_CAPACITY);

        Self {
            name: name.into(),
            histogram,
            realtime_tx: tx,
        }
    }

    pub fn record_duration(&self, duration: Duration) {
        // Convert Duration to TimeUnit and let the histogram handle unit conversion
        let time_unit: TimeUnit = duration.into();
        self.histogram.observe_time(time_unit);

        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Histogram {
                buckets: self.current_buckets(),
                sum: self.current_sum(),
                count: self.current_count(),
            },
            timestamp: Instant::now(),
        });
    }

    pub fn start_timer(&self) -> Timer {
        Timer {
            start: Instant::now(),
            metric: self,
        }
    }

    pub fn current_buckets(&self) -> Vec<(f64, u64)> {
        let boundaries = self.histogram.bucket_boundaries();
        let counts = self.histogram.bucket_counts();
        boundaries
            .iter()
            .zip(counts.iter())
            .map(|(&boundary, &count)| (boundary, count))
            .collect()
    }

    pub fn current_sum(&self) -> f64 {
        // Get sum as TimeUnit and convert to seconds for external API consistency
        self.histogram.sum_time().as_seconds()
    }

    pub fn current_count(&self) -> u64 {
        self.histogram.count()
    }

    pub fn reset(&self) {
        self.histogram.reset();

        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Histogram {
                buckets: vec![],
                sum: 0.0,
                count: 0,
            },
            timestamp: Instant::now(),
        });
    }
}

pub struct Timer<'a> {
    start: Instant,
    metric: &'a DurationMetric,
}

impl<'a> Timer<'a> {
    pub fn observe(self) -> Duration {
        let duration = self.start.elapsed();
        self.metric.record_duration(duration);
        duration
    }
}

impl<'a> Drop for Timer<'a> {
    fn drop(&mut self) {
        self.metric.record_duration(self.start.elapsed());
    }
}

impl Metric for DurationMetric {
    fn name(&self) -> &str {
        &self.name
    }

    fn snapshot(&self) -> MetricSnapshot {
        MetricSnapshot {
            name: self.name.clone(),
            metric_type: MetricType::Histogram,
            value: MetricValue::Histogram {
                buckets: self.current_buckets(),
                sum: self.current_sum(),
                count: self.current_count(),
            },
            timestamp: Instant::now(),
            labels: HashMap::new(),
        }
    }

    fn update(&self, value: MetricValue) {
        match value {
            MetricValue::Histogram { sum, count, .. } => {
                // Record individual observations to maintain histogram integrity
                if count > 0 {
                    let avg_duration = sum / count as f64;
                    for _ in 0..count {
                        self.histogram.observe(avg_duration);
                    }
                }
            }
            _ => {}
        }
    }

    fn subscribe(&self) -> broadcast::Receiver<MetricUpdate> {
        self.realtime_tx.subscribe()
    }
}

// Mark DurationMetric as externally observable
impl EventfulMetric for DurationMetric {}

/// Support trait for stages that can provide duration metrics
///
/// Duration metrics track processing times as histograms and can be
/// updated by external observation of operation timing.
pub trait DurationSupport {
    /// Get the duration metric instance for this stage
    fn duration_metric(&self) -> &DurationMetric;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_duration_metric_basic_functionality() {
        let metric = DurationMetric::new("test_duration");

        assert_eq!(metric.name(), "test_duration");
        assert_eq!(metric.current_count(), 0);
        assert_eq!(metric.current_sum(), 0.0);

        metric.record_duration(Duration::from_millis(100));
        assert_eq!(metric.current_count(), 1);
        assert!((metric.current_sum() - 0.1).abs() < 0.001); // ~100ms
    }

    #[tokio::test]
    async fn test_duration_metric_buckets() {
        let metric = DurationMetric::new("bucket_test"); // Uses default OTel-style millisecond buckets

        metric.record_duration(Duration::from_millis(3)); // Should hit 5ms bucket
        metric.record_duration(Duration::from_millis(15)); // Should hit 25ms bucket
        metric.record_duration(Duration::from_millis(80)); // Should hit 100ms bucket

        let buckets = metric.current_buckets();

        // Find the 5ms bucket (should contain the 3ms observation)
        let bucket_5ms = buckets
            .iter()
            .find(|(upper, _)| (*upper - 5.0).abs() < 0.001);
        assert!(bucket_5ms.is_some());
        assert!(bucket_5ms.unwrap().1 >= 1); // At least 1 observation

        // Find the 25ms bucket (should contain the 15ms observation)
        let bucket_25ms = buckets
            .iter()
            .find(|(upper, _)| (*upper - 25.0).abs() < 0.001);
        assert!(bucket_25ms.is_some());
        assert!(bucket_25ms.unwrap().1 >= 1); // At least 1 observation
    }

    #[tokio::test]
    async fn test_duration_metric_timer() {
        let metric = DurationMetric::new("timer_test");

        {
            let _timer = metric.start_timer();
            tokio::time::sleep(Duration::from_millis(10)).await;
        } // Timer drops here and records duration

        assert_eq!(metric.current_count(), 1);
        assert!(metric.current_sum() > 0.008); // Should be at least 8ms
    }

    #[tokio::test]
    async fn test_duration_metric_snapshot() {
        let metric = DurationMetric::new("snapshot_test");
        metric.record_duration(Duration::from_millis(50));

        let snapshot = metric.snapshot();
        assert_eq!(snapshot.name, "snapshot_test");
        assert_eq!(snapshot.metric_type, MetricType::Histogram);

        if let MetricValue::Histogram { count, sum, .. } = snapshot.value {
            assert_eq!(count, 1);
            assert!((sum - 0.05).abs() < 0.001); // ~50ms
        } else {
            panic!("Expected Histogram value");
        }
    }

    #[tokio::test]
    async fn test_duration_metric_reset() {
        let metric = DurationMetric::new("reset_test");

        metric.record_duration(Duration::from_millis(100));
        metric.record_duration(Duration::from_millis(200));
        assert_eq!(metric.current_count(), 2);

        metric.reset();
        assert_eq!(metric.current_count(), 0);
        assert_eq!(metric.current_sum(), 0.0);
    }
}

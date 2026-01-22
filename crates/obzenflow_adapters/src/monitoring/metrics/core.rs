use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::broadcast;

/// Core metric interface - format agnostic, always available
///
/// This trait separates metric collection from export formats, enabling:
/// - Clean feature flag support (metrics work without exporters)
/// - Easy testing (no external dependencies required)
/// - Pluggable export systems (Prometheus, StatsD, custom)
/// - Type safety (no runtime format dependencies)
pub trait Metric: Send + Sync {
    /// Unique identifier for this metric
    fn name(&self) -> &str;

    /// Current snapshot of metric value
    fn snapshot(&self) -> MetricSnapshot;

    /// Update metric with new value
    fn update(&self, value: MetricValue);

    /// Get realtime update stream for TUI/debugging
    fn subscribe(&self) -> broadcast::Receiver<MetricUpdate>;
}

/// Immutable snapshot of a metric at a point in time
#[derive(Debug, Clone)]
pub struct MetricSnapshot {
    pub name: String,
    pub metric_type: MetricType,
    pub value: MetricValue,
    pub timestamp: Instant,
    pub labels: HashMap<String, String>,
}

/// The fundamental types of metrics supported
#[derive(Debug, Clone, PartialEq)]
pub enum MetricType {
    /// Monotonically increasing counter (e.g., requests processed)
    Counter,
    /// Value that can go up or down (e.g., queue length)
    Gauge,
    /// Distribution of values with buckets (e.g., response times)
    Histogram,
    /// Distribution with calculated quantiles (e.g., latency percentiles)
    Summary,
}

/// Values that metrics can contain
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    /// Simple counter value
    Counter(u64),
    /// Current gauge reading
    Gauge(f64),
    /// Histogram with bucket counts, sum, and total count
    Histogram {
        buckets: Vec<(f64, u64)>, // (upper_bound, count)
        sum: f64,
        count: u64,
    },
    /// Summary with quantile values, sum, and total count
    Summary {
        quantiles: Vec<(f64, f64)>, // (quantile, value)
        sum: f64,
        count: u64,
    },
}

/// Real-time update notification for streaming/TUI
#[derive(Debug, Clone)]
pub struct MetricUpdate {
    pub metric_name: String,
    pub value: MetricValue,
    pub timestamp: Instant,
}

/// Errors that can occur during metric operations
#[derive(Debug, thiserror::Error)]
pub enum MetricError {
    #[error("Invalid metric value type: expected {expected}, got {actual}")]
    InvalidValueType { expected: String, actual: String },

    #[error("Metric update failed: {reason}")]
    UpdateFailed { reason: String },

    #[error("Metric subscription failed: {reason}")]
    SubscriptionFailed { reason: String },
}

impl MetricValue {
    /// Get the type of this metric value
    pub fn metric_type(&self) -> MetricType {
        match self {
            MetricValue::Counter(_) => MetricType::Counter,
            MetricValue::Gauge(_) => MetricType::Gauge,
            MetricValue::Histogram { .. } => MetricType::Histogram,
            MetricValue::Summary { .. } => MetricType::Summary,
        }
    }

    /// Extract counter value if this is a counter
    pub fn as_counter(&self) -> Option<u64> {
        match self {
            MetricValue::Counter(v) => Some(*v),
            _ => None,
        }
    }

    /// Extract gauge value if this is a gauge
    pub fn as_gauge(&self) -> Option<f64> {
        match self {
            MetricValue::Gauge(v) => Some(*v),
            _ => None,
        }
    }
}

impl std::fmt::Display for MetricType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricType::Counter => write!(f, "counter"),
            MetricType::Gauge => write!(f, "gauge"),
            MetricType::Histogram => write!(f, "histogram"),
            MetricType::Summary => write!(f, "summary"),
        }
    }
}

impl std::fmt::Display for MetricValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricValue::Counter(v) => write!(f, "{v}"),
            MetricValue::Gauge(v) => write!(f, "{v:.2}"),
            MetricValue::Histogram { count, sum, .. } => {
                write!(f, "count={count}, sum={sum:.2}")
            }
            MetricValue::Summary { count, sum, .. } => {
                write!(f, "count={count}, sum={sum:.2}")
            }
        }
    }
}

/// Marker trait for metrics that can be updated by external observation
///
/// EventfulMetrics are updated by monitoring wrappers that observe events
/// flowing through stages (requests, responses, processing times, etc.).
/// The stage itself doesn't need to participate in metric collection.
pub trait EventfulMetric: Metric {
    // Marker trait - no additional methods required
}

/// Marker trait for metrics that require internal state reporting
///
/// StatefulMetrics require the stage to actively report internal state
/// (queue depths, capacity utilization, resource saturation, etc.) that
/// only the stage can know about.
pub trait StatefulMetric: Metric {
    // Marker trait - no additional methods required
}

/// General support trait for stages that can provide metrics
///
/// This trait can be used when you need a general bound over any metric support.
/// Specific metrics define their own support traits (RateSupport, ErrorSupport, etc.)
/// but this provides a common interface for generic programming.
pub trait MetricSupport<M: Metric> {
    /// Get the metric instance
    fn metric(&self) -> &M;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::f64::consts::PI;

    #[test]
    fn test_metric_value_types() {
        let counter = MetricValue::Counter(42);
        assert_eq!(counter.metric_type(), MetricType::Counter);
        assert_eq!(counter.as_counter(), Some(42));
        assert_eq!(counter.as_gauge(), None);

        let gauge = MetricValue::Gauge(PI);
        assert_eq!(gauge.metric_type(), MetricType::Gauge);
        assert_eq!(gauge.as_gauge(), Some(PI));
        assert_eq!(gauge.as_counter(), None);
    }

    #[test]
    fn test_metric_value_display() {
        assert_eq!(MetricValue::Counter(100).to_string(), "100");
        assert_eq!(MetricValue::Gauge(PI).to_string(), "3.14");

        let histogram = MetricValue::Histogram {
            buckets: vec![(1.0, 10), (5.0, 25)],
            sum: 75.5,
            count: 35,
        };
        assert_eq!(histogram.to_string(), "count=35, sum=75.50");
    }

    #[test]
    fn test_metric_type_display() {
        assert_eq!(MetricType::Counter.to_string(), "counter");
        assert_eq!(MetricType::Gauge.to_string(), "gauge");
        assert_eq!(MetricType::Histogram.to_string(), "histogram");
        assert_eq!(MetricType::Summary.to_string(), "summary");
    }
}

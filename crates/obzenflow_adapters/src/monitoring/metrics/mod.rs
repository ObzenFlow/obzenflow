pub mod amendments;
pub mod anomalies;
pub mod core;
pub mod duration;
pub mod errors;
pub mod failures;
pub mod rate;
pub mod saturation;
pub mod utilities;
pub mod utilization;

use std::collections::HashMap;
use tokio::sync::broadcast;

// Re-export core traits
pub use core::{EventfulMetric, MetricError, MetricSupport, StatefulMetric};

// Re-export existing metric implementations
pub use amendments::{AmendmentMetric, AmendmentType};
pub use anomalies::AnomalyMetric;
pub use duration::{DurationMetric, DurationSupport};
pub use errors::{ErrorMetric, ErrorSupport};
pub use failures::{FailureMetric, FailureType};
pub use rate::{RateMetric, RateSupport};
pub use saturation::{SaturationMetric, SaturationSupport};
pub use utilities::{CircularBuffer, SimpleHistogram, SlidingWindow};
pub use utilization::{UtilizationMetric, UtilizationSupport};

#[derive(Debug, Clone)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(Vec<f64>),
    Summary {
        count: u64,
        sum: f64,
        quantiles: std::collections::BTreeMap<f64, f64>,
    },
}

#[derive(Debug, Clone)]
pub struct MetricUpdate {
    pub metric_name: String,
    pub value: MetricValue,
    pub timestamp: std::time::Instant,
}

/// Core metric trait - just the basics
pub trait Metric: Send + Sync {
    fn name(&self) -> &str;
    fn update(&self, value: MetricValue);
}

/// Real-time monitoring for TUI/UI
pub trait RealtimeMonitoring {
    fn subscribe(&self) -> broadcast::Receiver<MetricUpdate>;
}

/// A collection of metrics that work together
pub struct MetricSet {
    metrics: HashMap<String, Box<dyn Metric>>,
}

impl MetricSet {
    pub fn new() -> Self {
        Self {
            metrics: HashMap::new(),
        }
    }

    pub fn add<M: Metric + 'static>(mut self, metric: M) -> Self {
        self.metrics
            .insert(metric.name().to_string(), Box::new(metric));
        self
    }

    pub fn get(&self, name: &str) -> Option<&dyn Metric> {
        self.metrics.get(name).map(|m| m.as_ref())
    }

    pub fn update(&self, name: &str, value: MetricValue) {
        if let Some(metric) = self.metrics.get(name) {
            metric.update(value);
        }
    }
}

/// Builder pattern for composing metrics into sets
pub struct MetricSetBuilder {
    metrics: Vec<Box<dyn Metric>>,
}

impl MetricSetBuilder {
    pub fn new() -> Self {
        Self {
            metrics: Vec::new(),
        }
    }

    pub fn with<M: Metric + 'static>(mut self, metric: M) -> Self {
        self.metrics.push(Box::new(metric));
        self
    }

    pub fn build(self) -> MetricSet {
        let mut set = MetricSet::new();
        for metric in self.metrics {
            set.metrics.insert(metric.name().to_string(), metric);
        }
        set
    }
}

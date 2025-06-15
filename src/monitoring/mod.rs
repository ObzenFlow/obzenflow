// src/monitoring/mod.rs
//! Native monitoring taxonomy support for FlowState RS
//! 
//! FlowState RS is the first streaming framework where monitoring taxonomies
//! are first-class citizens. Every stage must declare which taxonomy it uses.
//! 
//! Metrics are available both:
//! - Directly for TUI/UI (real-time, no scraping)
//! - Via Prometheus for external monitoring

use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use prometheus::Histogram;

pub mod metrics;
pub mod taxonomies;
pub mod exporters;

pub use taxonomies::{RED, USE, GoldenSignals, SAAFE};
pub use metrics::*;

/// Core trait that all monitoring taxonomies must implement
pub trait Taxonomy: Send + Sync + 'static {
    /// Name of the taxonomy (e.g., "RED", "USE", "SAAFE")
    const NAME: &'static str;
    
    /// Human-readable description
    const DESCRIPTION: &'static str;
    
    /// The metrics type this taxonomy provides
    type Metrics: TaxonomyMetrics;
    
    /// Create a new instance of metrics for this taxonomy
    fn create_metrics(stage_name: &str) -> Self::Metrics;
}

/// Base trait for all taxonomy metrics
pub trait TaxonomyMetrics: Send + Sync {
    /// Get current metric values for TUI - no Prometheus overhead
    fn current_values(&self) -> MetricSnapshot;
    
    /// Subscribe to real-time metric updates for TUI
    fn subscribe_updates(&self) -> broadcast::Receiver<MetricUpdate>;
    
    /// Export metrics to Prometheus registry
    fn export_prometheus(&self);
    
    /// Get the taxonomy name
    fn taxonomy_name(&self) -> &'static str;
}

/// Real-time metric updates for TUI
#[derive(Clone, Debug)]
pub enum MetricUpdate {
    Rate { 
        value: f64, 
        timestamp: Instant,
        stage: String,
    },
    Error { 
        count: u64, 
        error_type: String,
        timestamp: Instant,
        stage: String,
    },
    Duration { 
        value: Duration,
        quantile: f64,
        timestamp: Instant,
        stage: String,
    },
    Utilization {
        value: f64,
        timestamp: Instant,
        stage: String,
    },
    Saturation { 
        value: f64,
        queue_depth: usize,
        timestamp: Instant,
        stage: String,
    },
    Anomaly {
        description: String,
        severity: f64,
        timestamp: Instant,
        stage: String,
    },
}

/// Snapshot of current metrics for UI rendering
#[derive(Clone, Debug)]
pub struct MetricSnapshot {
    pub timestamp: Instant,
    
    // RED metrics
    pub rate_per_sec: f64,
    pub error_count: u64,
    pub error_rate: f64,
    pub duration_p50: Duration,
    pub duration_p99: Duration,
    pub duration_p999: Duration,
    
    // USE metrics
    pub utilization: f64,
    pub saturation: f64,
    pub queue_depth: usize,
    
    // SAAFE metrics
    pub amendments: u64,
    pub anomalies: u64,
    pub failures: u64,
    
    // Golden Signals
    pub traffic_per_sec: f64,
    pub latency_p99: Duration,
}

impl Default for MetricSnapshot {
    fn default() -> Self {
        Self {
            timestamp: Instant::now(),
            rate_per_sec: 0.0,
            error_count: 0,
            error_rate: 0.0,
            duration_p50: Duration::default(),
            duration_p99: Duration::default(),
            duration_p999: Duration::default(),
            utilization: 0.0,
            saturation: 0.0,
            queue_depth: 0,
            amendments: 0,
            anomalies: 0,
            failures: 0,
            traffic_per_sec: 0.0,
            latency_p99: Duration::default(),
        }
    }
}

/// Helper for timing operations
pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Self { start: Instant::now() }
    }
    
    pub fn observe(self, histogram: &Histogram) -> Duration {
        let duration = self.start.elapsed();
        histogram.observe(duration.as_secs_f64());
        duration
    }
    
    pub fn elapsed(self) -> Duration {
        self.start.elapsed()
    }
}

/// Initialize the monitoring subsystem
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    // Register any global collectors
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timer() {
        let timer = Timer::start();
        std::thread::sleep(Duration::from_millis(10));
        let elapsed = timer.elapsed();
        assert!(elapsed >= Duration::from_millis(10));
    }
}
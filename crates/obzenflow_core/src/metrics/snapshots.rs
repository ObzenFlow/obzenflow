//! Snapshot DTOs for metrics collection
//!
//! These DTOs define the contract between metrics collectors and exporters,
//! implementing the dual collection pattern for application and infrastructure metrics.

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Snapshot of application-level metrics derived from the event stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppMetricsSnapshot {
    /// Timestamp when this snapshot was created
    pub timestamp: chrono::DateTime<chrono::Utc>,
    
    /// Event counts by stage
    pub event_counts: HashMap<String, u64>,
    
    /// Error counts by stage
    pub error_counts: HashMap<String, u64>,
    
    /// Processing time histograms by stage (in seconds)
    pub processing_times: HashMap<String, HistogramSnapshot>,
    
    /// In-flight events by stage
    pub in_flight: HashMap<String, f64>,
    
    /// Queue depth by stage
    pub queue_depth: HashMap<String, f64>,
    
    /// CPU usage ratio by stage (0.0-1.0)
    pub cpu_usage_ratio: HashMap<String, f64>,
    
    /// Memory usage in bytes by stage
    pub memory_bytes: HashMap<String, f64>,
    
    /// SAAFE metrics - anomalies total by stage
    pub anomalies_total: HashMap<String, u64>,
    
    /// SAAFE metrics - amendments total by stage
    pub amendments_total: HashMap<String, u64>,
    
    /// SAAFE metrics - saturation ratio by stage (0.0-1.0)
    pub saturation_ratio: HashMap<String, f64>,
    
    /// Flow-level latency histograms by flow name (in seconds)
    pub flow_latency_seconds: HashMap<String, HistogramSnapshot>,
    
    /// Dropped events by flow name
    pub dropped_events: HashMap<String, f64>,
    
    /// Circuit breaker state by stage (0=closed, 0.5=half_open, 1=open)
    pub circuit_breaker_state: HashMap<String, f64>,
    
    /// Circuit breaker rejection rate by stage (0.0-1.0)
    pub circuit_breaker_rejection_rate: HashMap<String, f64>,
    
    /// Circuit breaker consecutive failures by stage
    pub circuit_breaker_consecutive_failures: HashMap<String, f64>,
    
    /// Rate limiter delay rate by stage (0.0-1.0)
    pub rate_limiter_delay_rate: HashMap<String, f64>,
    
    /// Rate limiter utilization by stage (0.0-1.0)
    pub rate_limiter_utilization: HashMap<String, f64>,
    
    /// Flow-level metrics (if journey events are implemented)
    pub flow_metrics: Option<FlowMetricsSnapshot>,
}

/// Snapshot of infrastructure-level metrics from direct observation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraMetricsSnapshot {
    /// Timestamp when this snapshot was created
    pub timestamp: chrono::DateTime<chrono::Utc>,
    
    /// Journal write metrics
    pub journal_metrics: JournalMetricsSnapshot,
    
    /// Stage-level infrastructure metrics
    pub stage_metrics: HashMap<String, StageInfraMetrics>,
}

/// Histogram data for a single metric
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramSnapshot {
    /// Number of observations
    pub count: u64,
    
    /// Sum of all observations
    pub sum: f64,
    
    /// Minimum value observed
    pub min: f64,
    
    /// Maximum value observed
    pub max: f64,
    
    /// Percentiles (0.5, 0.9, 0.95, 0.99)
    pub percentiles: HashMap<String, f64>,
}

/// Flow-level metrics (for future journey events)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowMetricsSnapshot {
    /// Number of opened journeys
    pub journeys_opened: u64,
    
    /// Number of sealed journeys
    pub journeys_sealed: u64,
    
    /// End-to-end latency histogram
    pub e2e_latency: HistogramSnapshot,
}

/// Journal performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalMetricsSnapshot {
    /// Total write operations
    pub writes_total: u64,
    
    /// Write latency histogram (in microseconds)
    pub write_latency: HistogramSnapshot,
    
    /// Current throughput (events per second)
    pub throughput: f64,
    
    /// Total bytes written
    pub bytes_written: u64,
}


/// Stage-specific infrastructure metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageInfraMetrics {
    /// Current queue depth
    pub queue_depth: u64,
    
    /// Events currently being processed
    pub in_flight: u64,
}

impl Default for AppMetricsSnapshot {
    fn default() -> Self {
        Self {
            timestamp: chrono::Utc::now(),
            event_counts: HashMap::new(),
            error_counts: HashMap::new(),
            processing_times: HashMap::new(),
            in_flight: HashMap::new(),
            queue_depth: HashMap::new(),
            cpu_usage_ratio: HashMap::new(),
            memory_bytes: HashMap::new(),
            anomalies_total: HashMap::new(),
            amendments_total: HashMap::new(),
            saturation_ratio: HashMap::new(),
            flow_latency_seconds: HashMap::new(),
            dropped_events: HashMap::new(),
            circuit_breaker_state: HashMap::new(),
            circuit_breaker_rejection_rate: HashMap::new(),
            circuit_breaker_consecutive_failures: HashMap::new(),
            rate_limiter_delay_rate: HashMap::new(),
            rate_limiter_utilization: HashMap::new(),
            flow_metrics: None,
        }
    }
}

impl Default for InfraMetricsSnapshot {
    fn default() -> Self {
        Self {
            timestamp: chrono::Utc::now(),
            journal_metrics: JournalMetricsSnapshot::default(),
            stage_metrics: HashMap::new(),
        }
    }
}

impl Default for HistogramSnapshot {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            percentiles: HashMap::new(),
        }
    }
}

impl Default for JournalMetricsSnapshot {
    fn default() -> Self {
        Self {
            writes_total: 0,
            write_latency: HistogramSnapshot::default(),
            throughput: 0.0,
            bytes_written: 0,
        }
    }
}


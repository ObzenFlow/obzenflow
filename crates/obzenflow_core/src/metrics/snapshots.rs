//! Snapshot DTOs for metrics collection
//!
//! These DTOs define the contract between metrics collectors and exporters,
//! implementing the dual collection pattern for application and infrastructure metrics.

use crate::event::context::StageType;
use crate::event::status::processing_status::ErrorKind;
use crate::id::StageId;
use crate::metrics::Percentile;
use crate::time::MetricsDuration;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Snapshot of application-level metrics derived from the event stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppMetricsSnapshot {
    /// Timestamp when this snapshot was created
    pub timestamp: chrono::DateTime<chrono::Utc>,

    /// Event counts by stage
    pub event_counts: HashMap<StageId, u64>,

    /// Error counts by stage
    pub error_counts: HashMap<StageId, u64>,

    /// Error counts by stage and ErrorKind
    pub error_counts_by_kind: HashMap<StageId, HashMap<ErrorKind, u64>>,

    /// Processing time histograms by stage (in seconds)
    pub processing_times: HashMap<StageId, HistogramSnapshot>,

    /// In-flight events by stage
    pub in_flight: HashMap<StageId, f64>,

    /// CPU usage ratio by stage (0.0-1.0)
    pub cpu_usage_ratio: HashMap<StageId, f64>,

    /// Memory usage in bytes by stage
    pub memory_bytes: HashMap<StageId, f64>,

    /// SAAFE metrics - anomalies total by stage
    pub anomalies_total: HashMap<StageId, u64>,

    /// SAAFE metrics - amendments total by stage
    pub amendments_total: HashMap<StageId, u64>,

    /// SAAFE metrics - saturation ratio by stage (0.0-1.0)
    pub saturation_ratio: HashMap<StageId, f64>,

    /// SAAFE metrics - failures total by stage (critical failures)
    pub failures_total: HashMap<StageId, u64>,

    /// USE metrics - event loops total by stage
    pub event_loops_total: HashMap<StageId, u64>,

    /// USE metrics - event loops with work by stage
    pub event_loops_with_work_total: HashMap<StageId, u64>,

    /// Flow-level latency histograms by flow name (in seconds)
    pub flow_latency_seconds: HashMap<StageId, HistogramSnapshot>,

    /// Dropped events by flow name
    pub dropped_events: HashMap<StageId, f64>,

    /// Circuit breaker state by stage (0=closed, 0.5=half_open, 1=open)
    pub circuit_breaker_state: HashMap<StageId, f64>,

    /// Circuit breaker rejection rate by stage (0.0-1.0)
    pub circuit_breaker_rejection_rate: HashMap<StageId, f64>,

    /// Circuit breaker consecutive failures by stage
    pub circuit_breaker_consecutive_failures: HashMap<StageId, f64>,

    /// Rate limiter delay rate by stage (0.0-1.0)
    pub rate_limiter_delay_rate: HashMap<StageId, f64>,

    /// Rate limiter utilization by stage (0.0-1.0)
    pub rate_limiter_utilization: HashMap<StageId, f64>,

    /// Flow-level metrics (if journey events are implemented)
    pub flow_metrics: Option<FlowMetricsSnapshot>,

    /// Stage metadata for display and categorization
    pub stage_metadata: HashMap<StageId, StageMetadata>,

    /// First event time for each stage (for rate calculation)
    pub stage_first_event_time: HashMap<StageId, chrono::DateTime<chrono::Utc>>,

    /// Last event time for each stage (for rate calculation)
    pub stage_last_event_time: HashMap<StageId, chrono::DateTime<chrono::Utc>>,

    /// Stage lifecycle states (FLOWIP-059b - essential events only)
    /// Maps (StageId, state_name) to whether that state has been seen
    pub stage_lifecycle_states: HashMap<(StageId, String), bool>,

    /// Pipeline state (FLOWIP-059b)
    pub pipeline_state: String,

    /// Per-stage vector clock watermark (FLOWIP-059c).
    /// This mirrors MetricsStore.stage_vector_clocks and is used by exporters
    /// to expose obzenflow_stage_vector_clock metrics.
    pub stage_vector_clocks: HashMap<StageId, u64>,
}

/// Snapshot of infrastructure-level metrics from direct observation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfraMetricsSnapshot {
    /// Timestamp when this snapshot was created
    pub timestamp: chrono::DateTime<chrono::Utc>,

    /// Journal write metrics
    pub journal_metrics: JournalMetricsSnapshot,

    /// Stage-level infrastructure metrics
    pub stage_metrics: HashMap<StageId, StageInfraMetrics>,
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
    pub percentiles: HashMap<Percentile, f64>,
}

/// Flow-level metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowMetricsSnapshot {
    /// Total duration of the flow (wall clock time)
    pub flow_duration: MetricsDuration,

    /// Total number of events processed across all stages
    pub total_events_processed: u64,

    /// Events entering from sources only
    pub events_in: u64,

    /// Events exiting through sinks only
    pub events_out: u64,

    /// Total errors across all stages
    pub errors_total: u64,

    /// Total event loops across all stages
    pub event_loops_total: u64,

    /// Event loops with work across all stages
    pub event_loops_with_work_total: u64,
}

/// Stage-level metrics snapshot for lifecycle events (UI-focused)
///
/// This is a narrow, UI-oriented view of stage metrics derived from
/// `RuntimeContext` / `StageInstrumentation`. It intentionally exposes only
/// the fields needed to render topology cards and lifecycle transitions,
/// not the full Prometheus schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageMetricsSnapshot {
    /// Total events processed by this stage
    pub events_processed_total: u64,

    /// Total errors observed at this stage
    pub errors_total: u64,

    /// Error breakdown by kind (authoritative, journal-backed)
    pub errors_by_kind: std::collections::HashMap<
        crate::event::status::processing_status::ErrorKind,
        u64,
    >,

    /// Number of in-flight events at snapshot time
    pub in_flight: u32,

    /// Recent latency percentiles in milliseconds
    pub recent_p50_ms: u64,
    pub recent_p90_ms: u64,
    pub recent_p95_ms: u64,
    pub recent_p99_ms: u64,
    pub recent_p999_ms: u64,

    /// Event loop utilization counters for this stage
    pub event_loops_total: u64,
    pub event_loops_with_work_total: u64,
}

/// Flow-level lifecycle metrics snapshot for UI events
///
/// This complements `FlowMetricsSnapshot` by providing a minimal view that
/// is cheap to serialize on every lifecycle event and sufficient to drive
/// the Flow Summary panel in the UI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowLifecycleMetricsSnapshot {
    /// Events entering the flow from all sources
    pub events_in_total: u64,

    /// Events exiting the flow through all sinks
    pub events_out_total: u64,

    /// Total errors across all stages in the flow
    pub errors_total: u64,
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
    /// Events currently being processed
    pub in_flight: u64,
}

/// Stage metadata for display and categorization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageMetadata {
    /// Human-readable stage name (e.g., "event_source", "processor", "event_sink")
    pub name: String,

    /// Stage type for categorization
    pub stage_type: StageType,

    /// Flow name this stage belongs to
    pub flow_name: String,
}

impl Default for AppMetricsSnapshot {
    fn default() -> Self {
        Self {
            timestamp: chrono::Utc::now(),
            event_counts: HashMap::new(),
            error_counts: HashMap::new(),
            error_counts_by_kind: HashMap::new(),
            processing_times: HashMap::new(),
            in_flight: HashMap::new(),
            cpu_usage_ratio: HashMap::new(),
            memory_bytes: HashMap::new(),
            anomalies_total: HashMap::new(),
            amendments_total: HashMap::new(),
            saturation_ratio: HashMap::new(),
            failures_total: HashMap::new(),
            event_loops_total: HashMap::new(),
            event_loops_with_work_total: HashMap::new(),
            flow_latency_seconds: HashMap::new(),
            dropped_events: HashMap::new(),
            circuit_breaker_state: HashMap::new(),
            circuit_breaker_rejection_rate: HashMap::new(),
            circuit_breaker_consecutive_failures: HashMap::new(),
            rate_limiter_delay_rate: HashMap::new(),
            rate_limiter_utilization: HashMap::new(),
            flow_metrics: None,
            stage_metadata: HashMap::new(),
            stage_first_event_time: HashMap::new(),
            stage_last_event_time: HashMap::new(),
            stage_lifecycle_states: HashMap::new(),
            pipeline_state: String::new(),
            stage_vector_clocks: HashMap::new(),
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

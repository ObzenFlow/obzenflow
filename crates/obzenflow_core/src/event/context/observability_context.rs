//! Observability context that can be attached to any event (wide events pattern)

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Observability context that can be attached to any event
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObservabilityContext {
    /// Metrics snapshots at event time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsSnapshot>,
    
    /// Middleware state and statistics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub middleware: Option<MiddlewareStats>,
    
    /// Service level indicators
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sli: Option<SliSnapshot>,
    
    /// Custom observability data
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom: Option<Value>,
}

/// Snapshot of metrics at event creation time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub events_processed: u64,
    pub events_in_flight: u32,
    pub queue_depth: u32,
    pub processing_rate: f64,
    pub error_rate: f64,
    pub latency_p50_ms: f64,
    pub latency_p99_ms: f64,
}

/// Middleware statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiddlewareStats {
    pub timing_ms: u64,
    pub retry_count: u32,
    pub circuit_breaker_state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_remaining: Option<u32>,
}

/// Service level indicator snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SliSnapshot {
    pub availability: f64,
    pub error_budget_remaining: f64,
    pub latency_budget_used: f64,
}
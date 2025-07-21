//! Runtime context for FSM instrumentation (FLOWIP-056c)

use serde::{Serialize, Deserialize};

/// Runtime context snapshot injected into events by supervisors
/// Following Honeycomb's wide events philosophy - contains accurate point-in-time metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeContext {
    // Gauge snapshots - current values
    pub in_flight: u32,
    
    // Histogram percentiles - pre-computed for efficiency
    pub recent_p50_ms: u64,
    pub recent_p90_ms: u64,
    pub recent_p95_ms: u64,
    pub recent_p99_ms: u64,
    pub recent_p999_ms: u64,
    
    // Counter snapshots - raw totals (Prometheus computes rates)
    pub events_processed_total: u64,
    pub errors_total: u64,
    pub failures_total: u64,
    pub event_loops_total: u64,
    pub event_loops_with_work_total: u64,
    
    // FSM state
    pub fsm_state: String,
    pub time_in_state_ms: u64,
}
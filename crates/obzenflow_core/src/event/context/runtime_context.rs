//! Runtime context for FSM instrumentation (FLOWIP-056c)

use crate::event::identity::journal_writer_id::JournalWriterId;
use crate::event::status::processing_status::ErrorKind;
use crate::event::vector_clock::VectorClock;
use crate::{EventId, WriterId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

    // Actual sum of processing times (nanoseconds) - never reconstructed from percentiles
    #[serde(default)]
    pub processing_time_sum_nanos: u64,

    // Counter snapshots - raw totals (Prometheus computes rates)
    pub events_processed_total: u64,
    pub errors_total: u64,
    pub failures_total: u64,
    pub event_loops_total: u64,
    pub event_loops_with_work_total: u64,

    /// Error breakdown by kind at snapshot time
    pub errors_by_kind: HashMap<ErrorKind, u64>,

    // FSM state
    pub fsm_state: String,
    pub time_in_state_ms: u64,

    // Wide-event observability: capture positions at the time of instrumentation snapshot
    pub reader_seq: u64,
    pub writer_seq: u64,
    pub last_consumed_event_id: Option<EventId>,
    pub last_consumed_writer: Option<JournalWriterId>,
    pub last_consumed_vector_clock: Option<VectorClock>,
    pub last_emitted_event_id: Option<EventId>,
    pub last_emitted_writer: Option<WriterId>,

    // ---- Control middleware cumulative metrics (FLOWIP-059a-2) ----
    //
    // These fields are embedded into wide events so that the last event in a
    // journal always carries authoritative cumulative control-middleware state,
    // even when observers (e.g. MetricsAggregator) attach late and tail-start.

    /// Total requests that reached the wrapped handler (allowed calls).
    #[serde(default)]
    pub cb_requests_total: u64,

    /// Allowed calls classified as non-failures by the breaker.
    #[serde(default)]
    pub cb_successes_total: u64,

    /// Allowed calls classified as failures by the breaker.
    #[serde(default)]
    pub cb_failures_total: u64,

    /// Total requests rejected by the breaker (Open / HalfOpen non-probe).
    #[serde(default)]
    pub cb_rejections_total: u64,

    /// Total times the breaker has opened.
    #[serde(default)]
    pub cb_opened_total: u64,

    /// Cumulative time spent in Closed state (seconds).
    #[serde(default)]
    pub cb_time_closed_seconds: f64,

    /// Cumulative time spent in Open state (seconds).
    #[serde(default)]
    pub cb_time_open_seconds: f64,

    /// Cumulative time spent in HalfOpen state (seconds).
    #[serde(default)]
    pub cb_time_half_open_seconds: f64,

    /// Current circuit breaker state (0=closed, 0.5=half_open, 1=open).
    #[serde(default)]
    pub cb_state: f64,

    /// Total events that successfully consumed tokens (i.e., passed the limiter).
    #[serde(default, alias = "rl_requests_allowed_total")]
    pub rl_events_total: u64,

    /// Total events that experienced backpressure (blocked waiting for tokens).
    #[serde(default, alias = "rl_requests_delayed_total")]
    pub rl_delayed_total: u64,

    /// Total tokens consumed (capacity planning).
    #[serde(default)]
    pub rl_tokens_consumed_total: f64,

    /// Total time spent blocked waiting for tokens (seconds).
    #[serde(default)]
    pub rl_delay_seconds_total: f64,

    // ---- Rate limiter bucket state (FLOWIP-059a-3 Issue 3) ----
    //
    // These gauge fields embed the current bucket state into wide events so that
    // RL utilization can be computed reliably from the last event in a journal,
    // without depending on unreliable WindowUtilization events.

    /// Current tokens available in the rate limiter bucket.
    #[serde(default)]
    pub rl_bucket_tokens: f64,

    /// Maximum capacity of the rate limiter bucket.
    #[serde(default)]
    pub rl_bucket_capacity: f64,
}

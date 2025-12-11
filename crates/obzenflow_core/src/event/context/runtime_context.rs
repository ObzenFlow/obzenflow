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
}

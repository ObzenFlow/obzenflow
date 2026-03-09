// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::types::{
    Count, DurationMs, EventId, JournalIndex, JournalPath, RouteKey, SeqNo, ViolationCause,
};
use crate::event::vector_clock::VectorClock;
use crate::id::StageId;

#[derive(Debug, Clone)]
pub struct SourceContractEventParams {
    pub expected_count: Option<Count>,
    pub source_id: StageId,
    pub route: Option<RouteKey>,
    pub journal_path: JournalPath,
    pub journal_index: JournalIndex,
    pub writer_seq: Option<SeqNo>,
    pub vector_clock: Option<VectorClock>,
}

#[derive(Debug, Clone)]
pub struct ConsumptionProgressEventParams {
    pub reader_seq: SeqNo,
    pub last_event_id: Option<EventId>,
    pub vector_clock: Option<VectorClock>,
    pub eof_seen: bool,
    pub reader_path: JournalPath,
    pub reader_index: JournalIndex,
    pub advertised_writer_seq: Option<SeqNo>,
    pub advertised_vector_clock: Option<VectorClock>,
    pub stalled_since: Option<DurationMs>,
}

#[derive(Debug, Clone)]
pub struct ConsumptionFinalEventParams {
    pub pass: bool,
    pub consumed_count: Count,
    pub expected_count: Option<Count>,
    pub eof_seen: bool,
    pub last_event_id: Option<EventId>,
    pub reader_seq: SeqNo,
    pub advertised_writer_seq: Option<SeqNo>,
    pub advertised_vector_clock: Option<VectorClock>,
    pub failure_reason: Option<ViolationCause>,
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerSummaryEventParams {
    pub window_duration_s: u64,
    pub requests_processed: u64,
    pub requests_rejected: u64,
    pub state: String,
    pub consecutive_failures: usize,
    pub rejection_rate: f64,
    pub successes_total: u64,
    pub failures_total: u64,
    pub opened_total: u64,
    pub time_in_closed_seconds: f64,
    pub time_in_open_seconds: f64,
    pub time_in_half_open_seconds: f64,
}

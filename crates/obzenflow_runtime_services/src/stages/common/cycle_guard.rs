// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Supervisor-level cycle protection (FLOWIP-051l).
//!
//! This guard is intended to live in stage supervisors, not in the middleware chain.
//! It handles both:
//! - Data events: abort after too many iterations of the same correlation ID.
//! - Flow control signals: attenuate amplification in topologies with backflow edges.

use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::types::{JournalIndex, JournalPath, SeqNo};
use obzenflow_core::event::{ChainEvent, CorrelationId, EventId};
use obzenflow_core::{StageId, WriterId};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

const DEFAULT_ENTRY_TTL: Duration = Duration::from_secs(300);
const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

/// Entry in the cycle tracking map with timestamp for TTL.
#[derive(Debug, Clone)]
struct CycleEntry {
    iterations: usize,
    last_accessed: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SignalKind {
    Eof,
    Watermark,
    Checkpoint,
    Drain,
    PipelineAbort,
    SourceContract,
    ConsumptionGap,
    ConsumptionFinal,
    ReaderStalled,
    AtLeastOnceViolation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct OneShotKey {
    kind: SignalKind,
    origin: WriterId,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProgressKey {
    origin: WriterId,
    reader_path: JournalPath,
    reader_index: JournalIndex,
}

#[derive(Debug, Clone)]
struct ProgressEntry {
    last_seq: SeqNo,
    last_accessed: Instant,
}

/// Unified cycle protection for stages participating in a backflow cycle.
#[derive(Debug)]
pub(crate) struct CycleGuard {
    stage_name: String,

    max_iterations: usize,
    cycle_tracking: HashMap<CorrelationId, CycleEntry>,
    entry_ttl: Duration,
    cleanup_interval: Duration,
    last_cleanup: Instant,

    // --- Flow signal attenuation (FLOWIP-051l) ---
    forwarded_signal_ids: HashMap<EventId, Instant>,
    forwarded_one_shots: HashMap<OneShotKey, Instant>,
    progress_watermarks: HashMap<ProgressKey, ProgressEntry>,

    // --- Cycle EOF tracking (liveness in SCCs) ---
    upstream_eofs_seen: HashMap<StageId, Instant>,
}

impl CycleGuard {
    pub(crate) fn new(max_iterations: usize, stage_name: impl Into<String>) -> Self {
        Self {
            stage_name: stage_name.into(),
            max_iterations,
            cycle_tracking: HashMap::new(),
            entry_ttl: DEFAULT_ENTRY_TTL,
            cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
            last_cleanup: Instant::now(),
            forwarded_signal_ids: HashMap::new(),
            forwarded_one_shots: HashMap::new(),
            progress_watermarks: HashMap::new(),
            upstream_eofs_seen: HashMap::new(),
        }
    }

    /// Record that an EOF was observed from a specific upstream stage.
    ///
    /// This is used as a liveness heuristic for SCC (cycle) topologies: even when
    /// an upstream-authored EOF cannot be observed due to cyclical dependency,
    /// seeing an EOF on every upstream reader is sufficient to begin draining.
    pub(crate) fn note_upstream_eof(&mut self, upstream: StageId) {
        let now = Instant::now();
        self.maybe_cleanup(now);

        self.upstream_eofs_seen.insert(upstream, now);
    }

    /// Returns true once an EOF has been observed on every upstream reader.
    ///
    /// This does not imply the standard EOF contract has fully completed; it is
    /// a cycle-aware boundary used to avoid liveness deadlocks in SCCs.
    pub(crate) fn has_seen_all_upstream_eofs(&self, expected_upstreams: usize) -> bool {
        expected_upstreams > 0 && self.upstream_eofs_seen.len() >= expected_upstreams
    }

    /// Decide whether a flow control signal should be forwarded downstream.
    ///
    /// For cycle-member stages, this attenuates flow-control signal amplification
    /// by applying:
    /// - Event-id dedup for all signals (suppresses exact re-arrivals).
    /// - Watermark/checkpoint pass-through (advancing state).
    /// - One-shot suppression for "terminal" signals keyed by (kind, origin).
    /// - ConsumptionProgress watermark suppression keyed by (origin, reader, index).
    pub(crate) fn should_forward_signal(&mut self, event: &ChainEvent) -> bool {
        let now = Instant::now();
        self.maybe_cleanup(now);

        if self.try_mark_signal_id(event.id, now).is_err() {
            return false;
        }

        let payload = match &event.content {
            obzenflow_core::event::ChainEventContent::FlowControl(payload) => payload,
            _ => return true,
        };

        match payload {
            FlowControlPayload::ConsumptionProgress {
                reader_seq,
                reader_path,
                reader_index,
                ..
            } => {
                // ConsumptionProgress: forward only when reader_seq advances for this origin+reader.
                let key = ProgressKey {
                    origin: event.writer_id,
                    reader_path: reader_path.clone(),
                    reader_index: *reader_index,
                };

                match self.progress_watermarks.get_mut(&key) {
                    Some(entry) => {
                        entry.last_accessed = now;
                        if *reader_seq <= entry.last_seq {
                            debug!(
                                stage_name = %self.stage_name,
                                origin = %event.writer_id,
                                reader_path = %key.reader_path.0,
                                reader_index = key.reader_index.0,
                                reader_seq = reader_seq.0,
                                last_seq = entry.last_seq.0,
                                "CycleGuard: suppressing stale consumption_progress signal"
                            );
                            return false;
                        }

                        entry.last_seq = *reader_seq;
                        debug!(
                            stage_name = %self.stage_name,
                            origin = %event.writer_id,
                            reader_path = %key.reader_path.0,
                            reader_index = key.reader_index.0,
                            reader_seq = reader_seq.0,
                            "CycleGuard: allowing advanced consumption_progress signal"
                        );
                        true
                    }
                    None => {
                        self.progress_watermarks.insert(
                            key,
                            ProgressEntry {
                                last_seq: *reader_seq,
                                last_accessed: now,
                            },
                        );
                        true
                    }
                }
            }

            other => {
                let kind = match other {
                    FlowControlPayload::Eof { .. } => SignalKind::Eof,
                    FlowControlPayload::Watermark { .. } => SignalKind::Watermark,
                    FlowControlPayload::Checkpoint { .. } => SignalKind::Checkpoint,
                    FlowControlPayload::Drain => SignalKind::Drain,
                    FlowControlPayload::PipelineAbort { .. } => SignalKind::PipelineAbort,
                    FlowControlPayload::SourceContract { .. } => SignalKind::SourceContract,
                    FlowControlPayload::ConsumptionGap { .. } => SignalKind::ConsumptionGap,
                    FlowControlPayload::ConsumptionFinal { .. } => SignalKind::ConsumptionFinal,
                    FlowControlPayload::ReaderStalled { .. } => SignalKind::ReaderStalled,
                    FlowControlPayload::AtLeastOnceViolation { .. } => {
                        SignalKind::AtLeastOnceViolation
                    }
                    FlowControlPayload::ConsumptionProgress { .. } => {
                        unreachable!("handled above")
                    }
                };

                // Allow repeated watermarks/checkpoints as they represent advancing state.
                // The event-id dedup above is sufficient to prevent cycle amplification.
                if matches!(kind, SignalKind::Watermark | SignalKind::Checkpoint) {
                    return true;
                }

                // Coarse dedup for one-shot style signals to avoid repeated emissions
                // (new event IDs) amplifying in cycles.
                let key = OneShotKey {
                    kind,
                    origin: event.writer_id,
                };
                match self.forwarded_one_shots.get_mut(&key) {
                    Some(last_seen) => {
                        *last_seen = now;
                        debug!(
                            stage_name = %self.stage_name,
                            signal_kind = ?kind,
                            origin = %event.writer_id,
                            "CycleGuard: suppressing repeated one-shot flow control signal"
                        );
                        false
                    }
                    None => {
                        self.forwarded_one_shots.insert(key, now);
                        true
                    }
                }
            }
        }
    }

    /// Track iterations for a data event by correlation id.
    ///
    /// Returns an error-marked clone of `event` when the iteration limit is exceeded.
    pub(crate) fn check_data(&mut self, event: &ChainEvent) -> Result<(), Box<ChainEvent>> {
        let Some(correlation_id) = event.correlation_id else {
            return Ok(());
        };

        let now = Instant::now();
        self.maybe_cleanup(now);

        let entry = self
            .cycle_tracking
            .entry(correlation_id)
            .and_modify(|e| {
                e.iterations = e.iterations.saturating_add(1);
                e.last_accessed = now;
            })
            .or_insert_with(|| CycleEntry {
                iterations: 1,
                last_accessed: now,
            });

        debug!(
            stage_name = %self.stage_name,
            event_id = %event.id,
            correlation_id = %correlation_id,
            iterations = entry.iterations,
            max_iterations = self.max_iterations,
            "CycleGuard: processing data event"
        );

        if entry.iterations > self.max_iterations {
            warn!(
                stage_name = %self.stage_name,
                event_id = %event.id,
                correlation_id = %correlation_id,
                iterations = entry.iterations,
                max_iterations = self.max_iterations,
                "CycleGuard: aborting data event, cycle exceeded max iterations"
            );

            let mut error_event = event.clone();
            error_event.processing_info.status = ProcessingStatus::error(format!(
                "Cycle limit exceeded after {} iterations (max: {}) for correlation {} in stage {}",
                entry.iterations, self.max_iterations, correlation_id, self.stage_name
            ));
            return Err(Box::new(error_event));
        }

        Ok(())
    }

    fn try_mark_signal_id(&mut self, event_id: EventId, now: Instant) -> Result<(), ()> {
        match self.forwarded_signal_ids.get_mut(&event_id) {
            Some(last_seen) => {
                *last_seen = now;
                debug!(
                    stage_name = %self.stage_name,
                    event_id = %event_id,
                    "CycleGuard: suppressing re-arrived flow control signal (event_id)"
                );
                Err(())
            }
            None => {
                self.forwarded_signal_ids.insert(event_id, now);
                Ok(())
            }
        }
    }

    fn maybe_cleanup(&mut self, now: Instant) {
        if now.duration_since(self.last_cleanup) < self.cleanup_interval {
            return;
        }

        self.last_cleanup = now;

        self.cycle_tracking.retain(|correlation_id, entry| {
            let age = now.duration_since(entry.last_accessed);
            if age > self.entry_ttl {
                debug!(
                    stage_name = %self.stage_name,
                    correlation_id = %correlation_id,
                    age = ?age,
                    "CycleGuard: removing expired correlation entry"
                );
                false
            } else {
                true
            }
        });

        self.forwarded_signal_ids.retain(|event_id, last_seen| {
            let age = now.duration_since(*last_seen);
            if age > self.entry_ttl {
                debug!(
                    stage_name = %self.stage_name,
                    event_id = %event_id,
                    age = ?age,
                    "CycleGuard: removing expired signal entry"
                );
                false
            } else {
                true
            }
        });

        self.forwarded_one_shots.retain(|key, last_seen| {
            let age = now.duration_since(*last_seen);
            if age > self.entry_ttl {
                debug!(
                    stage_name = %self.stage_name,
                    signal_kind = ?key.kind,
                    origin = %key.origin,
                    age = ?age,
                    "CycleGuard: removing expired one-shot signal entry"
                );
                false
            } else {
                true
            }
        });

        self.progress_watermarks.retain(|key, entry| {
            let age = now.duration_since(entry.last_accessed);
            if age > self.entry_ttl {
                debug!(
                    stage_name = %self.stage_name,
                    origin = %key.origin,
                    reader_path = %key.reader_path.0,
                    reader_index = key.reader_index.0,
                    age = ?age,
                    "CycleGuard: removing expired progress watermark entry"
                );
                false
            } else {
                true
            }
        });

        self.upstream_eofs_seen.retain(|upstream, last_seen| {
            let age = now.duration_since(*last_seen);
            if age > self.entry_ttl {
                debug!(
                    stage_name = %self.stage_name,
                    upstream_stage_id = %upstream,
                    age = ?age,
                    "CycleGuard: removing expired upstream EOF entry"
                );
                false
            } else {
                true
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::event::status::processing_status::ProcessingStatus;
    use obzenflow_core::StageId;
    use obzenflow_core::WriterId;
    use serde_json::json;
    use std::time::{Duration, Instant};

    fn progress_event(origin: WriterId, seq: u64) -> ChainEvent {
        ChainEventFactory::consumption_progress_event(
            origin,
            obzenflow_core::event::ConsumptionProgressEventParams {
                reader_seq: SeqNo(seq),
                last_event_id: None,
                vector_clock: None,
                eof_seen: false,
                reader_path: JournalPath("upstream".to_string()),
                reader_index: JournalIndex(0),
                advertised_writer_seq: None,
                advertised_vector_clock: None,
                stalled_since: None,
            },
        )
    }

    #[test]
    fn check_signal_deduplicates_by_event_id() {
        let mut guard = CycleGuard::new(10, "stage_a");
        let writer_id = WriterId::from(StageId::new());
        let event = ChainEventFactory::drain_event(writer_id);

        assert!(guard.should_forward_signal(&event));
        assert!(!guard.should_forward_signal(&event));
    }

    #[test]
    fn check_signal_suppresses_stale_consumption_progress() {
        let mut guard = CycleGuard::new(10, "stage_a");
        let origin = WriterId::from(StageId::new());

        let event1 = progress_event(origin, 1);
        let event2 = progress_event(origin, 1);

        assert!(guard.should_forward_signal(&event1));
        assert!(
            !guard.should_forward_signal(&event2),
            "same reader_seq should be suppressed even with new event_id"
        );
    }

    #[test]
    fn check_signal_allows_advanced_consumption_progress() {
        let mut guard = CycleGuard::new(10, "stage_a");
        let origin = WriterId::from(StageId::new());

        let event1 = progress_event(origin, 1);
        let event2 = progress_event(origin, 2);

        assert!(guard.should_forward_signal(&event1));
        assert!(
            guard.should_forward_signal(&event2),
            "advanced reader_seq should be forwarded"
        );
    }

    #[test]
    fn check_signal_suppresses_repeated_one_shots_by_kind_and_origin() {
        let mut guard = CycleGuard::new(10, "stage_a");
        let origin = WriterId::from(StageId::new());

        let event1 = ChainEventFactory::drain_event(origin);
        let event2 = ChainEventFactory::drain_event(origin);

        assert!(guard.should_forward_signal(&event1));
        assert!(
            !guard.should_forward_signal(&event2),
            "repeated drain (new event_id) from same origin should be suppressed"
        );
    }

    #[test]
    fn check_signal_allows_one_shots_from_distinct_origins() {
        let mut guard = CycleGuard::new(10, "stage_a");
        let origin_a = WriterId::from(StageId::new());
        let origin_b = WriterId::from(StageId::new());

        assert!(guard.should_forward_signal(&ChainEventFactory::drain_event(origin_a)));
        assert!(guard.should_forward_signal(&ChainEventFactory::drain_event(origin_b)));

        assert!(!guard.should_forward_signal(&ChainEventFactory::drain_event(origin_a)));
        assert!(!guard.should_forward_signal(&ChainEventFactory::drain_event(origin_b)));
    }

    #[test]
    fn check_signal_allows_repeated_watermarks_and_checkpoints() {
        let mut guard = CycleGuard::new(10, "stage_a");
        let origin = WriterId::from(StageId::new());

        assert!(guard.should_forward_signal(&ChainEventFactory::watermark_event(origin, 1, None)));
        assert!(guard.should_forward_signal(&ChainEventFactory::watermark_event(origin, 2, None)));

        assert!(
            guard.should_forward_signal(&ChainEventFactory::checkpoint_event(
                origin,
                "c1".to_string(),
                None
            ))
        );
        assert!(
            guard.should_forward_signal(&ChainEventFactory::checkpoint_event(
                origin,
                "c2".to_string(),
                None
            ))
        );
    }

    #[test]
    fn cleanup_removes_expired_entries_across_all_maps() {
        let mut guard = CycleGuard::new(10, "stage_a");
        guard.entry_ttl = Duration::from_millis(0);
        guard.cleanup_interval = Duration::from_millis(0);

        let now = Instant::now();
        guard.last_cleanup = now - Duration::from_secs(1);
        let old = now - Duration::from_secs(1);

        let correlation_id = CorrelationId::new();
        guard.cycle_tracking.insert(
            correlation_id,
            CycleEntry {
                iterations: 1,
                last_accessed: old,
            },
        );

        let origin = WriterId::from(StageId::new());
        let event_id = ChainEventFactory::drain_event(origin).id;
        guard.forwarded_signal_ids.insert(event_id, old);
        guard.forwarded_one_shots.insert(
            OneShotKey {
                kind: SignalKind::Drain,
                origin,
            },
            old,
        );
        guard.progress_watermarks.insert(
            ProgressKey {
                origin,
                reader_path: JournalPath("upstream".to_string()),
                reader_index: JournalIndex(0),
            },
            ProgressEntry {
                last_seq: SeqNo(1),
                last_accessed: old,
            },
        );

        let upstream = StageId::new();
        guard.upstream_eofs_seen.insert(upstream, old);

        guard.maybe_cleanup(now);

        assert!(guard.cycle_tracking.is_empty());
        assert!(guard.forwarded_signal_ids.is_empty());
        assert!(guard.forwarded_one_shots.is_empty());
        assert!(guard.progress_watermarks.is_empty());
        assert!(guard.upstream_eofs_seen.is_empty());
    }

    #[test]
    fn upstream_eof_tracking_reaches_expected_boundary() {
        let mut guard = CycleGuard::new(10, "stage_a");
        let a = StageId::new();
        let b = StageId::new();

        assert!(!guard.has_seen_all_upstream_eofs(2));
        guard.note_upstream_eof(a);
        assert!(!guard.has_seen_all_upstream_eofs(2));

        // Duplicate EOF from the same upstream shouldn't advance the boundary.
        guard.note_upstream_eof(a);
        assert!(!guard.has_seen_all_upstream_eofs(2));

        guard.note_upstream_eof(b);
        assert!(guard.has_seen_all_upstream_eofs(2));
    }

    #[test]
    fn check_data_tracks_iterations_by_correlation_id() {
        let mut guard = CycleGuard::new(2, "stage_a");

        let writer_id = WriterId::from(StageId::new());
        let mut event = ChainEventFactory::data_event(writer_id, "t", json!({"x": 1}));
        let correlation_id = CorrelationId::new();
        event.correlation_id = Some(correlation_id);

        assert!(guard.check_data(&event).is_ok());
        assert!(guard.check_data(&event).is_ok());

        let err = guard
            .check_data(&event)
            .expect_err("third iteration should abort");
        assert!(matches!(
            &err.processing_info.status,
            ProcessingStatus::Error { .. }
        ));
    }

    #[test]
    fn check_data_skips_events_without_correlation_id() {
        let mut guard = CycleGuard::new(1, "stage_a");
        let writer_id = WriterId::from(StageId::new());
        let event = ChainEventFactory::data_event(writer_id, "t", json!({"x": 1}));
        assert!(guard.check_data(&event).is_ok());
    }
}

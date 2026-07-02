// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Supervisor-level cycle protection
//!
//! This guard is intended to live in stage supervisors, not in the middleware chain.
//! It handles both:
//! - Data events: per-event cycle depth tracking, abort after too many round trips.
//! - Flow control signals: attenuate amplification in topologies with backflow edges.

use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::{ChainEvent, EventId};
use obzenflow_core::{CycleDepth, SccId, StageId, WriterId};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use crate::pipeline::MaxIterations;

const DEFAULT_ENTRY_TTL: Duration = Duration::from_secs(300);
const DEFAULT_CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SignalKind {
    Eof,
    Drain,
    PipelineAbort,
    SourceContract,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct OneShotKey {
    kind: SignalKind,
    origin: WriterId,
}

/// Unified cycle protection for stages participating in a backflow cycle.
#[derive(Debug)]
pub(crate) struct CycleGuard {
    stage_name: String,

    max_iterations: MaxIterations,
    scc_id: SccId,
    is_entry_point: bool,
    entry_ttl: Duration,
    cleanup_interval: Duration,
    last_cleanup: Instant,

    // --- Flow signal attenuation (FLOWIP-051l) ---
    forwarded_signal_ids: HashMap<EventId, Instant>,
    forwarded_one_shots: HashMap<OneShotKey, Instant>,

    // --- Cycle EOF tracking (liveness in SCCs) ---
    upstream_eofs_seen: HashMap<StageId, Instant>,
}

impl CycleGuard {
    pub(crate) fn new(
        max_iterations: MaxIterations,
        scc_id: SccId,
        is_entry_point: bool,
        stage_name: impl Into<String>,
    ) -> Self {
        Self {
            stage_name: stage_name.into(),
            max_iterations,
            scc_id,
            is_entry_point,
            entry_ttl: DEFAULT_ENTRY_TTL,
            cleanup_interval: DEFAULT_CLEANUP_INTERVAL,
            last_cleanup: Instant::now(),
            forwarded_signal_ids: HashMap::new(),
            forwarded_one_shots: HashMap::new(),
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
    ///
    /// Reader telemetry never reaches this decision: it is filtered at the
    /// read side of every `TransportOnly` subscription (FLOWIP-095d), so it
    /// cannot circulate in a cycle in the first place.
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

        let kind = match payload {
            FlowControlPayload::Eof { .. } => SignalKind::Eof,
            FlowControlPayload::Drain => SignalKind::Drain,
            FlowControlPayload::PipelineAbort { .. } => SignalKind::PipelineAbort,
            FlowControlPayload::SourceContract { .. } => SignalKind::SourceContract,
            // Allow repeated watermarks/checkpoints as they represent advancing
            // state. The event-id dedup above is sufficient to prevent cycle
            // amplification.
            FlowControlPayload::Watermark { .. } | FlowControlPayload::Checkpoint { .. } => {
                return true;
            }
            FlowControlPayload::ConsumptionProgress { .. }
            | FlowControlPayload::ConsumptionGap { .. }
            | FlowControlPayload::ConsumptionFinal { .. }
            | FlowControlPayload::ReaderStalled { .. }
            | FlowControlPayload::AtLeastOnceViolation { .. } => {
                unreachable!(
                    "FLOWIP-095d: reader telemetry is filtered at the read side of every \
                     TransportOnly subscription and never reaches signal forwarding"
                )
            }
            FlowControlPayload::CatchUpComplete { .. } => {
                unreachable!(
                    "FLOWIP-120n F13: --resume-from rejects cyclic flows at build, so the \
                     catch-up watermark never reaches a cycle guard; the condensation-frontier \
                     follow-on adds the SignalKind arm"
                )
            }
        };

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

    /// Per-event cycle depth check (FLOWIP-051p).
    ///
    /// Only the SCC entry point increments `cycle_depth`; non-entry stages
    /// pass through without modification. When an event enters a different
    /// SCC, its depth is reset.
    ///
    /// Returns an error-marked clone of `event` when the iteration limit is exceeded.
    pub(crate) fn check_data(&mut self, event: &mut ChainEvent) -> Result<(), Box<ChainEvent>> {
        if !self.is_entry_point {
            return Ok(());
        }

        // First visit to any SCC: initialise tracking fields.
        let depth = match (event.cycle_scc_id, event.cycle_depth) {
            // Same SCC: increment depth.
            (Some(id), Some(d)) if id == self.scc_id => d.increment(),
            // Different SCC or first encounter: reset to 1.
            _ => CycleDepth::first(),
        };

        event.cycle_depth = Some(depth);
        event.cycle_scc_id = Some(self.scc_id);

        debug!(
            stage_name = %self.stage_name,
            event_id = %event.id,
            correlation_id = ?event.correlation_id(),
            cycle_depth = %depth,
            max_iterations = %self.max_iterations,
            scc_id = %self.scc_id,
            "CycleGuard: processing data event (per-event depth)"
        );

        if depth.as_u16() > self.max_iterations.as_u16() {
            warn!(
                stage_name = %self.stage_name,
                event_id = %event.id,
                correlation_id = ?event.correlation_id(),
                cycle_depth = %depth,
                max_iterations = %self.max_iterations,
                scc_id = %self.scc_id,
                "CycleGuard: aborting data event, cycle depth exceeded max iterations"
            );

            let mut error_event = event.clone();
            error_event.processing_info.status = ProcessingStatus::error(format!(
                "Cycle depth {} exceeds max iterations {} ({}) in stage {}",
                depth, self.max_iterations, self.scc_id, self.stage_name
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
    fn test_scc_id(n: u128) -> SccId {
        SccId::from_ulid(obzenflow_core::Ulid::from(n))
    }

    #[test]
    fn check_signal_deduplicates_by_event_id() {
        let mut guard = CycleGuard::new(MaxIterations::new(10), test_scc_id(0), true, "stage_a");
        let writer_id = WriterId::from(StageId::new());
        let event = ChainEventFactory::drain_event(writer_id);

        assert!(guard.should_forward_signal(&event));
        assert!(!guard.should_forward_signal(&event));
    }

    #[test]
    fn check_signal_suppresses_repeated_one_shots_by_kind_and_origin() {
        let mut guard = CycleGuard::new(MaxIterations::new(10), test_scc_id(0), true, "stage_a");
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
        let mut guard = CycleGuard::new(MaxIterations::new(10), test_scc_id(0), true, "stage_a");
        let origin_a = WriterId::from(StageId::new());
        let origin_b = WriterId::from(StageId::new());

        assert!(guard.should_forward_signal(&ChainEventFactory::drain_event(origin_a)));
        assert!(guard.should_forward_signal(&ChainEventFactory::drain_event(origin_b)));

        assert!(!guard.should_forward_signal(&ChainEventFactory::drain_event(origin_a)));
        assert!(!guard.should_forward_signal(&ChainEventFactory::drain_event(origin_b)));
    }

    #[test]
    fn check_signal_allows_repeated_watermarks_and_checkpoints() {
        let mut guard = CycleGuard::new(MaxIterations::new(10), test_scc_id(0), true, "stage_a");
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
        let mut guard = CycleGuard::new(MaxIterations::new(10), test_scc_id(0), true, "stage_a");
        guard.entry_ttl = Duration::from_millis(0);
        guard.cleanup_interval = Duration::from_millis(0);

        let now = Instant::now();
        guard.last_cleanup = now - Duration::from_secs(1);
        let old = now - Duration::from_secs(1);

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

        let upstream = StageId::new();
        guard.upstream_eofs_seen.insert(upstream, old);

        guard.maybe_cleanup(now);

        assert!(guard.forwarded_signal_ids.is_empty());
        assert!(guard.forwarded_one_shots.is_empty());
        assert!(guard.upstream_eofs_seen.is_empty());
    }

    #[test]
    fn upstream_eof_tracking_reaches_expected_boundary() {
        let mut guard = CycleGuard::new(MaxIterations::new(10), test_scc_id(0), true, "stage_a");
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
    fn check_data_increments_depth_at_entry_point() {
        let mut guard = CycleGuard::new(MaxIterations::new(2), test_scc_id(1), true, "stage_a");

        let writer_id = WriterId::from(StageId::new());
        let mut event = ChainEventFactory::data_event(writer_id, "t", json!({"x": 1}));

        // First pass: depth set to 1, scc_id set to 1.
        assert!(guard.check_data(&mut event).is_ok());
        assert_eq!(event.cycle_depth, Some(CycleDepth::new(1)));
        assert_eq!(event.cycle_scc_id, Some(test_scc_id(1)));

        // Second pass: depth incremented to 2 (at max_iterations).
        assert!(guard.check_data(&mut event).is_ok());
        assert_eq!(event.cycle_depth, Some(CycleDepth::new(2)));

        // Third pass: depth 3 exceeds max_iterations=2, should abort.
        let err = guard
            .check_data(&mut event)
            .expect_err("third iteration should abort");
        assert!(matches!(
            &err.processing_info.status,
            ProcessingStatus::Error { .. }
        ));
    }

    #[test]
    fn check_data_non_entry_point_passes_through() {
        let mut guard = CycleGuard::new(MaxIterations::new(1), test_scc_id(1), false, "stage_b");

        let writer_id = WriterId::from(StageId::new());
        let mut event = ChainEventFactory::data_event(writer_id, "t", json!({"x": 1}));
        event.cycle_depth = Some(CycleDepth::new(5));
        event.cycle_scc_id = Some(test_scc_id(1));

        // Non-entry point should not modify or check the event.
        assert!(guard.check_data(&mut event).is_ok());
        assert_eq!(
            event.cycle_depth,
            Some(CycleDepth::new(5)),
            "depth should be unchanged"
        );
    }

    #[test]
    fn check_data_resets_depth_on_scc_boundary() {
        let mut guard = CycleGuard::new(MaxIterations::new(10), test_scc_id(2), true, "stage_c");

        let writer_id = WriterId::from(StageId::new());
        let mut event = ChainEventFactory::data_event(writer_id, "t", json!({"x": 1}));
        // Simulate event arriving from a different SCC.
        event.cycle_depth = Some(CycleDepth::new(8));
        event.cycle_scc_id = Some(test_scc_id(1));

        assert!(guard.check_data(&mut event).is_ok());
        assert_eq!(
            event.cycle_depth,
            Some(CycleDepth::first()),
            "depth should reset on SCC boundary"
        );
        assert_eq!(
            event.cycle_scc_id,
            Some(test_scc_id(2)),
            "scc_id should update"
        );
    }

    #[test]
    fn check_data_passes_through_events_without_prior_cycle_state() {
        let mut guard = CycleGuard::new(MaxIterations::new(10), test_scc_id(1), true, "stage_a");

        let writer_id = WriterId::from(StageId::new());
        let mut event = ChainEventFactory::data_event(writer_id, "t", json!({"x": 1}));

        assert!(guard.check_data(&mut event).is_ok());
        assert_eq!(event.cycle_depth, Some(CycleDepth::first()));
        assert_eq!(event.cycle_scc_id, Some(test_scc_id(1)));
    }
}

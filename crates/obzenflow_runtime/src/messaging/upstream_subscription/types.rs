// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::control_middleware::ControlMiddlewareProvider;
use obzenflow_core::event::system_event::SystemEvent;
use obzenflow_core::event::types::{
    Count, DurationMs, SeqNo, ViolationCause as EventViolationCause,
};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::ChainEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::{EventEnvelope, EventId, StageId, WriterId};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use tokio::time::Instant;

use crate::pipeline::config::CycleGuardConfig;

/// Status of contract checking
#[derive(Debug)]
#[must_use]
pub enum ContractStatus {
    /// All contracts are healthy
    Healthy,
    /// An upstream is stalled
    Stalled(StageId),
    /// Contract violation detected (event-level cause for system events)
    Violated {
        upstream: StageId,
        cause: EventViolationCause,
    },
    /// Progress emitted
    ProgressEmitted,
}

/// Encapsulates the mechanical state of subscription management
/// This is "mechanism" state - HOW we're reading, not WHY or WHEN
#[derive(Debug)]
pub struct SubscriptionState {
    /// Round-robin fairness state
    pub(super) current_reader_index: usize,

    /// EOF tracking per reader (true terminal EOF observed via FlowControl::Eof)
    pub(super) eof_received: Vec<bool>,

    /// True when a reader was created at the journal tail position.
    /// This is used for "logical EOF" checks where starting at tail
    /// should count as having no historical data to consume, while
    /// still allowing new events appended after subscription creation
    /// to be observed.
    pub(super) baseline_at_tail: Vec<bool>,

    /// Buffering for events that need to be returned later
    /// (Currently unused but available for future ordering requirements)
    pub(super) pending_events: VecDeque<EventEnvelope<ChainEvent>>,
}

impl SubscriptionState {
    pub(super) fn new(reader_count: usize) -> Self {
        Self {
            current_reader_index: 0,
            eof_received: vec![false; reader_count],
            baseline_at_tail: vec![false; reader_count],
            pending_events: VecDeque::new(),
        }
    }

    /// Get count of readers that have sent EOF
    pub fn eof_count(&self) -> usize {
        self.eof_received.iter().filter(|&&eof| eof).count()
    }

    /// Check if a specific reader has sent EOF
    pub fn is_reader_eof(&self, index: usize) -> bool {
        self.eof_received.get(index).copied().unwrap_or(false)
    }

    /// Mark a reader as having sent EOF
    pub fn mark_reader_eof(&mut self, index: usize) {
        if let Some(eof) = self.eof_received.get_mut(index) {
            *eof = true;
        }
    }

    /// Mark a reader as having started at tail (logical EOF baseline).
    pub fn mark_reader_baseline_at_tail(&mut self, index: usize) {
        if let Some(flag) = self.baseline_at_tail.get_mut(index) {
            *flag = true;
        }
    }

    /// Clear the "started at tail" baseline once a reader has observed any new events.
    pub fn clear_reader_baseline_at_tail(&mut self, index: usize) {
        if let Some(flag) = self.baseline_at_tail.get_mut(index) {
            *flag = false;
        }
    }

    /// Returns true if a reader is logically at EOF: either it has
    /// observed a true EOF event, or it was created at the journal
    /// tail and has no historical data to consume.
    pub fn is_reader_logically_eof(&self, index: usize) -> bool {
        let eof = self.eof_received.get(index).copied().unwrap_or(false);
        let baseline = self.baseline_at_tail.get(index).copied().unwrap_or(false);
        eof || baseline
    }

    /// Count of readers that are logically at EOF.
    pub fn logical_eof_count(&self) -> usize {
        (0..self.eof_received.len())
            .filter(|&i| self.is_reader_logically_eof(i))
            .count()
    }

    /// Get the next reader index in round-robin order
    pub fn next_reader_index(&mut self) -> usize {
        let current = self.current_reader_index;
        self.current_reader_index = (self.current_reader_index + 1) % self.eof_received.len();
        current
    }

    /// Check if there are pending buffered events
    pub fn has_pending(&self) -> bool {
        !self.pending_events.is_empty()
    }
}

/// Outcome for EOF accounting to let FSMs decide when to drain
#[derive(Clone, Debug)]
pub struct EofOutcome {
    pub stage_id: StageId,
    pub stage_name: String,
    pub reader_index: usize,
    pub eof_count: usize,
    pub total_readers: usize,
    pub is_final: bool,
}

/// Progress tracking for a single upstream reader.
///
/// This struct is intentionally pure contract state (no I/O handles) so that it
/// can be owned by FSM contexts and inspected/snapshotted independently of the
/// subscription mechanics.
#[derive(Debug, Clone)]
pub(crate) struct PendingReceiptMeta {
    pub seq: SeqNo,
    pub event_id: EventId,
    pub vector_clock: VectorClock,
    pub event: ChainEvent,
}

#[derive(Debug)]
pub struct ReaderProgress {
    pub stage_id: StageId,

    /// Sequences and positions
    pub reader_seq: SeqNo,
    pub receipted_seq: SeqNo,
    pub advertised_writer_seq: Option<SeqNo>,
    pub last_event_id: Option<obzenflow_core::EventId>,
    pub last_vector_clock: Option<VectorClock>,
    pub last_receipted_event_id: Option<EventId>,
    pub last_receipted_vector_clock: Option<VectorClock>,
    pub(crate) pending_receipts: HashMap<EventId, PendingReceiptMeta>,
    pub(crate) committed_out_of_order: BTreeMap<u64, PendingReceiptMeta>,

    /// Progress timing
    pub last_progress_seq: SeqNo,
    pub last_progress_instant: Option<Instant>,
    pub last_read_instant: Option<Instant>,

    /// Last reader_seq for which we emitted a mid-flight ContractResult heartbeat.
    ///
    /// This is used to avoid emitting redundant `"healthy"` contract results on
    /// every wall-clock tick when no new data has been consumed on the edge.
    pub last_contract_result_seq: SeqNo,

    /// Stall detection
    pub stalled_since: Option<Instant>,
    pub consecutive_stall_checks: u32,

    /// Contract status
    pub final_emitted: bool,
    pub contract_violated: bool,
}

impl ReaderProgress {
    /// Create a new reader progress record for the given upstream stage.
    ///
    /// Kept `pub(crate)` so FSM contexts within this crate can construct
    /// contract state without exposing construction details outside the crate.
    pub(crate) fn new(stage_id: StageId) -> Self {
        Self {
            stage_id,
            reader_seq: SeqNo(0),
            receipted_seq: SeqNo(0),
            advertised_writer_seq: None,
            last_event_id: None,
            last_vector_clock: None,
            last_receipted_event_id: None,
            last_receipted_vector_clock: None,
            pending_receipts: HashMap::new(),
            committed_out_of_order: BTreeMap::new(),
            last_progress_seq: SeqNo(0),
            last_progress_instant: None,
            last_read_instant: None,
            last_contract_result_seq: SeqNo(0),
            stalled_since: None,
            consecutive_stall_checks: 0,
            final_emitted: false,
            contract_violated: false,
        }
    }

    pub(crate) fn track_pending_receipt(
        &mut self,
        event_id: EventId,
        event: ChainEvent,
        vector_clock: VectorClock,
    ) {
        let meta = PendingReceiptMeta {
            seq: self.reader_seq,
            event_id,
            vector_clock,
            event,
        };
        self.pending_receipts.insert(event_id, meta);
    }

    pub(crate) fn mark_receipted(&mut self, event_id: EventId) -> bool {
        let Some(meta) = self.pending_receipts.remove(&event_id) else {
            return false;
        };

        if meta.seq.0 == self.receipted_seq.0.saturating_add(1) {
            self.advance_receipted(meta);
            loop {
                let next_seq = self.receipted_seq.0.saturating_add(1);
                let Some(next_meta) = self.committed_out_of_order.remove(&next_seq) else {
                    break;
                };
                self.advance_receipted(next_meta);
            }
        } else if meta.seq.0 > self.receipted_seq.0 {
            self.committed_out_of_order.insert(meta.seq.0, meta);
        }

        true
    }

    fn advance_receipted(&mut self, meta: PendingReceiptMeta) {
        self.receipted_seq = meta.seq;
        self.last_receipted_event_id = Some(meta.event_id);
        self.last_receipted_vector_clock = Some(meta.vector_clock);
    }
}

/// Contract tracking state - separated from subscription mechanics.
///
/// This struct owns configuration and journal handles, but **does not** own
/// per-reader contract state. Reader progress lives in FSM contexts and is
/// passed in as `&mut [ReaderProgress]` when contracts are evaluated.
pub struct ContractTracker {
    /// Configuration for contract behaviour
    pub(super) config: ContractConfig,

    /// References for emission (not owned)
    pub(super) writer_id: WriterId,
    pub(super) journal: Arc<dyn Journal<ChainEvent>>,
    pub(super) system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
    pub(super) reader_stage: Option<StageId>,
    pub(super) receipt_aware_progress: bool,

    /// Tracks output events written by this stage
    pub(super) output_events_written: SeqNo,
}

/// Wiring configuration for enabling contracts on an upstream subscription.
pub struct ContractsWiring {
    pub writer_id: WriterId,
    pub contract_journal: Arc<dyn Journal<ChainEvent>>,
    pub config: ContractConfig,
    pub system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
    pub reader_stage: Option<StageId>,
    pub control_middleware: Arc<dyn ControlMiddlewareProvider>,
    pub include_delivery_contract: bool,
    pub cycle_guard_config: Option<CycleGuardConfig>,
}

/// Runtime configuration for contract emissions
#[derive(Clone)]
pub struct ContractConfig {
    pub progress_min_events: Count,
    pub progress_max_interval: DurationMs,
    pub stall_threshold: DurationMs,
    pub stall_cooloff: DurationMs,
    pub stall_checks_before_emit: u32,
}

impl Default for ContractConfig {
    fn default() -> Self {
        Self {
            progress_min_events: Count(1),
            progress_max_interval: DurationMs(1000),
            stall_threshold: DurationMs(2000),
            stall_cooloff: DurationMs(0),
            stall_checks_before_emit: 3,
        }
    }
}

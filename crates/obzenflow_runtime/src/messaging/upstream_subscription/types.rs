// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::control_plane::ControlPlaneProvider;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::system_event::{SystemEvent, SystemFeedRole};
use obzenflow_core::event::types::{
    Count, DurationMs, SeqNo, ViolationCause as EventViolationCause,
};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::ChainEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::{EventEnvelope, EventId, EventType, StageId, WriterId};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;
use tokio::time::Instant;

use crate::pipeline::config::CycleGuardConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StageInputPosition(pub u64);

/// Stable cross-run upstream identity (FLOWIP-095d): the descriptor stage
/// name, never the per-run `StageId` ULID. This is a replay-order
/// correctness key, not display text; it is a tiebreak component of the
/// canonical deterministic merge.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StageKey(String);

impl StageKey {
    pub fn new(name: impl Into<String>) -> Self {
        Self(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StageKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Selected-feed identity component of the merge tiebreak (FLOWIP-095d): the
/// sorted set of (role, event type) feeds a reader consumes, empty when the
/// reader is unfiltered. Structural rather than a joined string, so the
/// ordering can never be perturbed by delimiter collisions inside event-type
/// names. The derived `Ord` is lexicographic over the sorted entries;
/// `SelectedFeedRole`'s variant order is part of the total order and is
/// code-defined, so it is stable across runs.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct FeedIdentity(Vec<(SelectedFeedRole, EventType)>);

impl FeedIdentity {
    /// Identity of an unfiltered reader (compares before every filtered one).
    pub fn unfiltered() -> Self {
        Self::default()
    }

    pub fn from_feeds(feeds: &[SelectedFeedMetadata]) -> Self {
        let mut entries: Vec<(SelectedFeedRole, EventType)> = feeds
            .iter()
            .map(|feed| (feed.role(), feed.event_type().clone()))
            .collect();
        entries.sort_unstable();
        Self(entries)
    }
}

impl std::fmt::Display for FeedIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, (role, event_type)) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str(",")?;
            }
            write!(f, "{role:?}:{}", event_type.as_str())?;
        }
        Ok(())
    }
}

/// The canonical merge tiebreak key for one reader (FLOWIP-095d): stage key,
/// then feed identity, both stable across runs. Compared after the delivered
/// ordinal; per-run identifiers never participate. The derived `Ord` follows
/// field order.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReaderTiebreakKey {
    pub stage_key: StageKey,
    pub feed_identity: FeedIdentity,
}

/// Count of transport events delivered from one reader (FLOWIP-095d): the
/// canonical merge's ordinal source and its entire checkpointable state.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeliveredCount(pub u64);

impl DeliveredCount {
    pub fn increment(&mut self) {
        self.0 = self.0.saturating_add(1);
    }

    /// The tiebreak ordinal this reader's next delivery would take.
    pub fn next_ordinal(self) -> DeliveredOrdinal {
        DeliveredOrdinal(self.0.saturating_add(1))
    }
}

/// The tiebreak ordinal a pending delivery would take (FLOWIP-095d).
/// Deliberately distinct from `DeliveredCount` so a count can never be
/// compared against an ordinal; both the subscription-internal merge and the
/// join's cross-side rule compare ordinals only.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeliveredOrdinal(pub u64);

/// Reader-selection policy for a multi-reader subscription (FLOWIP-095d).
///
/// `AvailabilityRoundRobin` is today's behaviour: cycle through readers and
/// deliver whatever is available first. Delivery order then depends on arrival
/// timing, which is fine for stages that do not need deterministic fan-in.
///
/// `CanonicalMerge` is the Kahn-style deterministic merge: hold one head per
/// reader, deliver nothing while any non-exhausted reader is quiet, and choose
/// among heads by happened-before then a (delivered ordinal, stage name, feed
/// identity) tiebreak. Delivery order becomes a pure function of the per-input
/// streams, so live, replay, and resume catch-up all compute the same order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ReaderSelectionPolicy {
    #[default]
    AvailabilityRoundRobin,
    CanonicalMerge,
}

/// Why a canonical-merge poll delivered nothing: at least one non-exhausted
/// input had no head. Surfaced so supervisors can report idle-by-rule waits
/// (the input being waited on) instead of looking hung.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeWaitState {
    pub quiet_inputs: Vec<(StageId, String)>,
}

/// Status of a canonical-merge candidate acquisition pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeCandidateStatus {
    /// Every non-exhausted reader presents a head and a winner is selected.
    Candidate,
    /// A transport-filtered row was consumed while acquiring heads. Candidate
    /// selection is deferred to the next bounded poll.
    CursorAdvanced {
        upstream: StageId,
        completed_data_rows: u64,
    },
    /// At least one non-exhausted reader has no head; nothing may deliver.
    Quiet,
    /// Every reader has delivered its authored EOF; the merge is finished.
    AllExhausted,
}

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

    /// Per-reader terminal EOF kind, folded worst-wins on duplicates
    /// (FLOWIP-095k). Slots persist for the subscription's lifetime, so a
    /// resumed run's terminal kind inherits catch-up-synthesized kinds.
    pub(super) eof_kinds: Vec<Option<EofKind>>,

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
            eof_kinds: vec![None; reader_count],
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

    /// Record a reader's terminal EOF kind, folding worst-wins per slot so a
    /// duplicate EOF on one input never downgrades an earlier kind.
    pub fn mark_reader_eof_kind(&mut self, index: usize, kind: EofKind) {
        if let Some(slot) = self.eof_kinds.get_mut(index) {
            *slot = Some(slot.map_or(kind, |current| current.worst(kind)));
        }
    }

    /// Worst-wins join over every input's terminal kind; `None` until any
    /// EOF has arrived.
    pub fn worst_eof_kind(&self) -> Option<EofKind> {
        self.eof_kinds
            .iter()
            .flatten()
            .copied()
            .reduce(EofKind::worst)
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
    /// Worst-wins join over the inputs' terminal kinds so far (FLOWIP-095k).
    pub worst_kind: Option<EofKind>,
}

/// The variant order participates in `FeedIdentity`'s total order
/// (FLOWIP-095d); it is code-defined and therefore stable across runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SelectedFeedRole {
    /// Compatibility path for callers that select only by event type and do not
    /// distinguish input/reference/stream roles.
    Unspecified,
    Input,
    Reference,
    Stream,
}

impl SelectedFeedRole {
    pub fn as_system_feed_role(self) -> Option<SystemFeedRole> {
        match self {
            Self::Unspecified => None,
            Self::Input => Some(SystemFeedRole::Input),
            Self::Reference => Some(SystemFeedRole::Reference),
            Self::Stream => Some(SystemFeedRole::Stream),
        }
    }
}

impl From<SystemFeedRole> for SelectedFeedRole {
    fn from(value: SystemFeedRole) -> Self {
        match value {
            SystemFeedRole::Input => Self::Input,
            SystemFeedRole::Reference => Self::Reference,
            SystemFeedRole::Stream => Self::Stream,
        }
    }
}

impl From<crate::feed_plan::FeedRole> for SelectedFeedRole {
    fn from(value: crate::feed_plan::FeedRole) -> Self {
        match value {
            crate::feed_plan::FeedRole::Input => Self::Input,
            crate::feed_plan::FeedRole::Reference => Self::Reference,
            crate::feed_plan::FeedRole::Stream => Self::Stream,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedFeedMetadata {
    event_type: EventType,
    role: SelectedFeedRole,
}

impl SelectedFeedMetadata {
    pub fn new(event_type: EventType, role: SelectedFeedRole) -> Self {
        Self { event_type, role }
    }

    pub fn unscoped(event_type: EventType) -> Self {
        Self::new(event_type, SelectedFeedRole::Unspecified)
    }

    pub fn event_type(&self) -> &EventType {
        &self.event_type
    }

    pub fn role(&self) -> SelectedFeedRole {
        self.role
    }

    pub fn system_feed_role(&self) -> Option<SystemFeedRole> {
        self.role.as_system_feed_role()
    }

    pub(super) fn matches_event_type(&self, event_type: &str) -> bool {
        crate::feed_plan::declared_event_type_matches(self.event_type.as_str(), event_type, None)
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct SelectedDataSeqByEventType {
    by_event_type: HashMap<EventType, SeqNo>,
}

impl SelectedDataSeqByEventType {
    pub(super) fn increment(&mut self, event_type: impl Into<EventType>) {
        let seq = self
            .by_event_type
            .entry(event_type.into())
            .or_insert(SeqNo(0));
        seq.0 = seq.0.saturating_add(1);
    }

    pub(super) fn seq_for_feed(&self, feed: &SelectedFeedMetadata) -> SeqNo {
        self.by_event_type
            .iter()
            .find(|(event_type, _)| feed.matches_event_type(event_type.as_str()))
            .map(|(_, seq)| *seq)
            .unwrap_or(SeqNo(0))
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct AdvertisedWriterSeqByEventType {
    by_event_type: BTreeMap<EventType, SeqNo>,
}

impl AdvertisedWriterSeqByEventType {
    pub(super) fn is_empty(&self) -> bool {
        self.by_event_type.is_empty()
    }

    pub(super) fn replace_from_eof(
        &mut self,
        writer_seq_by_event_type: &BTreeMap<EventType, SeqNo>,
    ) {
        self.by_event_type = writer_seq_by_event_type.clone();
    }

    pub(super) fn seq_for_feed(&self, feed: &SelectedFeedMetadata) -> Option<SeqNo> {
        self.by_event_type
            .iter()
            .find(|(event_type, _)| feed.matches_event_type(event_type.as_str()))
            .map(|(_, seq)| *seq)
    }
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
    pub(crate) committed_out_of_order: BTreeMap<SeqNo, PendingReceiptMeta>,

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
    pub last_stall_emitted_instant: Option<Instant>,

    /// Contract status
    pub final_emitted: bool,
    pub contract_violated: bool,
}

impl ReaderProgress {
    /// Create a new reader progress record for the given upstream stage.
    pub fn new(stage_id: StageId) -> Self {
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
            last_stall_emitted_instant: None,
            final_emitted: false,
            contract_violated: false,
        }
    }

    /// Stores receipt metadata for a just-read event that requires durable delivery evidence.
    ///
    /// `reader_seq` at the time of tracking is used as the receipt-ordering key for advancing
    /// the contiguous receipt watermark (`receipted_seq`).
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

    /// Mark a pending event as durably receipted.
    ///
    /// Returns `false` if the event is not pending (duplicate receipt or missing read-side
    /// bookkeeping). When receipts arrive out-of-order, this buffers them until the receipt
    /// watermark can advance contiguously.
    pub(crate) fn mark_receipted(&mut self, event_id: EventId) -> bool {
        let Some(meta) = self.pending_receipts.remove(&event_id) else {
            return false;
        };

        let next_seq = SeqNo(self.receipted_seq.0.saturating_add(1));
        if meta.seq == next_seq {
            self.advance_receipted(meta);
            loop {
                let next_seq = SeqNo(self.receipted_seq.0.saturating_add(1));
                let Some(next_meta) = self.committed_out_of_order.remove(&next_seq) else {
                    break;
                };
                self.advance_receipted(next_meta);
            }
        } else if meta.seq.0 > self.receipted_seq.0 {
            let seq = meta.seq;
            self.committed_out_of_order.insert(seq, meta);
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
    pub control_plane: Arc<dyn ControlPlaneProvider>,
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
            stall_threshold: DurationMs(30000),
            stall_cooloff: DurationMs(30000),
            stall_checks_before_emit: 3,
        }
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Subscription coordinator for reading from upstream journals.
//!
//! This module provides a non-blocking subscription mechanism that coordinates
//! reading from multiple upstream journals without owning the event loop.
//!
//! Key design principles:
//! - Separates mechanism (how to read) from policy (when to read)
//! - Returns immediately with PollResult, never blocks or loops internally
//! - FSM owns control flow decisions (sleep, retry, transition)
//! - Contract tracking is separated from subscription mechanics

mod construction;
mod contract_checking;
mod polling;
mod recorded;
mod types;

#[cfg(test)]
mod tests;

pub use super::subscription_poller::{PollResult, SubscriptionPoller};
pub use recorded::{load_selection_record, AfterRecord, ArchivedSelection, RecordedOrder};
pub(crate) use recorded::{selection_failure, NamedHead, RecordedStep};
use types::{AdvertisedWriterSeqByEventType, SelectedDataSeqByEventType};
pub use types::{
    CompositeEntrySpec, ContractConfig, ContractFailure, ContractStatus, ContractTracker,
    ContractsWiring, DeliveredCount, DeliveredOrdinal, EofOutcome, FeedIdentity,
    MergeCandidateStatus, MergeWaitState, ReaderProgress, ReaderSelectionPolicy, ReaderTiebreakKey,
    SelectedFeedMetadata, SelectedFeedRole, StageInputPosition, StageKey, SubscriptionState,
};

use crate::contracts::ContractChain;
use crate::control_plane::ControlPlaneProvider;
use crate::messaging::upstream_subscription_policy::ContractPolicyStack;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{JournalEvent, JournalRecord};
use obzenflow_core::journal::reader::JournalReader;
use obzenflow_core::{ReaderGeneration, StageId};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ReaderFrontier {
    least_open: Option<ReaderGeneration>,
    highest: ReaderGeneration,
}

impl ReaderFrontier {
    #[cfg(test)]
    pub(crate) fn for_tests(
        least_open: Option<ReaderGeneration>,
        highest: ReaderGeneration,
    ) -> Self {
        Self {
            least_open,
            highest,
        }
    }

    pub(crate) fn caught_up(self, target: ReaderGeneration) -> bool {
        self.least_open.is_none_or(|least| least >= target)
    }

    pub(crate) fn highest(self) -> ReaderGeneration {
        self.highest
    }

    pub(crate) fn and(self, other: Self) -> Self {
        let least_open = match (self.least_open, other.least_open) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };
        Self {
            least_open,
            highest: self.highest.max(other.highest),
        }
    }
}

struct FeedContractChain {
    metadata: SelectedFeedMetadata,
    chain: ContractChain,
    last_contract_result_seq: SeqNo,
}

/// One upstream reader binding (FLOWIP-095d).
///
/// `stage_key` is the stable cross-run ordering identity (the descriptor
/// stage name); `stage_id` is the per-run ULID and never participates in
/// ordering decisions.
pub(super) struct ReaderSlot<T: JournalEvent> {
    pub(super) stage_id: StageId,
    pub(super) stage_key: StageKey,
    pub(super) reader: Box<dyn JournalReader<T>>,
}

/// A head event acquired from a reader but not yet delivered (FLOWIP-095d).
///
/// Classification happens at acquisition so the merge can treat authored EOFs
/// specially (tiebreak-only ordering, exhaustion at delivery) without
/// re-deriving it on the delivery side.
pub(super) struct HeldHead<T: JournalEvent> {
    pub(super) envelope: Arc<JournalRecord<T::Payload>>,
    pub(super) is_authored_eof: bool,
    pub(super) is_drain: bool,
    /// The announced generation when this head is a catch-up watermark
    /// (FLOWIP-120n). Classified at acquisition by arrival edge; the held
    /// head sorts at the reader's current generation, and delivery advances
    /// the reader to this announced value.
    pub(super) catch_up: Option<ReaderGeneration>,
}

impl<T: JournalEvent> HeldHead<T> {
    pub(super) fn ends_recorded_rows(&self) -> bool {
        self.is_authored_eof || self.catch_up.is_some()
    }
}

/// Comparison metadata for the canonical merge's currently selected candidate.
///
/// The join supervisor composes two subscriptions by comparing each side's
/// candidate with the same rule the subscription applies internally.
pub struct MergeCandidateMeta<'a> {
    /// The reader's current generation, the coarsest ordering axis
    /// (FLOWIP-120n): a recorded-generation head orders ahead of any live
    /// head, applied after causality and above the (ordinal, key) tiebreak.
    pub generation: ReaderGeneration,
    /// The tiebreak ordinal this delivery would take.
    pub ordinal: DeliveredOrdinal,
    /// The reader's stable tiebreak key (stage key, feed identity).
    pub key: &'a ReaderTiebreakKey,
    /// The head's envelope clock, for cross-subscription causality checks.
    pub vector_clock: &'a VectorClock,
    /// Authored EOFs are exempt from causality and order by tiebreak alone.
    pub is_authored_eof: bool,
}

impl FeedContractChain {
    fn new(metadata: SelectedFeedMetadata, chain: ContractChain) -> Self {
        Self {
            metadata,
            chain,
            last_contract_result_seq: SeqNo(0),
        }
    }
}

/// Subscription coordinator that manages reading from multiple upstream journals
///
/// This struct coordinates subscription mechanics without owning control flow.
/// The FSM retains control over when to poll, sleep, check contracts, and transition states.
pub struct UpstreamSubscription<T>
where
    T: JournalEvent,
{
    /// Delivery filter for subscription events.
    ///
    /// Stage runtime subscriptions should generally avoid delivering stage-local
    /// observability events (lifecycle/middleware metrics) to downstream handlers.
    /// Observability events are still persisted to journals for tail readers (e.g. the
    /// metrics aggregator), but delivering them to business-stage handlers forces
    /// downstream stages to "drain" huge volumes of non-transport events before EOF.
    delivery_filter: DeliveryFilter,

    /// Friendly owner label (stage or subsystem) for logging
    owner_label: String,

    /// Readers for each upstream journal
    readers: Vec<ReaderSlot<T>>,

    archived_stage_ids_by_current: HashMap<StageId, Vec<StageId>>,

    /// Selected Data event types by upstream reader stage.
    ///
    /// When populated for a reader, non-selected Data events are consumed from
    /// the journal but not delivered to the stage handler.
    selected_event_types_by_stage: HashMap<StageId, HashSet<obzenflow_core::EventDescriptor>>,

    /// Selected logical feed metadata by upstream reader stage.
    selected_feeds_by_stage: HashMap<StageId, Vec<SelectedFeedMetadata>>,

    /// Input-boundary activation stamps keyed by the physical upstream stage.
    composite_entries_by_stage: HashMap<StageId, Vec<CompositeEntrySpec>>,

    /// Per-reader count of selected Data authored by that reader's journal
    /// owner. Forwarded rows remain deliverable but are outside this contract
    /// population.
    selected_data_seq_by_reader: Vec<SeqNo>,

    /// Per-reader, per-event-type owner-authored selected Data count.
    selected_data_seq_by_reader_event_type: Vec<SelectedDataSeqByEventType>,

    /// Per-reader producer EOF evidence keyed by event type.
    advertised_writer_seq_by_reader_event_type: Vec<AdvertisedWriterSeqByEventType>,

    /// Subscription state (mechanism)
    state: SubscriptionState,

    /// Optional contract tracker (guarantees)
    contract_tracker: Option<ContractTracker>,

    /// Optional contract chains for each upstream reader (edge-scoped contracts).
    ///
    /// When `with_contracts` is used, this vector is sized to match `readers`
    /// and each entry holds the contract chain for the corresponding edge.
    contract_chains: Vec<Option<ContractChain>>,

    /// Optional selected-feed contract chains for each upstream reader.
    ///
    /// Multi-selected-feed readers need one transport contract state per
    /// logical feed so two selected event types between the same stage pair do
    /// not collapse into one aggregate contract chain.
    contract_feed_chains: Vec<Vec<FeedContractChain>>,

    /// Optional contract policies for each upstream reader (edge-scoped policies).
    ///
    /// When `with_contracts` is used, this vector is sized to match `readers`
    /// and each entry holds the policy stack for the corresponding edge.
    contract_policies: Vec<Option<ContractPolicyStack>>,
    advisory_contract_upstreams: std::collections::HashSet<obzenflow_core::StageId>,

    /// Flow-scoped typed control-state provider used by lifecycle policies.
    control_plane: Arc<dyn ControlPlaneProvider>,

    /// Last EOF accounting outcome (set when an EOF is observed)
    last_eof_outcome: Option<EofOutcome>,

    /// Upstream stage ID for the last event returned by `poll_next_with_state`.
    ///
    /// This is the topology-relevant upstream stage (the journal reader that produced
    /// the envelope), and MUST NOT be derived from `envelope.event.writer_id`, which
    /// can be intentionally preserved across stages for causal attribution.
    last_delivered_upstream_stage: Option<StageId>,

    /// Next stage-local data-input position to assign after transport filtering.
    next_stage_input_position: u64,

    /// Stage-local data-input position for the last delivered data event.
    last_delivered_stage_input_position: Option<StageInputPosition>,

    /// Reader-selection policy (FLOWIP-095d). Default is availability-driven
    /// round-robin; ordered stages get the canonical deterministic merge.
    reader_selection: types::ReaderSelectionPolicy,

    recorded: Option<RecordedOrder>,

    selection_base: u64,

    entered_generation: ReaderGeneration,

    /// One held head per reader, populated only under `CanonicalMerge`.
    held_heads: Vec<Option<HeldHead<T>>>,

    /// Per-reader count of delivered transport events (data and flow control),
    /// post-filter, on the delivery side. This is the canonical merge's
    /// tiebreak ordinal source and its entire checkpointable state.
    ///
    /// Deliberately distinct from `selected_data_seq_by_reader`, which counts
    /// Data events only and feeds selected-feed contract accounting.
    delivered_count_by_reader: Vec<DeliveredCount>,

    /// Per-reader stable tiebreak keys. Stage keys are the cross-run
    /// identity; `StageId` ULIDs are per-run and must never participate in
    /// ordering decisions.
    reader_tiebreak_keys: Vec<ReaderTiebreakKey>,

    /// Per-reader generation (FLOWIP-120n): 0 until the reader's catch-up
    /// watermark is delivered in merge order, then the announced value.
    /// Never advanced while a watermark head is merely held.
    generation_by_reader: Vec<ReaderGeneration>,

    promoted_generation: ReaderGeneration,

    /// Generation of the last delivered event: the delivering reader's
    /// generation at delivery time (a watermark delivers at the generation it
    /// closes, not the one it announces).
    last_delivered_generation: Option<ReaderGeneration>,

    /// Set when a canonical-merge poll returned no event because a quiet input
    /// blocked delivery; cleared on the next delivery.
    last_merge_wait: Option<types::MergeWaitState>,

    /// Cached winner index from `ensure_merge_candidate`, consumed by
    /// `take_merge_candidate`. Invalidated on delivery.
    merge_candidate_index: Option<usize>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeliveryFilter {
    /// Deliver all events to the caller (used by tail readers like the metrics aggregator).
    All,
    /// Deliver only transport-relevant events to the caller (used by stage runtime):
    /// - Data events
    /// - Flow control signals (EOF, drain, etc.)
    ///
    /// Observability events are consumed from journals but skipped (not returned).
    TransportOnly,
}

impl<T> UpstreamSubscription<T>
where
    T: JournalEvent + 'static,
{
    /// Stage ID of the upstream reader that produced the last delivered event.
    pub fn last_delivered_upstream_stage(&self) -> Option<StageId> {
        self.last_delivered_upstream_stage
    }

    pub(crate) fn upstream_key(&self, stage_id: StageId) -> Option<&StageKey> {
        self.readers
            .iter()
            .find(|slot| slot.stage_id == stage_id)
            .map(|slot| &slot.stage_key)
    }

    pub fn last_delivered_stage_input_position(&self) -> Option<StageInputPosition> {
        self.last_delivered_stage_input_position
    }

    /// The quiet-input wait that blocked the most recent canonical-merge poll,
    /// if any. Supervisors surface this through heartbeat liveness so a
    /// blocked-by-rule stage names the input it is waiting on.
    pub fn merge_wait(&self) -> Option<&types::MergeWaitState> {
        self.last_merge_wait.as_ref()
    }

    /// Per-reader delivered transport-event counts (the canonical merge's
    /// ordinal source and checkpointable state).
    pub fn delivered_counts(&self) -> &[DeliveredCount] {
        &self.delivered_count_by_reader
    }

    /// The reader-selection policy this subscription was built with.
    pub fn reader_selection(&self) -> types::ReaderSelectionPolicy {
        self.reader_selection
    }

    /// Whether this subscription records its own selections.
    pub fn records_order(&self) -> bool {
        self.recorded.is_some()
    }

    pub(crate) fn held_head_count(&self) -> usize {
        self.held_heads.iter().filter(|head| head.is_some()).count()
    }

    /// The stage's delivered count through this subscription, every delivered
    /// transport row plus the deliveries the stage made before it.
    pub fn delivered_total(&self) -> u64 {
        self.delivered_count_by_reader
            .iter()
            .fold(self.selection_base, |total, count| {
                total.saturating_add(count.0)
            })
    }

    /// Count deliveries the stage made before this subscription's first, such
    /// as a hydrating join's reference side, into its decision rows.
    pub fn set_selection_base(&mut self, base: u64) {
        self.selection_base = base;
    }

    fn has_selected_event_type_filter(&self, stage_id: StageId) -> bool {
        self.selected_event_types_by_stage
            .get(&stage_id)
            .is_some_and(|selected| !selected.is_empty())
    }

    fn data_event_selected_for_stage(
        &self,
        stage_id: StageId,
        descriptor: &obzenflow_core::EventDescriptor,
    ) -> bool {
        self.selected_event_types_by_stage
            .get(&stage_id)
            .filter(|selected| !selected.is_empty())
            .map(|selected| selected.contains(descriptor))
            .unwrap_or(true)
    }

    fn selected_writer_seq_from_eof_map(
        &self,
        stage_id: StageId,
        writer_seq_by_event_type: &BTreeMap<obzenflow_core::EventDescriptor, SeqNo>,
        complete: bool,
    ) -> Option<SeqNo> {
        let selected = self.selected_event_types_by_stage.get(&stage_id)?;
        if selected.is_empty() || !complete {
            return None;
        }

        let selected_total = writer_seq_by_event_type
            .iter()
            .filter(|(descriptor, _)| selected.contains(*descriptor))
            .fold(0u64, |total, (_, seq)| total.saturating_add(seq.0));
        Some(SeqNo(selected_total))
    }

    fn selected_feed_matches_event_type(
        feed: &SelectedFeedMetadata,
        descriptor: &obzenflow_core::EventDescriptor,
    ) -> bool {
        feed.matches_descriptor(descriptor)
    }

    fn selected_reader_seq_for_feed(
        &self,
        reader_index: usize,
        feed: &SelectedFeedMetadata,
    ) -> SeqNo {
        self.selected_data_seq_by_reader_event_type
            .get(reader_index)
            .map(|reader_by_type| reader_by_type.seq_for_feed(feed))
            .unwrap_or(SeqNo(0))
    }

    fn advertised_writer_seq_for_feed(
        &self,
        reader_index: usize,
        feed: &SelectedFeedMetadata,
    ) -> Option<SeqNo> {
        self.advertised_writer_seq_by_reader_event_type
            .get(reader_index)
            .and_then(|advertised_by_type| advertised_by_type.seq_for_feed(feed))
    }

    fn unique_selected_feed_for_stage(
        &self,
        stage_id: StageId,
    ) -> (
        Option<obzenflow_core::EventDescriptor>,
        Option<obzenflow_core::event::payloads::system_payload::SystemFeedRole>,
    ) {
        let Some(feeds) = self.selected_feeds_by_stage.get(&stage_id) else {
            return (None, None);
        };

        let mut unique_feeds = feeds.iter();
        let Some(first) = unique_feeds.next() else {
            return (None, None);
        };
        if unique_feeds.next().is_some() {
            return (None, None);
        }

        (Some(first.descriptor().clone()), first.system_feed_role())
    }

    /// Retrieve and clear the most recent EOF accounting outcome, if any.
    pub fn take_last_eof_outcome(&mut self) -> Option<EofOutcome> {
        self.last_eof_outcome.take()
    }

    /// Peek at the most recent EOF accounting outcome, if any.
    ///
    /// This does not clear the stored outcome. Supervisors typically call
    /// `take_last_eof_outcome()` once they have accepted the EOF decision.
    pub fn last_eof_outcome(&self) -> Option<&EofOutcome> {
        self.last_eof_outcome.as_ref()
    }

    /// Check if there are pending buffered events
    pub fn has_pending(&self) -> bool {
        self.state.has_pending()
    }

    /// Get the number of upstream readers
    pub fn upstream_count(&self) -> usize {
        self.readers.len()
    }

    /// Returns true when all upstream readers have reached terminal EOF.
    pub fn all_readers_eof(&self) -> bool {
        self.state.eof_count() == self.readers.len()
    }

    /// Returns true when all upstream readers are logically at EOF
    /// (either they have observed a terminal EOF event, or they were
    /// created at the journal tail position with no historical data
    /// to consume). This is used by tail-first observers like the
    /// metrics aggregator that seed from tail snapshots and do not
    /// need to re-observe historical EOF events.
    pub fn all_readers_logically_eof(&self) -> bool {
        self.state.logical_eof_count() == self.readers.len()
    }

    /// Generation of the last delivered event (FLOWIP-120n): the delivering
    /// reader's generation at delivery time.
    pub fn last_delivered_generation(&self) -> Option<ReaderGeneration> {
        self.last_delivered_generation
    }

    pub(crate) fn reader_frontier(&self) -> ReaderFrontier {
        (0..self.readers.len()).fold(ReaderFrontier::default(), |frontier, index| {
            let generation = self.effective_generation(index);
            let open = (!self.state.is_reader_eof(index)).then_some(generation);
            frontier.and(ReaderFrontier {
                least_open: open,
                highest: generation,
            })
        })
    }

    pub(crate) fn effective_generation(&self, index: usize) -> ReaderGeneration {
        self.generation_by_reader[index].max(self.promoted_generation)
    }

    pub(crate) fn promote_held_readers(&mut self, generation: ReaderGeneration) {
        self.promoted_generation = self.promoted_generation.max(generation);
    }

    /// Check if there are any upstream journals
    pub fn has_upstream(&self) -> bool {
        !self.readers.is_empty()
    }
}

/// Implement the SubscriptionPoller trait for UpstreamSubscription
#[async_trait::async_trait]
impl<T> SubscriptionPoller for UpstreamSubscription<T>
where
    T: JournalEvent + 'static,
{
    type Event = T;

    async fn poll_next(&mut self) -> PollResult<Self::Event> {
        // Delegate to the inherent poll_next implementation (avoid recursion)
        UpstreamSubscription::poll_next(self).await
    }

    fn name(&self) -> &str {
        "upstream_subscription"
    }
}

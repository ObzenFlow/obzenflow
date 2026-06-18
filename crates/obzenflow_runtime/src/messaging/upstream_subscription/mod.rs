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
mod types;

#[cfg(test)]
mod tests;

pub use super::subscription_poller::{PollResult, SubscriptionPoller};
use types::{AdvertisedWriterSeqByEventType, SelectedDataSeqByEventType};
pub use types::{
    ContractConfig, ContractStatus, ContractTracker, ContractsWiring, DeliveredCount,
    DeliveredOrdinal, EofOutcome, FeedIdentity, MergeCandidateStatus, MergeWaitState,
    ReaderProgress, ReaderSelectionPolicy, ReaderTiebreakKey, SelectedFeedMetadata,
    SelectedFeedRole, StageInputPosition, StageKey, SubscriptionState,
};

use crate::contracts::ContractChain;
use crate::control_plane::ControlPlaneProvider;
use crate::feed_plan::declared_event_type_matches;
use crate::messaging::upstream_subscription_policy::ContractPolicyStack;
use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{ChainEvent, EventEnvelope, JournalEvent, JournalWriterId};
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::{EventId, EventType, StageId};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use tokio::time::Instant;

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
    pub(super) envelope: EventEnvelope<T>,
    pub(super) is_authored_eof: bool,
    pub(super) is_drain: bool,
}

/// Comparison metadata for the canonical merge's currently selected candidate.
///
/// The join supervisor composes two subscriptions by comparing each side's
/// candidate with the same rule the subscription applies internally.
pub struct MergeCandidateMeta<'a> {
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

    /// Selected Data event types by upstream reader stage.
    ///
    /// When populated for a reader, non-selected Data events are consumed from
    /// the journal but not delivered to the stage handler.
    selected_event_types_by_stage: HashMap<StageId, HashSet<EventType>>,

    /// Selected logical feed metadata by upstream reader stage.
    selected_feeds_by_stage: HashMap<StageId, Vec<SelectedFeedMetadata>>,

    /// Per-reader count of Data events that survived the selected event-type
    /// filter and were delivered as stage inputs.
    selected_data_seq_by_reader: Vec<SeqNo>,

    /// Per-reader, per-event-type count of selected Data events delivered as
    /// stage inputs.
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

    /// Flow-scoped control middleware provider (breaker-aware contract hints).
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

    fn uses_receipt_watermark(&self) -> bool {
        self.contract_tracker
            .as_ref()
            .map(|tracker| tracker.receipt_aware_progress)
            .unwrap_or(false)
    }

    fn progress_seq(&self, progress: &ReaderProgress) -> SeqNo {
        if self.uses_receipt_watermark() {
            progress.receipted_seq
        } else {
            progress.reader_seq
        }
    }

    fn progress_last_event_id(&self, progress: &ReaderProgress) -> Option<EventId> {
        if self.uses_receipt_watermark() {
            progress.last_receipted_event_id
        } else {
            progress.last_event_id
        }
    }

    fn progress_vector_clock(&self, progress: &ReaderProgress) -> Option<VectorClock> {
        if self.uses_receipt_watermark() {
            progress.last_receipted_vector_clock.clone()
        } else {
            progress.last_vector_clock.clone()
        }
    }

    fn has_selected_event_type_filter(&self, stage_id: StageId) -> bool {
        self.selected_event_types_by_stage
            .get(&stage_id)
            .is_some_and(|selected| !selected.is_empty())
    }

    fn data_event_selected_for_stage(&self, stage_id: StageId, event_type: &str) -> bool {
        self.selected_event_types_by_stage
            .get(&stage_id)
            .filter(|selected| !selected.is_empty())
            .map(|selected| {
                selected.iter().any(|selected_event_type| {
                    declared_event_type_matches(selected_event_type.as_str(), event_type, None)
                })
            })
            .unwrap_or(true)
    }

    fn selected_writer_seq_for_reader(&self, reader_index: usize, stage_id: StageId) -> SeqNo {
        if self.has_selected_event_type_filter(stage_id) {
            self.selected_data_seq_by_reader
                .get(reader_index)
                .copied()
                .unwrap_or(SeqNo(0))
        } else {
            SeqNo(0)
        }
    }

    fn selected_writer_seq_from_eof_map(
        &self,
        stage_id: StageId,
        writer_seq_by_event_type: &BTreeMap<EventType, SeqNo>,
    ) -> Option<SeqNo> {
        let selected = self.selected_event_types_by_stage.get(&stage_id)?;
        if selected.is_empty() || writer_seq_by_event_type.is_empty() {
            return None;
        }

        let mut matched_event_types = HashSet::new();
        let mut selected_total = 0u64;
        for selected_event_type in selected {
            let Some((actual_event_type, seq)) =
                writer_seq_by_event_type
                    .iter()
                    .find(|(actual_event_type, _)| {
                        declared_event_type_matches(
                            selected_event_type.as_str(),
                            actual_event_type.as_str(),
                            None,
                        )
                    })
            else {
                continue;
            };
            if matched_event_types.insert(actual_event_type.clone()) {
                selected_total = selected_total.saturating_add(seq.0);
            }
        }
        Some(SeqNo(selected_total))
    }

    fn selected_feed_matches_event_type(feed: &SelectedFeedMetadata, event_type: &str) -> bool {
        feed.matches_event_type(event_type)
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
        Option<obzenflow_core::EventType>,
        Option<obzenflow_core::event::system_event::SystemFeedRole>,
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

        (Some(first.event_type().clone()), first.system_feed_role())
    }

    /// Bridge a sink delivery receipt write into the edge-scoped `ContractChain`
    /// for the upstream that delivered the consumed parent event.
    ///
    /// This is used by sink supervisors to feed `ChainEventContent::Delivery`
    /// events (written to the sink's own journal) into the same per-edge
    /// contract chain that observed the consumed input event via `on_read`.
    pub fn notify_delivery_receipt(&mut self, receipt: &ChainEvent, upstream_stage: StageId) {
        let Some(reader_stage) = self.contract_tracker.as_ref().and_then(|t| t.reader_stage) else {
            // Contracts are not configured for this subscription.
            return;
        };

        let Some(index) = self
            .readers
            .iter()
            .position(|slot| slot.stage_id == upstream_stage)
        else {
            tracing::warn!(
                owner = %self.owner_label,
                ?upstream_stage,
                "notify_delivery_receipt: no reader slot for upstream stage"
            );
            return;
        };

        let Some(chain_slot) = self.contract_chains.get_mut(index) else {
            return;
        };
        let Some(chain) = chain_slot.as_mut() else {
            return;
        };

        // The receipt is written by the sink (the reader stage for this subscription).
        // SeqNo(0) because receipt accounting does not use sequence numbers.
        chain.on_write(receipt, reader_stage, SeqNo(0));
    }

    /// Record a just-journalled delivery receipt and advance the receipt watermark if possible.
    ///
    /// This is called by sink supervisors after appending a `ChainEventContent::Delivery` event.
    /// It updates per-upstream `ReaderProgress` bookkeeping and returns the new receipt watermark
    /// triple when (and only when) receipts become contiguous.
    ///
    /// `DeliveryResult::Buffered` receipts are recorded for auditing but do **not** advance the
    /// receipt watermark or clear pending receipt metadata.
    pub fn record_delivery_receipt(
        &mut self,
        receipt: &ChainEvent,
        reader_progress: &mut [ReaderProgress],
    ) -> Option<(SeqNo, EventId, VectorClock)> {
        if !self.uses_receipt_watermark() {
            return None;
        }

        let Some(parent_id) = receipt.causality.parent_ids.first().copied() else {
            tracing::warn!(
                owner = %self.owner_label,
                receipt_id = %receipt.id,
                "record_delivery_receipt: receipt missing parent causality"
            );
            return None;
        };

        let Some(index) = reader_progress
            .iter()
            .enumerate()
            .find_map(|(index, progress)| progress.pending_receipts.get(&parent_id).map(|_| index))
        else {
            tracing::warn!(
                owner = %self.owner_label,
                receipt_id = %receipt.id,
                ?parent_id,
                "record_delivery_receipt: no pending receipt metadata for parent"
            );
            return None;
        };

        let upstream_stage = reader_progress[index].stage_id;
        self.notify_delivery_receipt(receipt, upstream_stage);

        let obzenflow_core::event::ChainEventContent::Delivery(payload) = &receipt.content else {
            tracing::warn!(
                owner = %self.owner_label,
                receipt_id = %receipt.id,
                ?upstream_stage,
                ?parent_id,
                "record_delivery_receipt: non-delivery event passed to receipt recorder"
            );
            return None;
        };

        if matches!(&payload.result, DeliveryResult::Buffered { .. }) {
            return None;
        }

        let previous_seq = reader_progress[index].receipted_seq;
        if reader_progress[index].mark_receipted(parent_id) {
            reader_progress[index].last_read_instant = Some(Instant::now());
            if reader_progress[index].receipted_seq != previous_seq {
                if let (Some(event_id), Some(vector_clock)) = (
                    reader_progress[index].last_receipted_event_id,
                    reader_progress[index].last_receipted_vector_clock.clone(),
                ) {
                    return Some((reader_progress[index].receipted_seq, event_id, vector_clock));
                }
            }
        } else {
            tracing::debug!(
                owner = %self.owner_label,
                ?upstream_stage,
                ?parent_id,
                "record_delivery_receipt: parent was not pending when receipt arrived"
            );
        }

        None
    }

    pub fn pending_receipt_envelope(
        &self,
        parent_event_id: EventId,
        reader_progress: &[ReaderProgress],
    ) -> Option<(StageId, EventEnvelope<ChainEvent>)> {
        reader_progress.iter().find_map(|progress| {
            progress
                .pending_receipts
                .get(&parent_event_id)
                .map(|pending| {
                    let envelope = EventEnvelope {
                        journal_writer_id: JournalWriterId::default(),
                        vector_clock: pending.vector_clock.clone(),
                        timestamp: chrono::Utc::now(),
                        event: pending.event.clone(),
                    };
                    (progress.stage_id, envelope)
                })
        })
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

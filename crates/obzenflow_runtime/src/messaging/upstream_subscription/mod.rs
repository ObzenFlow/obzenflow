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
pub use types::{
    ContractConfig, ContractStatus, ContractTracker, ContractsWiring, EofOutcome, ReaderProgress,
    SubscriptionState,
};

use crate::contracts::ContractChain;
use crate::messaging::upstream_subscription_policy::ContractPolicyStack;
use obzenflow_core::control_middleware::ControlMiddlewareProvider;
use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{ChainEvent, EventEnvelope, JournalEvent, JournalWriterId};
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::{EventId, StageId};
use std::sync::Arc;
use tokio::time::Instant;

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
    readers: Vec<(StageId, String, Box<dyn JournalReader<T>>)>,

    /// Subscription state (mechanism)
    state: SubscriptionState,

    /// Optional contract tracker (guarantees)
    contract_tracker: Option<ContractTracker>,

    /// Optional contract chains for each upstream reader (edge-scoped contracts).
    ///
    /// When `with_contracts` is used, this vector is sized to match `readers`
    /// and each entry holds the contract chain for the corresponding edge.
    contract_chains: Vec<Option<ContractChain>>,

    /// Optional contract policies for each upstream reader (edge-scoped policies).
    ///
    /// When `with_contracts` is used, this vector is sized to match `readers`
    /// and each entry holds the policy stack for the corresponding edge.
    contract_policies: Vec<Option<ContractPolicyStack>>,

    /// Flow-scoped control middleware provider (breaker-aware contract hints).
    control_middleware: Arc<dyn ControlMiddlewareProvider>,

    /// Last EOF accounting outcome (set when an EOF is observed)
    last_eof_outcome: Option<EofOutcome>,

    /// Upstream stage ID for the last event returned by `poll_next_with_state`.
    ///
    /// This is the topology-relevant upstream stage (the journal reader that produced
    /// the envelope), and MUST NOT be derived from `envelope.event.writer_id`, which
    /// can be intentionally preserved across stages for causal attribution.
    last_delivered_upstream_stage: Option<StageId>,
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
            .position(|(id, _, _)| *id == upstream_stage)
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

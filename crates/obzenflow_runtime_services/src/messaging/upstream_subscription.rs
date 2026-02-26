// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Subscription coordinator for reading from upstream journals
//!
//! This module provides a non-blocking subscription mechanism that coordinates
//! reading from multiple upstream journals without owning the event loop.
//!
//! Key design principles:
//! - Separates mechanism (how to read) from policy (when to read)
//! - Returns immediately with PollResult, never blocks or loops internally
//! - FSM owns control flow decisions (sleep, retry, transition)
//! - Contract tracking is separated from subscription mechanics

use obzenflow_core::control_middleware::{ControlMiddlewareProvider, NoControlMiddleware};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::system_event::{SystemEvent, SystemEventType};
use obzenflow_core::event::types::{
    Count, DurationMs, JournalIndex, JournalPath, SeqNo, ViolationCause as EventViolationCause,
};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, ChainEventFactory, ConsumptionFinalEventParams,
    ConsumptionProgressEventParams, JournalEvent,
};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::ContractResult;
use obzenflow_core::DeliveryContract;
use obzenflow_core::EventEnvelope;
use obzenflow_core::Result;
use obzenflow_core::StageId;
use obzenflow_core::TransportContract;
use obzenflow_core::ViolationCause;
use obzenflow_core::WriterId;
use std::any::Any;
use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use tokio::time::Instant;

use crate::contracts::ContractChain;
use crate::messaging::upstream_subscription_policy::{
    build_policy_stack_for_upstream, ContractPolicyStack, EdgeContext, EdgeContractDecision,
    PolicyHints,
};
use async_trait::async_trait;

// Import PollResult from the trait module
pub use super::subscription_poller::{PollResult, SubscriptionPoller};

/// Fallback reader used when a real journal reader cannot be created.
///
/// This reader behaves as an always-empty journal (EOF), allowing the
/// subscription machinery to continue operating without failing the FSM.
struct EmptyJournalReader<T: JournalEvent> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T: JournalEvent> EmptyJournalReader<T> {
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T> JournalReader<T> for EmptyJournalReader<T>
where
    T: JournalEvent + Send + Sync + 'static,
{
    async fn next(&mut self) -> std::result::Result<Option<EventEnvelope<T>>, JournalError> {
        Ok(None)
    }

    async fn skip(&mut self, _n: u64) -> std::result::Result<u64, JournalError> {
        Ok(0)
    }

    fn position(&self) -> u64 {
        0
    }

    fn is_at_end(&self) -> bool {
        true
    }
}

/// Status of contract checking
#[derive(Debug)]
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
    current_reader_index: usize,

    /// EOF tracking per reader (true terminal EOF observed via FlowControl::Eof)
    eof_received: Vec<bool>,

    /// True when a reader was created at the journal tail position.
    /// This is used for "logical EOF" checks where starting at tail
    /// should count as having no historical data to consume, while
    /// still allowing new events appended after subscription creation
    /// to be observed.
    baseline_at_tail: Vec<bool>,

    /// Buffering for events that need to be returned later
    /// (Currently unused but available for future ordering requirements)
    pending_events: VecDeque<EventEnvelope<ChainEvent>>,
}

impl SubscriptionState {
    fn new(reader_count: usize) -> Self {
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
#[derive(Debug)]
pub struct ReaderProgress {
    pub stage_id: StageId,

    /// Sequences and positions
    pub reader_seq: SeqNo,
    pub advertised_writer_seq: Option<SeqNo>,
    pub last_event_id: Option<obzenflow_core::EventId>,
    pub last_vector_clock: Option<VectorClock>,

    /// Progress timing
    pub last_progress_seq: SeqNo,
    pub last_progress_instant: Option<Instant>,

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
            advertised_writer_seq: None,
            last_event_id: None,
            last_vector_clock: None,
            last_progress_seq: SeqNo(0),
            last_progress_instant: None,
            stalled_since: None,
            consecutive_stall_checks: 0,
            final_emitted: false,
            contract_violated: false,
        }
    }
}

/// Contract tracking state - separated from subscription mechanics.
///
/// This struct owns configuration and journal handles, but **does not** own
/// per-reader contract state. Reader progress lives in FSM contexts and is
/// passed in as `&mut [ReaderProgress]` when contracts are evaluated.
pub struct ContractTracker {
    /// Configuration for contract behavior
    config: ContractConfig,

    /// References for emission (not owned)
    writer_id: WriterId,
    journal: Arc<dyn Journal<ChainEvent>>,
    system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
    reader_stage: Option<StageId>,

    /// Tracks output events written by this stage
    output_events_written: SeqNo,
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
    /// Create a new subscription from upstream journals
    pub async fn new_with_names(
        owner_label: &str,
        upstream_journals: &[(StageId, String, Arc<dyn Journal<T>>)],
    ) -> Result<Self> {
        // Delegate to the position-aware constructor with all starting
        // positions at 0 (from-beginning semantics).
        let start_positions = vec![0u64; upstream_journals.len()];
        Self::new_with_names_from_positions(owner_label, upstream_journals, &start_positions).await
    }

    /// Create a new subscription from upstream journals, starting each reader
    /// from an explicit position.
    ///
    /// This is used by the metrics aggregator (FLOWIP-059 Phase 6) to
    /// fast-forward readers to the tail while still seeding snapshot metrics
    /// from wide events. Other callers should generally prefer `new_with_names`.
    pub async fn new_with_names_from_positions(
        owner_label: &str,
        upstream_journals: &[(StageId, String, Arc<dyn Journal<T>>)],
        start_positions: &[u64],
    ) -> Result<Self> {
        if upstream_journals.len() != start_positions.len() {
            return Err(format!(
                "start_positions length {} does not match upstream_journals length {}",
                start_positions.len(),
                upstream_journals.len()
            )
            .into());
        }

        let mut readers = Vec::new();

        tracing::info!(
            "Creating subscription for {} upstream journals",
            upstream_journals.len()
        );

        tracing::info!(
            target: "flowip-080o",
            owner = owner_label,
            readers = ?upstream_journals
                .iter()
                .map(|(_id, name, journal)| format!("{} ({})", name, journal.id()))
                .collect::<Vec<_>>(),
            "UpstreamSubscription::new_with_names binding readers"
        );

        for ((stage_id, stage_name, journal), position) in
            upstream_journals.iter().zip(start_positions.iter())
        {
            // Get journal ID for debugging
            let journal_id = journal.id();
            tracing::info!(
                target: "flowip-080o",
                stage_id = ?stage_id,
                stage_name = stage_name,
                journal_id = ?journal_id,
                "Creating reader for upstream journal"
            );
            let reader_result = if *position == 0 {
                journal.reader().await
            } else {
                journal.reader_from(*position).await
            };

            let reader: Box<dyn JournalReader<T>> = match reader_result {
                Ok(reader) => reader,
                Err(e)
                    if crate::runtime_resource_limits::journal_error_is_too_many_open_files(&e) =>
                {
                    return Err(format!(
                        "Too many open files while creating reader for upstream journal (owner={owner_label}, stage_id={stage_id:?}, stage_name={stage_name}, journal_id={journal_id:?}). Increase RLIMIT_NOFILE / `ulimit -n` or reduce pipeline size (consider `OBZENFLOW_METRICS_ENABLED=false` for development). Underlying error: {e}"
                    )
                    .into());
                }
                Err(JournalError::Implementation { message, source }) => {
                    // Best-effort: log the failure and use an empty reader so the
                    // FSM can continue operating (upstream treated as having no events).
                    tracing::error!(
                        target: "flowip-080o",
                        stage_id = ?stage_id,
                        stage_name = stage_name,
                        journal_id = ?journal_id,
                        journal_error_message = %message,
                        journal_error_source = %source,
                        "Failed to create reader for upstream journal; using EmptyJournalReader (no events)"
                    );
                    Box::new(EmptyJournalReader::<T>::new()) as Box<dyn JournalReader<T>>
                }
                Err(e) => {
                    return Err(
                        format!("Failed to create reader for stage {stage_id:?}: {e}").into(),
                    );
                }
            };
            readers.push((*stage_id, stage_name.clone(), reader));
        }

        let state = SubscriptionState::new(readers.len());

        Ok(Self {
            delivery_filter: DeliveryFilter::All,
            owner_label: owner_label.to_string(),
            readers,
            state,
            contract_tracker: None,
            contract_chains: Vec::new(),
            contract_policies: Vec::new(),
            control_middleware: Arc::new(NoControlMiddleware),
            last_eof_outcome: None,
            last_delivered_upstream_stage: None,
        })
    }

    /// Stage ID of the upstream reader that produced the last delivered event.
    pub fn last_delivered_upstream_stage(&self) -> Option<StageId> {
        self.last_delivered_upstream_stage
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

    /// Configure this subscription to deliver only transport-relevant events to the caller.
    ///
    /// This is intended for *stage runtime* subscriptions where downstream stages should
    /// not be forced to process upstream observability events (e.g. middleware metrics)
    /// as part of normal draining / shutdown.
    pub fn transport_only(mut self) -> Self {
        self.delivery_filter = DeliveryFilter::TransportOnly;
        self
    }

    /// Create a subscription starting from explicit tail positions.
    ///
    /// Readers are treated as logically at EOF for historical data
    /// (baseline_at_tail = true) but will still observe any new
    /// events appended after subscription creation. This is used by
    /// tail-first observers like the metrics aggregator which seed
    /// from tail snapshots and do not need to re-observe historical
    /// EOF control events.
    pub async fn new_at_tail(
        owner_label: &str,
        upstream_journals: &[(StageId, String, Arc<dyn Journal<T>>)],
        tail_positions: &[u64],
    ) -> Result<Self> {
        let mut sub =
            Self::new_with_names_from_positions(owner_label, upstream_journals, tail_positions)
                .await?;

        // Only mark a reader as baseline-at-tail if we're actually skipping historical
        // events (i.e., the computed tail position is non-zero). For fresh/empty
        // journals, setting this baseline would incorrectly allow "logical EOF"
        // termination before new events are observed.
        let mut baseline_count = 0usize;
        for (idx, tail_position) in tail_positions.iter().take(sub.readers.len()).enumerate() {
            if *tail_position > 0 {
                sub.state.mark_reader_baseline_at_tail(idx);
                baseline_count += 1;
            }
        }

        tracing::info!(
            target: "flowip-059d",
            owner = owner_label,
            reader_count = sub.readers.len(),
            baseline_count = baseline_count,
            "Created tail-start upstream subscription"
        );

        Ok(sub)
    }

    /// Backwards-compatible constructor using stage IDs as names
    pub async fn new(upstream_journals: &[(StageId, Arc<dyn Journal<T>>)]) -> Result<Self> {
        let with_names: Vec<(StageId, String, Arc<dyn Journal<T>>)> = upstream_journals
            .iter()
            .map(|(id, journal)| (*id, format!("{id:?}"), journal.clone()))
            .collect();
        Self::new_with_names("unknown_owner", &with_names).await
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

    /// Enable contract emission for at-least-once delivery guarantees
    pub fn with_contracts(mut self, wiring: ContractsWiring) -> Self {
        let ContractsWiring {
            writer_id,
            contract_journal,
            config,
            system_journal,
            reader_stage,
            control_middleware,
            include_delivery_contract,
        } = wiring;

        self.control_middleware = control_middleware.clone();

        self.contract_tracker = Some(ContractTracker {
            config,
            writer_id,
            journal: contract_journal,
            system_journal,
            reader_stage,
            output_events_written: SeqNo(0),
        });

        // Initialize per-reader contract chains using the new Contract framework.
        // For 090c v1, we attach a TransportContract to each upstream edge.
        if !self.readers.is_empty() {
            self.contract_chains = self
                .readers
                .iter()
                .map(|_| {
                    let mut chain = ContractChain::new()
                        .with_contract(TransportContract::new())
                        .with_contract(obzenflow_core::SourceContract::new());
                    if include_delivery_contract {
                        chain = chain.with_contract(DeliveryContract::default());
                    }
                    Some(chain)
                })
                .collect();

            // Initialize per-reader policy stacks using the upstream stage IDs.
            self.contract_policies = self
                .readers
                .iter()
                .map(|(upstream_stage, _, _)| {
                    let stack =
                        build_policy_stack_for_upstream(*upstream_stage, &control_middleware);
                    Some(stack)
                })
                .collect();
        }

        self
    }

    /// Poll for the next event without blocking
    ///
    /// This method tries once through all readers and returns immediately.
    /// The caller (FSM) decides whether to retry, sleep, or transition states.
    /// `fsm_state` is the caller's current FSM state (for diagnostics).
    pub async fn poll_next_with_state(
        &mut self,
        fsm_state: &str,
        mut reader_progress: Option<&mut [ReaderProgress]>,
    ) -> PollResult<T> {
        // Check for buffered events first (future enhancement)
        // For now, we don't buffer events

        if self.readers.is_empty() {
            tracing::error!("poll_next() called with no upstream readers");
            return PollResult::Error(Box::new(io::Error::other("No upstream readers configured")));
        }

        let total_readers = self.readers.len();
        let starting_index = self.state.current_reader_index;

        tracing::debug!(
            target: "flowip-080o",
            owner = %self.owner_label,
            total_readers = total_readers,
            starting_index = starting_index,
            eof_status = ?self.state.eof_received,
            fsm_state = fsm_state,
            "subscription: poll_next() starting round-robin"
        );

        // Try each reader once in round-robin fashion
        let mut readers_checked = 0;
        loop {
            let current_index = self.state.current_reader_index;

            tracing::debug!(
                target: "flowip-080o",
                owner = %self.owner_label,
                current_index = current_index,
                starting_index = starting_index,
                readers_checked = readers_checked,
                is_eof = self.state.is_reader_eof(current_index),
                fsm_state = fsm_state,
                "subscription: checking reader in round-robin"
            );

            // Skip EOF'd readers
            if self.state.is_reader_eof(current_index) {
                tracing::trace!(
                    target: "flowip-080o",
                    owner = %self.owner_label,
                    current_index = current_index,
                    "subscription: skipping EOF'd reader"
                );
                self.state.next_reader_index();

                // Have we checked all readers?
                if self.state.current_reader_index == starting_index {
                    tracing::debug!(
                        target: "flowip-080o",
                        owner = %self.owner_label,
                        "subscription: back to starting_index after skipping EOF'd readers, breaking"
                    );
                    break;
                }
                continue;
            }

            let (stage_id, stage_name, reader) = &mut self.readers[current_index];
            let stage_id = *stage_id;
            let stage_name = stage_name.clone();

            tracing::debug!(
                target: "flowip-080o",
                owner = %self.owner_label,
                stage_id = ?stage_id,
                stage_name = %stage_name,
                current_index = current_index,
                fsm_state = fsm_state,
                "subscription: calling reader.next()"
            );

            match reader.next().await {
                Ok(Some(envelope)) => {
                    // This reader has observed post-baseline data; it is no longer
                    // logically at EOF due to a tail-start baseline.
                    self.state.clear_reader_baseline_at_tail(current_index);

                    // Stage runtime subscriptions should not deliver upstream observability
                    // events to handlers. We still consume them from journals so that the
                    // subscription can make progress toward transport events and EOF.
                    if matches!(self.delivery_filter, DeliveryFilter::TransportOnly) {
                        if let Some(chain_event) =
                            (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>()
                        {
                            if matches!(chain_event.content, ChainEventContent::Observability(_)) {
                                // Observability events are not part of the transport stream.
                                // Skip delivering to the caller, but keep progressing.
                                self.state.next_reader_index();
                                continue;
                            }
                        }
                    }

                    tracing::debug!(
                            target: "flowip-080o",
                            owner = %self.owner_label,
                            stage_id = ?stage_id,
                        stage_name = %stage_name,
                        current_index = current_index,
                        fsm_state = fsm_state,
                        event_type = %envelope.event.event_type_name(),
                        "subscription: reader.next() returned Some(event)"
                    );
                    // Detect terminal EOF for this *upstream stage* (drain should NOT be treated as EOF).
                    //
                    // Important subtlety (FLOWIP-080o):
                    // - Journals can contain EOFs authored by *other* stages that were merely
                    //   forwarded (e.g., a join forwarding source EOFs).
                    // - Contracts and EOF accounting for this subscription must only treat
                    //   EOFs authored by the upstream stage associated with this reader as
                    //   terminal. Otherwise, downstream stages can observe "early EOF"
                    //   and stop consuming before the real writer has finished.
                    let (is_eof, is_drain) = if let Some(chain_event) =
                        (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>()
                    {
                        match &chain_event.content {
                            ChainEventContent::FlowControl(FlowControlPayload::Eof {
                                writer_id,
                                ..
                            }) => {
                                // Only treat as terminal EOF when the EOF is authored by
                                // the upstream stage for this reader. If writer_id is
                                // missing, fall back to treating it as terminal.
                                let authored_by_upstream = match writer_id {
                                    Some(WriterId::Stage(eof_stage)) => *eof_stage == stage_id,
                                    Some(_) => false,
                                    None => true,
                                };
                                (authored_by_upstream, false)
                            }
                            ChainEventContent::FlowControl(FlowControlPayload::Drain) => {
                                (false, true)
                            }
                            _ => (false, false),
                        }
                    } else {
                        (false, false)
                    };

                    // Update legacy contract tracking state if enabled and
                    // contract progress has been provided by the owning FSM
                    // context. Reader progress lives in FSM contexts; we only
                    // update it here so that later contract checks can emit
                    // progress/final events based on the same state during
                    // replay.
                    let mut reader_seq_for_contracts: Option<SeqNo> = None;
                    if self.contract_tracker.is_some() {
                        if let Some(progress_slice) = reader_progress.as_deref_mut() {
                            if let Some(progress) = progress_slice.get_mut(current_index) {
                                // Update progress for data events
                                if let Some(chain_event) =
                                    (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>()
                                {
                                    if chain_event.is_data() {
                                        progress.reader_seq.0 += 1;
                                    }

                                    // Capture advertised positions from EOF authored by this upstream
                                    if let ChainEventContent::FlowControl(
                                        FlowControlPayload::Eof {
                                            writer_id,
                                            writer_seq,
                                            vector_clock,
                                            ..
                                        },
                                    ) = &chain_event.content
                                    {
                                        let authored_by_upstream = match writer_id {
                                            Some(WriterId::Stage(eof_stage)) => {
                                                *eof_stage == stage_id
                                            }
                                            Some(_) => false,
                                            None => true,
                                        };

                                        if authored_by_upstream {
                                            progress.advertised_writer_seq = *writer_seq;
                                            progress.last_vector_clock = vector_clock.clone();
                                        }
                                    }
                                }

                                progress.last_event_id = Some(*envelope.event.id());
                                if (&envelope.event as &dyn Any)
                                    .downcast_ref::<ChainEvent>()
                                    .is_some()
                                {
                                    progress.last_vector_clock =
                                        Some(envelope.vector_clock.clone());
                                }

                                // Capture the updated reader_seq for contract chains.
                                reader_seq_for_contracts = Some(progress.reader_seq);
                            }
                        }
                    }

                    // Feed the event into the ContractChain for this edge, if configured.
                    if let (Some(chain_event), Some(reader_stage)) = (
                        (&envelope.event as &dyn Any).downcast_ref::<ChainEvent>(),
                        self.contract_tracker.as_ref().and_then(|t| t.reader_stage),
                    ) {
                        if let Some(chain_slot) = self.contract_chains.get_mut(current_index) {
                            if let Some(chain) = chain_slot.as_mut() {
                                let reader_seq = reader_seq_for_contracts.unwrap_or(SeqNo(0));
                                // From the contract framework's perspective, the
                                // upstream writer is `stage_id` and the
                                // downstream reader is `reader_stage`.
                                chain.on_read(chain_event, reader_stage, reader_seq, stage_id);

                                // Also feed the event into the writer side of
                                // the transport contract via `on_write`. For
                                // transport, the writer-side count is derived
                                // from EOF writer_seq; `TransportContract`
                                // ignores non-EOF events on the write side.
                                chain.on_write(chain_event, stage_id, SeqNo(0));
                            }
                        }
                    }

                    tracing::debug!(
                        target: "flowip-080o",
                        owner = %self.owner_label,
                        stage_id = ?stage_id,
                        stage_name = %stage_name,
                        reader_index = current_index,
                        fsm_state = fsm_state,
                        event_type = %envelope.event.event_type_name(),
                        is_eof = is_eof,
                        "subscription: received event"
                    );

                    if is_eof {
                        self.state.mark_reader_eof(current_index);
                        let eof_count = self.state.eof_count();
                        let is_final = eof_count == total_readers;
                        let outcome = EofOutcome {
                            stage_id,
                            stage_name: stage_name.clone(),
                            reader_index: current_index,
                            eof_count,
                            total_readers,
                            is_final,
                        };
                        self.last_eof_outcome = Some(outcome.clone());
                        tracing::info!(
                            target: "flowip-080o",
                            owner = %self.owner_label,
                            stage_id = ?stage_id,
                            stage_name = %stage_name,
                            reader_index = current_index,
                            total_readers = total_readers,
                            eof_status = ?self.state.eof_received,
                            is_final = is_final,
                            "Received EOF from stage"
                        );
                    } else if is_drain {
                        tracing::info!(
                            target: "flowip-080o",
                            owner = %self.owner_label,
                            stage_id = ?stage_id,
                            stage_name = %stage_name,
                            reader_index = current_index,
                            total_readers = total_readers,
                            "Received drain from stage — continuing to consume until EOF"
                        );
                    }

                    tracing::debug!(
                        "Received event from stage {:?} (reader {}/{})",
                        stage_id,
                        current_index + 1,
                        total_readers
                    );

                    // Advance to next reader for fairness
                    self.state.next_reader_index();
                    self.last_delivered_upstream_stage = Some(stage_id);

                    return PollResult::Event(envelope);
                }
                Ok(None) => {
                    // No events from this reader right now
                    tracing::debug!(
                        target: "flowip-080o",
                        owner = %self.owner_label,
                        stage_id = ?stage_id,
                        current_index = current_index,
                        fsm_state = fsm_state,
                        "subscription: reader.next() returned None (no events available)"
                    );
                    tracing::debug!(
                        "No events from stage {:?} (reader {}/{})",
                        stage_id,
                        current_index + 1,
                        total_readers
                    );
                    readers_checked += 1;
                }
                Err(e) => {
                    tracing::error!(
                        target: "flowip-080o",
                        owner = %self.owner_label,
                        stage_id = ?stage_id,
                        current_index = current_index,
                        error = %e,
                        "subscription: reader.next() returned Error"
                    );
                    tracing::error!("Error reading from stage {:?}: {}", stage_id, e);
                    return PollResult::Error(Box::new(e));
                }
            }

            // Move to next reader
            let old_index = self.state.current_reader_index;
            self.state.next_reader_index();
            let new_index = self.state.current_reader_index;

            tracing::debug!(
                target: "flowip-080o",
                owner = %self.owner_label,
                old_index = old_index,
                new_index = new_index,
                starting_index = starting_index,
                fsm_state = fsm_state,
                "subscription: advanced to next reader"
            );

            // Have we tried all readers once?
            if self.state.current_reader_index == starting_index {
                tracing::debug!(
                    target: "flowip-080o",
                    owner = %self.owner_label,
                    readers_checked = readers_checked,
                    total_readers = total_readers,
                    "subscription: completed one round-robin cycle, breaking"
                );
                break;
            }
        }

        // No events available right now
        tracing::debug!(
            target: "flowip-080o",
            owner = %self.owner_label,
            eof_count = self.state.eof_count(),
            total_readers = self.state.eof_received.len(),
            readers_checked = readers_checked,
            "subscription: returning NoEvents - no events available in this poll cycle"
        );
        tracing::debug!(
            target: "flowip-080o",
            "No events available. EOF status: {}/{} readers complete",
            self.state.eof_count(),
            self.state.eof_received.len()
        );
        PollResult::NoEvents
    }

    /// Backwards-compatible wrapper when FSM state is not provided
    pub async fn poll_next(&mut self) -> PollResult<T> {
        self.poll_next_with_state("unknown", None).await
    }

    /// Check contracts and emit progress/stall/final events as needed.
    ///
    /// This is a separate method that the FSM calls when it decides
    /// contract checking is appropriate (e.g., after idle cycles).
    ///
    /// Per-reader contract state is supplied by the caller so that it can live
    /// inside FSM contexts rather than inside the subscription itself.
    pub async fn check_contracts(
        &mut self,
        reader_progress: &mut [ReaderProgress],
    ) -> Result<ContractStatus> {
        let tracker = match &mut self.contract_tracker {
            Some(t) => t,
            None => return Ok(ContractStatus::Healthy),
        };

        let now = Instant::now();
        let mut status = ContractStatus::Healthy;

        // Check each reader for progress/stalls
        for (index, progress) in reader_progress.iter_mut().enumerate() {
            if progress.final_emitted {
                continue;
            }

            // Check if we should emit progress
            let should_emit_progress = {
                let delta_events = progress
                    .reader_seq
                    .0
                    .saturating_sub(progress.last_progress_seq.0);
                let time_elapsed = progress
                    .last_progress_instant
                    .map(|t| now.duration_since(t).as_millis() as u64)
                    .unwrap_or(0);

                delta_events >= tracker.config.progress_min_events.0
                    || time_elapsed >= tracker.config.progress_max_interval.0
                    || self.state.is_reader_eof(index)
            };

            if should_emit_progress {
                // Emit progress event
                let stalled_duration = progress
                    .stalled_since
                    .map(|s| DurationMs(now.duration_since(s).as_millis() as u64));

                let progress_event = ChainEventFactory::consumption_progress_event(
                    tracker.writer_id,
                    ConsumptionProgressEventParams {
                        reader_seq: progress.reader_seq,
                        last_event_id: progress.last_event_id,
                        vector_clock: progress.last_vector_clock.clone(),
                        eof_seen: self.state.is_reader_eof(index),
                        reader_path: JournalPath(progress.stage_id.to_string()),
                        reader_index: JournalIndex(index as u64),
                        advertised_writer_seq: progress.advertised_writer_seq,
                        advertised_vector_clock: progress.last_vector_clock.clone(),
                        stalled_since: stalled_duration,
                    },
                );

                tracker
                    .journal
                    .append(progress_event, None)
                    .await
                    .map_err(|e| format!("Failed to append progress event: {e}"))?;

                progress.last_progress_seq = progress.reader_seq;
                progress.last_progress_instant = Some(now);
                progress.stalled_since = None;
                progress.consecutive_stall_checks = 0;

                status = ContractStatus::ProgressEmitted;

                // Check for EOF contract validation
                if self.state.is_reader_eof(index) && !progress.final_emitted {
                    let mut pass = true;
                    let mut failure_reason = None;

                    // Prefer the new contract framework (TransportContract via
                    // ContractChain) when available. This ensures that the
                    // same verification logic is used for both runtime gating
                    // and contract evidence. Policies are applied on top.
                    if let (Some(reader_stage), Some(chain_slot)) = (
                        tracker.reader_stage,
                        self.contract_chains.get(index).and_then(|c| c.as_ref()),
                    ) {
                        let results = chain_slot.verify_all(progress.stage_id, reader_stage);
                        let results_only: Vec<ContractResult> =
                            results.iter().map(|(_, r)| r.clone()).collect();

                        // Emit per-contract verification results to the system journal so that
                        // MetricsAggregator can derive contract metrics without interfering with
                        // pipeline gating (which uses ContractStatus + policies).
                        if let Some(system_journal) = &tracker.system_journal {
                            for (contract_name, result) in &results {
                                let (status_label, cause_label) = match result {
                                    ContractResult::Passed(_) => ("passed".to_string(), None),
                                    ContractResult::Failed(v) => {
                                        let cause = match &v.cause {
                                            ViolationCause::SeqDivergence { .. } => {
                                                "seq_divergence"
                                            }
                                            ViolationCause::ContentMismatch { .. } => {
                                                "content_mismatch"
                                            }
                                            ViolationCause::DeliveryMismatch { .. } => {
                                                "delivery_mismatch"
                                            }
                                            ViolationCause::AccountingMismatch { .. } => {
                                                "accounting_mismatch"
                                            }
                                            ViolationCause::Other(_) => "other",
                                        }
                                        .to_string();
                                        ("failed".to_string(), Some(cause))
                                    }
                                    ContractResult::Pending => ("pending".to_string(), None),
                                };

                                let result_event = SystemEvent::new(
                                    tracker.writer_id,
                                    SystemEventType::ContractResult {
                                        upstream: progress.stage_id,
                                        reader: reader_stage,
                                        contract_name: contract_name.clone(),
                                        status: status_label,
                                        cause: cause_label,
                                        reader_seq: Some(progress.reader_seq),
                                        advertised_writer_seq: progress.advertised_writer_seq,
                                    },
                                );
                                system_journal
                                    .append(result_event, None)
                                    .await
                                    .map_err(|e| {
                                        format!("Failed to append contract result event: {e}")
                                    })?;
                            }
                        }

                        let edge = EdgeContext {
                            upstream_stage: progress.stage_id,
                            downstream_stage: reader_stage,
                            advertised_writer_seq: progress.advertised_writer_seq,
                            reader_seq: progress.reader_seq,
                        };

                        let cb_info = self
                            .control_middleware
                            .circuit_breaker_contract_info(&progress.stage_id);
                        let hints = PolicyHints {
                            breaker_mode: cb_info.map(|i| i.mode),
                            has_opened_since_registration: cb_info
                                .map(|i| i.has_opened_since_registration)
                                .unwrap_or(false),
                            has_fallback_configured: cb_info
                                .map(|i| i.has_fallback_configured)
                                .unwrap_or(false),
                        };

                        if let Some(policy_stack) =
                            self.contract_policies.get(index).and_then(|p| p.as_ref())
                        {
                            let raw_failure: Option<(String, ViolationCause)> =
                                results.iter().find_map(|(name, r)| match r {
                                    ContractResult::Failed(v) => {
                                        Some((name.clone(), v.cause.clone()))
                                    }
                                    _ => None,
                                });

                            let decision = policy_stack.decide(&results_only, &edge, &hints);

                            match decision {
                                EdgeContractDecision::Pass => {
                                    pass = true;
                                    failure_reason = None;

                                    // If raw contracts reported a failure but policies
                                    // overrode it to Pass, emit an override system event.
                                    if let (
                                        Some(system_journal),
                                        Some(reader_stage),
                                        Some((contract_name, cause)),
                                    ) =
                                        (&tracker.system_journal, tracker.reader_stage, raw_failure)
                                    {
                                        let override_event = SystemEvent::new(
                                            tracker.writer_id,
                                            SystemEventType::ContractOverrideByPolicy {
                                                upstream: progress.stage_id,
                                                reader: reader_stage,
                                                contract_name,
                                                original_cause: cause,
                                                policy: "breaker_aware".to_string(),
                                            },
                                        );
                                        system_journal.append(override_event, None).await.map_err(
                                            |e| {
                                                format!(
                                                    "Failed to append contract override event: {e}"
                                                )
                                            },
                                        )?;
                                    }
                                }
                                EdgeContractDecision::Fail(cause) => {
                                    pass = false;
                                    failure_reason = Some(cause.clone());
                                    status = ContractStatus::Violated {
                                        upstream: progress.stage_id,
                                        cause: cause.clone(),
                                    };

                                    // For transport SeqDivergence, emit a gap event
                                    // when we are missing events.
                                    if let EventViolationCause::SeqDivergence {
                                        advertised: Some(advertised),
                                        reader,
                                    } = cause
                                    {
                                        if advertised.0 > reader.0 {
                                            let gap_event =
                                                ChainEventFactory::consumption_gap_event(
                                                    tracker.writer_id,
                                                    SeqNo(reader.0 + 1),
                                                    advertised,
                                                    progress.stage_id,
                                                );
                                            tracker.journal.append(gap_event, None).await.map_err(
                                                |e| format!("Failed to append gap event: {e}"),
                                            )?;
                                        }
                                    }
                                }
                            }
                        }
                    } else if let Some(advertised) = progress.advertised_writer_seq {
                        // Legacy fallback: compare advertised vs reader seq.
                        if advertised.0 != progress.reader_seq.0 {
                            pass = false;
                            let cause = EventViolationCause::SeqDivergence {
                                advertised: Some(advertised),
                                reader: progress.reader_seq,
                            };
                            failure_reason = Some(cause.clone());

                            if advertised.0 > progress.reader_seq.0 {
                                // Missing events
                                let gap_event = ChainEventFactory::consumption_gap_event(
                                    tracker.writer_id,
                                    SeqNo(progress.reader_seq.0 + 1),
                                    advertised,
                                    progress.stage_id,
                                );
                                tracker
                                    .journal
                                    .append(gap_event, None)
                                    .await
                                    .map_err(|e| format!("Failed to append gap event: {e}"))?;
                            }

                            status = ContractStatus::Violated {
                                upstream: progress.stage_id,
                                cause,
                            };
                        }
                    }

                    // Capture reason for downstream system status before moving it into events
                    let status_reason = failure_reason.clone();

                    // Emit an explicit at-least-once violation event when we detect
                    // a SeqDivergence (advertised > reader). This complements the
                    // generic ContractStatus system event and makes at-least-once
                    // violations first-class in the data journal for observability.
                    if !pass {
                        if let Some(EventViolationCause::SeqDivergence { advertised, reader }) =
                            failure_reason.clone()
                        {
                            let violation_event = ChainEventFactory::at_least_once_violation_event(
                                tracker.writer_id,
                                progress.stage_id,
                                EventViolationCause::SeqDivergence { advertised, reader },
                                progress.reader_seq,
                                progress.advertised_writer_seq,
                            );

                            tracker
                                .journal
                                .append(violation_event, None)
                                .await
                                .map_err(|e| {
                                    format!("Failed to append at_least_once_violation event: {e}")
                                })?;
                        }
                    }

                    // Emit final event
                    let final_event = ChainEventFactory::consumption_final_event(
                        tracker.writer_id,
                        ConsumptionFinalEventParams {
                            pass,
                            consumed_count: Count(progress.reader_seq.0),
                            expected_count: None,
                            eof_seen: true,
                            last_event_id: progress.last_event_id,
                            reader_seq: progress.reader_seq,
                            advertised_writer_seq: progress.advertised_writer_seq,
                            advertised_vector_clock: progress.last_vector_clock.clone(),
                            failure_reason,
                        },
                    );

                    tracker
                        .journal
                        .append(final_event, None)
                        .await
                        .map_err(|e| format!("Failed to append final event: {e}"))?;

                    // Emit contract status to system journal (if available)
                    if let (Some(system_journal), Some(reader_stage)) =
                        (&tracker.system_journal, tracker.reader_stage)
                    {
                        let status_event = SystemEvent::new(
                            tracker.writer_id,
                            SystemEventType::ContractStatus {
                                upstream: progress.stage_id,
                                reader: reader_stage,
                                pass,
                                reader_seq: Some(progress.reader_seq),
                                advertised_writer_seq: progress.advertised_writer_seq,
                                reason: status_reason,
                            },
                        );
                        system_journal
                            .append(status_event, None)
                            .await
                            .map_err(|e| format!("Failed to append contract status: {e}"))?;
                    }

                    progress.final_emitted = true;
                    progress.contract_violated = !pass;
                }
            } else {
                // Check for stalls
                if progress.last_progress_instant.is_none() {
                    progress.last_progress_instant = Some(now);
                    continue;
                }

                let elapsed = now
                    .duration_since(progress.last_progress_instant.unwrap())
                    .as_millis() as u64;

                if elapsed >= tracker.config.stall_threshold.0 {
                    progress.consecutive_stall_checks += 1;

                    if progress.consecutive_stall_checks >= tracker.config.stall_checks_before_emit
                        && progress.stalled_since.is_none()
                    {
                        progress.stalled_since = progress.last_progress_instant;

                        let stalled_duration = DurationMs(
                            now.duration_since(progress.last_progress_instant.unwrap())
                                .as_millis() as u64,
                        );

                        let stalled_event = ChainEventFactory::reader_stalled_event(
                            tracker.writer_id,
                            progress.stage_id,
                            stalled_duration,
                        );

                        tracker
                            .journal
                            .append(stalled_event, None)
                            .await
                            .map_err(|e| format!("Failed to append stalled event: {e}"))?;

                        // Emit stalled contract status fail to system journal (if available)
                        if let (Some(system_journal), Some(reader_stage)) =
                            (&tracker.system_journal, tracker.reader_stage)
                        {
                            let status_event = SystemEvent::new(
                                tracker.writer_id,
                                SystemEventType::ContractStatus {
                                    upstream: progress.stage_id,
                                    reader: reader_stage,
                                    pass: false,
                                    reader_seq: Some(progress.reader_seq),
                                    advertised_writer_seq: progress.advertised_writer_seq,
                                    reason: Some(EventViolationCause::Other(
                                        "reader_stalled".into(),
                                    )),
                                },
                            );
                            system_journal
                                .append(status_event, None)
                                .await
                                .map_err(|e| {
                                    format!("Failed to append stalled contract status: {e}")
                                })?;
                        }

                        status = ContractStatus::Stalled(progress.stage_id);
                        progress.contract_violated = true;
                    }
                } else {
                    progress.consecutive_stall_checks = 0;
                }
            }
        }

        Ok(status)
    }

    /// Track that this stage has emitted an output event
    pub fn track_output_event(&mut self) {
        if let Some(tracker) = &mut self.contract_tracker {
            tracker.output_events_written.0 += 1;
            tracing::trace!(
                "Tracked output event, total: {}",
                tracker.output_events_written.0
            );
        }
    }

    // Query methods for FSM decision-making

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

    /// Hint to FSM about whether contract check might be useful.
    ///
    /// Uses per-reader timestamps from FSM-owned contract state to decide if
    /// enough time has passed since the last check.
    pub fn should_check_contracts(&self, reader_progress: &[ReaderProgress]) -> bool {
        if let Some(tracker) = &self.contract_tracker {
            let now = Instant::now();

            // Check if enough time has passed since last check
            for progress in reader_progress {
                if let Some(last) = progress.last_progress_instant {
                    let elapsed = now.duration_since(last).as_millis() as u64;
                    if elapsed >= tracker.config.progress_max_interval.0 / 2 {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Convenience method that combines `should_check_contracts` and
    /// `check_contracts` into a single call.
    ///
    /// Returns `None` when the check interval has not elapsed yet (no work
    /// done). Returns `Some(result)` when a check was performed.
    pub async fn maybe_check_contracts(
        &mut self,
        reader_progress: &mut [ReaderProgress],
    ) -> Option<Result<ContractStatus>> {
        if self.should_check_contracts(reader_progress) {
            Some(self.check_contracts(reader_progress).await)
        } else {
            None
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::control_middleware::{CircuitBreakerSnapshotter, RateLimiterSnapshotter};
    use obzenflow_core::event::event_envelope::EventEnvelope;
    use obzenflow_core::event::identity::JournalWriterId;
    use obzenflow_core::event::journal_event::JournalEvent;
    use obzenflow_core::event::system_event::SystemEvent;
    use obzenflow_core::id::JournalId;
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::journal_reader::JournalReader;
    use obzenflow_core::journal::Journal;
    use obzenflow_core::{CircuitBreakerContractInfo, CircuitBreakerContractMode};
    use serde_json::json;
    use std::collections::HashMap;
    use std::io;
    use std::sync::atomic::AtomicU8;
    use std::sync::{Arc, Mutex, RwLock};

    #[derive(Debug, Default)]
    struct TestControlMiddlewareProvider {
        breaker_contracts: RwLock<HashMap<StageId, CircuitBreakerContractInfo>>,
    }

    impl TestControlMiddlewareProvider {
        fn new() -> Self {
            Self::default()
        }

        fn register_stage_mode(
            &self,
            stage_id: StageId,
            mode: CircuitBreakerContractMode,
            has_fallback: bool,
        ) {
            let mut reg = self
                .breaker_contracts
                .write()
                .expect("TestControlMiddlewareProvider: poisoned lock");
            reg.insert(
                stage_id,
                CircuitBreakerContractInfo {
                    mode,
                    has_opened_since_registration: false,
                    has_fallback_configured: has_fallback,
                },
            );
        }
    }

    impl ControlMiddlewareProvider for TestControlMiddlewareProvider {
        fn circuit_breaker_snapshotter(
            &self,
            _: &StageId,
        ) -> Option<Arc<CircuitBreakerSnapshotter>> {
            None
        }

        fn rate_limiter_snapshotter(&self, _: &StageId) -> Option<Arc<RateLimiterSnapshotter>> {
            None
        }

        fn circuit_breaker_state(&self, _: &StageId) -> Option<Arc<AtomicU8>> {
            None
        }

        fn circuit_breaker_contract_info(
            &self,
            stage_id: &StageId,
        ) -> Option<CircuitBreakerContractInfo> {
            self.breaker_contracts
                .read()
                .expect("TestControlMiddlewareProvider: poisoned lock")
                .get(stage_id)
                .copied()
        }

        fn mark_circuit_breaker_opened(&self, stage_id: &StageId) {
            let mut reg = self
                .breaker_contracts
                .write()
                .expect("TestControlMiddlewareProvider: poisoned lock");
            if let Some(info) = reg.get_mut(stage_id) {
                info.has_opened_since_registration = true;
            }
        }
    }

    /// Minimal in-memory journal implementation for tests.
    struct TestJournal<T: JournalEvent> {
        id: JournalId,
        owner: Option<JournalOwner>,
        events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    }

    impl<T: JournalEvent> TestJournal<T> {
        fn new(owner: JournalOwner) -> Self {
            Self {
                id: JournalId::new(),
                owner: Some(owner),
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    struct TestJournalReader<T: JournalEvent> {
        events: Vec<EventEnvelope<T>>,
        pos: usize,
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> Journal<T> for TestJournal<T> {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&JournalOwner> {
            self.owner.as_ref()
        }

        async fn append(
            &self,
            event: T,
            _parent: Option<&EventEnvelope<T>>,
        ) -> std::result::Result<EventEnvelope<T>, JournalError> {
            let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
            let mut guard = self.events.lock().unwrap();
            guard.push(envelope.clone());
            Ok(envelope)
        }

        async fn read_causally_ordered(
            &self,
        ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(guard.clone())
        }

        async fn read_causally_after(
            &self,
            _after_event_id: &obzenflow_core::EventId,
        ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &obzenflow_core::EventId,
        ) -> std::result::Result<Option<EventEnvelope<T>>, JournalError> {
            Ok(None)
        }

        async fn reader(&self) -> std::result::Result<Box<dyn JournalReader<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(Box::new(TestJournalReader {
                events: guard.clone(),
                pos: 0,
            }))
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> std::result::Result<Box<dyn JournalReader<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(Box::new(TestJournalReader {
                events: guard.clone(),
                pos: position as usize,
            }))
        }

        async fn read_last_n(
            &self,
            count: usize,
        ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            let len = guard.len();
            let start = len.saturating_sub(count);
            // Return most recent first, matching Journal contract.
            Ok(guard[start..].iter().rev().cloned().collect())
        }
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> JournalReader<T> for TestJournalReader<T> {
        async fn next(&mut self) -> std::result::Result<Option<EventEnvelope<T>>, JournalError> {
            if self.pos >= self.events.len() {
                Ok(None)
            } else {
                let envelope = self.events.get(self.pos).cloned();
                self.pos += 1;
                Ok(envelope)
            }
        }

        async fn skip(&mut self, n: u64) -> std::result::Result<u64, JournalError> {
            let start = self.pos as u64;
            self.pos = (self.pos as u64 + n) as usize;
            Ok(self.pos as u64 - start)
        }

        fn position(&self) -> u64 {
            self.pos as u64
        }

        fn is_at_end(&self) -> bool {
            self.pos >= self.events.len()
        }
    }

    #[cfg(unix)]
    struct EmfileJournal<T: JournalEvent> {
        id: JournalId,
        owner: Option<JournalOwner>,
        _phantom: std::marker::PhantomData<T>,
    }

    #[cfg(unix)]
    impl<T: JournalEvent> EmfileJournal<T> {
        fn new(owner: JournalOwner) -> Self {
            Self {
                id: JournalId::new(),
                owner: Some(owner),
                _phantom: std::marker::PhantomData,
            }
        }
    }

    #[cfg(unix)]
    #[async_trait]
    impl<T: JournalEvent + 'static> Journal<T> for EmfileJournal<T> {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&JournalOwner> {
            self.owner.as_ref()
        }

        async fn append(
            &self,
            _event: T,
            _parent: Option<&EventEnvelope<T>>,
        ) -> std::result::Result<EventEnvelope<T>, JournalError> {
            Err(JournalError::Implementation {
                message: "append not supported".to_string(),
                source: "append not supported".into(),
            })
        }

        async fn read_causally_ordered(
            &self,
        ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_causally_after(
            &self,
            _after_event_id: &obzenflow_core::EventId,
        ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &obzenflow_core::EventId,
        ) -> std::result::Result<Option<EventEnvelope<T>>, JournalError> {
            Ok(None)
        }

        async fn reader(&self) -> std::result::Result<Box<dyn JournalReader<T>>, JournalError> {
            Err(JournalError::Implementation {
                message: "open failed".to_string(),
                source: Box::new(io::Error::from_raw_os_error(libc::EMFILE)),
            })
        }

        async fn reader_from(
            &self,
            _position: u64,
        ) -> std::result::Result<Box<dyn JournalReader<T>>, JournalError> {
            self.reader().await
        }

        async fn read_last_n(
            &self,
            _count: usize,
        ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    #[cfg(unix)]
    async fn fails_fast_on_too_many_open_files() {
        let upstream_stage = StageId::new();
        let upstream_owner = JournalOwner::stage(upstream_stage);

        let upstream_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(EmfileJournal::new(upstream_owner));

        let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

        let err = UpstreamSubscription::<ChainEvent>::new_with_names_from_positions(
            "downstream",
            &upstreams,
            &[0u64],
        )
        .await
        .err()
        .expect("Expected Too many open files error")
        .to_string();

        assert!(err.contains("Too many open files"));
    }

    async fn build_upstream_with_seq_divergence(
        control_middleware: Arc<dyn ControlMiddlewareProvider>,
    ) -> (
        UpstreamSubscription<ChainEvent>,
        Arc<dyn Journal<ChainEvent>>,
        Arc<dyn Journal<SystemEvent>>,
        StageId,
        StageId,
    ) {
        let upstream_stage = StageId::new();
        let reader_stage = StageId::new();

        let upstream_owner = JournalOwner::stage(upstream_stage);
        let reader_owner = JournalOwner::stage(reader_stage);

        let upstream_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(TestJournal::new(upstream_owner));
        let contract_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(TestJournal::new(reader_owner.clone()));
        let system_journal: Arc<dyn Journal<SystemEvent>> =
            Arc::new(TestJournal::new(reader_owner));

        // One data event followed by EOF that advertises more events than read.
        let writer_id = WriterId::Stage(upstream_stage);
        let data_event = ChainEventFactory::data_event(writer_id, "test.event", json!({}));
        upstream_journal.append(data_event, None).await.unwrap();

        let mut eof_event = ChainEventFactory::eof_event(writer_id, true);
        if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
            writer_id: writer_id_field,
            writer_seq,
            ..
        }) = &mut eof_event.content
        {
            *writer_id_field = Some(writer_id);
            *writer_seq = Some(SeqNo(3));
        }
        upstream_journal.append(eof_event, None).await.unwrap();

        let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

        let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
            .await
            .unwrap();

        let contract_config = ContractConfig::default();
        let writer_id_for_contracts = WriterId::from(reader_stage);
        subscription = subscription.with_contracts(ContractsWiring {
            writer_id: writer_id_for_contracts,
            contract_journal: contract_journal.clone(),
            config: contract_config,
            system_journal: Some(system_journal.clone()),
            reader_stage: Some(reader_stage),
            control_middleware,
            include_delivery_contract: false,
        });

        (
            subscription,
            contract_journal,
            system_journal,
            upstream_stage,
            reader_stage,
        )
    }

    async fn drive_subscription_to_eof(
        subscription: &mut UpstreamSubscription<ChainEvent>,
        reader_progress: &mut [ReaderProgress],
    ) {
        loop {
            match subscription
                .poll_next_with_state("test_fsm", Some(reader_progress))
                .await
            {
                PollResult::Event(_env) => continue,
                PollResult::NoEvents => break,
                PollResult::Error(e) => {
                    panic!("poll_next_with_state returned error: {e:?}");
                }
            }
        }
    }

    #[tokio::test]
    async fn strict_mode_produces_seq_divergence_and_gap_event() {
        let (mut subscription, contract_journal, system_journal, upstream_stage, reader_stage) =
            build_upstream_with_seq_divergence(Arc::new(NoControlMiddleware)).await;

        let mut reader_progress = [ReaderProgress::new(upstream_stage)];
        drive_subscription_to_eof(&mut subscription, &mut reader_progress).await;

        let status = subscription
            .check_contracts(&mut reader_progress)
            .await
            .unwrap();

        match status {
            ContractStatus::Violated { upstream, cause } => {
                assert_eq!(upstream, upstream_stage);
                match cause {
                    EventViolationCause::SeqDivergence { advertised, reader } => {
                        assert_eq!(advertised, Some(SeqNo(3)));
                        assert_eq!(reader, SeqNo(1));
                    }
                    other => panic!("expected SeqDivergence cause, got {other:?}"),
                }
            }
            other => panic!("expected violated status, got {other:?}"),
        }

        let events = contract_journal.read_causally_ordered().await.unwrap();

        let mut final_found = false;
        let mut gap_found = false;
        let mut violation_found = false;

        for env in &events {
            match &env.event.content {
                ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal {
                    pass,
                    reader_seq,
                    advertised_writer_seq,
                    failure_reason,
                    ..
                }) => {
                    final_found = true;
                    assert!(!pass);
                    assert_eq!(*reader_seq, SeqNo(1));
                    assert_eq!(*advertised_writer_seq, Some(SeqNo(3)));
                    match failure_reason {
                        Some(EventViolationCause::SeqDivergence { advertised, reader }) => {
                            assert_eq!(*advertised, Some(SeqNo(3)));
                            assert_eq!(*reader, SeqNo(1));
                        }
                        other => panic!("expected SeqDivergence failure_reason, got {other:?}"),
                    }
                }
                ChainEventContent::FlowControl(FlowControlPayload::ConsumptionGap {
                    from_seq,
                    to_seq,
                    upstream,
                }) => {
                    gap_found = true;
                    assert_eq!(*from_seq, SeqNo(2));
                    assert_eq!(*to_seq, SeqNo(3));
                    assert_eq!(*upstream, upstream_stage);
                }
                ChainEventContent::FlowControl(FlowControlPayload::AtLeastOnceViolation {
                    upstream,
                    reason,
                    reader_seq,
                    advertised_writer_seq,
                }) => {
                    violation_found = true;
                    assert_eq!(*upstream, upstream_stage);
                    assert_eq!(*reader_seq, SeqNo(1));
                    assert_eq!(*advertised_writer_seq, Some(SeqNo(3)));
                    match reason {
                        EventViolationCause::SeqDivergence { advertised, reader } => {
                            assert_eq!(*advertised, Some(SeqNo(3)));
                            assert_eq!(*reader, SeqNo(1));
                        }
                        other => panic!(
                            "expected SeqDivergence reason in AtLeastOnceViolation, got {other:?}"
                        ),
                    }
                }
                _ => {}
            }
        }

        assert!(final_found, "expected a ConsumptionFinal event");
        assert!(gap_found, "expected a ConsumptionGap event");
        assert!(
            violation_found,
            "expected an AtLeastOnceViolation event for SeqDivergence"
        );

        let system_events = system_journal.read_causally_ordered().await.unwrap();
        let mut status_found = false;
        let mut override_found = false;

        for env in &system_events {
            match &env.event.event {
                SystemEventType::ContractStatus {
                    upstream,
                    reader,
                    pass,
                    reason,
                    ..
                } => {
                    status_found = true;
                    assert_eq!(*upstream, upstream_stage);
                    assert_eq!(*reader, reader_stage);
                    assert!(!pass);
                    match reason {
                        Some(EventViolationCause::SeqDivergence { .. }) => {}
                        other => {
                            panic!("expected SeqDivergence reason in ContractStatus, got {other:?}")
                        }
                    }
                }
                SystemEventType::ContractOverrideByPolicy { .. } => {
                    override_found = true;
                }
                _ => {}
            }
        }

        assert!(status_found, "expected ContractStatus system event");
        assert!(
            !override_found,
            "did not expect ContractOverrideByPolicy in strict mode"
        );
    }

    #[tokio::test]
    async fn breaker_aware_mode_overrides_seq_divergence_and_emits_override_event() {
        let control_middleware = Arc::new(TestControlMiddlewareProvider::new());
        let (mut subscription, contract_journal, system_journal, upstream_stage, reader_stage) =
            build_upstream_with_seq_divergence(control_middleware.clone()).await;

        // Register breaker-aware contract mode with fallback configured and mark
        // that the breaker has opened at least once. This makes the policy
        // layer eligible to override pure SeqDivergence failures.
        control_middleware.register_stage_mode(
            upstream_stage,
            CircuitBreakerContractMode::BreakerAware,
            true,
        );
        control_middleware.mark_circuit_breaker_opened(&upstream_stage);

        // Rebuild the policy stack so that it includes BreakerAwarePolicy.
        let control_provider: Arc<dyn ControlMiddlewareProvider> = control_middleware.clone();
        subscription.contract_policies = subscription
            .readers
            .iter()
            .map(|(upstream, _name, _reader)| {
                let stack = build_policy_stack_for_upstream(*upstream, &control_provider);
                Some(stack)
            })
            .collect();

        let mut reader_progress = [ReaderProgress::new(upstream_stage)];
        drive_subscription_to_eof(&mut subscription, &mut reader_progress).await;

        let status = subscription
            .check_contracts(&mut reader_progress)
            .await
            .unwrap();

        // With breaker-aware contracts, the SeqDivergence should be treated as pass.
        match status {
            ContractStatus::ProgressEmitted | ContractStatus::Healthy => {}
            other => panic!("expected non-violated status under BreakerAware, got {other:?}"),
        }

        let events = contract_journal.read_causally_ordered().await.unwrap();

        let mut final_pass_found = false;
        let mut gap_found = false;

        for env in &events {
            match &env.event.content {
                ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal {
                    pass,
                    reader_seq,
                    advertised_writer_seq,
                    failure_reason,
                    ..
                }) => {
                    final_pass_found = true;
                    assert!(*pass, "expected pass=true in ConsumptionFinal");
                    assert_eq!(*reader_seq, SeqNo(1));
                    assert_eq!(*advertised_writer_seq, Some(SeqNo(3)));
                    assert!(
                        failure_reason.is_none(),
                        "expected no failure_reason when overridden by policy"
                    );
                }
                ChainEventContent::FlowControl(FlowControlPayload::ConsumptionGap { .. }) => {
                    gap_found = true;
                }
                _ => {}
            }
        }

        assert!(
            final_pass_found,
            "expected a ConsumptionFinal event under BreakerAware mode"
        );
        assert!(
            !gap_found,
            "did not expect a ConsumptionGap event when override is applied"
        );

        let system_events = system_journal.read_causally_ordered().await.unwrap();
        let mut status_found = false;
        let mut override_found = false;

        for env in &system_events {
            match &env.event.event {
                SystemEventType::ContractStatus {
                    upstream,
                    reader,
                    pass,
                    reason,
                    ..
                } => {
                    status_found = true;
                    assert_eq!(*upstream, upstream_stage);
                    assert_eq!(*reader, reader_stage);
                    assert!(*pass, "expected pass=true in ContractStatus");
                    assert!(
                        reason.is_none(),
                        "expected no reason when contracts are overridden to pass"
                    );
                }
                SystemEventType::ContractOverrideByPolicy {
                    upstream,
                    reader,
                    policy,
                    ..
                } => {
                    override_found = true;
                    assert_eq!(*upstream, upstream_stage);
                    assert_eq!(*reader, reader_stage);
                    assert_eq!(policy, "breaker_aware");
                }
                _ => {}
            }
        }

        assert!(status_found, "expected ContractStatus system event");
        assert!(
            override_found,
            "expected ContractOverrideByPolicy system event in BreakerAware mode"
        );
    }

    #[tokio::test]
    async fn transport_only_skips_observability_events() {
        let upstream_stage = StageId::new();
        let upstream_owner = JournalOwner::stage(upstream_stage);
        let upstream_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(TestJournal::new(upstream_owner));

        let writer_id = WriterId::Stage(upstream_stage);

        // Many real-world stage journals contain large volumes of lifecycle/observability events.
        // Downstream stage subscriptions should not be forced to "process" them as part of normal
        // transport draining; they should be skipped at the subscription layer.
        upstream_journal
            .append(
                ChainEventFactory::stage_running(writer_id, upstream_stage),
                None,
            )
            .await
            .unwrap();
        upstream_journal
            .append(
                ChainEventFactory::stage_running(writer_id, upstream_stage),
                None,
            )
            .await
            .unwrap();

        upstream_journal
            .append(
                ChainEventFactory::data_event(writer_id, "test.event", json!({"n": 1})),
                None,
            )
            .await
            .unwrap();

        upstream_journal
            .append(
                ChainEventFactory::stage_running(writer_id, upstream_stage),
                None,
            )
            .await
            .unwrap();

        upstream_journal
            .append(ChainEventFactory::eof_event(writer_id, true), None)
            .await
            .unwrap();

        let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

        let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
            .await
            .unwrap()
            .transport_only();

        let mut reader_progress = [ReaderProgress::new(upstream_stage)];

        let first = subscription
            .poll_next_with_state("test_fsm", Some(&mut reader_progress[..]))
            .await;
        match first {
            PollResult::Event(env) => match env.event.content {
                ChainEventContent::Data { .. } => {}
                other => panic!("expected first delivered event to be Data, got {other:?}"),
            },
            other => panic!("expected PollResult::Event, got {other:?}"),
        }

        let second = subscription
            .poll_next_with_state("test_fsm", Some(&mut reader_progress[..]))
            .await;
        match second {
            PollResult::Event(env) => match env.event.content {
                ChainEventContent::FlowControl(FlowControlPayload::Eof { .. }) => {}
                other => panic!("expected second delivered event to be EOF, got {other:?}"),
            },
            other => panic!("expected PollResult::Event, got {other:?}"),
        }

        let outcome = subscription
            .take_last_eof_outcome()
            .expect("expected subscription to mark authoritative EOF");
        assert!(outcome.is_final);
        assert_eq!(outcome.stage_id, upstream_stage);
        assert_eq!(outcome.reader_index, 0);
        assert_eq!(outcome.eof_count, 1);
        assert_eq!(outcome.total_readers, 1);
    }
}

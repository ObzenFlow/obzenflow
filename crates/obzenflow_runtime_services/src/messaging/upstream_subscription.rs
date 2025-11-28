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

use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::system_event::{SystemEvent, SystemEventType};
use obzenflow_core::event::types::{
    Count, DurationMs, JournalIndex, JournalPath, SeqNo, ViolationCause as EventViolationCause,
};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{ChainEvent, ChainEventContent, ChainEventFactory, JournalEvent};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::ContractResult;
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

use async_trait::async_trait;
use crate::contracts::ContractChain;

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

    /// EOF tracking per reader
    eof_received: Vec<bool>,

    /// Buffering for events that need to be returned later
    /// (Currently unused but available for future ordering requirements)
    pending_events: VecDeque<EventEnvelope<ChainEvent>>,
}

impl SubscriptionState {
    fn new(reader_count: usize) -> Self {
        Self {
            current_reader_index: 0,
            eof_received: vec![false; reader_count],
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

    /// Last EOF accounting outcome (set when an EOF is observed)
    last_eof_outcome: Option<EofOutcome>,
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

        for (stage_id, stage_name, journal) in upstream_journals {
            // Get journal ID for debugging
            let journal_id = journal.id();
            tracing::info!(
                target: "flowip-080o",
                stage_id = ?stage_id,
                stage_name = stage_name,
                journal_id = ?journal_id,
                "Creating reader for upstream journal"
            );
            let reader = match journal.reader().await {
                Ok(reader) => reader,
                Err(JournalError::Implementation { .. }) => {
                    // Best-effort: log the failure and use an empty reader so the
                    // FSM can continue operating (upstream treated as having no events).
                    tracing::error!(
                        target: "flowip-080o",
                        stage_id = ?stage_id,
                        stage_name = stage_name,
                        journal_id = ?journal_id,
                        "Failed to create reader for upstream journal; using EmptyJournalReader (no events)"
                    );
                    Box::new(EmptyJournalReader::<T>::new()) as Box<dyn JournalReader<T>>
                }
                Err(e) => {
                    return Err(format!(
                        "Failed to create reader for stage {:?}: {}",
                        stage_id, e
                    )
                    .into());
                }
            };
            readers.push((*stage_id, stage_name.clone(), reader));
        }

        let state = SubscriptionState::new(readers.len());

        Ok(Self {
            owner_label: owner_label.to_string(),
            readers,
            state,
            contract_tracker: None,
            contract_chains: Vec::new(),
            last_eof_outcome: None,
        })
    }

    /// Backwards-compatible constructor using stage IDs as names
    pub async fn new(upstream_journals: &[(StageId, Arc<dyn Journal<T>>)]) -> Result<Self> {
        let with_names: Vec<(StageId, String, Arc<dyn Journal<T>>)> = upstream_journals
            .iter()
            .map(|(id, journal)| (*id, format!("{:?}", id), journal.clone()))
            .collect();
        Self::new_with_names("unknown_owner", &with_names).await
    }

    /// Retrieve and clear the most recent EOF accounting outcome, if any.
    pub fn take_last_eof_outcome(&mut self) -> Option<EofOutcome> {
        self.last_eof_outcome.take()
    }

    /// Enable contract emission for at-least-once delivery guarantees
    pub fn with_contracts(
        mut self,
        writer_id: WriterId,
        journal: Arc<dyn Journal<ChainEvent>>,
        config: ContractConfig,
        system_journal: Option<Arc<dyn Journal<SystemEvent>>>,
        reader_stage: Option<StageId>,
    ) -> Self {
        self.contract_tracker = Some(ContractTracker {
            config,
            writer_id,
            journal,
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
                    let chain = ContractChain::new().with_contract(TransportContract::new());
                    Some(chain)
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
            return PollResult::Error(Box::new(io::Error::new(
                io::ErrorKind::Other,
                "No upstream readers configured",
            )));
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
                                            progress.advertised_writer_seq = writer_seq.clone();
                                            progress.last_vector_clock = vector_clock.clone();
                                        }
                                    }
                                }

                                progress.last_event_id = Some(envelope.event.id().clone());
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
                        self.contract_tracker
                            .as_ref()
                            .and_then(|t| t.reader_stage),
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
                    tracker.writer_id.clone(),
                    progress.reader_seq,
                    progress.last_event_id.clone(),
                    progress.last_vector_clock.clone(),
                    self.state.is_reader_eof(index),
                    JournalPath(progress.stage_id.to_string()),
                    JournalIndex(index as u64),
                    progress.advertised_writer_seq,
                    progress.last_vector_clock.clone(),
                    stalled_duration,
                );

                tracker
                    .journal
                    .append(progress_event, None)
                    .await
                    .map_err(|e| format!("Failed to append progress event: {}", e))?;

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
                    // and contract evidence.
                    if let (Some(reader_stage), Some(chain_slot)) = (
                        tracker.reader_stage,
                        self.contract_chains.get(index).and_then(|c| c.as_ref()),
                    ) {
                        for result in chain_slot.verify_all(progress.stage_id, reader_stage) {
                            match result {
                                ContractResult::Passed(_) => {
                                    // Leave `pass = true`
                                }
                                ContractResult::Failed(violation) => {
                                    pass = false;
                                    // Map contract-level cause into event-level cause
                                    let event_cause = match &violation.cause {
                                        ViolationCause::SeqDivergence { advertised, reader } => {
                                            EventViolationCause::SeqDivergence {
                                                advertised: *advertised,
                                                reader: *reader,
                                            }
                                        }
                                        ViolationCause::ContentMismatch { .. } => {
                                            EventViolationCause::Other("content_mismatch".into())
                                        }
                                        ViolationCause::DeliveryMismatch { .. } => {
                                            EventViolationCause::Other("delivery_mismatch".into())
                                        }
                                        ViolationCause::AccountingMismatch { .. } => {
                                            EventViolationCause::Other(
                                                "accounting_mismatch".into(),
                                            )
                                        }
                                        ViolationCause::Other(msg) => {
                                            EventViolationCause::Other(msg.clone())
                                        }
                                    };
                                    failure_reason = Some(event_cause.clone());

                                    // Any explicit contract failure is surfaced
                                    // as a violation for this upstream.
                                    status = ContractStatus::Violated {
                                        upstream: progress.stage_id,
                                        cause: event_cause.clone(),
                                    };

                                    // For transport SeqDivergence, emit a gap
                                    // event when we are missing events.
                                    if let ViolationCause::SeqDivergence { advertised, reader } =
                                        &violation.cause
                                    {
                                        if let Some(advertised) = advertised {
                                            if advertised.0 > reader.0 {
                                                let gap_event =
                                                    ChainEventFactory::consumption_gap_event(
                                                        tracker.writer_id.clone(),
                                                        SeqNo(reader.0 + 1),
                                                        *advertised,
                                                        progress.stage_id,
                                                    );
                                                tracker
                                                    .journal
                                                    .append(gap_event, None)
                                                    .await
                                                    .map_err(|e| {
                                                        format!(
                                                            "Failed to append gap event: {}",
                                                            e
                                                        )
                                                    })?;
                                            }
                                        }
                                    }

                                    break;
                                }
                                ContractResult::Pending => {
                                    // At EOF we expect a definitive result; treat
                                    // Pending as a violation with a generic cause.
                                    pass = false;
                                    let cause = EventViolationCause::Other(
                                        "contract_pending_at_eof".into(),
                                    );
                                    failure_reason = Some(cause.clone());
                                    status = ContractStatus::Violated {
                                        upstream: progress.stage_id,
                                        cause,
                                    };
                                    break;
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
                                    tracker.writer_id.clone(),
                                    SeqNo(progress.reader_seq.0 + 1),
                                    advertised,
                                    progress.stage_id,
                                );
                                tracker
                                    .journal
                                    .append(gap_event, None)
                                    .await
                                    .map_err(|e| {
                                        format!("Failed to append gap event: {}", e)
                                    })?;
                            }

                            status = ContractStatus::Violated {
                                upstream: progress.stage_id,
                                cause,
                            };
                        }
                    }

                    // Capture reason for downstream system status before moving it into events
                    let status_reason = failure_reason.clone();

                    // Emit final event
                    let final_event = ChainEventFactory::consumption_final_event(
                        tracker.writer_id.clone(),
                        pass,
                        Count(progress.reader_seq.0),
                        None,
                        true,
                        progress.last_event_id.clone(),
                        progress.reader_seq,
                        progress.advertised_writer_seq,
                        progress.last_vector_clock.clone(),
                        failure_reason,
                    );

                    tracker
                        .journal
                        .append(final_event, None)
                        .await
                        .map_err(|e| format!("Failed to append final event: {}", e))?;

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
                            .map_err(|e| format!("Failed to append contract status: {}", e))?;
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
                    {
                        if progress.stalled_since.is_none() {
                            progress.stalled_since = progress.last_progress_instant;

                            let stalled_duration = DurationMs(
                                now.duration_since(progress.last_progress_instant.unwrap())
                                    .as_millis() as u64,
                            );

                            let stalled_event = ChainEventFactory::reader_stalled_event(
                                tracker.writer_id.clone(),
                                progress.stage_id,
                                stalled_duration,
                            );

                            tracker
                                .journal
                                .append(stalled_event, None)
                                .await
                                .map_err(|e| format!("Failed to append stalled event: {}", e))?;

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
                                        format!("Failed to append stalled contract status: {}", e)
                                    })?;
                            }

                            status = ContractStatus::Stalled(progress.stage_id);
                            progress.contract_violated = true;
                        }
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

    /// Query methods for FSM decision-making

    /// Check if there are pending buffered events
    pub fn has_pending(&self) -> bool {
        self.state.has_pending()
    }

    /// Get the number of upstream readers
    pub fn upstream_count(&self) -> usize {
        self.readers.len()
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

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::payloads::delivery_payload::DeliveryResult;
use crate::event::payloads::system_payload::ContractName;
use crate::event::{
    types::{Count, JournalIndex, SeqNo},
    ChainEvent, ChainPayload, EventId,
};
use crate::id::StageId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
pub mod evidence;
pub use evidence::*;
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Result of contract verification for a single contract on an edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "outcome", content = "evidence", rename_all = "snake_case")]
pub enum ContractResult {
    /// Contract passed and produced evidence suitable for audit trail.
    Passed(ContractEvidence),
    /// Contract failed with a concrete violation cause.
    Failed(ContractViolation),
    /// Contract is not yet verifiable (e.g., waiting for EOF / more evidence).
    Pending {
        reason: PendingReason,
        evidence: ContractEvidence,
    },
    /// An explicit declaration establishes that this check does not apply.
    Skipped {
        reason: SkippedReason,
        evidence: ContractEvidence,
    },
}

impl ContractResult {
    pub fn pending(name: ContractName, ctx: &ContractContext<'_>, reason: PendingReason) -> Self {
        Self::Pending {
            reason,
            evidence: contract_evidence(name, ctx, ContractEvidenceDetails::Progress),
        }
    }

    pub fn status(&self) -> crate::event::payloads::system_payload::ContractResultStatusLabel {
        use crate::event::payloads::system_payload::ContractResultStatusLabel as Status;
        match self {
            Self::Passed(_) => Status::Passed,
            Self::Failed(_) => Status::Failed,
            Self::Pending { .. } => Status::Pending,
            Self::Skipped { .. } => Status::Skipped,
        }
    }

    pub fn details(&self) -> &ContractEvidenceDetails {
        match self {
            Self::Passed(e)
            | Self::Pending { evidence: e, .. }
            | Self::Skipped { evidence: e, .. } => &e.details,
            Self::Failed(v) => &v.details,
        }
    }

    pub fn subject(&self) -> (&ContractName, StageId, StageId) {
        match self {
            Self::Passed(e)
            | Self::Pending { evidence: e, .. }
            | Self::Skipped { evidence: e, .. } => {
                (&e.contract_name, e.upstream_stage, e.downstream_stage)
            }
            Self::Failed(v) => (&v.contract_name, v.upstream_stage, v.downstream_stage),
        }
    }
}

/// Observations retained by a contract evaluation, independently of its outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractEvidence {
    pub contract_name: ContractName,
    pub upstream_stage: StageId,
    pub downstream_stage: StageId,
    pub evaluated_at: DateTime<Utc>,
    pub details: ContractEvidenceDetails,
}

/// Details of a contract violation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractViolation {
    pub contract_name: ContractName,
    pub upstream_stage: StageId,
    pub downstream_stage: StageId,
    pub detected_at: DateTime<Utc>,
    pub cause: ViolationCause,
    pub details: ContractEvidenceDetails,
}

/// Well-known categories of contract violations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ViolationCause {
    /// Writer/read counts diverged on a transport edge.
    SeqDivergence {
        advertised: Option<SeqNo>,
        reader: SeqNo,
    },
    /// Per-event content hashes do not match.
    ContentMismatch { mismatches: Vec<HashMismatch> },
    /// Delivery records and consumed events do not agree.
    DeliveryMismatch {
        missing_deliveries: usize,
        orphan_deliveries: usize,
    },
    /// Stateful accounting did not balance.
    AccountingMismatch {
        inputs_observed: Count,
        accounted_for: Count,
    },
    /// Mid-flight divergence detection predicate fired.
    Divergence {
        /// Stable predicate identifier (for example, "signal_to_data_ratio", "cycle_depth").
        predicate: String,
        /// Observed value for this predicate.
        observed: f64,
        /// Threshold that was exceeded.
        threshold: f64,
        /// Window size in seconds for windowed predicates (when applicable).
        #[serde(skip_serializing_if = "Option::is_none")]
        window_seconds: Option<u64>,
    },
    /// Generic string message for future / ad-hoc contracts.
    Other(String),
}

impl ViolationCause {
    /// Stable, snake_case label for metrics and evidence emission.
    pub fn cause_label(&self) -> &'static str {
        match self {
            ViolationCause::SeqDivergence { .. } => "seq_divergence",
            ViolationCause::ContentMismatch { .. } => "content_mismatch",
            ViolationCause::DeliveryMismatch { .. } => "delivery_mismatch",
            ViolationCause::AccountingMismatch { .. } => "accounting_mismatch",
            ViolationCause::Divergence { .. } => "divergence",
            ViolationCause::Other(_) => "other",
        }
    }
}

/// A single hash mismatch between write/read sides.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashMismatch {
    pub index: JournalIndex,
    pub writer_event_id: Option<EventId>,
    pub reader_event_id: Option<EventId>,
}

/// Type-erased container for contract-specific state.
#[derive(Default, Debug)]
pub struct ContractState {
    inner: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl ContractState {
    /// Get a shared reference to a typed value if present.
    pub fn get<T: 'static>(&self) -> Option<&T> {
        self.inner
            .get(&TypeId::of::<T>())
            .and_then(|b| b.downcast_ref::<T>())
    }

    /// Get a mutable reference to a typed value if present.
    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.inner
            .get_mut(&TypeId::of::<T>())
            .and_then(|b| b.downcast_mut::<T>())
    }

    /// Insert or replace a typed value.
    pub fn insert<T: 'static + Send + Sync>(&mut self, value: T) {
        self.inner.insert(
            TypeId::of::<T>(),
            Box::new(value) as Box<dyn Any + Send + Sync>,
        );
    }

    /// Get a mutable reference to a typed value, inserting `default` if missing.
    pub fn get_or_insert_with<T, F>(&mut self, default: F) -> &mut T
    where
        T: 'static + Send + Sync,
        F: FnOnce() -> T,
    {
        if !self.inner.contains_key(&TypeId::of::<T>()) {
            self.insert(default());
        }
        self.get_mut::<T>()
            .expect("type just inserted should be present")
    }
}

/// Context available when writing events (upstream side).
#[derive(Debug)]
pub struct ContractWriteContext {
    pub writer_stage: StageId,
    pub writer_seq: SeqNo,
    pub state: ContractState,
}

impl ContractWriteContext {
    pub fn new(writer_stage: StageId) -> Self {
        Self {
            writer_stage,
            writer_seq: SeqNo(0),
            state: ContractState::default(),
        }
    }
}

/// Context available when reading events (downstream side).
#[derive(Debug)]
pub struct ContractReadContext {
    pub reader_stage: StageId,
    pub reader_seq: SeqNo,
    pub upstream_stage: StageId,
    pub state: ContractState,
}

impl ContractReadContext {
    pub fn new(reader_stage: StageId, upstream_stage: StageId) -> Self {
        Self {
            reader_stage,
            reader_seq: SeqNo(0),
            upstream_stage,
            state: ContractState::default(),
        }
    }
}

/// Shared context used during verification.
#[derive(Debug)]
pub struct ContractContext<'a> {
    pub upstream_stage: StageId,
    pub downstream_stage: StageId,
    pub write_state: &'a ContractState,
    pub read_state: &'a ContractState,
}

/// Which delivered rows belong to a contract's evidence population.
///
/// Most contracts certify evidence authored by the journal-owning upstream.
/// Physical-edge diagnostics instead observe every delivered row, including
/// control signals forwarded through that journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractEventScope {
    /// Only rows whose author resolves to the journal-owning upstream.
    UpstreamAuthored,
    /// Original producer declarations, before selected-feed transport accounting.
    SourceProduction,
    /// Every row physically delivered across the edge.
    PhysicalEdge,
}

/// Core abstraction for edge-scoped verification between stages.
pub trait Contract: Send + Sync {
    /// Human-readable contract identifier for logs and evidence.
    fn name(&self) -> &str;

    /// Typed contract identifier for persisted evidence and metrics.
    fn contract_name(&self) -> ContractName {
        ContractName::from(self.name())
    }

    /// Declare the evidence population observed at an edge delivery.
    fn event_scope(&self) -> ContractEventScope {
        ContractEventScope::UpstreamAuthored
    }

    /// Called when the upstream side writes an event on the edge.
    fn on_write(&self, event: &ChainEvent, ctx: &mut ContractWriteContext);

    /// Called when the downstream side reads an event from the edge.
    fn on_read(&self, event: &ChainEvent, ctx: &mut ContractReadContext);

    /// Called at edge completion to verify the contract.
    fn verify(&self, ctx: &ContractContext<'_>) -> ContractResult;

    /// Optional incremental check, for early warnings or streaming policies.
    fn check_progress(&self, _ctx: &ContractContext<'_>) -> Option<ContractViolation> {
        None
    }
}

pub fn contract_evidence(
    name: ContractName,
    ctx: &ContractContext<'_>,
    details: ContractEvidenceDetails,
) -> ContractEvidence {
    ContractEvidence {
        contract_name: name,
        upstream_stage: ctx.upstream_stage,
        downstream_stage: ctx.downstream_stage,
        evaluated_at: Utc::now(),
        details,
    }
}

// ======================================================================
// Built-in transport contract (FLOWIP-080o / 090c)
// ======================================================================

/// Internal counter type used by TransportContract for writer-side counts.
#[derive(Debug, Default)]
struct WriterCount(pub u64);

/// Internal counter type used by TransportContract for reader-side counts.
#[derive(Debug, Default)]
struct ReaderCount(pub u64);

/// Verifies that the number of data events written on an edge matches the
/// number of data events read, as defined in FLOWIP-080o.
pub struct TransportContract;

impl Default for TransportContract {
    fn default() -> Self {
        Self::new()
    }
}

impl TransportContract {
    pub const NAME: &'static str = "TransportContract";

    pub fn new() -> Self {
        Self
    }
}

impl Contract for TransportContract {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn on_write(&self, event: &ChainEvent, ctx: &mut ContractWriteContext) {
        // For transport, the authoritative writer count comes from EOF:
        // the upstream writer advertises the total number of data events
        // it believes it has written via `writer_seq`.
        //
        // We therefore only update writer-side counts when we observe a
        // FlowControl::Eof with an explicit writer_seq, and treat that
        // as the final writer count for the edge.
        if let ChainPayload::FlowControl(
            crate::event::payloads::flow_control_payload::FlowControlPayload::Eof {
                writer_seq: Some(seq),
                ..
            },
        ) = &event.payload
        {
            let counter = ctx
                .state
                .get_or_insert_with::<WriterCount, _>(WriterCount::default);
            counter.0 = seq.0;
        }
    }

    fn on_read(&self, event: &ChainEvent, ctx: &mut ContractReadContext) {
        if event.consumes_data_credit() {
            let counter = ctx
                .state
                .get_or_insert_with::<ReaderCount, _>(ReaderCount::default);
            counter.0 = counter.0.saturating_add(1);
        }
    }

    fn verify(&self, ctx: &ContractContext<'_>) -> ContractResult {
        let advertised = ctx.write_state.get::<WriterCount>().map(|c| SeqNo(c.0));
        let consumed = ctx.read_state.get::<ReaderCount>().map_or(0, |c| c.0);
        let details = ContractEvidenceDetails::Transport {
            advertised_writer_seq: advertised,
            consumed_count: Count(consumed),
        };
        let evidence = contract_evidence(self.contract_name(), ctx, details.clone());
        match advertised {
            None => ContractResult::Pending {
                reason: PendingReason::WriterAdvertisementUnavailable,
                evidence,
            },
            Some(written) if written.0 == consumed => ContractResult::Passed(evidence),
            Some(written) => ContractResult::Failed(ContractViolation {
                contract_name: self.contract_name(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                detected_at: Utc::now(),
                cause: ViolationCause::SeqDivergence {
                    advertised: Some(written),
                    reader: SeqNo(consumed),
                },
                details,
            }),
        }
    }
}

// ======================================================================
// Source contract (FLOWIP-081b)
// ======================================================================

/// Internal writer-side state for SourceContract.
#[derive(Debug, Default)]
struct SourceWriterState {
    declaration_received: bool,
    expected_count: Option<u64>,
    eof_writer_seq: Option<u64>,
}

/// Verifies that a finite source's declared expectations (when configured)
/// match what it ultimately reports at EOF.
///
/// A received declaration without an expectation makes the check inapplicable.
/// Missing declarations or production counts leave verification pending.
pub struct SourceContract;

impl Default for SourceContract {
    fn default() -> Self {
        Self::new()
    }
}

impl SourceContract {
    pub const NAME: &'static str = "SourceContract";

    pub fn new() -> Self {
        Self
    }
}

impl Contract for SourceContract {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn event_scope(&self) -> ContractEventScope {
        ContractEventScope::SourceProduction
    }

    fn on_write(&self, event: &ChainEvent, ctx: &mut ContractWriteContext) {
        use crate::event::payloads::flow_control_payload::FlowControlPayload;

        if let ChainPayload::FlowControl(payload) = &event.payload {
            match payload {
                FlowControlPayload::SourceContract { expected_count, .. } => {
                    let state = ctx
                        .state
                        .get_or_insert_with::<SourceWriterState, _>(SourceWriterState::default);
                    state.declaration_received = true;
                    state.expected_count = expected_count.map(|count| count.0);
                }
                FlowControlPayload::Eof {
                    writer_seq: Some(seq),
                    ..
                } => {
                    let state = ctx
                        .state
                        .get_or_insert_with::<SourceWriterState, _>(SourceWriterState::default);
                    state.eof_writer_seq = Some(seq.0);
                }
                _ => {}
            }
        }
    }

    fn on_read(&self, _event: &ChainEvent, _ctx: &mut ContractReadContext) {
        // For 081b we don't need reader-side state for the source contract.
    }

    fn verify(&self, ctx: &ContractContext<'_>) -> ContractResult {
        let empty = SourceWriterState::default();
        let state = ctx.write_state.get::<SourceWriterState>().unwrap_or(&empty);
        let details = ContractEvidenceDetails::Source {
            declaration_received: state.declaration_received,
            expected_count: state.expected_count.map(Count),
            produced_count: state.eof_writer_seq.map(Count),
        };
        let evidence = contract_evidence(self.contract_name(), ctx, details.clone());
        if !state.declaration_received {
            return ContractResult::Pending {
                reason: PendingReason::ProducerDeclarationUnavailable,
                evidence,
            };
        }
        let Some(expected) = state.expected_count else {
            return ContractResult::Skipped {
                reason: SkippedReason::NoProductionExpectation,
                evidence,
            };
        };
        let Some(observed) = state.eof_writer_seq else {
            return ContractResult::Pending {
                reason: PendingReason::ProductionCountUnavailable,
                evidence,
            };
        };
        if expected == observed {
            ContractResult::Passed(evidence)
        } else {
            ContractResult::Failed(ContractViolation {
                contract_name: self.contract_name(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                detected_at: Utc::now(),
                cause: ViolationCause::Other("source_expected_count_mismatch".into()),
                details,
            })
        }
    }
}

// ======================================================================
// Delivery contract (FLOWIP-090f)
// ======================================================================

#[derive(Debug, Default)]
struct DeliveryState {
    /// Consumed data events awaiting a delivery receipt.
    pending: HashSet<EventId>,
    pending_peak: usize,

    /// Aggregate counters for evidence and policy.
    consumed_total: u64,
    receipted_total: u64,
    buffered_count: u64,
    success_count: u64,
    partial_count: u64,
    failed_count: u64,

    /// Receipts whose immediate parent does not match any pending consumed event ID.
    ///
    /// Under correct receipt routing, this indicates a wiring defect.
    orphan_deliveries: u64,
}

/// Verifies that every data event consumed by a sink handler produces a delivery
/// receipt journalled with causality-parent linkage back to that consumed event.
///
/// This contract deliberately stores bounded state: only the set of consumed
/// event IDs that are still awaiting receipts, plus aggregate counters. This
/// keeps memory usage O(pending) rather than O(total events).
pub struct DeliveryContract {
    state: Mutex<DeliveryState>,
}

impl Default for DeliveryContract {
    fn default() -> Self {
        Self {
            state: Mutex::new(DeliveryState::default()),
        }
    }
}

impl DeliveryContract {
    pub const NAME: &'static str = "DeliveryContract";
}

impl Contract for DeliveryContract {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn on_write(&self, event: &ChainEvent, _ctx: &mut ContractWriteContext) {
        let ChainPayload::Delivery(payload) = &event.payload else {
            return;
        };

        if event.causality.parent_ids.is_empty() {
            return;
        }

        let mut st = self.state.lock().expect("DeliveryContract state poisoned");

        if matches!(&payload.result, DeliveryResult::Buffered { .. }) {
            st.buffered_count = st.buffered_count.saturating_add(1);
            return;
        }

        if !event
            .causality
            .parent_ids
            .contains(&payload.subject.input.event_id)
        {
            return;
        }
        if st.pending.remove(&payload.subject.input.event_id) {
            st.receipted_total = st.receipted_total.saturating_add(1);
        } else {
            st.orphan_deliveries = st.orphan_deliveries.saturating_add(1);
            return;
        }

        match &payload.result {
            DeliveryResult::Buffered { .. } => {}
            DeliveryResult::Success { .. } => {
                st.success_count = st.success_count.saturating_add(1);
            }
            DeliveryResult::Partial { .. } => {
                st.partial_count = st.partial_count.saturating_add(1);
            }
            DeliveryResult::Failed { .. } | DeliveryResult::Rejected { .. } => {
                st.failed_count = st.failed_count.saturating_add(1);
            }
        }
    }

    fn on_read(&self, event: &ChainEvent, _ctx: &mut ContractReadContext) {
        // Only data events require receipts.
        if !event.consumes_data_credit() {
            return;
        }

        let mut st = self.state.lock().expect("DeliveryContract state poisoned");

        st.consumed_total = st.consumed_total.saturating_add(1);
        st.pending.insert(event.id);
        st.pending_peak = st.pending_peak.max(st.pending.len());
    }

    fn verify(&self, ctx: &ContractContext<'_>) -> ContractResult {
        let st = self.state.lock().expect("DeliveryContract state poisoned");
        let mut missing_event_ids: Vec<_> = st.pending.iter().copied().collect();
        missing_event_ids.sort();
        missing_event_ids.truncate(100);
        let details = ContractEvidenceDetails::Delivery {
            consumed_total: st.consumed_total,
            receipted_total: st.receipted_total,
            pending_peak: st.pending_peak,
            buffered_count: st.buffered_count,
            success_count: st.success_count,
            partial_count: st.partial_count,
            failed_count: st.failed_count,
            missing_count: st.pending.len(),
            orphan_count: st.orphan_deliveries,
            missing_event_ids,
        };
        if st.pending.is_empty() && st.orphan_deliveries == 0 {
            ContractResult::Passed(contract_evidence(self.contract_name(), ctx, details))
        } else {
            ContractResult::Failed(ContractViolation {
                contract_name: self.contract_name(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                detected_at: Utc::now(),
                cause: ViolationCause::DeliveryMismatch {
                    missing_deliveries: st.pending.len(),
                    orphan_deliveries: st.orphan_deliveries as usize,
                },
                details,
            })
        }
    }
}

// ======================================================================
// Divergence contract (FLOWIP-080r)
// ======================================================================

/// Threshold configuration for divergence detection predicates (FLOWIP-080r).
///
/// This configuration is evaluated per edge by [`DivergenceContract`] on a tumbling
/// window. Phase 1 implements:
/// - windowed signal-to-data ratio bounds
/// - windowed absolute caps when no data is observed
/// - cycle depth bounds for SCC-internal data events
#[derive(Debug, Clone)]
pub struct DivergenceThresholds {
    /// Evaluation window for windowed predicates.
    pub window: Duration,

    /// Maximum ratio of flow control signals to data events per window.
    pub signal_to_data_ratio: f64,

    /// Absolute cap on flow control signals per window when `data_events == 0`.
    pub max_signals_when_no_data: u64,

    /// Maximum per-event cycle depth allowed before failing.
    pub max_cycle_depth: u16,

    /// TTL for per-key state (dedup keys, counters) to bound memory in long-running flows.
    ///
    /// Phase 1 implementation uses bounded counters, but this is retained for follow-up
    /// predicates that require per-key maps (mirroring CycleGuard's TTL behaviour).
    pub state_ttl: Duration,
}

impl Default for DivergenceThresholds {
    fn default() -> Self {
        Self {
            window: Duration::from_secs(60),
            signal_to_data_ratio: 10.0,
            max_signals_when_no_data: 1_000,
            // Match MaxIterations::DEFAULT in runtime_services (FLOWIP-051p).
            max_cycle_depth: 30,
            state_ttl: Duration::from_secs(300),
        }
    }
}

#[derive(Debug, Default)]
struct DivergenceState {
    window_start: Option<Instant>,
    data_events: u64,
    flow_control_signals: u64,
    max_cycle_depth_observed: u16,
}

/// Contract that detects mid-flight divergence on SCC-internal edges (FLOWIP-080r).
///
/// This contract is observational: it records counts in `on_read` and reports
/// violations from `check_progress`. It does not suppress or rewrite events.
pub struct DivergenceContract {
    scc_id: crate::SccId,
    thresholds: DivergenceThresholds,
    state: Mutex<DivergenceState>,
}

impl DivergenceContract {
    pub const NAME: &'static str = "DivergenceContract";

    /// Create a new divergence contract for a specific SCC using default thresholds.
    pub fn new(scc_id: crate::SccId) -> Self {
        Self::with_thresholds(scc_id, DivergenceThresholds::default())
    }

    /// Create a new divergence contract for a specific SCC using explicit thresholds.
    pub fn with_thresholds(scc_id: crate::SccId, thresholds: DivergenceThresholds) -> Self {
        Self {
            scc_id,
            thresholds,
            state: Mutex::new(DivergenceState::default()),
        }
    }

    fn evidence_details(
        &self,
        st: &DivergenceState,
        evaluated_predicates: Vec<DivergencePredicate>,
    ) -> ContractEvidenceDetails {
        ContractEvidenceDetails::Divergence {
            scc_id: self.scc_id,
            window_seconds: self.thresholds.window.as_secs(),
            elapsed_ms: st
                .window_start
                .map(|start| start.elapsed().as_millis() as u64),
            evaluated_predicates,
            data_events: st.data_events,
            flow_control_signals: st.flow_control_signals,
            signal_to_data_ratio_threshold: self.thresholds.signal_to_data_ratio,
            max_signals_when_no_data: self.thresholds.max_signals_when_no_data,
            max_cycle_depth: self.thresholds.max_cycle_depth,
            max_cycle_depth_observed: st.max_cycle_depth_observed,
        }
    }

    fn check_signal_to_data_ratio(
        &self,
        ctx: &ContractContext<'_>,
        st: &DivergenceState,
    ) -> Option<ContractViolation> {
        let window_seconds = Some(self.thresholds.window.as_secs());

        if st.data_events == 0 {
            if st.flow_control_signals > self.thresholds.max_signals_when_no_data {
                return Some(ContractViolation {
                    contract_name: self.contract_name(),
                    upstream_stage: ctx.upstream_stage,
                    downstream_stage: ctx.downstream_stage,
                    detected_at: Utc::now(),
                    cause: ViolationCause::Divergence {
                        predicate: "signals_when_no_data".to_string(),
                        observed: st.flow_control_signals as f64,
                        threshold: self.thresholds.max_signals_when_no_data as f64,
                        window_seconds,
                    },
                    details: self
                        .evidence_details(st, vec![DivergencePredicate::SignalsWhenNoData]),
                });
            }
            return None;
        }

        let observed_ratio = st.flow_control_signals as f64 / st.data_events as f64;
        if observed_ratio > self.thresholds.signal_to_data_ratio {
            return Some(ContractViolation {
                contract_name: self.contract_name(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                detected_at: Utc::now(),
                cause: ViolationCause::Divergence {
                    predicate: "signal_to_data_ratio".to_string(),
                    observed: observed_ratio,
                    threshold: self.thresholds.signal_to_data_ratio,
                    window_seconds,
                },
                details: self.evidence_details(st, vec![DivergencePredicate::SignalToDataRatio]),
            });
        }

        None
    }

    fn check_cycle_depth(
        &self,
        ctx: &ContractContext<'_>,
        st: &DivergenceState,
    ) -> Option<ContractViolation> {
        if st.max_cycle_depth_observed > self.thresholds.max_cycle_depth {
            return Some(ContractViolation {
                contract_name: self.contract_name(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                detected_at: Utc::now(),
                cause: ViolationCause::Divergence {
                    predicate: "cycle_depth".to_string(),
                    observed: st.max_cycle_depth_observed as f64,
                    threshold: self.thresholds.max_cycle_depth as f64,
                    window_seconds: None,
                },
                details: self.evidence_details(st, vec![DivergencePredicate::CycleDepth]),
            });
        }
        None
    }
}

impl Contract for DivergenceContract {
    fn name(&self) -> &str {
        DivergenceContract::NAME
    }

    fn event_scope(&self) -> ContractEventScope {
        ContractEventScope::PhysicalEdge
    }

    fn on_write(&self, _event: &ChainEvent, _ctx: &mut ContractWriteContext) {
        // Divergence predicates are evaluated on the reader side in Phase 1.
    }

    fn on_read(&self, event: &ChainEvent, _ctx: &mut ContractReadContext) {
        let mut st = self
            .state
            .lock()
            .expect("DivergenceContract state poisoned");

        if st.window_start.is_none() {
            st.window_start = Some(Instant::now());
        }

        match &event.payload {
            _ if event.consumes_data_credit() => {
                st.data_events = st.data_events.saturating_add(1);
            }
            ChainPayload::FlowControl(_) => {
                st.flow_control_signals = st.flow_control_signals.saturating_add(1);
            }
            _ => {}
        }

        // Cycle depth applies to data events only; flow control signals do not carry
        // `cycle_depth` in the current model (FLOWIP-051p).
        if event.consumes_data_credit() && event.cycle_scc_id == Some(self.scc_id) {
            if let Some(depth) = event.cycle_depth {
                st.max_cycle_depth_observed = st.max_cycle_depth_observed.max(depth.as_u16());
            }
        }
    }

    fn verify(&self, ctx: &ContractContext<'_>) -> ContractResult {
        let st = self
            .state
            .lock()
            .expect("DivergenceContract state poisoned");
        // This contract evaluates predicates through check_progress. EOF has
        // never introduced another admission check; the final snapshot alone
        // cannot prove a predicate was evaluated for this observation window.
        ContractResult::Pending {
            reason: PendingReason::EvaluationUnavailable,
            evidence: contract_evidence(
                self.contract_name(),
                ctx,
                self.evidence_details(&st, vec![]),
            ),
        }
    }

    fn check_progress(&self, ctx: &ContractContext<'_>) -> Option<ContractViolation> {
        let now = Instant::now();
        let mut st = self
            .state
            .lock()
            .expect("DivergenceContract state poisoned");

        let Some(window_start) = st.window_start else {
            st.window_start = Some(now);
            return None;
        };

        let window_elapsed = now.duration_since(window_start) >= self.thresholds.window;

        if let Some(v) = self.check_cycle_depth(ctx, &st) {
            return Some(v);
        }
        if let Some(v) = self.check_signal_to_data_ratio(ctx, &st) {
            return Some(v);
        }

        if window_elapsed {
            st.window_start = Some(now);
            st.data_events = 0;
            st.flow_control_signals = 0;
            st.max_cycle_depth_observed = 0;
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::payloads::delivery_payload::DeliveryMethod;
    use crate::event::provenance::causality_context::CausalityContext;
    use crate::event::types::SeqNo;
    use crate::event::{ChainEventFactory, ConsumptionProgressEventParams};
    use crate::{CycleDepth, WriterId};
    use serde_json::json;

    fn dummy_ctx() -> (ContractWriteContext, ContractReadContext, StageId, StageId) {
        let upstream_stage = StageId::new();
        let downstream_stage = StageId::new();
        let write_ctx = ContractWriteContext::new(upstream_stage);
        let read_ctx = ContractReadContext::new(downstream_stage, upstream_stage);
        (write_ctx, read_ctx, upstream_stage, downstream_stage)
    }

    fn evaluate(
        contract: &dyn Contract,
        write: &ContractWriteContext,
        read: &ContractReadContext,
    ) -> ContractResult {
        contract.verify(&ContractContext {
            upstream_stage: write.writer_stage,
            downstream_stage: read.reader_stage,
            write_state: &write.state,
            read_state: &read.state,
        })
    }

    #[test]
    fn transport_keeps_missing_advertisement_distinct_from_known_zero() {
        use crate::event::payloads::flow_control_payload::FlowControlPayload;
        for consumed in [0, 10] {
            let contract = TransportContract::new();
            let (mut write, mut read, upstream, _) = dummy_ctx();
            for _ in 0..consumed {
                contract.on_read(
                    &ChainEventFactory::data_event(
                        upstream.into(),
                        "item",
                        std::num::NonZeroU32::MIN,
                        json!({}),
                    ),
                    &mut read,
                );
            }
            assert!(
                matches!(evaluate(&contract, &write, &read), ContractResult::Pending {
                reason: PendingReason::WriterAdvertisementUnavailable,
                evidence: ContractEvidence { details: ContractEvidenceDetails::Transport { advertised_writer_seq: None, consumed_count: Count(count) }, .. }
            } if count == consumed)
            );
            let mut eof = ChainEventFactory::eof_event(upstream.into(), true);
            let ChainPayload::FlowControl(FlowControlPayload::Eof { writer_seq, .. }) =
                &mut eof.payload
            else {
                unreachable!()
            };
            *writer_seq = Some(SeqNo(0));
            contract.on_write(&eof, &mut write);
            let result = evaluate(&contract, &write, &read);
            assert_eq!(matches!(result, ContractResult::Passed(_)), consumed == 0);
            assert_eq!(matches!(result, ContractResult::Failed(_)), consumed != 0);
        }
    }

    #[test]
    fn source_requires_a_declaration_and_the_original_production_count() {
        use crate::event::payloads::flow_control_payload::FlowControlPayload;
        use crate::event::types::{JournalIndex, JournalPath};
        for expected in [None, Some(Count(10))] {
            let contract = SourceContract::new();
            let (mut write, read, upstream, _) = dummy_ctx();
            assert!(matches!(
                evaluate(&contract, &write, &read),
                ContractResult::Pending {
                    reason: PendingReason::ProducerDeclarationUnavailable,
                    ..
                }
            ));
            let declaration = ChainEventFactory::source_contract_event(
                upstream.into(),
                crate::event::SourceContractEventParams {
                    expected_count: expected,
                    source_id: upstream,
                    route: None,
                    journal_path: JournalPath("source".into()),
                    journal_index: JournalIndex(0),
                    writer_seq: None,
                    vector_clock: None,
                },
            );
            contract.on_write(&declaration, &mut write);
            if expected.is_none() {
                assert!(matches!(
                    evaluate(&contract, &write, &read),
                    ContractResult::Skipped {
                        reason: SkippedReason::NoProductionExpectation,
                        ..
                    }
                ));
            } else {
                assert!(matches!(
                    evaluate(&contract, &write, &read),
                    ContractResult::Pending {
                        reason: PendingReason::ProductionCountUnavailable,
                        ..
                    }
                ));
                let mut eof = ChainEventFactory::eof_event(upstream.into(), true);
                let ChainPayload::FlowControl(FlowControlPayload::Eof { writer_seq, .. }) =
                    &mut eof.payload
                else {
                    unreachable!()
                };
                *writer_seq = Some(SeqNo(10));
                contract.on_write(&eof, &mut write);
                assert!(matches!(
                    evaluate(&contract, &write, &read),
                    ContractResult::Passed(ContractEvidence {
                        details: ContractEvidenceDetails::Source {
                            produced_count: Some(Count(10)),
                            ..
                        },
                        ..
                    })
                ));
            }
        }
    }

    #[test]
    fn final_divergence_snapshot_does_not_add_a_new_policy_check() {
        let contract = DivergenceContract::with_thresholds(
            crate::SccId::from(crate::Ulid::new()),
            DivergenceThresholds {
                max_signals_when_no_data: 0,
                ..DivergenceThresholds::default()
            },
        );
        let (write, mut read, upstream, downstream) = dummy_ctx();
        contract.on_read(&ChainEventFactory::drain_event(upstream.into()), &mut read);
        assert!(matches!(
            evaluate(&contract, &write, &read),
            ContractResult::Pending {
                reason: PendingReason::EvaluationUnavailable,
                ..
            }
        ));
        let violation = contract
            .check_progress(&ContractContext {
                upstream_stage: upstream,
                downstream_stage: downstream,
                write_state: &write.state,
                read_state: &read.state,
            })
            .expect("the existing progress evaluator detects the violation");
        assert!(
            matches!(violation.details, ContractEvidenceDetails::Divergence { evaluated_predicates, flow_control_signals: 1, .. } if evaluated_predicates == vec![DivergencePredicate::SignalsWhenNoData])
        );
    }

    #[test]
    fn delivery_contract_empty_passes() {
        let contract = DeliveryContract::default();
        let (write_ctx, read_ctx, upstream, downstream) = dummy_ctx();
        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };
        assert!(matches!(contract.verify(&ctx), ContractResult::Passed(_)));
    }

    #[test]
    fn delivery_contract_missing_receipt_fails() {
        let contract = DeliveryContract::default();
        let (write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        let consumed = ChainEventFactory::data_event(
            WriterId::from(upstream),
            "test.event",
            std::num::NonZeroU32::MIN,
            json!({"a": 1}),
        );
        contract.on_read(&consumed, &mut read_ctx);

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };

        match contract.verify(&ctx) {
            ContractResult::Failed(v) => match v.cause {
                ViolationCause::DeliveryMismatch {
                    missing_deliveries,
                    orphan_deliveries,
                } => {
                    assert_eq!(missing_deliveries, 1);
                    assert_eq!(orphan_deliveries, 0);
                }
                other => panic!("unexpected cause: {other:?}"),
            },
            other => panic!("expected failure, got: {other:?}"),
        }
    }

    #[test]
    fn delivery_contract_failed_receipt_is_accounted_for() {
        let contract = DeliveryContract::default();
        let (mut write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        let consumed = ChainEventFactory::data_event(
            WriterId::from(upstream),
            "test.event",
            std::num::NonZeroU32::MIN,
            json!({"a": 1}),
        );
        let parent_id = consumed.id;
        contract.on_read(&consumed, &mut read_ctx);

        let receipt_payload = crate::event::payloads::delivery_payload::DeliveryOutcome::failed(
            DeliveryMethod::Noop,
            "sink_error",
            "boom",
        );
        let receipt = ChainEventFactory::delivery_event(
            WriterId::from(downstream),
            crate::event::payloads::delivery_payload::test_receipt(parent_id, receipt_payload),
        )
        .with_causality(CausalityContext::with_parent(parent_id));

        contract.on_write(&receipt, &mut write_ctx);

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };
        assert!(matches!(
            contract.verify(&ctx),
            ContractResult::Passed(ContractEvidence {
                details: ContractEvidenceDetails::Delivery {
                    consumed_total: 1,
                    receipted_total: 1,
                    failed_count: 1,
                    success_count: 0,
                    missing_count: 0,
                    ..
                },
                ..
            })
        ));
    }

    #[test]
    fn delivery_contract_buffered_receipt_does_not_clear_pending() {
        let contract = DeliveryContract::default();
        let (mut write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        let consumed = ChainEventFactory::data_event(
            WriterId::from(upstream),
            "test.event",
            std::num::NonZeroU32::MIN,
            json!({"a": 1}),
        );
        let parent_id = consumed.id;
        contract.on_read(&consumed, &mut read_ctx);

        let receipt_payload = crate::event::payloads::delivery_payload::DeliveryOutcome::buffered(
            DeliveryMethod::Noop,
            /* bytes */ None,
        );
        let receipt = ChainEventFactory::delivery_event(
            WriterId::from(downstream),
            crate::event::payloads::delivery_payload::test_receipt(parent_id, receipt_payload),
        )
        .with_causality(CausalityContext::with_parent(parent_id));

        contract.on_write(&receipt, &mut write_ctx);

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };

        match contract.verify(&ctx) {
            ContractResult::Failed(v) => match v.cause {
                ViolationCause::DeliveryMismatch {
                    missing_deliveries,
                    orphan_deliveries,
                } => {
                    assert_eq!(missing_deliveries, 1);
                    assert_eq!(orphan_deliveries, 0);
                }
                other => panic!("unexpected cause: {other:?}"),
            },
            other => panic!("expected failure, got: {other:?}"),
        }
    }

    #[test]
    fn delivery_contract_extra_dependencies_do_not_settle_other_inputs() {
        let contract = DeliveryContract::default();
        let (mut write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        let consumed_a = ChainEventFactory::data_event(
            WriterId::from(upstream),
            "test.event",
            std::num::NonZeroU32::MIN,
            json!({"a": 1}),
        );
        let consumed_b = ChainEventFactory::data_event(
            WriterId::from(upstream),
            "test.event",
            std::num::NonZeroU32::MIN,
            json!({"b": 2}),
        );
        contract.on_read(&consumed_a, &mut read_ctx);
        contract.on_read(&consumed_b, &mut read_ctx);

        let receipt_payload = crate::event::payloads::delivery_payload::DeliveryOutcome::success(
            DeliveryMethod::Noop,
            /* bytes */ None,
        );
        let receipt = ChainEventFactory::delivery_event(
            WriterId::from(downstream),
            crate::event::payloads::delivery_payload::test_receipt(consumed_a.id, receipt_payload),
        )
        .with_causality(CausalityContext::with_parent(consumed_a.id).add_parent(consumed_b.id));

        contract.on_write(&receipt, &mut write_ctx);

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };
        assert!(matches!(
            contract.verify(&ctx),
            ContractResult::Failed(ContractViolation {
                cause: ViolationCause::DeliveryMismatch {
                    missing_deliveries: 1,
                    orphan_deliveries: 0
                },
                ..
            })
        ));
    }

    #[test]
    fn delivery_contract_orphan_receipt_fails() {
        let contract = DeliveryContract::default();
        let (mut write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        let parent_id = EventId::new();
        // No consumed event observed, but a receipt arrives.
        let receipt_payload = crate::event::payloads::delivery_payload::DeliveryOutcome::success(
            DeliveryMethod::Noop,
            /* bytes */ None,
        );
        let receipt = ChainEventFactory::delivery_event(
            WriterId::from(downstream),
            crate::event::payloads::delivery_payload::test_receipt(parent_id, receipt_payload),
        )
        .with_causality(CausalityContext::with_parent(parent_id));

        contract.on_write(&receipt, &mut write_ctx);

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };

        match contract.verify(&ctx) {
            ContractResult::Failed(v) => match v.cause {
                ViolationCause::DeliveryMismatch {
                    missing_deliveries,
                    orphan_deliveries,
                } => {
                    assert_eq!(missing_deliveries, 0);
                    assert_eq!(orphan_deliveries, 1);
                }
                other => panic!("unexpected cause: {other:?}"),
            },
            other => panic!("expected failure, got: {other:?}"),
        }

        // Keep the compiler honest about the unused read_ctx.
        let _ = &mut read_ctx;
    }

    #[test]
    fn divergence_contract_signal_ratio_violation_emits_progress_violation() {
        let scc_id = crate::SccId::from(crate::Ulid::new());
        let thresholds = DivergenceThresholds {
            window: Duration::from_secs(60),
            signal_to_data_ratio: 2.0,
            max_signals_when_no_data: 10,
            max_cycle_depth: 30,
            state_ttl: Duration::from_secs(300),
        };
        let contract = DivergenceContract::with_thresholds(scc_id, thresholds);
        let (write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        // 1 data event, 3 signals -> ratio 3.0 > 2.0.
        let data = ChainEventFactory::data_event(
            crate::WriterId::from(upstream),
            "test.event",
            std::num::NonZeroU32::MIN,
            json!({"a": 1}),
        );
        contract.on_read(&data, &mut read_ctx);

        let progress = ChainEventFactory::consumption_progress_event(
            crate::WriterId::from(upstream),
            ConsumptionProgressEventParams {
                scope: obzenflow_core::contracts::SubscriptionScope {
                    upstream,
                    reader: downstream,
                    selection: obzenflow_core::contracts::SubscriptionSelection::All,
                },
                consumed_count: Count(0),
                receipts: None,
                reader_seq: SeqNo(1),
                last_event_id: None,
                vector_clock: None,
                eof_seen: false,
                reader_path: crate::event::types::JournalPath("x".to_string()),
                reader_index: crate::event::types::JournalIndex(0),
                advertised_writer_seq: None,
                advertised_vector_clock: None,
                stalled_since: None,
            },
        );
        contract.on_read(&progress, &mut read_ctx);
        contract.on_read(&progress, &mut read_ctx);
        contract.on_read(&progress, &mut read_ctx);

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };

        let Some(v) = contract.check_progress(&ctx) else {
            panic!("expected divergence violation, got None");
        };

        match v.cause {
            ViolationCause::Divergence {
                predicate,
                observed,
                threshold,
                window_seconds,
            } => {
                assert_eq!(predicate, "signal_to_data_ratio");
                assert!(observed > threshold);
                assert_eq!(window_seconds, Some(60));
            }
            other => panic!("unexpected cause: {other:?}"),
        }
    }

    #[test]
    fn divergence_contract_does_not_apply_cycle_depth_to_flow_control_signals() {
        let scc_id = crate::SccId::from(crate::Ulid::new());
        let thresholds = DivergenceThresholds {
            window: Duration::from_secs(60),
            signal_to_data_ratio: 10.0,
            max_signals_when_no_data: 1_000,
            max_cycle_depth: 1,
            state_ttl: Duration::from_secs(300),
        };
        let contract = DivergenceContract::with_thresholds(scc_id, thresholds);
        let (write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        let progress = ChainEventFactory::consumption_progress_event(
            crate::WriterId::from(upstream),
            ConsumptionProgressEventParams {
                scope: obzenflow_core::contracts::SubscriptionScope {
                    upstream,
                    reader: downstream,
                    selection: obzenflow_core::contracts::SubscriptionSelection::All,
                },
                consumed_count: Count(0),
                receipts: None,
                reader_seq: SeqNo(0),
                last_event_id: None,
                vector_clock: None,
                eof_seen: false,
                reader_path: crate::event::types::JournalPath("x".to_string()),
                reader_index: crate::event::types::JournalIndex(0),
                advertised_writer_seq: None,
                advertised_vector_clock: None,
                stalled_since: None,
            },
        );
        contract.on_read(&progress, &mut read_ctx);

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };

        // No data event with cycle_depth observed, so no cycle-depth violation should be produced.
        assert!(contract.check_progress(&ctx).is_none());
    }

    #[test]
    fn divergence_contract_signals_when_no_data_violation_emits_progress_violation() {
        let scc_id = crate::SccId::from(crate::Ulid::new());
        let thresholds = DivergenceThresholds {
            window: Duration::from_secs(60),
            signal_to_data_ratio: 10.0,
            max_signals_when_no_data: 2,
            max_cycle_depth: 30,
            state_ttl: Duration::from_secs(300),
        };
        let contract = DivergenceContract::with_thresholds(scc_id, thresholds);
        let (write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        let progress = ChainEventFactory::consumption_progress_event(
            crate::WriterId::from(upstream),
            ConsumptionProgressEventParams {
                scope: obzenflow_core::contracts::SubscriptionScope {
                    upstream,
                    reader: downstream,
                    selection: obzenflow_core::contracts::SubscriptionSelection::All,
                },
                consumed_count: Count(0),
                receipts: None,
                reader_seq: SeqNo(0),
                last_event_id: None,
                vector_clock: None,
                eof_seen: false,
                reader_path: crate::event::types::JournalPath("x".to_string()),
                reader_index: crate::event::types::JournalIndex(0),
                advertised_writer_seq: None,
                advertised_vector_clock: None,
                stalled_since: None,
            },
        );
        contract.on_read(&progress, &mut read_ctx);
        contract.on_read(&progress, &mut read_ctx);
        contract.on_read(&progress, &mut read_ctx);

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };

        let Some(v) = contract.check_progress(&ctx) else {
            panic!("expected divergence violation, got None");
        };

        match v.cause {
            ViolationCause::Divergence {
                predicate,
                observed,
                threshold,
                window_seconds,
            } => {
                assert_eq!(predicate, "signals_when_no_data");
                assert!(observed > threshold);
                assert_eq!(window_seconds, Some(60));
            }
            other => panic!("unexpected cause: {other:?}"),
        }
    }

    #[test]
    fn divergence_contract_cycle_depth_violation_emits_progress_violation() {
        let scc_id = crate::SccId::from(crate::Ulid::new());
        let thresholds = DivergenceThresholds {
            window: Duration::from_secs(60),
            signal_to_data_ratio: 10.0,
            max_signals_when_no_data: 1_000,
            max_cycle_depth: 3,
            state_ttl: Duration::from_secs(300),
        };
        let contract = DivergenceContract::with_thresholds(scc_id, thresholds);
        let (write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        let mut data = ChainEventFactory::data_event(
            crate::WriterId::from(upstream),
            "test.event",
            std::num::NonZeroU32::MIN,
            json!({}),
        );
        data.cycle_scc_id = Some(scc_id);
        data.cycle_depth = Some(CycleDepth::new(4));

        contract.on_read(&data, &mut read_ctx);

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };

        let Some(v) = contract.check_progress(&ctx) else {
            panic!("expected divergence violation, got None");
        };

        match v.cause {
            ViolationCause::Divergence {
                predicate,
                observed,
                threshold,
                window_seconds,
            } => {
                assert_eq!(predicate, "cycle_depth");
                assert_eq!(observed, 4.0);
                assert_eq!(threshold, 3.0);
                assert_eq!(window_seconds, None);
            }
            other => panic!("unexpected cause: {other:?}"),
        }
    }

    #[test]
    fn divergence_contract_within_bounds_returns_none() {
        let scc_id = crate::SccId::from(crate::Ulid::new());
        let thresholds = DivergenceThresholds {
            window: Duration::from_secs(60),
            signal_to_data_ratio: 10.0,
            max_signals_when_no_data: 1_000,
            max_cycle_depth: 30,
            state_ttl: Duration::from_secs(300),
        };
        let contract = DivergenceContract::with_thresholds(scc_id, thresholds);
        let (write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        for _ in 0..10 {
            let data = ChainEventFactory::data_event(
                crate::WriterId::from(upstream),
                "test.event",
                std::num::NonZeroU32::MIN,
                json!({"a": 1}),
            );
            contract.on_read(&data, &mut read_ctx);
        }

        let signal = ChainEventFactory::watermark_event(crate::WriterId::from(upstream), 0, None);
        for _ in 0..50 {
            contract.on_read(&signal, &mut read_ctx);
        }

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };

        assert!(contract.check_progress(&ctx).is_none());
    }

    #[test]
    fn divergence_contract_window_rollover_resets_counters() {
        let scc_id = crate::SccId::from(crate::Ulid::new());
        let thresholds = DivergenceThresholds {
            window: Duration::from_secs(60),
            signal_to_data_ratio: 10.0,
            max_signals_when_no_data: 1_000,
            max_cycle_depth: 30,
            state_ttl: Duration::from_secs(300),
        };
        let contract = DivergenceContract::with_thresholds(scc_id, thresholds.clone());
        let (write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        let data = ChainEventFactory::data_event(
            crate::WriterId::from(upstream),
            "test.event",
            std::num::NonZeroU32::MIN,
            json!({"a": 1}),
        );
        contract.on_read(&data, &mut read_ctx);

        let signal = ChainEventFactory::watermark_event(crate::WriterId::from(upstream), 0, None);
        contract.on_read(&signal, &mut read_ctx);

        {
            let mut st = contract.state.lock().expect("state poisoned");
            if let Some(backdated) =
                Instant::now().checked_sub(thresholds.window + Duration::from_secs(1))
            {
                st.window_start = Some(backdated);
            } else {
                st.window_start = Some(Instant::now());
            }
        }

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };

        assert!(contract.check_progress(&ctx).is_none());

        let st = contract.state.lock().expect("state poisoned");
        assert_eq!(st.data_events, 0);
        assert_eq!(st.flow_control_signals, 0);
        assert_eq!(st.max_cycle_depth_observed, 0);
    }
}

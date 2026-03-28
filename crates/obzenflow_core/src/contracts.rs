// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::payloads::delivery_payload::DeliveryResult;
use crate::event::{
    types::{Count, JournalIndex, SeqNo},
    ChainEvent, ChainEventContent, EventId,
};
use crate::id::StageId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Result of contract verification for a single contract on an edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContractResult {
    /// Contract passed and produced evidence suitable for audit trail.
    Passed(ContractEvidence),
    /// Contract failed with a concrete violation cause.
    Failed(ContractViolation),
    /// Contract is not yet verifiable (e.g., waiting for EOF / more evidence).
    Pending,
}

/// Evidence that a contract was satisfied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractEvidence {
    pub contract_name: String,
    pub upstream_stage: StageId,
    pub downstream_stage: StageId,
    pub verified_at: DateTime<Utc>,
    pub details: JsonValue,
}

/// Details of a contract violation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractViolation {
    pub contract_name: String,
    pub upstream_stage: StageId,
    pub downstream_stage: StageId,
    pub detected_at: DateTime<Utc>,
    pub cause: ViolationCause,
    pub details: JsonValue,
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

/// Core abstraction for edge-scoped verification between stages.
pub trait Contract: Send + Sync {
    /// Human-readable contract identifier for logs and evidence.
    fn name(&self) -> &str;

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
        if let crate::event::ChainEventContent::FlowControl(
            crate::event::payloads::flow_control_payload::FlowControlPayload::Eof {
                writer_seq: Some(seq),
                ..
            },
        ) = &event.content
        {
            let counter = ctx
                .state
                .get_or_insert_with::<WriterCount, _>(WriterCount::default);
            counter.0 = seq.0;
        }
    }

    fn on_read(&self, event: &ChainEvent, ctx: &mut ContractReadContext) {
        if event.is_data() {
            let counter = ctx
                .state
                .get_or_insert_with::<ReaderCount, _>(ReaderCount::default);
            counter.0 = counter.0.saturating_add(1);
        }
    }

    fn verify(&self, ctx: &ContractContext<'_>) -> ContractResult {
        let writer_count = ctx
            .write_state
            .get::<WriterCount>()
            .map(|c| c.0)
            .unwrap_or(0);
        let reader_count = ctx
            .read_state
            .get::<ReaderCount>()
            .map(|c| c.0)
            .unwrap_or(0);

        if writer_count == reader_count {
            ContractResult::Passed(ContractEvidence {
                contract_name: self.name().to_string(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                verified_at: Utc::now(),
                details: JsonValue::Object(
                    [
                        ("writer_seq".to_string(), JsonValue::from(writer_count)),
                        ("reader_seq".to_string(), JsonValue::from(reader_count)),
                    ]
                    .into_iter()
                    .collect(),
                ),
            })
        } else {
            ContractResult::Failed(ContractViolation {
                contract_name: self.name().to_string(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                detected_at: Utc::now(),
                cause: ViolationCause::SeqDivergence {
                    advertised: Some(SeqNo(writer_count)),
                    reader: SeqNo(reader_count),
                },
                details: JsonValue::Object(
                    [
                        ("writer_seq".to_string(), JsonValue::from(writer_count)),
                        ("reader_seq".to_string(), JsonValue::from(reader_count)),
                        (
                            "delta".to_string(),
                            JsonValue::from(writer_count as i64 - reader_count as i64),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
            })
        }
    }
}

// ======================================================================
// Source contract (FLOWIP-081b)
// ======================================================================

/// Internal writer-side state for SourceContract.
#[derive(Debug, Default)]
struct SourceWriterState {
    expected_count: Option<u64>,
    eof_writer_seq: Option<u64>,
}

/// Verifies that a finite source's declared expectations (when configured)
/// match what it ultimately reports at EOF.
///
/// This contract is intentionally conservative for 081b:
/// - If no `expected_count` is ever observed, it always passes.
/// - If `expected_count` is present and an EOF with `writer_seq` is seen,
///   it fails only when the two disagree.
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

    fn on_write(&self, event: &ChainEvent, ctx: &mut ContractWriteContext) {
        use crate::event::payloads::flow_control_payload::FlowControlPayload;

        if let crate::event::ChainEventContent::FlowControl(payload) = &event.content {
            match payload {
                FlowControlPayload::SourceContract {
                    expected_count: Some(count),
                    ..
                } => {
                    let state = ctx
                        .state
                        .get_or_insert_with::<SourceWriterState, _>(SourceWriterState::default);
                    state.expected_count = Some(count.0);
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
        let state = match ctx.write_state.get::<SourceWriterState>() {
            Some(s) => s,
            None => {
                // No writer-side evidence; treat as pass for now.
                return ContractResult::Passed(ContractEvidence {
                    contract_name: self.name().to_string(),
                    upstream_stage: ctx.upstream_stage,
                    downstream_stage: ctx.downstream_stage,
                    verified_at: Utc::now(),
                    details: JsonValue::String(
                        "no source_contract / EOF evidence observed".to_string(),
                    ),
                });
            }
        };

        match (state.expected_count, state.eof_writer_seq) {
            (Some(expected), Some(observed)) if expected != observed => {
                ContractResult::Failed(ContractViolation {
                    contract_name: self.name().to_string(),
                    upstream_stage: ctx.upstream_stage,
                    downstream_stage: ctx.downstream_stage,
                    detected_at: Utc::now(),
                    cause: ViolationCause::Other("source_expected_count_mismatch".into()),
                    details: JsonValue::Object(
                        [
                            ("expected_count".to_string(), JsonValue::from(expected)),
                            ("observed_writer_seq".to_string(), JsonValue::from(observed)),
                            (
                                "delta".to_string(),
                                JsonValue::from(observed as i64 - expected as i64),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                })
            }
            _ => ContractResult::Passed(ContractEvidence {
                contract_name: self.name().to_string(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                verified_at: Utc::now(),
                details: JsonValue::Object(
                    [
                        (
                            "expected_count".to_string(),
                            JsonValue::from(state.expected_count.unwrap_or(0)),
                        ),
                        (
                            "observed_writer_seq".to_string(),
                            JsonValue::from(state.eof_writer_seq.unwrap_or(0)),
                        ),
                    ]
                    .into_iter()
                    .collect(),
                ),
            }),
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
    failed_final_count: u64,

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
        let ChainEventContent::Delivery(payload) = &event.content else {
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

        let mut matched_any = false;
        for parent_id in &event.causality.parent_ids {
            if st.pending.remove(parent_id) {
                matched_any = true;
                st.receipted_total = st.receipted_total.saturating_add(1);
            } else {
                st.orphan_deliveries = st.orphan_deliveries.saturating_add(1);
            }
        }

        if !matched_any {
            return;
        }

        match &payload.result {
            DeliveryResult::Buffered { .. } => {}
            DeliveryResult::Success { .. } => {
                st.success_count = st
                    .success_count
                    .saturating_add(event.causality.parent_ids.len() as u64);
            }
            DeliveryResult::Partial { .. } => {
                st.partial_count = st
                    .partial_count
                    .saturating_add(event.causality.parent_ids.len() as u64);
            }
            DeliveryResult::Failed { final_attempt, .. } => {
                st.failed_count = st
                    .failed_count
                    .saturating_add(event.causality.parent_ids.len() as u64);
                if *final_attempt {
                    st.failed_final_count = st
                        .failed_final_count
                        .saturating_add(event.causality.parent_ids.len() as u64);
                }
            }
        }
    }

    fn on_read(&self, event: &ChainEvent, _ctx: &mut ContractReadContext) {
        // Only data events require receipts.
        if !event.is_data() {
            return;
        }

        let mut st = self.state.lock().expect("DeliveryContract state poisoned");

        st.consumed_total = st.consumed_total.saturating_add(1);
        st.pending.insert(event.id);
        st.pending_peak = st.pending_peak.max(st.pending.len());
    }

    fn verify(&self, ctx: &ContractContext<'_>) -> ContractResult {
        let st = self.state.lock().expect("DeliveryContract state poisoned");

        let missing_count = st.pending.len();
        let orphan_count = st.orphan_deliveries as usize;

        let mut missing_sample: Vec<EventId> = st.pending.iter().take(100).copied().collect();
        missing_sample.sort();

        if missing_count == 0 && orphan_count == 0 {
            ContractResult::Passed(ContractEvidence {
                contract_name: self.name().to_string(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                verified_at: Utc::now(),
                details: json!({
                    "consumed_total": st.consumed_total,
                    "receipted_total": st.receipted_total,
                    "pending_peak": st.pending_peak,
                    "buffered_count": st.buffered_count,
                    "has_failures": st.failed_count > 0,
                    "success_count": st.success_count,
                    "partial_count": st.partial_count,
                    "failed_count": st.failed_count,
                    "failed_final_count": st.failed_final_count,
                }),
            })
        } else {
            ContractResult::Failed(ContractViolation {
                contract_name: self.name().to_string(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                detected_at: Utc::now(),
                cause: ViolationCause::DeliveryMismatch {
                    missing_deliveries: missing_count,
                    orphan_deliveries: orphan_count,
                },
                details: json!({
                    "consumed_total": st.consumed_total,
                    "receipted_total": st.receipted_total,
                    "pending_peak": st.pending_peak,
                    "buffered_count": st.buffered_count,
                    "missing_count": missing_count,
                    "orphan_count": orphan_count,
                    "missing_event_ids": missing_sample
                        .iter()
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>(),
                }),
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

    fn check_signal_to_data_ratio(
        &self,
        ctx: &ContractContext<'_>,
        st: &DivergenceState,
    ) -> Option<ContractViolation> {
        let window_seconds = Some(self.thresholds.window.as_secs());

        if st.data_events == 0 {
            if st.flow_control_signals > self.thresholds.max_signals_when_no_data {
                return Some(ContractViolation {
                    contract_name: self.name().to_string(),
                    upstream_stage: ctx.upstream_stage,
                    downstream_stage: ctx.downstream_stage,
                    detected_at: Utc::now(),
                    cause: ViolationCause::Divergence {
                        predicate: "signals_when_no_data".to_string(),
                        observed: st.flow_control_signals as f64,
                        threshold: self.thresholds.max_signals_when_no_data as f64,
                        window_seconds,
                    },
                    details: json!({
                        "window_seconds": self.thresholds.window.as_secs(),
                        "flow_control_signals": st.flow_control_signals,
                        "data_events": st.data_events,
                        "max_signals_when_no_data": self.thresholds.max_signals_when_no_data,
                    }),
                });
            }
            return None;
        }

        let observed_ratio = st.flow_control_signals as f64 / st.data_events as f64;
        if observed_ratio > self.thresholds.signal_to_data_ratio {
            return Some(ContractViolation {
                contract_name: self.name().to_string(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                detected_at: Utc::now(),
                cause: ViolationCause::Divergence {
                    predicate: "signal_to_data_ratio".to_string(),
                    observed: observed_ratio,
                    threshold: self.thresholds.signal_to_data_ratio,
                    window_seconds,
                },
                details: json!({
                    "window_seconds": self.thresholds.window.as_secs(),
                    "flow_control_signals": st.flow_control_signals,
                    "data_events": st.data_events,
                    "observed_ratio": observed_ratio,
                    "threshold_ratio": self.thresholds.signal_to_data_ratio,
                }),
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
                contract_name: self.name().to_string(),
                upstream_stage: ctx.upstream_stage,
                downstream_stage: ctx.downstream_stage,
                detected_at: Utc::now(),
                cause: ViolationCause::Divergence {
                    predicate: "cycle_depth".to_string(),
                    observed: st.max_cycle_depth_observed as f64,
                    threshold: self.thresholds.max_cycle_depth as f64,
                    window_seconds: None,
                },
                details: json!({
                    "scc_id": self.scc_id.to_string(),
                    "max_cycle_depth_observed": st.max_cycle_depth_observed,
                    "max_cycle_depth": self.thresholds.max_cycle_depth,
                }),
            });
        }
        None
    }
}

impl Contract for DivergenceContract {
    fn name(&self) -> &str {
        DivergenceContract::NAME
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

        match &event.content {
            ChainEventContent::Data { .. } => {
                st.data_events = st.data_events.saturating_add(1);
            }
            ChainEventContent::FlowControl(_) => {
                st.flow_control_signals = st.flow_control_signals.saturating_add(1);
            }
            _ => {}
        }

        // Cycle depth applies to data events only; flow control signals do not carry
        // `cycle_depth` in the current model (FLOWIP-051p).
        if event.is_data() && event.cycle_scc_id == Some(self.scc_id) {
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
        let observed_ratio = if st.data_events == 0 {
            None
        } else {
            Some(st.flow_control_signals as f64 / st.data_events as f64)
        };

        ContractResult::Passed(ContractEvidence {
            contract_name: self.name().to_string(),
            upstream_stage: ctx.upstream_stage,
            downstream_stage: ctx.downstream_stage,
            verified_at: Utc::now(),
            details: json!({
                "scc_id": self.scc_id.to_string(),
                "window_seconds": self.thresholds.window.as_secs(),
                "signal_to_data_ratio_threshold": self.thresholds.signal_to_data_ratio,
                "max_signals_when_no_data": self.thresholds.max_signals_when_no_data,
                "max_cycle_depth": self.thresholds.max_cycle_depth,
                "data_events_observed_in_window": st.data_events,
                "flow_control_signals_observed_in_window": st.flow_control_signals,
                "signal_to_data_ratio_observed": observed_ratio,
                "max_cycle_depth_observed": st.max_cycle_depth_observed,
            }),
        })
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
    use crate::event::context::causality_context::CausalityContext;
    use crate::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use crate::event::types::SeqNo;
    use crate::event::{ChainEventFactory, ConsumptionProgressEventParams};
    use crate::CycleDepth;
    use crate::WriterId;

    fn dummy_ctx() -> (ContractWriteContext, ContractReadContext, StageId, StageId) {
        let upstream_stage = StageId::new();
        let downstream_stage = StageId::new();
        let write_ctx = ContractWriteContext::new(upstream_stage);
        let read_ctx = ContractReadContext::new(downstream_stage, upstream_stage);
        (write_ctx, read_ctx, upstream_stage, downstream_stage)
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

        let consumed =
            ChainEventFactory::data_event(WriterId::from(upstream), "test.event", json!({"a": 1}));
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

        let consumed =
            ChainEventFactory::data_event(WriterId::from(upstream), "test.event", json!({"a": 1}));
        let parent_id = consumed.id;
        contract.on_read(&consumed, &mut read_ctx);

        let receipt_payload = DeliveryPayload::failed(
            "dest",
            DeliveryMethod::Noop,
            "sink_error",
            "boom",
            /* final_attempt */ true,
        );
        let receipt =
            ChainEventFactory::delivery_event(WriterId::from(downstream), receipt_payload)
                .with_causality(CausalityContext::with_parent(parent_id));

        contract.on_write(&receipt, &mut write_ctx);

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };
        assert!(matches!(contract.verify(&ctx), ContractResult::Passed(_)));
    }

    #[test]
    fn delivery_contract_buffered_receipt_does_not_clear_pending() {
        let contract = DeliveryContract::default();
        let (mut write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        let consumed =
            ChainEventFactory::data_event(WriterId::from(upstream), "test.event", json!({"a": 1}));
        let parent_id = consumed.id;
        contract.on_read(&consumed, &mut read_ctx);

        let receipt_payload =
            DeliveryPayload::buffered("dest", DeliveryMethod::Noop, /* bytes */ None);
        let receipt =
            ChainEventFactory::delivery_event(WriterId::from(downstream), receipt_payload)
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
    fn delivery_contract_multi_parent_receipt_clears_all_parents() {
        let contract = DeliveryContract::default();
        let (mut write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        let consumed_a =
            ChainEventFactory::data_event(WriterId::from(upstream), "test.event", json!({"a": 1}));
        let consumed_b =
            ChainEventFactory::data_event(WriterId::from(upstream), "test.event", json!({"b": 2}));
        contract.on_read(&consumed_a, &mut read_ctx);
        contract.on_read(&consumed_b, &mut read_ctx);

        let receipt_payload =
            DeliveryPayload::success("dest", DeliveryMethod::Noop, /* bytes */ None);
        let receipt =
            ChainEventFactory::delivery_event(WriterId::from(downstream), receipt_payload)
                .with_causality(
                    CausalityContext::with_parent(consumed_a.id).add_parent(consumed_b.id),
                );

        contract.on_write(&receipt, &mut write_ctx);

        let ctx = ContractContext {
            upstream_stage: upstream,
            downstream_stage: downstream,
            write_state: &write_ctx.state,
            read_state: &read_ctx.state,
        };
        assert!(matches!(contract.verify(&ctx), ContractResult::Passed(_)));
    }

    #[test]
    fn delivery_contract_orphan_receipt_fails() {
        let contract = DeliveryContract::default();
        let (mut write_ctx, mut read_ctx, upstream, downstream) = dummy_ctx();

        // No consumed event observed, but a receipt arrives.
        let receipt_payload =
            DeliveryPayload::success("dest", DeliveryMethod::Noop, /* bytes */ None);
        let receipt =
            ChainEventFactory::delivery_event(WriterId::from(downstream), receipt_payload)
                .with_causality(CausalityContext::with_parent(EventId::new()));

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
            json!({"a": 1}),
        );
        contract.on_read(&data, &mut read_ctx);

        let progress = ChainEventFactory::consumption_progress_event(
            crate::WriterId::from(upstream),
            ConsumptionProgressEventParams {
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

        let mut data =
            ChainEventFactory::data_event(crate::WriterId::from(upstream), "test.event", json!({}));
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

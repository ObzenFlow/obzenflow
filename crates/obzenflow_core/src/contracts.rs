// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::payloads::delivery_payload::DeliveryResult;
use crate::event::{types::SeqNo, ChainEvent, ChainEventContent, EventId};
use crate::id::StageId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::any::{Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

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
        inputs_observed: u64,
        accounted_for: u64,
    },
    /// Generic string message for future / ad-hoc contracts.
    Other(String),
}

/// A single hash mismatch between write/read sides.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashMismatch {
    pub index: u64,
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
    pub fn new() -> Self {
        Self
    }
}

impl Contract for TransportContract {
    fn name(&self) -> &str {
        "TransportContract"
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
    pub fn new() -> Self {
        Self
    }
}

impl Contract for SourceContract {
    fn name(&self) -> &str {
        "SourceContract"
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

impl Contract for DeliveryContract {
    fn name(&self) -> &str {
        "DeliveryContract"
    }

    fn on_write(&self, event: &ChainEvent, _ctx: &mut ContractWriteContext) {
        let ChainEventContent::Delivery(payload) = &event.content else {
            return;
        };

        // Receipts must identify the consumed event via the immediate causality parent.
        let Some(parent_id) = event.causality.parent_ids.first().copied() else {
            return;
        };

        let mut st = self.state.lock().expect("DeliveryContract state poisoned");

        if !st.pending.remove(&parent_id) {
            st.orphan_deliveries = st.orphan_deliveries.saturating_add(1);
            return;
        }

        st.receipted_total = st.receipted_total.saturating_add(1);
        match &payload.result {
            DeliveryResult::Success { .. } => {
                st.success_count = st.success_count.saturating_add(1);
            }
            DeliveryResult::Partial { .. } => {
                st.partial_count = st.partial_count.saturating_add(1);
            }
            DeliveryResult::Failed { final_attempt, .. } => {
                st.failed_count = st.failed_count.saturating_add(1);
                if *final_attempt {
                    st.failed_final_count = st.failed_final_count.saturating_add(1);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::context::causality_context::CausalityContext;
    use crate::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use crate::event::ChainEventFactory;
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
}

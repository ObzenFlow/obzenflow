// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::{types::SeqNo, ChainEvent, EventId};
use crate::id::StageId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::any::{Any, TypeId};
use std::collections::HashMap;

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

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
    ContentMismatch {
        mismatches: Vec<HashMismatch>,
    },
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
        self.inner
            .insert(TypeId::of::<T>(), Box::new(value) as Box<dyn Any + Send + Sync>);
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

impl TransportContract {
    pub fn new() -> Self {
        Self
    }
}

impl Contract for TransportContract {
    fn name(&self) -> &str {
        "transport"
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
                writer_seq, ..
            },
        ) = &event.content
        {
            if let Some(seq) = writer_seq {
                let counter = ctx
                    .state
                    .get_or_insert_with::<WriterCount, _>(WriterCount::default);
                counter.0 = seq.0;
            }
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

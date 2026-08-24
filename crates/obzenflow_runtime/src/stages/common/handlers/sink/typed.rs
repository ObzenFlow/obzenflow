// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed sink authoring and runtime erasure (FLOWIP-134h).

use super::error::{
    SinkOperationError, SinkOperationResult, SinkWriteFailure, SinkWriteFailureDisposition,
    SinkWriteResult,
};
use super::traits::{
    CommitReceipt, SinkConsumeReport, SinkHandler, SinkLifecycleReport, SinkSettlementCommit,
};
use crate::stages::common::handler_error::{HandlerError, StageFatal};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{
    DeliveryMethod, DeliveryPayload, DeliveryResult,
};
use obzenflow_core::event::{ChainEventContent, StageFatalCode, StageFatalReason};
use obzenflow_core::{ChainEvent, EventId, StageId, TypedPayload};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

static NEXT_PENDING_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

/// FLOWIP-120i per-delivery provenance projection.
#[derive(Debug, Clone)]
pub struct DeliveryContext {
    provenance: DeliveryProvenance,
}

impl DeliveryContext {
    fn from_event_and_scope(
        event: &ChainEvent,
        scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> Self {
        Self {
            provenance: if scope.is_deterministic_replay() || event.replay_context.is_some() {
                DeliveryProvenance::Replayed
            } else {
                DeliveryProvenance::Live
            },
        }
    }

    /// Return whether this delivery is live or reconstructed from an archive.
    pub fn provenance(&self) -> DeliveryProvenance {
        self.provenance
    }

    /// Return `true` when the input is being reconstructed from an archive.
    pub fn is_replayed(&self) -> bool {
        matches!(self.provenance, DeliveryProvenance::Replayed)
    }
}

/// Whether an input is live or reconstructed from a recorded run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeliveryProvenance {
    /// The input belongs to the current live run.
    Live,
    /// The input was reconstructed from a recorded run.
    Replayed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingIdentity {
    registry_id: u64,
    stage_id: StageId,
    nonce: u64,
}

/// Runtime-authored, non-settlement witness for the deferred input that was
/// the subject of a failed sink operation.
///
/// The connector can copy this only through `PendingSinkInput`; the sealed
/// adapter validates it against the live pending registry before exposing its
/// event identity to durable evidence.
#[derive(Clone, Copy)]
pub(super) struct PendingOperationSubject {
    identity: PendingIdentity,
    parent_event_id: EventId,
}

impl std::fmt::Debug for PendingOperationSubject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingOperationSubject")
            .field("stage_scoped", &true)
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
struct PendingRegistry {
    registry_id: u64,
    stage_id: StageId,
    next_nonce: u64,
    phases: HashMap<u64, PendingPhase>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingPhase {
    Current,
    Deferred,
}

fn lock_pending_registry(registry: &Mutex<PendingRegistry>) -> MutexGuard<'_, PendingRegistry> {
    match registry.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

struct CurrentPendingGuard {
    registry: Arc<Mutex<PendingRegistry>>,
    identity: PendingIdentity,
    completed: bool,
}

impl CurrentPendingGuard {
    fn new(registry: Arc<Mutex<PendingRegistry>>, identity: PendingIdentity) -> Self {
        Self {
            registry,
            identity,
            completed: false,
        }
    }

    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for CurrentPendingGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        // This guard also runs while unwinding a handler panic. Recover the
        // small runtime-owned registry rather than panicking a second time.
        lock_pending_registry(&self.registry).abandon(self.identity);
    }
}

impl PendingRegistry {
    fn new(stage_id: StageId) -> Self {
        Self {
            registry_id: NEXT_PENDING_REGISTRY_ID.fetch_add(1, Ordering::Relaxed),
            stage_id,
            next_nonce: 0,
            phases: HashMap::new(),
        }
    }

    fn mint(&mut self, parent_event_id: EventId) -> PendingSinkInput {
        let nonce = self.next_nonce;
        self.next_nonce = self.next_nonce.wrapping_add(1);
        let previous = self.phases.insert(nonce, PendingPhase::Current);
        debug_assert!(previous.is_none());
        PendingSinkInput {
            identity: PendingIdentity {
                registry_id: self.registry_id,
                stage_id: self.stage_id,
                nonce,
            },
            parent_event_id,
        }
    }

    fn defer(&mut self, pending: &PendingSinkInput) {
        debug_assert_eq!(pending.identity.registry_id, self.registry_id);
        debug_assert_eq!(pending.identity.stage_id, self.stage_id);
        if pending.identity.registry_id != self.registry_id
            || pending.identity.stage_id != self.stage_id
        {
            return;
        }

        // `SinkWriteContext::defer` is intentionally infallible. Once the
        // adapter closes the current input, a retained context may still hand
        // its caller an opaque token, but it can never recreate settlement
        // authority in this registry.
        if let Some(phase @ PendingPhase::Current) = self.phases.get_mut(&pending.identity.nonce) {
            *phase = PendingPhase::Deferred;
        }
    }

    fn abandon(&mut self, identity: PendingIdentity) {
        if identity.registry_id == self.registry_id && identity.stage_id == self.stage_id {
            self.phases.remove(&identity.nonce);
        }
    }

    fn validate_identity(&self, identity: PendingIdentity) -> Result<(), HandlerError> {
        if identity.stage_id != self.stage_id {
            return Err(protocol_fatal(format!(
                "foreign sink settlement capability for stage {} submitted to stage {}",
                identity.stage_id, self.stage_id
            )));
        }
        if identity.registry_id != self.registry_id {
            return Err(protocol_fatal(
                "stale sink settlement capability from another adapter instance",
            ));
        }
        Ok(())
    }

    fn validate_operation_subject(
        &self,
        current: Option<PendingIdentity>,
        subject: PendingOperationSubject,
    ) -> Result<EventId, HandlerError> {
        self.validate_identity(subject.identity)?;
        if current == Some(subject.identity) {
            return Err(protocol_fatal(
                "current sink input cannot be its own deferred operation subject",
            ));
        }

        match self.phases.get(&subject.identity.nonce) {
            Some(PendingPhase::Deferred) => Ok(subject.parent_event_id),
            Some(PendingPhase::Current) => Err(protocol_fatal(
                "sink operation subject is not a deferred input",
            )),
            None => Err(protocol_fatal(
                "stale, settled, or closed sink operation subject",
            )),
        }
    }

    /// Validate the complete returned capability set before consuming any of
    /// it, then remove the set as one registry mutation.
    fn validate_write_batch(
        &self,
        current: PendingIdentity,
        primary_is_buffered: bool,
        settlements: &[PendingIdentity],
    ) -> Result<(), HandlerError> {
        self.validate_identity(current)?;
        let expected_current = if primary_is_buffered {
            PendingPhase::Deferred
        } else {
            PendingPhase::Current
        };
        match self.phases.get(&current.nonce) {
            Some(phase) if *phase == expected_current => {}
            Some(PendingPhase::Deferred) => {
                return Err(protocol_fatal(
                    "sink returned a terminal primary outcome after deferring the input",
                ));
            }
            Some(PendingPhase::Current) => {
                return Err(protocol_fatal(
                    "sink returned a buffered primary outcome without deferring the input",
                ));
            }
            None => {
                return Err(protocol_fatal(
                    "sink primary input capability was already closed",
                ));
            }
        }

        let mut unique = HashSet::with_capacity(settlements.len());
        for identity in settlements {
            self.validate_identity(*identity)?;
            if !unique.insert(identity.nonce) {
                return Err(protocol_fatal(
                    "duplicate sink settlement capability in one report",
                ));
            }
            match self.phases.get(&identity.nonce) {
                Some(PendingPhase::Deferred) => {}
                Some(PendingPhase::Current) => {
                    return Err(protocol_fatal("non-deferred sink settlement capability"));
                }
                None => {
                    return Err(protocol_fatal(
                        "duplicate, stale, or closed sink settlement capability",
                    ));
                }
            }
        }

        Ok(())
    }

    fn commit_write_batch(
        &mut self,
        current: PendingIdentity,
        primary_is_buffered: bool,
        settlements: &[PendingIdentity],
    ) -> Result<(), HandlerError> {
        self.validate_write_batch(current, primary_is_buffered, settlements)?;
        if !primary_is_buffered {
            self.phases.remove(&current.nonce);
        }
        for identity in settlements {
            self.phases.remove(&identity.nonce);
        }
        Ok(())
    }

    fn validate_lifecycle_batch(
        &self,
        settlements: &[PendingIdentity],
    ) -> Result<(), HandlerError> {
        let mut unique = HashSet::with_capacity(settlements.len());
        for identity in settlements {
            self.validate_identity(*identity)?;
            if !unique.insert(identity.nonce) {
                return Err(protocol_fatal(
                    "duplicate sink settlement capability in one lifecycle report",
                ));
            }
            match self.phases.get(&identity.nonce) {
                Some(PendingPhase::Deferred) => {}
                Some(PendingPhase::Current) => {
                    return Err(protocol_fatal("non-deferred sink settlement capability"));
                }
                None => {
                    return Err(protocol_fatal(
                        "duplicate, stale, or closed sink settlement capability",
                    ));
                }
            }
        }
        Ok(())
    }

    fn commit_lifecycle_batch(
        &mut self,
        settlements: &[PendingIdentity],
    ) -> Result<(), HandlerError> {
        self.validate_lifecycle_batch(settlements)?;
        for identity in settlements {
            self.phases.remove(&identity.nonce);
        }
        Ok(())
    }

    #[cfg(test)]
    fn settle(&mut self, pending: PendingSinkInput) -> Result<EventId, HandlerError> {
        let identity = pending.identity;
        self.commit_lifecycle_batch(&[identity])?;
        Ok(pending.parent_event_id)
    }

    #[cfg(test)]
    fn is_outstanding(&self, identity: PendingIdentity) -> bool {
        identity.registry_id == self.registry_id
            && identity.stage_id == self.stage_id
            && matches!(
                self.phases.get(&identity.nonce),
                Some(PendingPhase::Deferred)
            )
    }
}

struct WriteSettlementCommit {
    registry: Arc<Mutex<PendingRegistry>>,
    current: PendingIdentity,
    primary_is_buffered: bool,
    settlements: Vec<PendingIdentity>,
}

impl SinkSettlementCommit for WriteSettlementCommit {
    fn commit(self: Box<Self>) -> Result<(), HandlerError> {
        lock_pending_registry(&self.registry).commit_write_batch(
            self.current,
            self.primary_is_buffered,
            &self.settlements,
        )
    }
}

struct LifecycleSettlementCommit {
    registry: Arc<Mutex<PendingRegistry>>,
    settlements: Vec<PendingIdentity>,
}

impl SinkSettlementCommit for LifecycleSettlementCommit {
    fn commit(self: Box<Self>) -> Result<(), HandlerError> {
        lock_pending_registry(&self.registry).commit_lifecycle_batch(&self.settlements)
    }
}

fn protocol_fatal(detail: impl Into<String>) -> HandlerError {
    HandlerError::Fatal(StageFatal::new(
        StageFatalCode::Protocol,
        StageFatalReason::ProtocolInputIntegrity,
        detail,
    ))
}

/// Opaque, stage-scoped authority to settle one buffered input.
///
/// This capability is intentionally neither `Clone` nor serialisable. The
/// runtime is the only constructor and the erased adapter is the only reader.
#[must_use = "a deferred sink input must be returned in a typed commit receipt"]
pub struct PendingSinkInput {
    identity: PendingIdentity,
    parent_event_id: EventId,
}

impl std::fmt::Debug for PendingSinkInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingSinkInput")
            .field("stage_scoped", &true)
            .finish_non_exhaustive()
    }
}

impl PendingSinkInput {
    pub(super) fn operation_subject(&self) -> PendingOperationSubject {
        PendingOperationSubject {
            identity: self.identity,
            parent_event_id: self.parent_event_id,
        }
    }
}

/// Per-input typed sink context.
pub struct SinkWriteContext {
    delivery: DeliveryContext,
    pending: PendingSinkInput,
    registry: Arc<Mutex<PendingRegistry>>,
}

impl std::fmt::Debug for SinkWriteContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkWriteContext")
            .field("delivery", &self.delivery)
            .finish_non_exhaustive()
    }
}

impl SinkWriteContext {
    /// Borrow the read-only delivery provenance for this input.
    pub fn delivery(&self) -> &DeliveryContext {
        &self.delivery
    }

    /// Retain the single-use settlement authority for buffered work.
    ///
    /// A handler returning a buffered primary outcome must keep this value and
    /// return it in a [`SinkCommitReceipt`] only after the destination commits.
    pub fn defer(self) -> PendingSinkInput {
        let Self {
            pending, registry, ..
        } = self;
        lock_pending_registry(&registry).defer(&pending);
        pending
    }
}

/// Success or partial-success evidence for a terminal input.
#[derive(Debug, Clone)]
pub struct SinkTerminalOutcome {
    payload: DeliveryPayload,
    method_override: Option<DeliveryMethod>,
}

impl SinkTerminalOutcome {
    /// Describe successful terminal evidence using the connector's normal
    /// receipt method.
    pub fn success(bytes_processed: Option<u64>) -> Self {
        Self {
            payload: DeliveryPayload::success(DeliveryMethod::Noop, bytes_processed),
            method_override: None,
        }
    }

    /// Describe successful terminal evidence using a per-attempt method.
    pub fn success_via(method: DeliveryMethod, bytes_processed: Option<u64>) -> Self {
        Self {
            payload: DeliveryPayload::success(method.clone(), bytes_processed),
            method_override: Some(method),
        }
    }

    /// Describe partially successful evidence using the connector's normal
    /// receipt method.
    pub fn partial(
        successful_count: u64,
        failed_count: u64,
        error_summary: impl Into<String>,
        failed_items: Option<Vec<String>>,
    ) -> Self {
        Self {
            payload: DeliveryPayload::partial(
                DeliveryMethod::Noop,
                successful_count,
                failed_count,
                error_summary,
                failed_items,
            ),
            method_override: None,
        }
    }

    /// Describe partially successful evidence using a per-attempt method.
    pub fn partial_via(
        method: DeliveryMethod,
        successful_count: u64,
        failed_count: u64,
        error_summary: impl Into<String>,
        failed_items: Option<Vec<String>>,
    ) -> Self {
        Self {
            payload: DeliveryPayload::partial(
                method.clone(),
                successful_count,
                failed_count,
                error_summary,
                failed_items,
            ),
            method_override: Some(method),
        }
    }

    /// Attach the number of successfully delivered items.
    pub fn with_items(mut self, items: u64) -> Self {
        self.payload = self.payload.with_items(items);
        self
    }

    /// Attach structured middleware context to the terminal evidence.
    pub fn with_middleware_context(mut self, context: Value) -> Self {
        self.payload = self.payload.with_middleware_context(context);
        self
    }
}

/// Provisional evidence for an accepted but not yet committed input.
#[derive(Debug, Clone)]
pub struct SinkBufferedOutcome {
    payload: DeliveryPayload,
    method_override: Option<DeliveryMethod>,
}

impl SinkBufferedOutcome {
    /// Describe provisional evidence using the connector's normal receipt
    /// method.
    pub fn accepted(bytes_processed: Option<u64>) -> Self {
        Self {
            payload: DeliveryPayload::buffered(DeliveryMethod::Noop, bytes_processed),
            method_override: None,
        }
    }

    /// Describe provisional evidence using a per-attempt method.
    pub fn accepted_via(method: DeliveryMethod, bytes_processed: Option<u64>) -> Self {
        Self {
            payload: DeliveryPayload::buffered(method.clone(), bytes_processed),
            method_override: Some(method),
        }
    }

    /// Attach structured middleware context to the provisional evidence.
    pub fn with_middleware_context(mut self, context: Value) -> Self {
        self.payload = self.payload.with_middleware_context(context);
        self
    }
}

/// Lifecycle-only delivery evidence. It cannot settle an input.
#[derive(Debug, Clone)]
pub struct SinkAuditOutcome {
    payload: DeliveryPayload,
    method_override: Option<DeliveryMethod>,
}

impl SinkAuditOutcome {
    /// Describe a successful lifecycle action using the connector's normal
    /// receipt method.
    pub fn success(bytes_processed: Option<u64>) -> Self {
        Self {
            payload: DeliveryPayload::success(DeliveryMethod::Noop, bytes_processed),
            method_override: None,
        }
    }

    /// Describe a successful lifecycle action using a per-attempt method.
    pub fn success_via(method: DeliveryMethod, bytes_processed: Option<u64>) -> Self {
        Self {
            payload: DeliveryPayload::success(method.clone(), bytes_processed),
            method_override: Some(method),
        }
    }

    /// Describe a partially successful lifecycle action using the connector's
    /// normal receipt method.
    pub fn partial(
        successful_count: u64,
        failed_count: u64,
        error_summary: impl Into<String>,
        failed_items: Option<Vec<String>>,
    ) -> Self {
        Self {
            payload: DeliveryPayload::partial(
                DeliveryMethod::Noop,
                successful_count,
                failed_count,
                error_summary,
                failed_items,
            ),
            method_override: None,
        }
    }

    /// Describe a partially successful lifecycle action using a per-attempt
    /// method.
    pub fn partial_via(
        method: DeliveryMethod,
        successful_count: u64,
        failed_count: u64,
        error_summary: impl Into<String>,
        failed_items: Option<Vec<String>>,
    ) -> Self {
        Self {
            payload: DeliveryPayload::partial(
                method.clone(),
                successful_count,
                failed_count,
                error_summary,
                failed_items,
            ),
            method_override: Some(method),
        }
    }

    /// Attach the number of items handled by the lifecycle action.
    pub fn with_items(mut self, items: u64) -> Self {
        self.payload = self.payload.with_items(items);
        self
    }

    /// Attach structured middleware context to the lifecycle evidence.
    pub fn with_middleware_context(mut self, context: Value) -> Self {
        self.payload = self.payload.with_middleware_context(context);
        self
    }
}

/// The primary per-input evidence returned by a typed sink.
#[derive(Debug, Clone)]
pub enum SinkPrimaryOutcome {
    /// The input reached a terminal destination outcome during `write`.
    Terminal(SinkTerminalOutcome),
    /// The input remains pending and will be settled by a later commit receipt.
    Buffered(SinkBufferedOutcome),
}

/// A terminal receipt paired with the exact buffered input capability.
#[derive(Debug)]
pub struct SinkCommitReceipt {
    pending: PendingSinkInput,
    outcome: SinkTerminalOutcome,
}

impl SinkCommitReceipt {
    /// Pair a deferred input capability with its terminal delivery evidence.
    pub fn new(pending: PendingSinkInput, outcome: SinkTerminalOutcome) -> Self {
        Self { pending, outcome }
    }
}

/// Typed evidence returned after writing one input.
#[derive(Debug)]
pub struct SinkWriteReport {
    primary: SinkPrimaryOutcome,
    commit_receipts: Vec<SinkCommitReceipt>,
}

impl SinkWriteReport {
    /// Build a report for an input that reached a terminal outcome immediately.
    pub fn terminal(outcome: SinkTerminalOutcome) -> Self {
        Self {
            primary: SinkPrimaryOutcome::Terminal(outcome),
            commit_receipts: Vec::new(),
        }
    }

    /// Build a report for an input retained for later settlement.
    pub fn buffered(outcome: SinkBufferedOutcome) -> Self {
        Self {
            primary: SinkPrimaryOutcome::Buffered(outcome),
            commit_receipts: Vec::new(),
        }
    }

    /// Add one terminal receipt for previously deferred work.
    pub fn with_commit_receipt(mut self, receipt: SinkCommitReceipt) -> Self {
        self.commit_receipts.push(receipt);
        self
    }

    /// Add terminal receipts for previously deferred work.
    pub fn with_commit_receipts(
        mut self,
        receipts: impl IntoIterator<Item = SinkCommitReceipt>,
    ) -> Self {
        self.commit_receipts.extend(receipts);
        self
    }
}

/// Typed lifecycle evidence returned from `flush` or `drain`.
#[derive(Debug, Default)]
pub struct SinkWriterLifecycleReport {
    audit_outcome: Option<SinkAuditOutcome>,
    commit_receipts: Vec<SinkCommitReceipt>,
}

impl SinkWriterLifecycleReport {
    /// Build a lifecycle report with audit-only evidence.
    pub fn audit(outcome: SinkAuditOutcome) -> Self {
        Self {
            audit_outcome: Some(outcome),
            commit_receipts: Vec::new(),
        }
    }

    /// Add one terminal receipt for previously deferred work.
    pub fn with_commit_receipt(mut self, receipt: SinkCommitReceipt) -> Self {
        self.commit_receipts.push(receipt);
        self
    }

    /// Add terminal receipts for previously deferred work.
    pub fn with_commit_receipts(
        mut self,
        receipts: impl IntoIterator<Item = SinkCommitReceipt>,
    ) -> Self {
        self.commit_receipts.extend(receipts);
        self
    }
}

/// Mutable, stage-local execution role created by a [`SinkConnector`](super::connector::SinkConnector).
///
/// The runtime invokes `write` once for each admitted physical treatment. Policy
/// code can reject before that call and can observe its retained result afterwards,
/// but cannot receive the writer future or replace the result. A successful report
/// is validated as one atomic capability set before any capability is consumed.
/// The runtime then journals its primary receipt and commit receipts in report order.
///
/// `flush` runs after normal input exhaustion and may settle retained work. `drain`
/// runs during graceful cleanup. Their complete receipts precede contract
/// verification and completion. Once a write is poisoned, or a lifecycle operation
/// fails, the runtime performs drop-only teardown and calls neither lifecycle method
/// again.
#[async_trait]
#[diagnostic::on_unimplemented(
    message = "this sink writer does not witness its connector input",
    note = "implement SinkWriter with Input matching SinkConnector::Input (FLOWIP-134h)"
)]
pub trait SinkWriter: Send + Sync + 'static {
    /// The sole input type this writer accepts.
    type Input: TypedPayload + Send + Sync + 'static;

    /// Write one decoded data input and return typed delivery evidence.
    async fn write(&mut self, input: Self::Input, context: SinkWriteContext) -> SinkWriteResult;

    /// Flush buffered work and return lifecycle evidence and commit receipts.
    async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        Ok(SinkWriterLifecycleReport::default())
    }

    /// Drain outstanding work before shutdown.
    async fn drain(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.flush().await
    }
}

/// Sole bridge from a typed sink writer to the journal sink's erased input.
#[doc(hidden)]
pub struct SinkWriterAdapter<W> {
    writer: W,
    stage_id: StageId,
    default_method: Option<DeliveryMethod>,
    registry: Arc<Mutex<PendingRegistry>>,
}

impl<W> std::fmt::Debug for SinkWriterAdapter<W> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkWriterAdapter")
            .field("writer_type", &std::any::type_name::<W>())
            .field("stage_id", &self.stage_id)
            .finish_non_exhaustive()
    }
}

impl<W> SinkWriterAdapter<W> {
    pub fn new(writer: W, stage_id: StageId) -> Self {
        Self::with_default_method(writer, stage_id, None)
    }

    pub fn with_default_method(
        writer: W,
        stage_id: StageId,
        default_method: Option<DeliveryMethod>,
    ) -> Self {
        Self {
            writer,
            stage_id,
            default_method,
            registry: Arc::new(Mutex::new(PendingRegistry::new(stage_id))),
        }
    }

    fn resolve_method(
        &self,
        mut payload: DeliveryPayload,
        method_override: Option<DeliveryMethod>,
    ) -> Result<DeliveryPayload, HandlerError> {
        let method = method_override
            .or_else(|| self.default_method.clone())
            .ok_or_else(|| {
                protocol_fatal(
                    "sink outcome has no delivery method; configure a connector default or use a *_via outcome",
                )
            })?;
        payload.delivery_method = method;
        Ok(payload)
    }

    fn lower_terminal(
        &self,
        outcome: SinkTerminalOutcome,
    ) -> Result<DeliveryPayload, HandlerError> {
        debug_assert!(matches!(
            outcome.payload.result,
            DeliveryResult::Success { .. } | DeliveryResult::Partial { .. }
        ));
        self.resolve_method(outcome.payload, outcome.method_override)
    }

    fn lower_write_report(
        &self,
        current: PendingIdentity,
        report: SinkWriteReport,
    ) -> Result<SinkConsumeReport, HandlerError> {
        let SinkWriteReport {
            primary,
            commit_receipts: returned_receipts,
        } = report;
        let primary_is_buffered = matches!(primary, SinkPrimaryOutcome::Buffered(_));
        let primary = match primary {
            SinkPrimaryOutcome::Terminal(outcome) => self.lower_terminal(outcome)?,
            SinkPrimaryOutcome::Buffered(outcome) => {
                self.resolve_method(outcome.payload, outcome.method_override)?
            }
        };

        let mut commit_receipts = Vec::with_capacity(returned_receipts.len());
        let mut settlement_ids = Vec::with_capacity(returned_receipts.len());
        for receipt in returned_receipts {
            settlement_ids.push(receipt.pending.identity);
            commit_receipts.push(CommitReceipt {
                parent_event_id: receipt.pending.parent_event_id,
                payload: self.lower_terminal(receipt.outcome)?,
            });
        }
        lock_pending_registry(&self.registry).validate_write_batch(
            current,
            primary_is_buffered,
            &settlement_ids,
        )?;

        let mut report = SinkConsumeReport::new(primary);
        report.commit_receipts = commit_receipts;
        Ok(report.with_settlement(Box::new(WriteSettlementCommit {
            registry: Arc::clone(&self.registry),
            current,
            primary_is_buffered,
            settlements: settlement_ids,
        })))
    }

    fn lower_lifecycle_report(
        &self,
        report: SinkWriterLifecycleReport,
    ) -> Result<SinkLifecycleReport, HandlerError> {
        let SinkWriterLifecycleReport {
            audit_outcome,
            commit_receipts: returned_receipts,
        } = report;
        let audit_payload = audit_outcome
            .map(|outcome| self.resolve_method(outcome.payload, outcome.method_override))
            .transpose()?;
        let mut commit_receipts = Vec::with_capacity(returned_receipts.len());
        let mut settlement_ids = Vec::with_capacity(returned_receipts.len());
        for receipt in returned_receipts {
            settlement_ids.push(receipt.pending.identity);
            commit_receipts.push(CommitReceipt {
                parent_event_id: receipt.pending.parent_event_id,
                payload: self.lower_terminal(receipt.outcome)?,
            });
        }
        lock_pending_registry(&self.registry).validate_lifecycle_batch(&settlement_ids)?;
        Ok(
            SinkLifecycleReport::new(audit_payload, commit_receipts).with_settlement(Box::new(
                LifecycleSettlementCommit {
                    registry: Arc::clone(&self.registry),
                    settlements: settlement_ids,
                },
            )),
        )
    }

    fn validate_operation_error(
        &self,
        current: Option<PendingIdentity>,
        error: &mut SinkOperationError,
    ) -> Result<(), HandlerError> {
        let subject = error
            .take_pending_operation_subject()
            .map_err(|()| protocol_fatal("sink operation subject was already runtime-validated"))?;
        if let Some(subject) = subject {
            let event_id = lock_pending_registry(&self.registry)
                .validate_operation_subject(current, subject)?;
            error.set_validated_operation_subject(event_id);
        }
        Ok(())
    }

    fn validate_write_failure(
        &self,
        current: PendingIdentity,
        failure: &mut SinkWriteFailure,
    ) -> Result<(), HandlerError> {
        if failure.has_pending_operation_subject()
            && failure.disposition() != SinkWriteFailureDisposition::Poisoned
        {
            return Err(protocol_fatal(
                "a deferred sink operation subject requires a poisoned write failure",
            ));
        }
        self.validate_operation_error(Some(current), failure.error_mut())
    }

    async fn consume_report_in_scope(
        &mut self,
        event: ChainEvent,
        scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> Result<SinkConsumeReport, HandlerError>
    where
        W: SinkWriter,
    {
        let (event_type, payload) = match &event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => (event_type.as_str(), payload),
            _ => {
                return Ok(SinkConsumeReport::new(DeliveryPayload::success(
                    DeliveryMethod::Custom("Skipped".to_string()),
                    None,
                )))
            }
        };

        if !W::Input::event_type_matches(event_type) {
            return Err(HandlerError::Validation(format!(
                "SinkWriter expected event type '{}' (or '{}'), got '{}'",
                W::Input::EVENT_TYPE,
                W::Input::versioned_event_type(),
                event_type
            )));
        }

        let input: W::Input = serde_json::from_value(payload.clone()).map_err(|error| {
            HandlerError::Deserialization(format!(
                "SinkWriter failed to deserialize {}: {error}",
                std::any::type_name::<W::Input>()
            ))
        })?;

        let pending = lock_pending_registry(&self.registry).mint(event.id);
        let current = pending.identity;
        let context = SinkWriteContext {
            delivery: DeliveryContext::from_event_and_scope(&event, scope),
            pending,
            registry: Arc::clone(&self.registry),
        };
        let mut guard = CurrentPendingGuard::new(Arc::clone(&self.registry), current);

        let report = match self.writer.write(input, context).await {
            Ok(report) => report,
            Err(mut failure) => {
                self.validate_write_failure(current, &mut failure)?;
                return Err(HandlerError::SinkWrite(Box::new(failure)));
            }
        };
        let lowered = self.lower_write_report(current, report)?;
        guard.complete();
        Ok(lowered)
    }
}

#[async_trait]
impl<W> SinkHandler for SinkWriterAdapter<W>
where
    W: SinkWriter,
{
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(self.consume_report(event).await?.primary)
    }

    async fn consume_report(
        &mut self,
        event: ChainEvent,
    ) -> Result<SinkConsumeReport, HandlerError> {
        self.consume_report_in_scope(event, obzenflow_core::MiddlewareExecutionScope::default())
            .await
    }

    async fn consume_report_with_scope(
        &mut self,
        event: ChainEvent,
        scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> Result<SinkConsumeReport, HandlerError> {
        self.consume_report_in_scope(event, scope).await
    }

    async fn flush(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        Ok(self.flush_report().await?.audit_payload)
    }

    async fn flush_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        let report = match self.writer.flush().await {
            Ok(report) => report,
            Err(mut error) => {
                self.validate_operation_error(None, &mut error)?;
                return Err(HandlerError::SinkOperation(Box::new(error)));
            }
        };
        self.lower_lifecycle_report(report)
    }

    async fn drain(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        Ok(self.drain_report().await?.audit_payload)
    }

    async fn drain_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        let report = match self.writer.drain().await {
            Ok(report) => report,
            Err(mut error) => {
                self.validate_operation_error(None, &mut error)?;
                return Err(HandlerError::SinkOperation(Box::new(error)));
            }
        };
        self.lower_lifecycle_report(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::WriterId;
    use serde::{Deserialize, Serialize};
    use std::panic::AssertUnwindSafe;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Input {
        value: u64,
    }

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "sink.typed.input";
    }

    #[derive(Clone, Debug)]
    struct Buffered {
        pending: Arc<Mutex<Vec<PendingSinkInput>>>,
    }

    #[async_trait]
    impl SinkWriter for Buffered {
        type Input = Input;

        async fn write(&mut self, _input: Input, context: SinkWriteContext) -> SinkWriteResult {
            self.pending
                .lock()
                .expect("pending lock poisoned")
                .push(context.defer());
            Ok(SinkWriteReport::buffered(
                SinkBufferedOutcome::accepted_via(DeliveryMethod::Noop, None),
            ))
        }

        async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
            let receipts = self
                .pending
                .lock()
                .expect("pending lock poisoned")
                .drain(..)
                .map(|pending| {
                    SinkCommitReceipt::new(
                        pending,
                        SinkTerminalOutcome::success_via(DeliveryMethod::Noop, None),
                    )
                })
                .collect::<Vec<_>>();
            Ok(SinkWriterLifecycleReport::default().with_commit_receipts(receipts))
        }
    }

    fn event(value: u64) -> ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            Input::versioned_event_type(),
            serde_json::json!({ "value": value }),
        )
    }

    #[derive(Clone, Debug)]
    struct UsesConnectorMethod;

    #[async_trait]
    impl SinkWriter for UsesConnectorMethod {
        type Input = Input;

        async fn write(&mut self, _input: Input, _context: SinkWriteContext) -> SinkWriteResult {
            Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success(
                Some(7),
            )))
        }
    }

    #[tokio::test]
    async fn connector_default_supplies_the_normal_receipt_method() {
        let expected = DeliveryMethod::Custom("connector.default".to_string());
        let mut adapter = SinkWriterAdapter::with_default_method(
            UsesConnectorMethod,
            StageId::new(),
            Some(expected.clone()),
        );

        let report = adapter
            .consume_report(event(1))
            .await
            .expect("connector default resolves the terminal method");

        assert_eq!(report.primary.delivery_method, expected);
        assert_eq!(report.primary.bytes_processed, Some(7));
    }

    #[tokio::test]
    async fn missing_default_and_attempt_override_fail_loud() {
        let mut adapter = SinkWriterAdapter::new(UsesConnectorMethod, StageId::new());

        let error = adapter
            .consume_report(event(1))
            .await
            .expect_err("an unresolved receipt method is a protocol error");

        assert!(matches!(error, HandlerError::Fatal(_)));
        assert!(error.to_string().contains("no delivery method"));
    }

    #[tokio::test]
    async fn buffered_capability_lowers_to_the_original_parent() {
        let pending = Arc::new(Mutex::new(Vec::new()));
        let mut adapter = SinkWriterAdapter::new(
            Buffered {
                pending: Arc::clone(&pending),
            },
            StageId::new(),
        );
        let input = event(7);
        let parent_event_id = input.id;
        let consume = adapter.consume_report(input).await.expect("consume report");
        assert!(matches!(
            consume.primary.result,
            DeliveryResult::Buffered { .. }
        ));

        let lifecycle = adapter.flush_report().await.expect("flush report");
        assert_eq!(lifecycle.commit_receipts.len(), 1);
        assert_eq!(
            lifecycle.commit_receipts[0].parent_event_id,
            parent_event_id
        );
        assert!(matches!(
            lifecycle.commit_receipts[0].payload.result,
            DeliveryResult::Success { .. }
        ));
    }

    #[tokio::test]
    async fn buffered_primary_requires_deferral() {
        #[derive(Clone, Debug)]
        struct Invalid;

        #[async_trait]
        impl SinkWriter for Invalid {
            type Input = Input;

            async fn write(
                &mut self,
                _input: Input,
                _context: SinkWriteContext,
            ) -> SinkWriteResult {
                Ok(SinkWriteReport::buffered(
                    SinkBufferedOutcome::accepted_via(DeliveryMethod::Noop, None),
                ))
            }
        }

        let mut adapter = SinkWriterAdapter::new(Invalid, StageId::new());
        let error = adapter
            .consume_report(event(1))
            .await
            .expect_err("missing deferral is fatal");
        assert!(matches!(error, HandlerError::Fatal(_)));
    }

    #[tokio::test]
    async fn terminal_primary_rejects_a_deferred_current_input() {
        #[derive(Clone, Debug)]
        struct Invalid;

        #[async_trait]
        impl SinkWriter for Invalid {
            type Input = Input;

            async fn write(&mut self, _input: Input, context: SinkWriteContext) -> SinkWriteResult {
                drop(context.defer());
                Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
                    DeliveryMethod::Noop,
                    None,
                )))
            }
        }

        let mut adapter = SinkWriterAdapter::new(Invalid, StageId::new());
        let error = adapter
            .consume_report(event(1))
            .await
            .expect_err("deferred input cannot have a terminal primary");
        assert!(matches!(error, HandlerError::Fatal(_)));
    }

    #[tokio::test]
    async fn handler_panic_revokes_a_deferred_current_capability() {
        #[derive(Clone, Debug)]
        struct PanicsAfterDeferral {
            pending: Arc<Mutex<Vec<PendingSinkInput>>>,
        }

        #[async_trait]
        impl SinkWriter for PanicsAfterDeferral {
            type Input = Input;

            async fn write(&mut self, _input: Input, context: SinkWriteContext) -> SinkWriteResult {
                self.pending
                    .lock()
                    .expect("pending lock poisoned")
                    .push(context.defer());
                panic!("intentional typed sink panic after deferral");
            }

            async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
                let receipts = self
                    .pending
                    .lock()
                    .expect("pending lock poisoned")
                    .drain(..)
                    .map(|pending| {
                        SinkCommitReceipt::new(
                            pending,
                            SinkTerminalOutcome::success_via(DeliveryMethod::Noop, None),
                        )
                    })
                    .collect::<Vec<_>>();
                Ok(SinkWriterLifecycleReport::default().with_commit_receipts(receipts))
            }
        }

        let mut adapter = SinkWriterAdapter::new(
            PanicsAfterDeferral {
                pending: Arc::new(Mutex::new(Vec::new())),
            },
            StageId::new(),
        );

        let panic = AssertUnwindSafe(adapter.consume_report(event(1)))
            .catch_unwind()
            .await;
        assert!(panic.is_err());

        let error = adapter
            .flush_report()
            .await
            .expect_err("cleanup cannot settle authority revoked during unwind");
        assert!(matches!(error, HandlerError::Fatal(_)));
    }

    #[derive(Debug)]
    struct RetainsContext {
        retained: Arc<Mutex<Option<SinkWriteContext>>>,
        fail_consume: bool,
    }

    #[async_trait]
    impl SinkWriter for RetainsContext {
        type Input = Input;

        async fn write(&mut self, _input: Input, context: SinkWriteContext) -> SinkWriteResult {
            *self
                .retained
                .lock()
                .expect("retained context lock poisoned") = Some(context);
            if self.fail_consume {
                Err(super::super::error::SinkWriteFailure::current_only(
                    obzenflow_core::event::SinkWritePhase::Execute,
                    super::super::error::SinkOperationError::other(
                        "intentional consume failure after retaining context",
                    ),
                ))
            } else {
                Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
                    DeliveryMethod::Noop,
                    None,
                )))
            }
        }

        async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
            let context = self
                .retained
                .lock()
                .expect("retained context lock poisoned")
                .take()
                .expect("consume retained a context");
            Ok(
                SinkWriterLifecycleReport::default().with_commit_receipts(vec![
                    SinkCommitReceipt::new(
                        context.defer(),
                        SinkTerminalOutcome::success_via(DeliveryMethod::Noop, None),
                    ),
                ]),
            )
        }
    }

    #[tokio::test]
    async fn retained_context_cannot_defer_after_a_terminal_return() {
        let mut adapter = SinkWriterAdapter::new(
            RetainsContext {
                retained: Arc::new(Mutex::new(None)),
                fail_consume: false,
            },
            StageId::new(),
        );

        let mut report = adapter
            .consume_report(event(1))
            .await
            .expect("terminal consume succeeds");
        report
            .commit_settlements()
            .expect("runtime commits validated terminal authority");
        let error = adapter
            .flush_report()
            .await
            .expect_err("a retained context cannot recreate closed authority");
        assert!(matches!(error, HandlerError::Fatal(_)));
    }

    #[tokio::test]
    async fn retained_context_cannot_defer_after_a_handler_error() {
        let mut adapter = SinkWriterAdapter::new(
            RetainsContext {
                retained: Arc::new(Mutex::new(None)),
                fail_consume: true,
            },
            StageId::new(),
        );

        let error = adapter
            .consume_report(event(1))
            .await
            .expect_err("consume intentionally fails");
        assert!(matches!(error, HandlerError::SinkWrite(_)));

        let error = adapter
            .flush_report()
            .await
            .expect_err("a retained context cannot recreate revoked authority");
        assert!(matches!(error, HandlerError::Fatal(_)));
    }

    #[test]
    fn settlement_registry_rejects_foreign_stale_duplicate_and_nondeferred_tokens() {
        fn duplicate_for_test(pending: &PendingSinkInput) -> PendingSinkInput {
            PendingSinkInput {
                identity: pending.identity,
                parent_event_id: pending.parent_event_id,
            }
        }

        fn fatal_detail(error: HandlerError) -> String {
            match error {
                HandlerError::Fatal(fatal) => fatal.detail,
                other => panic!("expected protocol fatal, got {other:?}"),
            }
        }

        let first_stage = StageId::new();
        let second_stage = StageId::new();
        let parent = EventId::new();

        let mut first = PendingRegistry::new(first_stage);
        let foreign = first.mint(parent);
        first.defer(&foreign);
        let mut other_stage = PendingRegistry::new(second_stage);
        assert!(fatal_detail(other_stage.settle(foreign).unwrap_err()).contains("foreign"));

        let stale = first.mint(parent);
        first.defer(&stale);
        let mut replacement = PendingRegistry::new(first_stage);
        assert!(fatal_detail(replacement.settle(stale).unwrap_err()).contains("stale"));

        let original = first.mint(parent);
        first.defer(&original);
        let duplicate = duplicate_for_test(&original);
        assert_eq!(first.settle(original).expect("first settlement"), parent);
        assert!(fatal_detail(first.settle(duplicate).unwrap_err()).contains("duplicate"));

        let nondeferred = first.mint(parent);
        assert!(fatal_detail(first.settle(nondeferred).unwrap_err()).contains("non-deferred"));
    }

    #[test]
    fn operation_subject_registry_accepts_only_live_same_stage_deferred_inputs() {
        fn fatal_detail(error: HandlerError) -> String {
            match error {
                HandlerError::Fatal(fatal) => fatal.detail,
                other => panic!("expected protocol fatal, got {other:?}"),
            }
        }

        let stage_id = StageId::new();
        let parent = EventId::new();
        let mut registry = PendingRegistry::new(stage_id);

        let deferred = registry.mint(parent);
        registry.defer(&deferred);
        assert_eq!(
            registry
                .validate_operation_subject(None, deferred.operation_subject())
                .expect("live deferred subject validates"),
            parent
        );

        let current = registry.mint(EventId::new());
        assert!(fatal_detail(
            registry
                .validate_operation_subject(None, current.operation_subject())
                .unwrap_err()
        )
        .contains("not a deferred"));
        registry.defer(&current);
        assert!(fatal_detail(
            registry
                .validate_operation_subject(Some(current.identity), current.operation_subject())
                .unwrap_err()
        )
        .contains("current sink input"));

        let foreign_stage = StageId::new();
        let foreign_registry = PendingRegistry::new(foreign_stage);
        assert!(fatal_detail(
            foreign_registry
                .validate_operation_subject(None, deferred.operation_subject())
                .unwrap_err()
        )
        .contains("foreign"));

        let replacement = PendingRegistry::new(stage_id);
        assert!(fatal_detail(
            replacement
                .validate_operation_subject(None, deferred.operation_subject())
                .unwrap_err()
        )
        .contains("stale"));

        let settled = registry.mint(EventId::new());
        registry.defer(&settled);
        let settled_subject = settled.operation_subject();
        registry.settle(settled).expect("settle deferred input");
        assert!(fatal_detail(
            registry
                .validate_operation_subject(None, settled_subject)
                .unwrap_err()
        )
        .contains("settled"));
    }

    #[tokio::test]
    async fn deferred_origin_poison_is_validated_without_settling_the_subject() {
        #[derive(Debug)]
        struct DeferredOriginPoison {
            first: Option<PendingSinkInput>,
        }

        #[async_trait]
        impl SinkWriter for DeferredOriginPoison {
            type Input = Input;

            async fn write(&mut self, _input: Input, context: SinkWriteContext) -> SinkWriteResult {
                let current = context.defer();
                let Some(first) = self.first.as_ref() else {
                    self.first = Some(current);
                    return Ok(SinkWriteReport::buffered(
                        SinkBufferedOutcome::accepted_via(DeliveryMethod::Noop, None),
                    ));
                };
                drop(current);
                Err(SinkWriteFailure::poisoned_by_deferred(
                    first,
                    obzenflow_core::event::SinkWritePhase::Execute,
                    SinkOperationError::permanent("deferred input failed"),
                ))
            }
        }

        let mut adapter =
            SinkWriterAdapter::new(DeferredOriginPoison { first: None }, StageId::new());
        let first = event(1);
        let first_id = first.id;
        let mut first_report = adapter
            .consume_report(first)
            .await
            .expect("first input buffers");
        first_report
            .commit_settlements()
            .expect("buffered report validates");

        let error = adapter
            .consume_report(event(2))
            .await
            .expect_err("second write poisons on the deferred subject");
        let HandlerError::SinkWrite(failure) = error else {
            panic!("expected a sealed sink write failure");
        };
        assert_eq!(failure.disposition(), SinkWriteFailureDisposition::Poisoned);
        assert_eq!(failure.error().operation_subject_event_id(), Some(first_id));
        assert!(lock_pending_registry(&adapter.registry)
            .phases
            .values()
            .any(|phase| *phase == PendingPhase::Deferred));
    }

    #[tokio::test]
    async fn current_or_nonpoisoned_operation_subject_is_protocol_fatal() {
        #[derive(Debug)]
        struct InvalidSubject {
            nonpoisoned: bool,
        }

        #[async_trait]
        impl SinkWriter for InvalidSubject {
            type Input = Input;

            async fn write(&mut self, _input: Input, context: SinkWriteContext) -> SinkWriteResult {
                let current = context.defer();
                let error = SinkOperationError::other("invalid subject")
                    .with_deferred_operation_subject(&current);
                if self.nonpoisoned {
                    Err(SinkWriteFailure::confirmed_rollback(
                        obzenflow_core::event::SinkWritePhase::Execute,
                        error,
                    ))
                } else {
                    Err(SinkWriteFailure::poisoned(
                        obzenflow_core::event::SinkWritePhase::Execute,
                        error,
                    ))
                }
            }
        }

        for nonpoisoned in [false, true] {
            let mut adapter =
                SinkWriterAdapter::new(InvalidSubject { nonpoisoned }, StageId::new());
            let error = adapter
                .consume_report(event(1))
                .await
                .expect_err("invalid subject authority must be fatal");
            assert!(matches!(error, HandlerError::Fatal(_)));
        }
    }

    #[test]
    fn dropping_a_deferred_capability_leaves_the_input_pending() {
        let stage_id = StageId::new();
        let mut registry = PendingRegistry::new(stage_id);
        let pending = registry.mint(EventId::new());
        let identity = pending.identity;
        registry.defer(&pending);
        drop(pending);

        assert!(registry.is_outstanding(identity));
    }

    #[tokio::test]
    async fn commit_receipts_preserve_capability_order_not_fifo_order() {
        #[derive(Clone, Debug)]
        struct ReverseSettlement {
            pending: Arc<Mutex<Vec<PendingSinkInput>>>,
        }

        #[async_trait]
        impl SinkWriter for ReverseSettlement {
            type Input = Input;

            async fn write(&mut self, _input: Input, context: SinkWriteContext) -> SinkWriteResult {
                let mut pending = self.pending.lock().expect("pending lock poisoned");
                pending.push(context.defer());
                let receipts = if pending.len() == 2 {
                    pending
                        .drain(..)
                        .rev()
                        .map(|pending| {
                            SinkCommitReceipt::new(
                                pending,
                                SinkTerminalOutcome::success_via(DeliveryMethod::Noop, None),
                            )
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                Ok(SinkWriteReport::buffered(SinkBufferedOutcome::accepted_via(
                    DeliveryMethod::Noop,
                    None,
                ))
                .with_commit_receipts(receipts))
            }
        }

        let mut adapter = SinkWriterAdapter::new(
            ReverseSettlement {
                pending: Arc::new(Mutex::new(Vec::new())),
            },
            StageId::new(),
        );
        let first = event(1);
        let second = event(2);
        adapter
            .consume_report(first.clone())
            .await
            .expect("buffer first");
        let report = adapter
            .consume_report(second.clone())
            .await
            .expect("settle in reverse order");

        assert_eq!(report.commit_receipts.len(), 2);
        assert_eq!(report.commit_receipts[0].parent_event_id, second.id);
        assert_eq!(report.commit_receipts[1].parent_event_id, first.id);
    }

    #[tokio::test]
    async fn settlement_authority_is_delayed_and_revalidation_is_batch_atomic() {
        #[derive(Debug)]
        struct TwoInputBatch {
            first: Option<PendingSinkInput>,
        }

        #[async_trait]
        impl SinkWriter for TwoInputBatch {
            type Input = Input;

            async fn write(&mut self, _input: Input, context: SinkWriteContext) -> SinkWriteResult {
                let current = context.defer();
                let Some(first) = self.first.take() else {
                    self.first = Some(current);
                    return Ok(SinkWriteReport::buffered(
                        SinkBufferedOutcome::accepted_via(DeliveryMethod::Noop, None),
                    ));
                };
                Ok(SinkWriteReport::buffered(SinkBufferedOutcome::accepted_via(
                    DeliveryMethod::Noop,
                    None,
                ))
                .with_commit_receipts([
                    SinkCommitReceipt::new(
                        first,
                        SinkTerminalOutcome::success_via(DeliveryMethod::Noop, None),
                    ),
                    SinkCommitReceipt::new(
                        current,
                        SinkTerminalOutcome::success_via(DeliveryMethod::Noop, None),
                    ),
                ]))
            }
        }

        let mut adapter = SinkWriterAdapter::new(TwoInputBatch { first: None }, StageId::new());
        let mut first_report = adapter
            .consume_report(event(1))
            .await
            .expect("first input is accepted for buffering");
        first_report
            .commit_settlements()
            .expect("buffered primary commits its validation token");

        let mut batch_report = adapter
            .consume_report(event(2))
            .await
            .expect("the complete two-input settlement validates");
        let identities = {
            let registry = lock_pending_registry(&adapter.registry);
            assert_eq!(
                registry.phases.len(),
                2,
                "lowering must not consume authority"
            );
            let mut identities = registry.phases.keys().copied().collect::<Vec<_>>();
            identities.sort_unstable();
            identities
        };

        lock_pending_registry(&adapter.registry)
            .phases
            .remove(&identities[1]);
        let error = batch_report
            .commit_settlements()
            .expect_err("late invalidation rejects the whole affine commit token");
        assert!(matches!(error, HandlerError::Fatal(_)));

        let registry = lock_pending_registry(&adapter.registry);
        assert_eq!(
            registry.phases.get(&identities[0]),
            Some(&PendingPhase::Deferred),
            "a bad final capability must not consume an earlier valid capability"
        );
    }

    #[tokio::test]
    async fn independent_stage_adapters_cannot_settle_each_others_capabilities() {
        let pending = Arc::new(Mutex::new(Vec::new()));
        let handler = Buffered {
            pending: Arc::clone(&pending),
        };
        let mut first_stage = SinkWriterAdapter::new(handler.clone(), StageId::new());
        let mut second_stage = SinkWriterAdapter::new(handler, StageId::new());
        first_stage
            .consume_report(event(2))
            .await
            .expect("buffer in first independent stage");
        let error = second_stage
            .flush_report()
            .await
            .expect_err("another stage cannot settle the capability");
        assert!(matches!(error, HandlerError::Fatal(_)));
    }

    #[tokio::test]
    async fn decode_failures_stay_ordinary_handler_errors() {
        let pending = Arc::new(Mutex::new(Vec::new()));
        let mut adapter = SinkWriterAdapter::new(Buffered { pending }, StageId::new());
        let malformed = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            Input::versioned_event_type(),
            serde_json::json!({ "wrong": true }),
        );

        let error = adapter
            .consume_report(malformed)
            .await
            .expect_err("decode failure");
        assert!(matches!(error, HandlerError::Deserialization(_)));
    }
}

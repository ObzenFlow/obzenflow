// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed sink authoring and runtime erasure (FLOWIP-134h).

use super::traits::{CommitReceipt, SinkConsumeReport, SinkHandler, SinkLifecycleReport};
use crate::effects::SinkDeliverySafety;
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
use std::sync::{Arc, Mutex};

static NEXT_PENDING_REGISTRY_ID: AtomicU64 = AtomicU64::new(1);

/// The complete delivery declaration snapshotted before runtime adaptation.
///
/// Its representation is private so invalid combinations cannot be authored:
/// canonical destination coordinates exist only alongside a destination
/// family, and every destination-bearing declaration carries safety.
#[derive(Clone, Debug, PartialEq)]
pub struct SinkDeliveryDeclaration {
    kind: SinkDeliveryDeclarationKind,
}

#[derive(Clone, Debug, PartialEq)]
enum SinkDeliveryDeclarationKind {
    Undeclared,
    SafetyOnly(SinkDeliverySafety),
    Destination {
        delivery_type: &'static str,
        safety: SinkDeliverySafety,
        canonical_destination: Option<Value>,
    },
}

impl SinkDeliveryDeclaration {
    pub fn undeclared() -> Self {
        Self {
            kind: SinkDeliveryDeclarationKind::Undeclared,
        }
    }

    pub fn safety_only(safety: SinkDeliverySafety) -> Self {
        Self {
            kind: SinkDeliveryDeclarationKind::SafetyOnly(safety),
        }
    }

    pub fn destination(
        delivery_type: &'static str,
        safety: SinkDeliverySafety,
        canonical_destination: Option<Value>,
    ) -> Self {
        Self {
            kind: SinkDeliveryDeclarationKind::Destination {
                delivery_type,
                safety,
                canonical_destination,
            },
        }
    }

    #[doc(hidden)]
    pub fn safety(&self) -> Option<SinkDeliverySafety> {
        match self.kind {
            SinkDeliveryDeclarationKind::Undeclared => None,
            SinkDeliveryDeclarationKind::SafetyOnly(safety)
            | SinkDeliveryDeclarationKind::Destination { safety, .. } => Some(safety),
        }
    }

    #[doc(hidden)]
    pub fn delivery_type(&self) -> Option<&'static str> {
        match self.kind {
            SinkDeliveryDeclarationKind::Destination { delivery_type, .. } => Some(delivery_type),
            SinkDeliveryDeclarationKind::Undeclared
            | SinkDeliveryDeclarationKind::SafetyOnly(_) => None,
        }
    }

    #[doc(hidden)]
    pub fn canonical_destination(&self) -> Option<&Value> {
        match &self.kind {
            SinkDeliveryDeclarationKind::Destination {
                canonical_destination,
                ..
            } => canonical_destination.as_ref(),
            SinkDeliveryDeclarationKind::Undeclared
            | SinkDeliveryDeclarationKind::SafetyOnly(_) => None,
        }
    }
}

/// FLOWIP-120i per-delivery provenance projection.
#[derive(Debug, Clone)]
pub struct DeliveryContext {
    provenance: DeliveryProvenance,
}

impl DeliveryContext {
    fn from_event(event: &ChainEvent) -> Self {
        Self {
            provenance: if event.replay_context.is_some() {
                DeliveryProvenance::Replayed
            } else {
                DeliveryProvenance::Live
            },
        }
    }

    pub fn provenance(&self) -> DeliveryProvenance {
        self.provenance
    }

    pub fn is_replayed(&self) -> bool {
        matches!(self.provenance, DeliveryProvenance::Replayed)
    }
}

/// Whether an input is live or reconstructed from a recorded run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeliveryProvenance {
    Live,
    Replayed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingIdentity {
    registry_id: u64,
    stage_id: StageId,
    nonce: u64,
}

#[derive(Debug)]
struct PendingRegistry {
    registry_id: u64,
    stage_id: StageId,
    next_nonce: u64,
    outstanding: HashMap<u64, EventId>,
    settled: HashSet<u64>,
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

        // This guard also runs while unwinding a handler panic. Never panic a
        // second time from Drop; a poisoned registry is already unrecoverable.
        if let Ok(mut registry) = self.registry.lock() {
            registry.abandon(self.identity);
        }
    }
}

impl PendingRegistry {
    fn new(stage_id: StageId) -> Self {
        Self {
            registry_id: NEXT_PENDING_REGISTRY_ID.fetch_add(1, Ordering::Relaxed),
            stage_id,
            next_nonce: 0,
            outstanding: HashMap::new(),
            settled: HashSet::new(),
        }
    }

    fn mint(&mut self, parent_event_id: EventId) -> PendingSinkInput {
        let nonce = self.next_nonce;
        self.next_nonce = self.next_nonce.wrapping_add(1);
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
        let previous = self
            .outstanding
            .insert(pending.identity.nonce, pending.parent_event_id);
        debug_assert!(previous.is_none());
    }

    fn is_outstanding(&self, identity: PendingIdentity) -> bool {
        identity.registry_id == self.registry_id
            && identity.stage_id == self.stage_id
            && self.outstanding.contains_key(&identity.nonce)
    }

    fn abandon(&mut self, identity: PendingIdentity) {
        if identity.registry_id == self.registry_id && identity.stage_id == self.stage_id {
            self.outstanding.remove(&identity.nonce);
        }
    }

    fn settle(&mut self, pending: PendingSinkInput) -> Result<EventId, HandlerError> {
        let identity = pending.identity;
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
        if self.settled.contains(&identity.nonce) {
            return Err(protocol_fatal("duplicate sink settlement capability"));
        }
        let Some(parent_event_id) = self.outstanding.remove(&identity.nonce) else {
            return Err(protocol_fatal(
                "stale or non-deferred sink settlement capability",
            ));
        };
        if parent_event_id != pending.parent_event_id {
            return Err(protocol_fatal(
                "sink settlement capability parent identity was corrupted",
            ));
        }
        self.settled.insert(identity.nonce);
        Ok(parent_event_id)
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

/// Per-input typed sink context.
pub struct SinkInputContext {
    delivery: DeliveryContext,
    pending: Option<PendingSinkInput>,
    registry: Arc<Mutex<PendingRegistry>>,
}

impl std::fmt::Debug for SinkInputContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkInputContext")
            .field("delivery", &self.delivery)
            .finish_non_exhaustive()
    }
}

impl SinkInputContext {
    pub fn delivery(&self) -> &DeliveryContext {
        &self.delivery
    }

    pub fn defer(mut self) -> PendingSinkInput {
        let pending = self
            .pending
            .take()
            .expect("sink input context settlement capability already consumed");
        self.registry
            .lock()
            .expect("sink pending registry poisoned")
            .defer(&pending);
        pending
    }
}

/// Success or partial-success evidence for a terminal input.
#[derive(Debug, Clone)]
pub struct SinkTerminalOutcome {
    payload: DeliveryPayload,
}

impl SinkTerminalOutcome {
    pub fn success(method: DeliveryMethod, bytes_processed: Option<u64>) -> Self {
        Self {
            payload: DeliveryPayload::success(method, bytes_processed),
        }
    }

    pub fn partial(
        method: DeliveryMethod,
        successful_count: u64,
        failed_count: u64,
        error_summary: impl Into<String>,
        failed_items: Option<Vec<String>>,
    ) -> Self {
        Self {
            payload: DeliveryPayload::partial(
                method,
                successful_count,
                failed_count,
                error_summary,
                failed_items,
            ),
        }
    }

    pub fn with_items(mut self, items: u64) -> Self {
        self.payload = self.payload.with_items(items);
        self
    }

    pub fn with_middleware_context(mut self, context: Value) -> Self {
        self.payload = self.payload.with_middleware_context(context);
        self
    }
}

/// Provisional evidence for an accepted but not yet committed input.
#[derive(Debug, Clone)]
pub struct SinkBufferedOutcome {
    payload: DeliveryPayload,
}

impl SinkBufferedOutcome {
    pub fn new(method: DeliveryMethod, bytes_processed: Option<u64>) -> Self {
        Self {
            payload: DeliveryPayload::buffered(method, bytes_processed),
        }
    }

    pub fn with_middleware_context(mut self, context: Value) -> Self {
        self.payload = self.payload.with_middleware_context(context);
        self
    }
}

/// Lifecycle-only delivery evidence. It cannot settle an input.
#[derive(Debug, Clone)]
pub struct SinkAuditOutcome {
    payload: DeliveryPayload,
}

impl SinkAuditOutcome {
    pub fn success(method: DeliveryMethod, bytes_processed: Option<u64>) -> Self {
        Self {
            payload: DeliveryPayload::success(method, bytes_processed),
        }
    }

    pub fn partial(
        method: DeliveryMethod,
        successful_count: u64,
        failed_count: u64,
        error_summary: impl Into<String>,
        failed_items: Option<Vec<String>>,
    ) -> Self {
        Self {
            payload: DeliveryPayload::partial(
                method,
                successful_count,
                failed_count,
                error_summary,
                failed_items,
            ),
        }
    }

    pub fn with_items(mut self, items: u64) -> Self {
        self.payload = self.payload.with_items(items);
        self
    }

    pub fn with_middleware_context(mut self, context: Value) -> Self {
        self.payload = self.payload.with_middleware_context(context);
        self
    }
}

#[derive(Debug, Clone)]
pub enum SinkPrimaryOutcome {
    Terminal(SinkTerminalOutcome),
    Buffered(SinkBufferedOutcome),
}

/// A terminal receipt paired with the exact buffered input capability.
#[derive(Debug)]
pub struct TypedCommitReceipt {
    pending: PendingSinkInput,
    outcome: SinkTerminalOutcome,
}

impl TypedCommitReceipt {
    pub fn new(pending: PendingSinkInput, outcome: SinkTerminalOutcome) -> Self {
        Self { pending, outcome }
    }
}

#[derive(Debug)]
pub struct TypedSinkConsumeReport {
    primary: SinkPrimaryOutcome,
    commit_receipts: Vec<TypedCommitReceipt>,
}

impl TypedSinkConsumeReport {
    pub fn terminal(outcome: SinkTerminalOutcome) -> Self {
        Self {
            primary: SinkPrimaryOutcome::Terminal(outcome),
            commit_receipts: Vec::new(),
        }
    }

    pub fn buffered(outcome: SinkBufferedOutcome) -> Self {
        Self {
            primary: SinkPrimaryOutcome::Buffered(outcome),
            commit_receipts: Vec::new(),
        }
    }

    pub fn with_commit_receipt(mut self, receipt: TypedCommitReceipt) -> Self {
        self.commit_receipts.push(receipt);
        self
    }

    pub fn with_commit_receipts(
        mut self,
        receipts: impl IntoIterator<Item = TypedCommitReceipt>,
    ) -> Self {
        self.commit_receipts.extend(receipts);
        self
    }
}

#[derive(Debug, Default)]
pub struct TypedSinkLifecycleReport {
    audit_outcome: Option<SinkAuditOutcome>,
    commit_receipts: Vec<TypedCommitReceipt>,
}

impl TypedSinkLifecycleReport {
    pub fn audit(outcome: SinkAuditOutcome) -> Self {
        Self {
            audit_outcome: Some(outcome),
            commit_receipts: Vec::new(),
        }
    }

    pub fn with_commit_receipt(mut self, receipt: TypedCommitReceipt) -> Self {
        self.commit_receipts.push(receipt);
        self
    }

    pub fn with_commit_receipts(
        mut self,
        receipts: impl IntoIterator<Item = TypedCommitReceipt>,
    ) -> Self {
        self.commit_receipts.extend(receipts);
        self
    }
}

/// The sole public behavioural and input-authority trait for sink handlers.
#[async_trait]
#[diagnostic::on_unimplemented(
    message = "this sink handler does not witness its declared input",
    note = "implement TypedSinkHandler with Input matching the sink! arrow (FLOWIP-134h)"
)]
pub trait TypedSinkHandler: Send + Sync + 'static {
    type Input: TypedPayload + Send + Sync + 'static;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration;

    async fn consume(
        &mut self,
        input: Self::Input,
        context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError>;

    async fn flush(&mut self) -> Result<TypedSinkLifecycleReport, HandlerError> {
        Ok(TypedSinkLifecycleReport::default())
    }

    async fn drain(&mut self) -> Result<TypedSinkLifecycleReport, HandlerError> {
        self.flush().await
    }
}

/// Sole bridge from typed sink authoring to the journal sink's erased input.
#[doc(hidden)]
pub struct TypedSinkHandlerAdapter<H> {
    handler: H,
    stage_id: StageId,
    registry: Arc<Mutex<PendingRegistry>>,
}

impl<H: Clone> Clone for TypedSinkHandlerAdapter<H> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            stage_id: self.stage_id,
            registry: Arc::clone(&self.registry),
        }
    }
}

impl<H: std::fmt::Debug> std::fmt::Debug for TypedSinkHandlerAdapter<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TypedSinkHandlerAdapter")
            .field("handler", &self.handler)
            .field("stage_id", &self.stage_id)
            .finish_non_exhaustive()
    }
}

impl<H> TypedSinkHandlerAdapter<H> {
    pub fn new(handler: H, stage_id: StageId) -> Self {
        Self {
            handler,
            stage_id,
            registry: Arc::new(Mutex::new(PendingRegistry::new(stage_id))),
        }
    }

    fn lower_terminal(outcome: SinkTerminalOutcome) -> DeliveryPayload {
        debug_assert!(matches!(
            outcome.payload.result,
            DeliveryResult::Success { .. } | DeliveryResult::Partial { .. }
        ));
        outcome.payload
    }

    fn lower_consume_report(
        &self,
        current: PendingIdentity,
        report: TypedSinkConsumeReport,
    ) -> Result<SinkConsumeReport, HandlerError> {
        let current_is_outstanding = self
            .registry
            .lock()
            .expect("sink pending registry poisoned")
            .is_outstanding(current);

        let primary = match report.primary {
            SinkPrimaryOutcome::Terminal(outcome) => {
                if current_is_outstanding {
                    return Err(protocol_fatal(
                        "sink returned a terminal primary outcome after deferring the input",
                    ));
                }
                Self::lower_terminal(outcome)
            }
            SinkPrimaryOutcome::Buffered(outcome) => {
                if !current_is_outstanding {
                    return Err(protocol_fatal(
                        "sink returned a buffered primary outcome without deferring the input",
                    ));
                }
                outcome.payload
            }
        };

        let mut commit_receipts = Vec::with_capacity(report.commit_receipts.len());
        for receipt in report.commit_receipts {
            let parent_event_id = self
                .registry
                .lock()
                .expect("sink pending registry poisoned")
                .settle(receipt.pending)?;
            commit_receipts.push(CommitReceipt {
                parent_event_id,
                payload: Self::lower_terminal(receipt.outcome),
            });
        }

        Ok(SinkConsumeReport {
            primary,
            commit_receipts,
        })
    }

    fn lower_lifecycle_report(
        &self,
        report: TypedSinkLifecycleReport,
    ) -> Result<SinkLifecycleReport, HandlerError> {
        let mut commit_receipts = Vec::with_capacity(report.commit_receipts.len());
        for receipt in report.commit_receipts {
            let parent_event_id = self
                .registry
                .lock()
                .expect("sink pending registry poisoned")
                .settle(receipt.pending)?;
            commit_receipts.push(CommitReceipt {
                parent_event_id,
                payload: Self::lower_terminal(receipt.outcome),
            });
        }
        Ok(SinkLifecycleReport {
            audit_payload: report.audit_outcome.map(|outcome| outcome.payload),
            commit_receipts,
        })
    }
}

#[async_trait]
impl<H> SinkHandler for TypedSinkHandlerAdapter<H>
where
    H: TypedSinkHandler,
{
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(self.consume_report(event).await?.primary)
    }

    async fn consume_report(
        &mut self,
        event: ChainEvent,
    ) -> Result<SinkConsumeReport, HandlerError> {
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

        if !H::Input::event_type_matches(event_type) {
            return Err(HandlerError::Validation(format!(
                "TypedSinkHandler expected event type '{}' (or '{}'), got '{}'",
                H::Input::EVENT_TYPE,
                H::Input::versioned_event_type(),
                event_type
            )));
        }

        let input: H::Input = serde_json::from_value(payload.clone()).map_err(|error| {
            HandlerError::Deserialization(format!(
                "TypedSinkHandler failed to deserialize {}: {error}",
                std::any::type_name::<H::Input>()
            ))
        })?;

        let pending = self
            .registry
            .lock()
            .expect("sink pending registry poisoned")
            .mint(event.id);
        let current = pending.identity;
        let context = SinkInputContext {
            delivery: DeliveryContext::from_event(&event),
            pending: Some(pending),
            registry: Arc::clone(&self.registry),
        };
        let mut guard = CurrentPendingGuard::new(Arc::clone(&self.registry), current);

        let report = self.handler.consume(input, context).await?;
        let lowered = self.lower_consume_report(current, report)?;
        guard.complete();
        Ok(lowered)
    }

    async fn flush(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        Ok(self.flush_report().await?.audit_payload)
    }

    async fn flush_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        let report = self.handler.flush().await?;
        self.lower_lifecycle_report(report)
    }

    async fn drain(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        Ok(self.drain_report().await?.audit_payload)
    }

    async fn drain_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        let report = self.handler.drain().await?;
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
    impl TypedSinkHandler for Buffered {
        type Input = Input;

        fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
            SinkDeliveryDeclaration::safety_only(SinkDeliverySafety::IdempotentProjection)
        }

        async fn consume(
            &mut self,
            _input: Input,
            context: SinkInputContext,
        ) -> Result<TypedSinkConsumeReport, HandlerError> {
            self.pending
                .lock()
                .expect("pending lock poisoned")
                .push(context.defer());
            Ok(TypedSinkConsumeReport::buffered(SinkBufferedOutcome::new(
                DeliveryMethod::Noop,
                None,
            )))
        }

        async fn flush(&mut self) -> Result<TypedSinkLifecycleReport, HandlerError> {
            let receipts = self
                .pending
                .lock()
                .expect("pending lock poisoned")
                .drain(..)
                .map(|pending| {
                    TypedCommitReceipt::new(
                        pending,
                        SinkTerminalOutcome::success(DeliveryMethod::Noop, None),
                    )
                })
                .collect::<Vec<_>>();
            Ok(TypedSinkLifecycleReport::default().with_commit_receipts(receipts))
        }
    }

    fn event(value: u64) -> ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            Input::versioned_event_type(),
            serde_json::json!({ "value": value }),
        )
    }

    #[tokio::test]
    async fn buffered_capability_lowers_to_the_original_parent() {
        let pending = Arc::new(Mutex::new(Vec::new()));
        let mut adapter = TypedSinkHandlerAdapter::new(
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
        impl TypedSinkHandler for Invalid {
            type Input = Input;

            fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
                SinkDeliveryDeclaration::undeclared()
            }

            async fn consume(
                &mut self,
                _input: Input,
                _context: SinkInputContext,
            ) -> Result<TypedSinkConsumeReport, HandlerError> {
                Ok(TypedSinkConsumeReport::buffered(SinkBufferedOutcome::new(
                    DeliveryMethod::Noop,
                    None,
                )))
            }
        }

        let mut adapter = TypedSinkHandlerAdapter::new(Invalid, StageId::new());
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
        impl TypedSinkHandler for Invalid {
            type Input = Input;

            fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
                SinkDeliveryDeclaration::undeclared()
            }

            async fn consume(
                &mut self,
                _input: Input,
                context: SinkInputContext,
            ) -> Result<TypedSinkConsumeReport, HandlerError> {
                drop(context.defer());
                Ok(TypedSinkConsumeReport::terminal(
                    SinkTerminalOutcome::success(DeliveryMethod::Noop, None),
                ))
            }
        }

        let mut adapter = TypedSinkHandlerAdapter::new(Invalid, StageId::new());
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
        impl TypedSinkHandler for PanicsAfterDeferral {
            type Input = Input;

            fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
                SinkDeliveryDeclaration::undeclared()
            }

            async fn consume(
                &mut self,
                _input: Input,
                context: SinkInputContext,
            ) -> Result<TypedSinkConsumeReport, HandlerError> {
                self.pending
                    .lock()
                    .expect("pending lock poisoned")
                    .push(context.defer());
                panic!("intentional typed sink panic after deferral");
            }

            async fn flush(&mut self) -> Result<TypedSinkLifecycleReport, HandlerError> {
                let receipts = self
                    .pending
                    .lock()
                    .expect("pending lock poisoned")
                    .drain(..)
                    .map(|pending| {
                        TypedCommitReceipt::new(
                            pending,
                            SinkTerminalOutcome::success(DeliveryMethod::Noop, None),
                        )
                    })
                    .collect::<Vec<_>>();
                Ok(TypedSinkLifecycleReport::default().with_commit_receipts(receipts))
            }
        }

        let mut adapter = TypedSinkHandlerAdapter::new(
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
        impl TypedSinkHandler for ReverseSettlement {
            type Input = Input;

            fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
                SinkDeliveryDeclaration::undeclared()
            }

            async fn consume(
                &mut self,
                _input: Input,
                context: SinkInputContext,
            ) -> Result<TypedSinkConsumeReport, HandlerError> {
                let mut pending = self.pending.lock().expect("pending lock poisoned");
                pending.push(context.defer());
                let receipts = if pending.len() == 2 {
                    pending
                        .drain(..)
                        .rev()
                        .map(|pending| {
                            TypedCommitReceipt::new(
                                pending,
                                SinkTerminalOutcome::success(DeliveryMethod::Noop, None),
                            )
                        })
                        .collect()
                } else {
                    Vec::new()
                };
                Ok(TypedSinkConsumeReport::buffered(SinkBufferedOutcome::new(
                    DeliveryMethod::Noop,
                    None,
                ))
                .with_commit_receipts(receipts))
            }
        }

        let mut adapter = TypedSinkHandlerAdapter::new(
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
    async fn adapter_clones_share_one_stage_registry_but_independent_stages_do_not() {
        let pending = Arc::new(Mutex::new(Vec::new()));
        let handler = Buffered {
            pending: Arc::clone(&pending),
        };
        let mut first_clone = TypedSinkHandlerAdapter::new(handler.clone(), StageId::new());
        let mut second_clone = first_clone.clone();
        let input = event(1);
        first_clone
            .consume_report(input.clone())
            .await
            .expect("buffer through first adapter clone");
        let report = second_clone
            .flush_report()
            .await
            .expect("settle through second adapter clone");
        assert_eq!(report.commit_receipts[0].parent_event_id, input.id);

        let mut first_stage = TypedSinkHandlerAdapter::new(handler.clone(), StageId::new());
        let mut second_stage = TypedSinkHandlerAdapter::new(handler, StageId::new());
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
        let mut adapter = TypedSinkHandlerAdapter::new(Buffered { pending }, StageId::new());
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

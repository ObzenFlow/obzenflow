// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Closure conveniences for the canonical typed sink protocol.

use crate::effects::SinkDeliverySafety;
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::{
    DeliveryContext, SinkDeliveryDeclaration, SinkInputContext, SinkTerminalOutcome,
    TypedSinkConsumeReport, TypedSinkHandler,
};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use std::future::Future;
use std::marker::PhantomData;

#[doc(hidden)]
pub struct InfallibleSinkMode;
#[doc(hidden)]
pub struct WithDeliverySinkMode;
#[doc(hidden)]
pub struct FallibleSinkMode;

/// A typed closure sink.
///
/// `new`, `with_delivery`, and `fallible` are sealed modes of this one
/// convenience adapter; every mode implements [`TypedSinkHandler`].
pub struct SinkTyped<T, F, Fut, Mode = InfallibleSinkMode> {
    handler: F,
    declaration: SinkDeliveryDeclaration,
    _input: PhantomData<fn() -> T>,
    _future: PhantomData<fn() -> Fut>,
    _mode: PhantomData<fn() -> Mode>,
}

impl<T, F, Fut, Mode> Clone for SinkTyped<T, F, Fut, Mode>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            declaration: self.declaration.clone(),
            _input: PhantomData,
            _future: PhantomData,
            _mode: PhantomData,
        }
    }
}

impl<T, F, Fut, Mode> std::fmt::Debug for SinkTyped<T, F, Fut, Mode> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkTyped")
            .field("payload_type", &std::any::type_name::<T>())
            .field("declaration", &self.declaration)
            .finish()
    }
}

impl<T, F, Fut> SinkTyped<T, F, Fut, InfallibleSinkMode>
where
    T: TypedPayload + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    /// Build an infallible closure sink over `T`.
    pub fn new(handler: F) -> Self {
        Self {
            handler,
            declaration: SinkDeliveryDeclaration::undeclared(),
            _input: PhantomData,
            _future: PhantomData,
            _mode: PhantomData,
        }
    }
}

impl<T> SinkTyped<T, fn(T) -> std::future::Ready<()>, std::future::Ready<()>, InfallibleSinkMode>
where
    T: TypedPayload + Send + Sync + 'static,
{
    /// Build a fallible closure sink whose errors follow the sink error path.
    pub fn fallible<G, FutG>(handler: G) -> SinkTyped<T, G, FutG, FallibleSinkMode>
    where
        G: FnMut(T) -> FutG + Send + Sync + Clone + 'static,
        FutG: Future<Output = Result<(), HandlerError>> + Send + 'static,
    {
        SinkTyped {
            handler,
            declaration: SinkDeliveryDeclaration::undeclared(),
            _input: PhantomData,
            _future: PhantomData,
            _mode: PhantomData,
        }
    }

    /// Build a closure sink that also receives read-only delivery provenance.
    pub fn with_delivery<G, FutG>(handler: G) -> SinkTyped<T, G, FutG, WithDeliverySinkMode>
    where
        G: FnMut(T, DeliveryContext) -> FutG + Send + Sync + Clone + 'static,
        FutG: Future<Output = ()> + Send + 'static,
    {
        SinkTyped {
            handler,
            declaration: SinkDeliveryDeclaration::undeclared(),
            _input: PhantomData,
            _future: PhantomData,
            _mode: PhantomData,
        }
    }
}

impl<T, F, Fut, Mode> SinkTyped<T, F, Fut, Mode> {
    /// Declare this projection safe to re-run during replay or resume.
    pub fn idempotent(mut self) -> Self {
        self.declaration =
            SinkDeliveryDeclaration::safety_only(SinkDeliverySafety::IdempotentProjection);
        self
    }

    /// Declare that duplicate external delivery requires archive-verb opt-in.
    pub fn non_idempotent(mut self) -> Self {
        self.declaration =
            SinkDeliveryDeclaration::safety_only(SinkDeliverySafety::NonIdempotentExternal);
        self
    }
}

fn closure_success() -> TypedSinkConsumeReport {
    TypedSinkConsumeReport::terminal(SinkTerminalOutcome::success(
        DeliveryMethod::Custom("TypedSink".to_string()),
        Some(1),
    ))
}

#[async_trait]
impl<T, F, Fut> TypedSinkHandler for SinkTyped<T, F, Fut, InfallibleSinkMode>
where
    T: TypedPayload + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    type Input = T;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        self.declaration.clone()
    }

    async fn consume(
        &mut self,
        input: T,
        _context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError> {
        (self.handler)(input).await;
        Ok(closure_success())
    }
}

#[async_trait]
impl<T, F, Fut> TypedSinkHandler for SinkTyped<T, F, Fut, WithDeliverySinkMode>
where
    T: TypedPayload + Send + Sync + 'static,
    F: FnMut(T, DeliveryContext) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    type Input = T;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        self.declaration.clone()
    }

    async fn consume(
        &mut self,
        input: T,
        context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError> {
        (self.handler)(input, context.delivery().clone()).await;
        Ok(closure_success())
    }
}

#[async_trait]
impl<T, F, Fut> TypedSinkHandler for SinkTyped<T, F, Fut, FallibleSinkMode>
where
    T: TypedPayload + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<(), HandlerError>> + Send + 'static,
{
    type Input = T;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        self.declaration.clone()
    }

    async fn consume(
        &mut self,
        input: T,
        _context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError> {
        (self.handler)(input).await?;
        Ok(closure_success())
    }
}

mod sealed {
    pub trait Sealed {}
}

/// Sealed lowering target for the `sink!` macro's site-level `delivery:`
/// declaration.
#[diagnostic::on_unimplemented(
    message = "the `delivery:` clause applies to SinkTyped closure sinks only",
    note = "named TypedSinkHandler implementations own their declaration through delivery_declaration()"
)]
pub trait DeclareDeliverySafety: sealed::Sealed + Sized {
    fn declare_idempotent(self) -> Self;
    fn declare_non_idempotent(self) -> Self;
}

impl<T, F, Fut, Mode> sealed::Sealed for SinkTyped<T, F, Fut, Mode> {}

impl<T, F, Fut, Mode> DeclareDeliverySafety for SinkTyped<T, F, Fut, Mode> {
    fn declare_idempotent(self) -> Self {
        self.idempotent()
    }

    fn declare_non_idempotent(self) -> Self {
        self.non_idempotent()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::common::handlers::{SinkHandler, TypedSinkHandlerAdapter};
    use crate::stages::sink::DeliveryProvenance;
    use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct TestPayload {
        n: usize,
    }

    impl TypedPayload for TestPayload {
        const EVENT_TYPE: &'static str = "test.payload";
    }

    fn event(n: usize) -> obzenflow_core::ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            TestPayload::versioned_event_type(),
            serde_json::json!({ "n": n }),
        )
    }

    #[tokio::test]
    async fn every_closure_mode_uses_the_same_typed_adapter() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_closure = Arc::clone(&seen);
        let handler = SinkTyped::new(move |input: TestPayload| {
            let seen = Arc::clone(&seen_for_closure);
            async move { seen.lock().expect("seen lock poisoned").push(input) }
        });
        let mut adapter = TypedSinkHandlerAdapter::new(handler, StageId::new());
        let report = adapter
            .consume_report(event(4))
            .await
            .expect("typed closure consumes");
        assert!(matches!(
            report.primary.result,
            DeliveryResult::Success { .. }
        ));
        assert_eq!(
            *seen.lock().expect("seen lock poisoned"),
            vec![TestPayload { n: 4 }]
        );
    }

    async fn assert_legacy_closure_receipt<H>(handler: H)
    where
        H: TypedSinkHandler<Input = TestPayload>,
    {
        let mut adapter = TypedSinkHandlerAdapter::new(handler, StageId::new());
        let report = adapter
            .consume_report(event(9))
            .await
            .expect("closure mode consumes");
        assert!(matches!(
            report.primary.result,
            DeliveryResult::Success { .. }
        ));
        assert!(matches!(
            report.primary.delivery_method,
            DeliveryMethod::Custom(ref name) if name == "TypedSink"
        ));
        assert_eq!(report.primary.bytes_processed, Some(1));
        assert_eq!(report.primary.items_delivered, None);
    }

    #[tokio::test]
    async fn every_closure_mode_preserves_the_legacy_receipt_fields() {
        assert_legacy_closure_receipt(SinkTyped::new(|_input: TestPayload| async move {})).await;
        assert_legacy_closure_receipt(SinkTyped::with_delivery(
            |_input: TestPayload, _delivery| async move {},
        ))
        .await;
        assert_legacy_closure_receipt(SinkTyped::fallible(
            |_input: TestPayload| async move { Ok(()) },
        ))
        .await;
    }

    #[tokio::test]
    async fn with_delivery_receives_replay_provenance() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_closure = Arc::clone(&seen);
        let handler =
            SinkTyped::with_delivery(move |_input: TestPayload, delivery: DeliveryContext| {
                let seen = Arc::clone(&seen_for_closure);
                async move {
                    seen.lock()
                        .expect("seen lock poisoned")
                        .push(delivery.provenance())
                }
            });
        let mut adapter = TypedSinkHandlerAdapter::new(handler, StageId::new());
        let mut replayed = event(1);
        replayed.replay_context = Some(obzenflow_core::event::context::ReplayContext {
            original_event_id: obzenflow_core::EventId::new(),
            original_flow_id: "flow_01SOURCE".to_string(),
            original_stage_id: StageId::new(),
            archive_path: std::path::PathBuf::from("tmp/archive"),
            replayed_at: chrono::Utc::now(),
        });
        adapter
            .consume_report(replayed)
            .await
            .expect("replayed delivery consumes");
        assert_eq!(
            *seen.lock().expect("seen lock poisoned"),
            vec![DeliveryProvenance::Replayed]
        );
    }

    #[test]
    fn closure_declaration_is_explicit_and_replaceable() {
        let undeclared = SinkTyped::new(|_input: TestPayload| async move {});
        assert_eq!(undeclared.delivery_declaration().safety(), None);

        let declared = SinkTyped::new(|_input: TestPayload| async move {}).idempotent();
        assert_eq!(
            declared.delivery_declaration().safety(),
            Some(SinkDeliverySafety::IdempotentProjection)
        );
    }
}

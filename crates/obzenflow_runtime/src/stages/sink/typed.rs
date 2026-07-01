// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed sink helpers
//!
//! Sinks normally consume `ChainEvent` and must:
//! - filter by event type
//! - deserialize JSON payloads
//! - construct `DeliveryPayload` receipts
//!
//! `SinkTyped` and `FallibleSinkTyped` eliminate that boilerplate by working
//! with domain types that implement `TypedPayload`.

use crate::effects::SinkDeliverySafety;
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::SinkHandler;
use crate::typing::SinkTyping;
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::{ChainEvent, TypedPayload};
use serde::de::DeserializeOwned;
use std::future::Future;
use std::marker::PhantomData;

/// Typed sink handler that consumes domain values of type `T`.
///
/// Semantics:
/// - Non-data events are silently skipped (success noop)
/// - Data events with a non-matching event type error by default
///   (`Err(HandlerError::Validation(..))`), or are silently skipped with `.allow_skip()`
/// - Data events with a matching event type but invalid payload deserialize as
///   `Err(HandlerError::Deserialization(...))`
/// - On success, returns a success `DeliveryPayload`
pub struct SinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    handler: F,
    allow_skip: bool,
    delivery_safety: Option<SinkDeliverySafety>,
    _phantom: PhantomData<fn() -> (T, Fut)>,
}

impl<T, F, Fut> Clone for SinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            allow_skip: self.allow_skip,
            delivery_safety: self.delivery_safety,
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> SinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    /// Create a typed sink from an infallible async handler (primary constructor).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sink = SinkTyped::new(|event: MyEvent| async move {
    ///     println!("Received: {:?}", event);
    /// });
    /// ```
    pub fn new(handler: F) -> Self {
        Self {
            handler,
            allow_skip: false,
            delivery_safety: None,
            _phantom: PhantomData,
        }
    }

    /// Opt out of strict mode and silently skip non-matching event types.
    ///
    /// By default, `SinkTyped` returns `Err(HandlerError::Validation(..))` on a
    /// data event type mismatch to catch miswired pipelines early.
    pub fn allow_skip(mut self) -> Self {
        self.allow_skip = true;
        self
    }

    /// Declare the delivery path idempotent (safe to re-consume on resume).
    pub fn idempotent(mut self) -> Self {
        self.delivery_safety = Some(SinkDeliverySafety::IdempotentProjection);
        self
    }

    /// Declare a non-idempotent external write (resume refuses without opt-in).
    pub fn non_idempotent(mut self) -> Self {
        self.delivery_safety = Some(SinkDeliverySafety::NonIdempotentExternal);
        self
    }
}

impl<T> SinkTyped<T, fn(T) -> std::future::Ready<()>, std::future::Ready<()>>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
{
    /// Create a fallible typed sink.
    ///
    /// Returns `FallibleSinkTyped` for error-returning handlers.
    pub fn fallible<G, FutG>(handler: G) -> FallibleSinkTyped<T, G, FutG>
    where
        G: FnMut(T) -> FutG + Send + Sync + Clone,
        FutG: Future<Output = Result<(), HandlerError>> + Send,
    {
        FallibleSinkTyped::new(handler)
    }

    /// Create a typed sink whose handler also receives the delivery's
    /// [`DeliveryContext`], so it can tell a fresh outcome from one
    /// reconstructed during replay (FLOWIP-120i).
    ///
    /// ```ignore
    /// paid_orders = sink!(PaymentAuthorized => SinkTyped::with_delivery(
    ///     |authorized: PaymentAuthorized, delivery| async move {
    ///         send_to_shipping(authorized, delivery.provenance());
    ///     }
    /// ));
    /// ```
    pub fn with_delivery<G, FutG>(handler: G) -> SinkTypedWithDelivery<T, G, FutG>
    where
        G: FnMut(T, DeliveryContext) -> FutG + Send + Sync + Clone,
        FutG: Future<Output = ()> + Send,
    {
        SinkTypedWithDelivery::new(handler)
    }
}

#[async_trait]
impl<T, F, Fut> SinkHandler for SinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        let destination = std::any::type_name::<T>();

        let (event_type, payload) = match &event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => (event_type.as_str(), payload),
            _ => {
                return Ok(DeliveryPayload::success(
                    destination,
                    DeliveryMethod::Custom("Skipped".to_string()),
                    None,
                ))
            }
        };

        if !T::event_type_matches(event_type) {
            if self.allow_skip {
                return Ok(DeliveryPayload::success(
                    destination,
                    DeliveryMethod::Custom("Skipped".to_string()),
                    None,
                ));
            }

            return Err(HandlerError::Validation(format!(
                "SinkTyped expected event type '{}' (or '{}'), got '{}'",
                T::EVENT_TYPE,
                T::versioned_event_type(),
                event_type
            )));
        }

        let typed_event: T = serde_json::from_value(payload.clone()).map_err(|e| {
            HandlerError::Deserialization(format!(
                "SinkTyped failed to deserialize {}: {e}",
                std::any::type_name::<T>()
            ))
        })?;

        (self.handler)(typed_event).await;

        Ok(DeliveryPayload::success(
            destination,
            DeliveryMethod::Custom("TypedSink".to_string()),
            Some(1),
        ))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        self.delivery_safety
    }
}

impl<T, F, Fut> std::fmt::Debug for SinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkTyped")
            .field("payload_type", &std::any::type_name::<T>())
            .field("allow_skip", &self.allow_skip)
            .field("delivery_safety", &self.delivery_safety)
            .finish()
    }
}

impl<T, F, Fut> SinkTyping for SinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    type Input = T;
}

/// FLOWIP-120i: per-delivery provenance for typed sinks.
///
/// Built by the framework from the event envelope before payload
/// deserialization, so the raw `ChainEvent` never crosses the typed boundary.
/// Provenance derives from `event.replay_context`, which the replay driver
/// stamps on re-injected source events and every derived event inherits; that
/// per-event derivation is what keeps labels honest when FLOWIP-120n's resume
/// mixes a replayed prefix with a live tail in one run, where a run-scoped
/// flag would lie about the tail.
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

/// Whether a delivered outcome is fresh or reconstructed from a recorded run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeliveryProvenance {
    Live,
    Replayed,
}

/// Typed sink handler whose closure also receives the delivery's
/// [`DeliveryContext`] (FLOWIP-120i). Constructed via
/// [`SinkTyped::with_delivery`]; consumes through the same skip, mismatch,
/// and deserialization semantics as [`SinkTyped`]. The context is passed by
/// value: it is a small owned view, and an `async move` handler could not
/// borrow it across the returned future otherwise.
pub struct SinkTypedWithDelivery<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T, DeliveryContext) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    handler: F,
    allow_skip: bool,
    delivery_safety: Option<SinkDeliverySafety>,
    _phantom: PhantomData<fn() -> (T, Fut)>,
}

impl<T, F, Fut> Clone for SinkTypedWithDelivery<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T, DeliveryContext) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            allow_skip: self.allow_skip,
            delivery_safety: self.delivery_safety,
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> SinkTypedWithDelivery<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T, DeliveryContext) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    pub fn new(handler: F) -> Self {
        Self {
            handler,
            allow_skip: false,
            delivery_safety: None,
            _phantom: PhantomData,
        }
    }

    /// Opt out of strict mode and silently skip non-matching event types.
    pub fn allow_skip(mut self) -> Self {
        self.allow_skip = true;
        self
    }

    /// Declare the delivery path idempotent (safe to re-consume on resume).
    pub fn idempotent(mut self) -> Self {
        self.delivery_safety = Some(SinkDeliverySafety::IdempotentProjection);
        self
    }

    /// Declare a non-idempotent external write (resume refuses without opt-in).
    pub fn non_idempotent(mut self) -> Self {
        self.delivery_safety = Some(SinkDeliverySafety::NonIdempotentExternal);
        self
    }
}

#[async_trait]
impl<T, F, Fut> SinkHandler for SinkTypedWithDelivery<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T, DeliveryContext) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        let destination = std::any::type_name::<T>();

        let (event_type, payload) = match &event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => (event_type.as_str(), payload),
            _ => {
                return Ok(DeliveryPayload::success(
                    destination,
                    DeliveryMethod::Custom("Skipped".to_string()),
                    None,
                ))
            }
        };

        if !T::event_type_matches(event_type) {
            if self.allow_skip {
                return Ok(DeliveryPayload::success(
                    destination,
                    DeliveryMethod::Custom("Skipped".to_string()),
                    None,
                ));
            }

            return Err(HandlerError::Validation(format!(
                "SinkTypedWithDelivery expected event type '{}' (or '{}'), got '{}'",
                T::EVENT_TYPE,
                T::versioned_event_type(),
                event_type
            )));
        }

        let context = DeliveryContext::from_event(&event);

        let typed_event: T = serde_json::from_value(payload.clone()).map_err(|e| {
            HandlerError::Deserialization(format!(
                "SinkTypedWithDelivery failed to deserialize {}: {e}",
                std::any::type_name::<T>()
            ))
        })?;

        (self.handler)(typed_event, context).await;

        Ok(DeliveryPayload::success(
            destination,
            DeliveryMethod::Custom("TypedSink".to_string()),
            Some(1),
        ))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        self.delivery_safety
    }
}

impl<T, F, Fut> std::fmt::Debug for SinkTypedWithDelivery<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T, DeliveryContext) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SinkTypedWithDelivery")
            .field("payload_type", &std::any::type_name::<T>())
            .field("allow_skip", &self.allow_skip)
            .field("delivery_safety", &self.delivery_safety)
            .finish()
    }
}

impl<T, F, Fut> SinkTyping for SinkTypedWithDelivery<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T, DeliveryContext) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ()> + Send,
{
    type Input = T;
}

/// Typed sink handler for fallible async handlers.
///
/// Unlike `SinkTyped`, the handler returns `Result<(), HandlerError>`. Errors are
/// returned as `Err(HandlerError)` so the sink supervisor can journal failed
/// delivery receipts and emit error-marked events.
pub struct FallibleSinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<(), HandlerError>> + Send,
{
    handler: F,
    allow_skip: bool,
    delivery_safety: Option<SinkDeliverySafety>,
    _phantom: PhantomData<fn() -> (T, Fut)>,
}

impl<T, F, Fut> Clone for FallibleSinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<(), HandlerError>> + Send,
{
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            allow_skip: self.allow_skip,
            delivery_safety: self.delivery_safety,
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> FallibleSinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<(), HandlerError>> + Send,
{
    /// Create a typed sink from a fallible async handler (primary constructor).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let sink = FallibleSinkTyped::new(|event: MyEvent| async move {
    ///     database::insert(&event).await.map_err(|e| HandlerError::Other(e.to_string()))
    /// });
    /// ```
    pub fn new(handler: F) -> Self {
        Self {
            handler,
            allow_skip: false,
            delivery_safety: None,
            _phantom: PhantomData,
        }
    }

    /// Opt out of strict mode and silently skip non-matching event types.
    ///
    /// By default, `FallibleSinkTyped` returns `Err(HandlerError::Validation(..))`
    /// on a data event type mismatch to catch miswired pipelines early.
    pub fn allow_skip(mut self) -> Self {
        self.allow_skip = true;
        self
    }

    /// Declare the delivery path idempotent (safe to re-consume on resume).
    pub fn idempotent(mut self) -> Self {
        self.delivery_safety = Some(SinkDeliverySafety::IdempotentProjection);
        self
    }

    /// Declare a non-idempotent external write (resume refuses without opt-in).
    pub fn non_idempotent(mut self) -> Self {
        self.delivery_safety = Some(SinkDeliverySafety::NonIdempotentExternal);
        self
    }
}

impl<T, F, Fut> SinkTyping for FallibleSinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<(), HandlerError>> + Send,
{
    type Input = T;
}

#[async_trait]
impl<T, F, Fut> SinkHandler for FallibleSinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<(), HandlerError>> + Send,
{
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        let destination = std::any::type_name::<T>();

        let (event_type, payload) = match &event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => (event_type.as_str(), payload),
            _ => {
                return Ok(DeliveryPayload::success(
                    destination,
                    DeliveryMethod::Custom("Skipped".to_string()),
                    None,
                ))
            }
        };

        if !T::event_type_matches(event_type) {
            if self.allow_skip {
                return Ok(DeliveryPayload::success(
                    destination,
                    DeliveryMethod::Custom("Skipped".to_string()),
                    None,
                ));
            }

            return Err(HandlerError::Validation(format!(
                "FallibleSinkTyped expected event type '{}' (or '{}'), got '{}'",
                T::EVENT_TYPE,
                T::versioned_event_type(),
                event_type
            )));
        }

        let typed_event: T = serde_json::from_value(payload.clone()).map_err(|e| {
            HandlerError::Deserialization(format!(
                "FallibleSinkTyped failed to deserialize {}: {e}",
                std::any::type_name::<T>()
            ))
        })?;

        (self.handler)(typed_event).await?;

        Ok(DeliveryPayload::success(
            destination,
            DeliveryMethod::Custom("TypedSink".to_string()),
            Some(1),
        ))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        self.delivery_safety
    }
}

impl<T, F, Fut> std::fmt::Debug for FallibleSinkTyped<T, F, Fut>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: FnMut(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<(), HandlerError>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallibleSinkTyped")
            .field("payload_type", &std::any::type_name::<T>())
            .field("allow_skip", &self.allow_skip)
            .field("delivery_safety", &self.delivery_safety)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct TestPayload {
        n: usize,
    }

    impl TypedPayload for TestPayload {
        const EVENT_TYPE: &'static str = "test.payload";
    }

    #[tokio::test]
    async fn sink_typed_skips_non_data_events() {
        let called = Arc::new(AtomicUsize::new(0));
        let called_for_closure = called.clone();

        let mut sink = SinkTyped::new(move |_value: TestPayload| {
            let called_for_closure = called_for_closure.clone();
            async move {
                called_for_closure.fetch_add(1, Ordering::Relaxed);
            }
        });

        let event = ChainEventFactory::eof_event(WriterId::from(StageId::new()), true);
        let receipt = sink.consume(event).await.expect("consume should succeed");

        assert_eq!(called.load(Ordering::Relaxed), 0);
        assert!(matches!(
            receipt.delivery_method,
            DeliveryMethod::Custom(ref name) if name == "Skipped"
        ));
    }

    #[tokio::test]
    async fn sink_typed_errors_on_type_mismatches_by_default() {
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "other.type",
            serde_json::json!({"n": 1}),
        );

        let mut sink = SinkTyped::new(|_value: TestPayload| async move {});

        let err = sink
            .consume(event)
            .await
            .expect_err("expected validation error");
        assert!(matches!(err, HandlerError::Validation(_)));
    }

    #[tokio::test]
    async fn sink_typed_allow_skip_silently_skips_type_mismatches() {
        let called = Arc::new(AtomicUsize::new(0));
        let called_for_closure = called.clone();

        let mut sink = SinkTyped::new(move |_value: TestPayload| {
            let called_for_closure = called_for_closure.clone();
            async move {
                called_for_closure.fetch_add(1, Ordering::Relaxed);
            }
        })
        .allow_skip();

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "other.type",
            serde_json::json!({"n": 1}),
        );

        let receipt = sink.consume(event).await.expect("consume should succeed");
        assert_eq!(called.load(Ordering::Relaxed), 0);
        assert!(matches!(
            receipt.delivery_method,
            DeliveryMethod::Custom(ref name) if name == "Skipped"
        ));
    }

    #[tokio::test]
    async fn sink_typed_errors_on_parse_failure_for_matching_type() {
        let mut sink = SinkTyped::new(|_value: TestPayload| async move {});

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            TestPayload::EVENT_TYPE,
            serde_json::json!({"wrong": 1}),
        );

        let err = sink
            .consume(event)
            .await
            .expect_err("expected deserialization error");
        assert!(matches!(err, HandlerError::Deserialization(_)));
    }

    #[tokio::test]
    async fn sink_typed_calls_handler_on_success() {
        let seen = Arc::new(Mutex::new(Vec::<TestPayload>::new()));
        let seen_for_closure = seen.clone();

        let mut sink = SinkTyped::new(move |value: TestPayload| {
            let seen_for_closure = seen_for_closure.clone();
            async move {
                seen_for_closure
                    .lock()
                    .expect("seen lock poisoned")
                    .push(value);
            }
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            TestPayload::versioned_event_type(),
            serde_json::json!({"n": 42}),
        );

        let receipt = sink.consume(event).await.expect("consume should succeed");
        assert!(matches!(
            receipt.delivery_method,
            DeliveryMethod::Custom(ref name) if name == "TypedSink"
        ));

        let values = seen.lock().expect("seen lock poisoned").clone();
        assert_eq!(values, vec![TestPayload { n: 42 }]);
    }

    #[tokio::test]
    async fn fallible_sink_typed_propagates_handler_error() {
        let mut sink = FallibleSinkTyped::new(|_value: TestPayload| async move {
            Err(HandlerError::Timeout("boom".to_string()))
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            TestPayload::EVENT_TYPE,
            serde_json::json!({"n": 1}),
        );

        let err = sink
            .consume(event)
            .await
            .expect_err("expected handler error");
        assert!(matches!(err, HandlerError::Timeout(_)));
    }

    fn stamped_with_replay_context(mut event: ChainEvent) -> ChainEvent {
        event.replay_context = Some(obzenflow_core::event::context::ReplayContext {
            original_event_id: obzenflow_core::event::types::EventId::new(),
            original_flow_id: "flow_01SOURCE".to_string(),
            original_stage_id: StageId::new(),
            archive_path: std::path::PathBuf::from("tmp/archive"),
            replayed_at: chrono::Utc::now(),
        });
        event
    }

    #[tokio::test]
    async fn with_delivery_reports_live_provenance_for_unstamped_events() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_closure = seen.clone();

        let mut sink = SinkTyped::with_delivery(move |value: TestPayload, delivery| {
            let seen_for_closure = seen_for_closure.clone();
            async move {
                seen_for_closure
                    .lock()
                    .expect("seen lock poisoned")
                    .push((value, delivery.provenance()));
            }
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            TestPayload::EVENT_TYPE,
            serde_json::json!({"n": 7}),
        );
        let receipt = sink.consume(event).await.expect("consume should succeed");

        assert!(matches!(
            receipt.delivery_method,
            DeliveryMethod::Custom(ref name) if name == "TypedSink"
        ));
        assert_eq!(
            seen.lock().expect("seen lock poisoned").clone(),
            vec![(TestPayload { n: 7 }, DeliveryProvenance::Live)]
        );
    }

    #[tokio::test]
    async fn with_delivery_reports_replayed_provenance_for_stamped_events() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_for_closure = seen.clone();

        let mut sink = SinkTyped::with_delivery(move |value: TestPayload, delivery| {
            let seen_for_closure = seen_for_closure.clone();
            async move {
                seen_for_closure
                    .lock()
                    .expect("seen lock poisoned")
                    .push((value, delivery.is_replayed()));
            }
        });

        let event = stamped_with_replay_context(ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            TestPayload::EVENT_TYPE,
            serde_json::json!({"n": 9}),
        ));
        sink.consume(event).await.expect("consume should succeed");

        assert_eq!(
            seen.lock().expect("seen lock poisoned").clone(),
            vec![(TestPayload { n: 9 }, true)]
        );
    }

    #[tokio::test]
    async fn with_delivery_keeps_sink_typed_skip_and_mismatch_semantics() {
        let called = Arc::new(AtomicUsize::new(0));
        let called_for_closure = called.clone();
        let mut sink = SinkTyped::with_delivery(move |_value: TestPayload, _delivery| {
            let called_for_closure = called_for_closure.clone();
            async move {
                called_for_closure.fetch_add(1, Ordering::Relaxed);
            }
        });

        let eof = ChainEventFactory::eof_event(WriterId::from(StageId::new()), true);
        let receipt = sink.consume(eof).await.expect("non-data events skip");
        assert!(matches!(
            receipt.delivery_method,
            DeliveryMethod::Custom(ref name) if name == "Skipped"
        ));

        let mismatched = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "other.type",
            serde_json::json!({"n": 1}),
        );
        let err = sink
            .consume(mismatched.clone())
            .await
            .expect_err("type mismatches error by default");
        assert!(matches!(err, HandlerError::Validation(_)));

        let mut lenient = sink.allow_skip();
        let receipt = lenient
            .consume(mismatched)
            .await
            .expect("allow_skip skips mismatches");
        assert!(matches!(
            receipt.delivery_method,
            DeliveryMethod::Custom(ref name) if name == "Skipped"
        ));
        assert_eq!(called.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn typed_sinks_default_to_undeclared_delivery_safety() {
        let sink = SinkTyped::new(|_value: TestPayload| async move {});
        assert_eq!(SinkHandler::delivery_safety(&sink), None);

        let fallible = FallibleSinkTyped::new(|_value: TestPayload| async move { Ok(()) });
        assert_eq!(SinkHandler::delivery_safety(&fallible), None);

        let with_delivery =
            SinkTyped::with_delivery(|_value: TestPayload, _delivery| async move {});
        assert_eq!(SinkHandler::delivery_safety(&with_delivery), None);
    }

    #[test]
    fn typed_sink_builders_declare_delivery_safety() {
        let sink = SinkTyped::new(|_value: TestPayload| async move {}).idempotent();
        assert_eq!(
            SinkHandler::delivery_safety(&sink),
            Some(SinkDeliverySafety::IdempotentProjection)
        );

        let sink = SinkTyped::new(|_value: TestPayload| async move {}).non_idempotent();
        assert_eq!(
            SinkHandler::delivery_safety(&sink),
            Some(SinkDeliverySafety::NonIdempotentExternal)
        );

        let fallible = FallibleSinkTyped::new(|_value: TestPayload| async move { Ok(()) })
            .non_idempotent();
        assert_eq!(
            SinkHandler::delivery_safety(&fallible),
            Some(SinkDeliverySafety::NonIdempotentExternal)
        );

        let with_delivery = SinkTyped::with_delivery(|_value: TestPayload, _delivery| async move {})
            .idempotent();
        assert_eq!(
            SinkHandler::delivery_safety(&with_delivery),
            Some(SinkDeliverySafety::IdempotentProjection)
        );
    }
}

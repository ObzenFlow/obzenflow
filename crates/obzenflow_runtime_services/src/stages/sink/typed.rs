//! Typed sink helpers (FLOWIP-081c).
//!
//! Sinks normally consume `ChainEvent` and must:
//! - filter by event type
//! - deserialize JSON payloads
//! - construct `DeliveryPayload` receipts
//!
//! `SinkTyped` and `FallibleSinkTyped` eliminate that boilerplate by working
//! with domain types that implement `TypedPayload`.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::SinkHandler;
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
            .finish()
    }
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
}

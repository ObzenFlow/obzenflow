//! Typed source helpers (FLOWIP-081)
//!
//! These helpers let sources emit domain types (`T: TypedPayload + Serialize`) instead of
//! manually constructing `ChainEvent` values with stage `WriterId` boilerplate.
//!
//! Notes:
//! - EOF is supervisor-owned. Typed sources signal completion via `Ok(None)`.
//! - The runtime injects the stage `WriterId` via `bind_writer_id()` before the first `next()`.

use crate::stages::common::handlers::{AsyncFiniteSourceHandler, FiniteSourceHandler};
use crate::stages::common::handlers::source::SourceError;
use async_trait::async_trait;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, TypedPayload, WriterId};
use serde::Serialize;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;

/// Typed finite source for synchronous producers (use with `source!`).
///
/// The producer returns:
/// - `Some(vec![...])` to emit one or more items
/// - `Some(vec![])` for an idle poll (no data ready yet)
/// - `None` to signal completion (supervisor emits EOF)
#[derive(Clone)]
pub struct FiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Option<Vec<T>> + Send + Sync + Clone,
{
    producer: F,
    current_index: usize,
    writer_id: Option<WriterId>,
    _phantom: PhantomData<T>,
}

impl<T, F> FiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Option<Vec<T>> + Send + Sync + Clone,
{
    /// Create from a batch producer.
    pub fn from_fn(producer: F) -> Self {
        Self {
            producer,
            current_index: 0,
            writer_id: None,
            _phantom: PhantomData,
        }
    }
}

impl<T> FiniteSourceTyped<T, fn(usize) -> Option<Vec<T>>>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
{
    /// Create from an iterator (collects to a Vec; requires `T: Clone`).
    ///
    /// Not suitable for very large datasets. Use a dedicated streaming source
    /// (e.g., FLOWIP-084 connectors) when inputs are unbounded.
    pub fn from_iter<I>(
        iter: I,
    ) -> FiniteSourceTyped<T, impl FnMut(usize) -> Option<Vec<T>> + Send + Sync + Clone>
    where
        I: IntoIterator<Item = T>,
    {
        let items: Vec<T> = iter.into_iter().collect();
        let total = items.len();

        FiniteSourceTyped::from_fn(move |index| {
            if index >= total {
                None
            } else {
                Some(vec![items[index].clone()])
            }
        })
    }

    /// Convenience: create from a single-item producer.
    pub fn from_item_fn<G>(
        mut producer: G,
    ) -> FiniteSourceTyped<T, impl FnMut(usize) -> Option<Vec<T>> + Send + Sync + Clone>
    where
        G: FnMut(usize) -> Option<T> + Send + Sync + Clone,
    {
        FiniteSourceTyped::from_fn(move |index| producer(index).map(|item| vec![item]))
    }

    /// Create from a fallible batch producer.
    pub fn from_fallible_fn<F>(
        producer: F,
    ) -> FallibleFiniteSourceTyped<T, F>
    where
        F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync + Clone,
    {
        FallibleFiniteSourceTyped::from_fallible_fn(producer)
    }

    /// Convenience: create from a fallible single-item producer.
    pub fn from_fallible_item_fn<G>(
        producer: G,
    ) -> FallibleFiniteSourceTyped<
        T,
        impl FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync + Clone,
    >
    where
        G: FnMut(usize) -> Result<Option<T>, SourceError> + Send + Sync + Clone,
    {
        FallibleFiniteSourceTyped::from_fallible_item_fn(producer)
    }
}

impl<T, F> std::fmt::Debug for FiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Option<Vec<T>> + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .field("writer_id_bound", &self.writer_id.is_some())
            .finish()
    }
}

impl<T, F> FiniteSourceHandler for FiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Option<Vec<T>> + Send + Sync + Clone,
{
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| SourceError::Other("WriterId not bound".to_string()))?;

        match (self.producer)(self.current_index) {
            Some(items) if items.is_empty() => Ok(Some(Vec::new())),
            Some(items) => {
                let event_type = T::versioned_event_type();
                let item_count = items.len();
                let mut events = Vec::with_capacity(item_count);
                for item in items {
                    let event = ChainEventFactory::data_event_from(writer_id, &event_type, &item)
                        .map_err(|e| {
                            SourceError::Other(format!(
                                "SourceTyped failed to serialize {}: {e}",
                                std::any::type_name::<T>()
                            ))
                        })?;
                    events.push(event);
                }
                self.current_index = self.current_index.saturating_add(item_count);
                Ok(Some(events))
            }
            None => Ok(None),
        }
    }
}

// =============================================================================
// Fallible Sync Source
// =============================================================================

/// Typed finite source for fallible synchronous producers (use with `source!`).
///
/// Unlike `FiniteSourceTyped`, this accepts producers that can return `SourceError`,
/// enabling typed sources for error-injection testing and unreliable upstreams.
///
/// The producer returns:
/// - `Ok(Some(vec![...]))` to emit one or more items
/// - `Ok(Some(vec![]))` for an idle poll (no data ready yet)
/// - `Ok(None)` to signal completion (supervisor emits EOF)
/// - `Err(SourceError)` to signal a poll failure
#[derive(Clone)]
pub struct FallibleFiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync + Clone,
{
    producer: F,
    current_index: usize,
    writer_id: Option<WriterId>,
    _phantom: PhantomData<T>,
}

impl<T, F> FallibleFiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync + Clone,
{
    /// Create from a fallible batch producer.
    ///
    /// The producer can return `SourceError` to signal poll failures (timeout,
    /// transport errors, etc.), which propagate directly to the handler's `next()`.
    pub fn from_fallible_fn(producer: F) -> Self {
        Self {
            producer,
            current_index: 0,
            writer_id: None,
            _phantom: PhantomData,
        }
    }
}

impl<T> FallibleFiniteSourceTyped<T, fn(usize) -> Result<Option<Vec<T>>, SourceError>>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
{
    /// Convenience: create from a fallible single-item producer.
    ///
    /// Wraps the single-item producer into a batch producer internally.
    pub fn from_fallible_item_fn<G>(
        mut producer: G,
    ) -> FallibleFiniteSourceTyped<T, impl FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync + Clone>
    where
        G: FnMut(usize) -> Result<Option<T>, SourceError> + Send + Sync + Clone,
    {
        FallibleFiniteSourceTyped::from_fallible_fn(move |index| {
            producer(index).map(|opt| opt.map(|item| vec![item]))
        })
    }
}

impl<T, F> std::fmt::Debug for FallibleFiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallibleFiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .field("writer_id_bound", &self.writer_id.is_some())
            .finish()
    }
}

impl<T, F> FiniteSourceHandler for FallibleFiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync + Clone,
{
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| SourceError::Other("WriterId not bound".to_string()))?;

        // Propagate producer errors directly
        match (self.producer)(self.current_index)? {
            Some(items) if items.is_empty() => Ok(Some(Vec::new())),
            Some(items) => {
                let event_type = T::versioned_event_type();
                let item_count = items.len();
                let mut events = Vec::with_capacity(item_count);
                for item in items {
                    let event = ChainEventFactory::data_event_from(writer_id, &event_type, &item)
                        .map_err(|e| {
                            SourceError::Other(format!(
                                "FallibleSourceTyped failed to serialize {}: {e}",
                                std::any::type_name::<T>()
                            ))
                        })?;
                    events.push(event);
                }
                self.current_index = self.current_index.saturating_add(item_count);
                Ok(Some(events))
            }
            None => Ok(None),
        }
    }
}

// =============================================================================
// Infallible Async Source
// =============================================================================

/// Typed finite source for asynchronous producers (use with `async_source!`).
///
/// The producer returns:
/// - `Some(vec![...])` to emit one or more items
/// - `Some(vec![])` for an idle poll (no data ready yet)
/// - `None` to signal completion (supervisor emits EOF)
pub struct AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    producer: F,
    current_index: usize,
    writer_id: Option<WriterId>,
    _phantom: PhantomData<fn() -> (T, Fut)>,
}

impl<T, F, Fut> Clone for AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            current_index: self.current_index,
            writer_id: self.writer_id,
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    /// Create from an async batch producer.
    pub fn from_fn(producer: F) -> Self {
        Self {
            producer,
            current_index: 0,
            writer_id: None,
            _phantom: PhantomData,
        }
    }

    /// Alias for readability when constructing async sources.
    pub fn from_async_fn(producer: F) -> Self {
        Self::from_fn(producer)
    }
}

impl<T, F, Fut> std::fmt::Debug for AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncFiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .field("writer_id_bound", &self.writer_id.is_some())
            .finish()
    }
}

#[async_trait]
impl<T, F, Fut> AsyncFiniteSourceHandler for AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| SourceError::Other("WriterId not bound".to_string()))?;

        match (self.producer)(self.current_index).await {
            Some(items) if items.is_empty() => Ok(Some(Vec::new())),
            Some(items) => {
                let event_type = T::versioned_event_type();
                let item_count = items.len();
                let mut events = Vec::with_capacity(item_count);
                for item in items {
                    let event = ChainEventFactory::data_event_from(writer_id, &event_type, &item)
                        .map_err(|e| {
                            SourceError::Other(format!(
                                "SourceTyped failed to serialize {}: {e}",
                                std::any::type_name::<T>()
                            ))
                        })?;
                    events.push(event);
                }
                self.current_index = self.current_index.saturating_add(item_count);
                Ok(Some(events))
            }
            None => Ok(None),
        }
    }
}

// =============================================================================
// Fallible Async Source
// =============================================================================

/// Typed finite source for fallible asynchronous producers (use with `async_source!`).
///
/// Unlike `AsyncFiniteSourceTyped`, this accepts producers that can return `SourceError`,
/// enabling typed sources for error-injection testing and unreliable upstreams.
///
/// The producer returns:
/// - `Ok(Some(vec![...]))` to emit one or more items
/// - `Ok(Some(vec![]))` for an idle poll (no data ready yet)
/// - `Ok(None)` to signal completion (supervisor emits EOF)
/// - `Err(SourceError)` to signal a poll failure
pub struct FallibleAsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
{
    producer: F,
    current_index: usize,
    writer_id: Option<WriterId>,
    _phantom: PhantomData<fn() -> (T, Fut)>,
}

impl<T, F, Fut> Clone for FallibleAsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
{
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            current_index: self.current_index,
            writer_id: self.writer_id,
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> FallibleAsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
{
    /// Create from a fallible async batch producer.
    ///
    /// The producer can return `SourceError` to signal poll failures (timeout,
    /// transport errors, etc.), which propagate directly to the handler's `next()`.
    pub fn from_fallible_async_fn(producer: F) -> Self {
        Self {
            producer,
            current_index: 0,
            writer_id: None,
            _phantom: PhantomData,
        }
    }
}

impl<T>
    FallibleAsyncFiniteSourceTyped<
        T,
        fn(usize) -> std::future::Ready<Result<Option<Vec<T>>, SourceError>>,
        std::future::Ready<Result<Option<Vec<T>>, SourceError>>,
    >
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
{
    /// Convenience: create from a fallible async single-item producer.
    ///
    /// This internally wraps the item producer into a batch producer and boxes the future so
    /// callers can write `|i| async move { ... }` without additional adapters.
    pub fn from_fallible_async_item_fn<G, FutItem>(
        mut producer: G,
    ) -> FallibleAsyncFiniteSourceTyped<
        T,
        impl FnMut(usize) -> Pin<Box<dyn Future<Output = Result<Option<Vec<T>>, SourceError>> + Send>>
            + Send
            + Sync
            + Clone,
        Pin<Box<dyn Future<Output = Result<Option<Vec<T>>, SourceError>> + Send>>,
    >
    where
        G: FnMut(usize) -> FutItem + Send + Sync + Clone + 'static,
        FutItem: Future<Output = Result<Option<T>, SourceError>> + Send + 'static,
    {
        FallibleAsyncFiniteSourceTyped::from_fallible_async_fn(move |index| {
            let fut = producer(index);
            Box::pin(async move { fut.await.map(|opt| opt.map(|item| vec![item])) })
                as Pin<
                    Box<
                        dyn Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
                    >,
                >
        })
    }
}

impl<T>
    AsyncFiniteSourceTyped<
        T,
        fn(usize) -> std::future::Ready<Option<Vec<T>>>,
        std::future::Ready<Option<Vec<T>>>,
    >
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
{
    /// Create from a fallible async batch producer.
    pub fn from_fallible_async_fn<G, Fut>(
        producer: G,
    ) -> FallibleAsyncFiniteSourceTyped<T, G, Fut>
    where
        G: FnMut(usize) -> Fut + Send + Sync + Clone,
        Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
    {
        FallibleAsyncFiniteSourceTyped::from_fallible_async_fn(producer)
    }

    /// Convenience: create from a fallible async single-item producer.
    pub fn from_fallible_async_item_fn<G, FutItem>(
        producer: G,
    ) -> FallibleAsyncFiniteSourceTyped<
        T,
        impl FnMut(usize) -> Pin<Box<dyn Future<Output = Result<Option<Vec<T>>, SourceError>> + Send>>
            + Send
            + Sync
            + Clone,
        Pin<Box<dyn Future<Output = Result<Option<Vec<T>>, SourceError>> + Send>>,
    >
    where
        G: FnMut(usize) -> FutItem + Send + Sync + Clone + 'static,
        FutItem: Future<Output = Result<Option<T>, SourceError>> + Send + 'static,
    {
        FallibleAsyncFiniteSourceTyped::from_fallible_async_item_fn(producer)
    }
}

impl<T, F, Fut> std::fmt::Debug for FallibleAsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallibleAsyncFiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .field("writer_id_bound", &self.writer_id.is_some())
            .finish()
    }
}

#[async_trait]
impl<T, F, Fut> AsyncFiniteSourceHandler for FallibleAsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
{
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| SourceError::Other("WriterId not bound".to_string()))?;

        // Propagate producer errors directly
        match (self.producer)(self.current_index).await? {
            Some(items) if items.is_empty() => Ok(Some(Vec::new())),
            Some(items) => {
                let event_type = T::versioned_event_type();
                let item_count = items.len();
                let mut events = Vec::with_capacity(item_count);
                for item in items {
                    let event = ChainEventFactory::data_event_from(writer_id, &event_type, &item)
                        .map_err(|e| {
                            SourceError::Other(format!(
                                "FallibleAsyncSourceTyped failed to serialize {}: {e}",
                                std::any::type_name::<T>()
                            ))
                        })?;
                    events.push(event);
                }
                self.current_index = self.current_index.saturating_add(item_count);
                Ok(Some(events))
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::id::StageId;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestPayload {
        n: usize,
    }

    impl TypedPayload for TestPayload {
        const EVENT_TYPE: &'static str = "test.payload";
    }

    #[test]
    fn finite_source_typed_emits_and_completes() {
        let total = 3usize;
        let mut src = FiniteSourceTyped::from_item_fn(move |index| {
            if index >= total {
                None
            } else {
                Some(TestPayload { n: index })
            }
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        let first = src.next().expect("next should succeed").expect("not complete");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].writer_id, writer_id);
        assert_eq!(first[0].event_type(), TestPayload::versioned_event_type());

        let _ = src.next().expect("next should succeed");
        let _ = src.next().expect("next should succeed");
        let done = src.next().expect("next should succeed");
        assert!(done.is_none(), "expected completion via Ok(None)");
    }

    #[tokio::test]
    async fn async_finite_source_typed_emits_and_completes() {
        let total = 2usize;
        let mut src = AsyncFiniteSourceTyped::from_fn(move |index| async move {
            if index >= total {
                None
            } else {
                Some(vec![TestPayload { n: index }])
            }
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        let first = src
            .next()
            .await
            .expect("next should succeed")
            .expect("not complete");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].writer_id, writer_id);
        assert_eq!(first[0].event_type(), TestPayload::versioned_event_type());

        let _ = src.next().await.expect("next should succeed");
        let done = src.next().await.expect("next should succeed");
        assert!(done.is_none(), "expected completion via Ok(None)");
    }

    // =========================================================================
    // Fallible sync source tests
    // =========================================================================

    #[test]
    fn fallible_finite_source_typed_emits_and_completes() {
        let total = 3usize;
        let mut src = FallibleFiniteSourceTyped::from_fallible_item_fn(move |index| {
            if index >= total {
                Ok(None)
            } else {
                Ok(Some(TestPayload { n: index }))
            }
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        let first = src.next().expect("next should succeed").expect("not complete");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].writer_id, writer_id);
        assert_eq!(first[0].event_type(), TestPayload::versioned_event_type());

        let _ = src.next().expect("next should succeed");
        let _ = src.next().expect("next should succeed");
        let done = src.next().expect("next should succeed");
        assert!(done.is_none(), "expected completion via Ok(None)");
    }

    #[test]
    fn fallible_finite_source_typed_propagates_errors() {
        let mut src = FallibleFiniteSourceTyped::from_fallible_item_fn(|index| {
            if index == 0 {
                Ok(Some(TestPayload { n: 0 }))
            } else {
                Err(SourceError::Timeout("simulated timeout".to_string()))
            }
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        // First call succeeds
        let first = src.next().expect("next should succeed").expect("not complete");
        assert_eq!(first.len(), 1);

        // Second call returns error
        let err = src.next().expect_err("expected error");
        match err {
            SourceError::Timeout(msg) => assert_eq!(msg, "simulated timeout"),
            other => panic!("expected Timeout, got {:?}", other),
        }
    }

    #[test]
    fn fallible_finite_source_typed_retries_same_index_after_error() {
        let attempt_at_one = std::sync::Arc::new(std::sync::Mutex::new(0usize));

        let mut src = FallibleFiniteSourceTyped::from_fallible_item_fn(move |index| {
            if index >= 2 {
                return Ok(None);
            }

            if index == 1 {
                let mut attempts = attempt_at_one.lock().unwrap();
                *attempts += 1;
                if *attempts == 1 {
                    return Err(SourceError::Transport("network error".to_string()));
                }
            }

            Ok(Some(TestPayload { n: index }))
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        assert_eq!(src.current_index, 0);

        // First: success (index advances)
        let first = src.next().expect("next should succeed").expect("not complete");
        assert_eq!(first.len(), 1);
        assert_eq!(src.current_index, 1);

        // Second: error (index does not advance)
        let err = src.next().expect_err("expected error");
        assert!(matches!(err, SourceError::Transport(_)));
        assert_eq!(src.current_index, 1);

        // Third: success (retry same index)
        let second = src.next().expect("next should succeed").expect("not complete");
        assert_eq!(second.len(), 1);
        assert_eq!(src.current_index, 2);

        // Fourth: complete
        let result = src.next().expect("next should succeed");
        assert!(result.is_none());
        assert_eq!(src.current_index, 2);
    }

    // =========================================================================
    // Fallible async source tests
    // =========================================================================

    #[tokio::test]
    async fn fallible_async_finite_source_typed_emits_and_completes() {
        let total = 2usize;
        let mut src = FallibleAsyncFiniteSourceTyped::from_fallible_async_fn(move |index| async move {
            if index >= total {
                Ok(None)
            } else {
                Ok(Some(vec![TestPayload { n: index }]))
            }
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        let first = src.next().await.expect("next should succeed").expect("not complete");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].writer_id, writer_id);
        assert_eq!(first[0].event_type(), TestPayload::versioned_event_type());

        let _ = src.next().await.expect("next should succeed");
        let done = src.next().await.expect("next should succeed");
        assert!(done.is_none(), "expected completion via Ok(None)");
    }

    #[tokio::test]
    async fn fallible_async_finite_source_typed_propagates_errors() {
        let mut src = FallibleAsyncFiniteSourceTyped::from_fallible_async_fn(|index| async move {
            if index == 0 {
                Ok(Some(vec![TestPayload { n: 0 }]))
            } else {
                Err(SourceError::Deserialization("bad data".to_string()))
            }
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        // First call succeeds
        let first = src.next().await.expect("next should succeed").expect("not complete");
        assert_eq!(first.len(), 1);

        // Second call returns error
        let err = src.next().await.expect_err("expected error");
        match err {
            SourceError::Deserialization(msg) => assert_eq!(msg, "bad data"),
            other => panic!("expected Deserialization, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn fallible_async_finite_source_typed_item_ctor_emits_and_completes() {
        let total = 2usize;
        let mut src = AsyncFiniteSourceTyped::from_fallible_async_item_fn(move |index| async move {
            if index >= total {
                Ok(None)
            } else {
                Ok(Some(TestPayload { n: index }))
            }
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        let first = src.next().await.expect("next should succeed").expect("not complete");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].writer_id, writer_id);
        assert_eq!(first[0].event_type(), TestPayload::versioned_event_type());

        let _ = src.next().await.expect("next should succeed");
        let done = src.next().await.expect("next should succeed");
        assert!(done.is_none(), "expected completion via Ok(None)");
    }
}

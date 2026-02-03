// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed source helpers (FLOWIP-081 + FLOWIP-081d)
//!
//! These helpers let sources emit domain types (`T: TypedPayload + Serialize`) instead of
//! manually constructing `ChainEvent` values with stage `WriterId` boilerplate.
//!
//! Notes:
//! - Finite sources: EOF is supervisor-owned. Typed sources signal completion via `Ok(None)`.
//! - Infinite sources: never complete naturally; they run until external shutdown.
//! - The runtime injects the stage `WriterId` via `bind_writer_id()` before the first `next()`.

use crate::stages::common::handlers::source::SourceError;
use crate::stages::common::handlers::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler,
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, TypedPayload, WriterId};
use serde::Serialize;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as TokioMutex;

type BoxedFallibleBatchFuture<T> =
    Pin<Box<dyn Future<Output = Result<Option<Vec<T>>, SourceError>> + Send>>;
type BoxedFallibleVecFuture<T> = Pin<Box<dyn Future<Output = Result<Vec<T>, SourceError>> + Send>>;

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
    pub fn from_producer(producer: F) -> Self {
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
    /// Create from an iterator (primary constructor).
    ///
    /// Collects the iterator into a `Vec` and clones items on emission.
    /// Not suitable for very large datasets. Use a dedicated streaming source
    /// (e.g., FLOWIP-084 connectors) when inputs are unbounded.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let events = vec![MyEvent { id: 1 }, MyEvent { id: 2 }];
    /// let source = FiniteSourceTyped::new(events);
    /// ```
    pub fn new<I>(
        iter: I,
    ) -> FiniteSourceTyped<T, impl FnMut(usize) -> Option<Vec<T>> + Send + Sync + Clone>
    where
        I: IntoIterator<Item = T>,
    {
        let items: Vec<T> = iter.into_iter().collect();
        let total = items.len();

        FiniteSourceTyped::from_producer(move |index| {
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
        FiniteSourceTyped::from_producer(move |index| producer(index).map(|item| vec![item]))
    }

    /// Create a fallible finite source from a batch producer.
    ///
    /// Returns `FallibleFiniteSourceTyped` for error-returning producers.
    pub fn fallible<F>(producer: F) -> FallibleFiniteSourceTyped<T, F>
    where
        F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync + Clone,
    {
        FallibleFiniteSourceTyped::new(producer)
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
    /// Create from a fallible batch producer (primary constructor).
    ///
    /// The producer can return `SourceError` to signal poll failures (timeout,
    /// transport errors, etc.), which propagate directly to the handler's `next()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let source = FallibleFiniteSourceTyped::new(|index| {
    ///     if index >= 10 { Ok(None) } else { Ok(Some(vec![MyEvent { id: index }])) }
    /// });
    /// ```
    pub fn new(producer: F) -> Self {
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
    ) -> FallibleFiniteSourceTyped<
        T,
        impl FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync + Clone,
    >
    where
        G: FnMut(usize) -> Result<Option<T>, SourceError> + Send + Sync + Clone,
    {
        FallibleFiniteSourceTyped::new(move |index| {
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
    /// Create from an async batch producer (primary constructor).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let source = AsyncFiniteSourceTyped::new(|index| async move {
    ///     if index >= 10 { None } else { Some(vec![MyEvent { id: index }]) }
    /// });
    /// ```
    pub fn new(producer: F) -> Self {
        Self {
            producer,
            current_index: 0,
            writer_id: None,
            _phantom: PhantomData,
        }
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
    /// Create from a fallible async batch producer (primary constructor).
    ///
    /// The producer can return `SourceError` to signal poll failures (timeout,
    /// transport errors, etc.), which propagate directly to the handler's `next()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let source = FallibleAsyncFiniteSourceTyped::new(|index| async move {
    ///     if index >= 10 { Ok(None) } else { Ok(Some(vec![MyEvent { id: index }])) }
    /// });
    /// ```
    pub fn new(producer: F) -> Self {
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
        impl FnMut(usize) -> BoxedFallibleBatchFuture<T> + Send + Sync + Clone,
        BoxedFallibleBatchFuture<T>,
    >
    where
        G: FnMut(usize) -> FutItem + Send + Sync + Clone + 'static,
        FutItem: Future<Output = Result<Option<T>, SourceError>> + Send + 'static,
    {
        FallibleAsyncFiniteSourceTyped::new(move |index| {
            let fut = producer(index);
            Box::pin(async move { fut.await.map(|opt| opt.map(|item| vec![item])) })
                as BoxedFallibleBatchFuture<T>
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
    /// Create a fallible async source from a batch producer.
    ///
    /// Returns `FallibleAsyncFiniteSourceTyped` for error-returning producers.
    pub fn fallible<G, Fut>(producer: G) -> FallibleAsyncFiniteSourceTyped<T, G, Fut>
    where
        G: FnMut(usize) -> Fut + Send + Sync + Clone,
        Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
    {
        FallibleAsyncFiniteSourceTyped::new(producer)
    }

    /// Convenience: create from a fallible async single-item producer.
    pub fn from_fallible_async_item_fn<G, FutItem>(
        producer: G,
    ) -> FallibleAsyncFiniteSourceTyped<
        T,
        impl FnMut(usize) -> BoxedFallibleBatchFuture<T> + Send + Sync + Clone,
        BoxedFallibleBatchFuture<T>,
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

// =============================================================================
// Typed Infinite Sources (FLOWIP-081d)
// =============================================================================

const DEFAULT_INFINITE_RECEIVER_BATCH_CAP: usize = 100;

/// Typed infinite source for synchronous producers (use with `infinite_source!`).
///
/// The producer returns:
/// - `vec![...]` to emit one or more items
/// - `vec![]` for an idle poll (no data ready yet)
#[derive(Clone)]
pub struct InfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Vec<T> + Send + Sync + Clone,
{
    producer: F,
    current_index: usize,
    writer_id: Option<WriterId>,
    _phantom: PhantomData<T>,
}

impl<T, F> InfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Vec<T> + Send + Sync + Clone,
{
    /// Create from a batch producer (primary constructor).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let source = InfiniteSourceTyped::new(|index| {
    ///     vec![MyEvent { id: index }]
    /// });
    /// ```
    pub fn new(producer: F) -> Self {
        Self {
            producer,
            current_index: 0,
            writer_id: None,
            _phantom: PhantomData,
        }
    }
}

impl<T> InfiniteSourceTyped<T, fn(usize) -> Vec<T>>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
{
    /// Create a fallible infinite source from a batch producer.
    ///
    /// Returns `FallibleInfiniteSourceTyped` for error-returning producers.
    pub fn fallible<G>(producer: G) -> FallibleInfiniteSourceTyped<T, G>
    where
        G: FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync + Clone,
    {
        FallibleInfiniteSourceTyped::new(producer)
    }

    /// Create from a sync channel receiver with a batch size cap.
    ///
    /// Drains up to `batch_cap` messages per poll using `try_recv()`.
    /// Returns:
    /// - `Ok(vec![])` when the channel is empty (idle)
    /// - `Err(SourceError::Other("channel closed"))` when the channel is closed
    pub fn from_receiver(
        receiver: std::sync::mpsc::Receiver<T>,
        batch_cap: Option<usize>,
    ) -> FallibleInfiniteSourceTyped<
        T,
        impl FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync + Clone,
    > {
        let cap = batch_cap
            .unwrap_or(DEFAULT_INFINITE_RECEIVER_BATCH_CAP)
            .max(1);
        let shared = Arc::new(Mutex::new(receiver));

        FallibleInfiniteSourceTyped::new(move |_index| {
            let mut batch = Vec::new();
            let guard = shared
                .lock()
                .map_err(|e| SourceError::Other(format!("channel receiver lock poisoned: {e}")))?;

            for _ in 0..cap {
                match guard.try_recv() {
                    Ok(item) => batch.push(item),
                    Err(std::sync::mpsc::TryRecvError::Empty) => break,
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                        if batch.is_empty() {
                            return Err(SourceError::Other("channel closed".to_string()));
                        }
                        break;
                    }
                }
            }

            Ok(batch)
        })
    }
}

impl<T, F> std::fmt::Debug for InfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Vec<T> + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .field("writer_id_bound", &self.writer_id.is_some())
            .finish()
    }
}

impl<T, F> InfiniteSourceHandler for InfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Vec<T> + Send + Sync + Clone,
{
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| SourceError::Other("WriterId not bound".to_string()))?;

        let items = (self.producer)(self.current_index);
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let event_type = T::versioned_event_type();
        let item_count = items.len();
        let mut events = Vec::with_capacity(item_count);
        for item in items {
            let event =
                ChainEventFactory::data_event_from(writer_id, &event_type, &item).map_err(|e| {
                    SourceError::Other(format!(
                        "InfiniteSourceTyped failed to serialize {}: {e}",
                        std::any::type_name::<T>()
                    ))
                })?;
            events.push(event);
        }
        self.current_index = self.current_index.saturating_add(item_count);
        Ok(events)
    }
}

// =============================================================================
// Fallible Sync Infinite Source
// =============================================================================

/// Typed infinite source for fallible synchronous producers (use with `infinite_source!`).
///
/// Unlike `InfiniteSourceTyped`, this accepts producers that can return `SourceError`,
/// enabling typed sources for error-injection testing and unreliable upstreams.
///
/// The producer returns:
/// - `Ok(vec![...])` to emit one or more items
/// - `Ok(vec![])` for an idle poll (no data ready yet)
/// - `Err(SourceError)` to signal a poll failure
#[derive(Clone)]
pub struct FallibleInfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync + Clone,
{
    producer: F,
    current_index: usize,
    writer_id: Option<WriterId>,
    _phantom: PhantomData<T>,
}

impl<T, F> FallibleInfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync + Clone,
{
    /// Create from a fallible batch producer (primary constructor).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let source = FallibleInfiniteSourceTyped::new(|index| {
    ///     Ok(vec![MyEvent { id: index }])
    /// });
    /// ```
    pub fn new(producer: F) -> Self {
        Self {
            producer,
            current_index: 0,
            writer_id: None,
            _phantom: PhantomData,
        }
    }
}

impl<T, F> std::fmt::Debug for FallibleInfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallibleInfiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .field("writer_id_bound", &self.writer_id.is_some())
            .finish()
    }
}

impl<T, F> InfiniteSourceHandler for FallibleInfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync + Clone,
{
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| SourceError::Other("WriterId not bound".to_string()))?;

        let items = (self.producer)(self.current_index)?;
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let event_type = T::versioned_event_type();
        let item_count = items.len();
        let mut events = Vec::with_capacity(item_count);
        for item in items {
            let event =
                ChainEventFactory::data_event_from(writer_id, &event_type, &item).map_err(|e| {
                    SourceError::Other(format!(
                        "FallibleInfiniteSourceTyped failed to serialize {}: {e}",
                        std::any::type_name::<T>()
                    ))
                })?;
            events.push(event);
        }
        self.current_index = self.current_index.saturating_add(item_count);
        Ok(events)
    }
}

// =============================================================================
// Typed Async Infinite Sources
// =============================================================================

/// Typed infinite source for asynchronous producers (use with `async_infinite_source!`).
///
/// Note: Manual `Clone` impl to avoid requiring `Fut: Clone`.
pub struct AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Vec<T>> + Send,
{
    producer: F,
    current_index: usize,
    writer_id: Option<WriterId>,
    _phantom: PhantomData<fn() -> (T, Fut)>,
}

impl<T, F, Fut> Clone for AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Vec<T>> + Send,
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

impl<T, F, Fut> AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Vec<T>> + Send,
{
    /// Create from an async batch producer (primary constructor).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let source = AsyncInfiniteSourceTyped::new(|index| async move {
    ///     vec![MyEvent { id: index }]
    /// });
    /// ```
    pub fn new(producer: F) -> Self {
        Self {
            producer,
            current_index: 0,
            writer_id: None,
            _phantom: PhantomData,
        }
    }
}

impl<T>
    AsyncInfiniteSourceTyped<T, fn(usize) -> std::future::Ready<Vec<T>>, std::future::Ready<Vec<T>>>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
{
    /// Create a fallible async infinite source from a batch producer.
    ///
    /// Returns `FallibleAsyncInfiniteSourceTyped` for error-returning producers.
    pub fn fallible<G, FutG>(producer: G) -> FallibleAsyncInfiniteSourceTyped<T, G, FutG>
    where
        G: FnMut(usize) -> FutG + Send + Sync + Clone,
        FutG: Future<Output = Result<Vec<T>, SourceError>> + Send,
    {
        FallibleAsyncInfiniteSourceTyped::new(producer)
    }

    /// Create from an async `Stream` with shared progress.
    ///
    /// The stream is wrapped in `Arc<Mutex<...>>` so handler clones share progress.
    /// Stream end is modeled as `Err(SourceError::Other("stream ended"))`.
    pub fn from_stream<S>(
        stream: S,
    ) -> FallibleAsyncInfiniteSourceTyped<
        T,
        impl FnMut(usize) -> BoxedFallibleVecFuture<T> + Send + Sync + Clone,
        BoxedFallibleVecFuture<T>,
    >
    where
        S: Stream<Item = T> + Send + Unpin + 'static,
    {
        let shared = Arc::new(TokioMutex::new(stream));
        FallibleAsyncInfiniteSourceTyped::new(move |_index| {
            let stream = shared.clone();
            Box::pin(async move {
                let mut guard = stream.lock().await;
                match guard.next().await {
                    Some(item) => Ok(vec![item]),
                    None => Err(SourceError::Other("stream ended".to_string())),
                }
            }) as BoxedFallibleVecFuture<T>
        })
    }

    /// Create from an async channel receiver.
    ///
    /// The receiver is wrapped in `Arc<Mutex<...>>` so handler clones share progress.
    /// Channel close is modeled as `Err(SourceError::Other("channel closed"))`.
    pub fn from_receiver(
        receiver: tokio::sync::mpsc::Receiver<T>,
    ) -> FallibleAsyncInfiniteSourceTyped<
        T,
        impl FnMut(usize) -> BoxedFallibleVecFuture<T> + Send + Sync + Clone,
        BoxedFallibleVecFuture<T>,
    > {
        let shared = Arc::new(TokioMutex::new(receiver));
        FallibleAsyncInfiniteSourceTyped::new(move |_index| {
            let rx = shared.clone();
            Box::pin(async move {
                let mut guard = rx.lock().await;
                match guard.recv().await {
                    Some(item) => Ok(vec![item]),
                    None => Err(SourceError::Other("channel closed".to_string())),
                }
            }) as BoxedFallibleVecFuture<T>
        })
    }
}

impl<T, F, Fut> std::fmt::Debug for AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Vec<T>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncInfiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .field("writer_id_bound", &self.writer_id.is_some())
            .finish()
    }
}

#[async_trait]
impl<T, F, Fut> AsyncInfiniteSourceHandler for AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Vec<T>> + Send,
{
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| SourceError::Other("WriterId not bound".to_string()))?;

        let items = (self.producer)(self.current_index).await;
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let event_type = T::versioned_event_type();
        let item_count = items.len();
        let mut events = Vec::with_capacity(item_count);
        for item in items {
            let event =
                ChainEventFactory::data_event_from(writer_id, &event_type, &item).map_err(|e| {
                    SourceError::Other(format!(
                        "AsyncInfiniteSourceTyped failed to serialize {}: {e}",
                        std::any::type_name::<T>()
                    ))
                })?;
            events.push(event);
        }
        self.current_index = self.current_index.saturating_add(item_count);
        Ok(events)
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

// =============================================================================
// Fallible Async Infinite Source
// =============================================================================

/// Typed infinite source for fallible asynchronous producers (use with `async_infinite_source!`).
///
/// Unlike `AsyncInfiniteSourceTyped`, this accepts producers that can return `SourceError`,
/// enabling typed sources for error-injection testing and unreliable upstreams.
///
/// The producer returns:
/// - `Ok(vec![...])` to emit one or more items
/// - `Ok(vec![])` for an idle poll (no data ready yet)
/// - `Err(SourceError)` to signal a poll failure
pub struct FallibleAsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Vec<T>, SourceError>> + Send,
{
    producer: F,
    current_index: usize,
    writer_id: Option<WriterId>,
    _phantom: PhantomData<fn() -> (T, Fut)>,
}

impl<T, F, Fut> Clone for FallibleAsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Vec<T>, SourceError>> + Send,
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

impl<T, F, Fut> FallibleAsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Vec<T>, SourceError>> + Send,
{
    /// Create from a fallible async batch producer (primary constructor).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let source = FallibleAsyncInfiniteSourceTyped::new(|index| async move {
    ///     Ok(vec![MyEvent { id: index }])
    /// });
    /// ```
    pub fn new(producer: F) -> Self {
        Self {
            producer,
            current_index: 0,
            writer_id: None,
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> std::fmt::Debug for FallibleAsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Vec<T>, SourceError>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallibleAsyncInfiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .field("writer_id_bound", &self.writer_id.is_some())
            .finish()
    }
}

#[async_trait]
impl<T, F, Fut> AsyncInfiniteSourceHandler for FallibleAsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Vec<T>, SourceError>> + Send,
{
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        let writer_id = self
            .writer_id
            .ok_or_else(|| SourceError::Other("WriterId not bound".to_string()))?;

        let items = (self.producer)(self.current_index).await?;
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let event_type = T::versioned_event_type();
        let item_count = items.len();
        let mut events = Vec::with_capacity(item_count);
        for item in items {
            let event =
                ChainEventFactory::data_event_from(writer_id, &event_type, &item).map_err(|e| {
                    SourceError::Other(format!(
                        "FallibleAsyncInfiniteSourceTyped failed to serialize {}: {e}",
                        std::any::type_name::<T>()
                    ))
                })?;
            events.push(event);
        }
        self.current_index = self.current_index.saturating_add(item_count);
        Ok(events)
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::id::StageId;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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

        let first = src
            .next()
            .expect("next should succeed")
            .expect("not complete");
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
        let mut src = AsyncFiniteSourceTyped::new(move |index| async move {
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

        let first = src
            .next()
            .expect("next should succeed")
            .expect("not complete");
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
        let first = src
            .next()
            .expect("next should succeed")
            .expect("not complete");
        assert_eq!(first.len(), 1);

        // Second call returns error
        let err = src.next().expect_err("expected error");
        match err {
            SourceError::Timeout(msg) => assert_eq!(msg, "simulated timeout"),
            other => panic!("expected Timeout, got {other:?}"),
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
        let first = src
            .next()
            .expect("next should succeed")
            .expect("not complete");
        assert_eq!(first.len(), 1);
        assert_eq!(src.current_index, 1);

        // Second: error (index does not advance)
        let err = src.next().expect_err("expected error");
        assert!(matches!(err, SourceError::Transport(_)));
        assert_eq!(src.current_index, 1);

        // Third: success (retry same index)
        let second = src
            .next()
            .expect("next should succeed")
            .expect("not complete");
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
        let mut src = FallibleAsyncFiniteSourceTyped::new(move |index| async move {
            if index >= total {
                Ok(None)
            } else {
                Ok(Some(vec![TestPayload { n: index }]))
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

    #[tokio::test]
    async fn fallible_async_finite_source_typed_propagates_errors() {
        let mut src = FallibleAsyncFiniteSourceTyped::new(|index| async move {
            if index == 0 {
                Ok(Some(vec![TestPayload { n: 0 }]))
            } else {
                Err(SourceError::Deserialization("bad data".to_string()))
            }
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        // First call succeeds
        let first = src
            .next()
            .await
            .expect("next should succeed")
            .expect("not complete");
        assert_eq!(first.len(), 1);

        // Second call returns error
        let err = src.next().await.expect_err("expected error");
        match err {
            SourceError::Deserialization(msg) => assert_eq!(msg, "bad data"),
            other => panic!("expected Deserialization, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn fallible_async_finite_source_typed_item_ctor_emits_and_completes() {
        let total = 2usize;
        let mut src =
            AsyncFiniteSourceTyped::from_fallible_async_item_fn(move |index| async move {
                if index >= total {
                    Ok(None)
                } else {
                    Ok(Some(TestPayload { n: index }))
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
    // Infinite source tests (FLOWIP-081d)
    // =========================================================================

    #[test]
    fn infinite_source_typed_emits_and_idles() {
        let mut src = InfiniteSourceTyped::new(|index| {
            if index < 2 {
                vec![TestPayload { n: index }]
            } else {
                Vec::new()
            }
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        let first = src.next().expect("next should succeed");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].writer_id, writer_id);
        assert_eq!(first[0].event_type(), TestPayload::versioned_event_type());
        assert_eq!(src.current_index, 1);

        let second = src.next().expect("next should succeed");
        assert_eq!(second.len(), 1);
        assert_eq!(src.current_index, 2);

        let idle = src.next().expect("next should succeed");
        assert!(idle.is_empty(), "expected idle poll");
        assert_eq!(src.current_index, 2, "idle should not advance index");
    }

    #[test]
    fn fallible_infinite_source_typed_propagates_errors_and_retries() {
        let attempts = Arc::new(Mutex::new(0usize));
        let attempts_for_closure = attempts.clone();

        let mut src = FallibleInfiniteSourceTyped::new(move |index| {
            if index == 0 {
                return Ok(vec![TestPayload { n: 0 }]);
            }

            let mut attempts = attempts_for_closure.lock().expect("attempts lock poisoned");
            *attempts += 1;
            if *attempts == 1 {
                return Err(SourceError::Transport("network error".to_string()));
            }

            Ok(vec![TestPayload { n: index }])
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        assert_eq!(src.current_index, 0);

        // First: success (index advances)
        let first = src.next().expect("next should succeed");
        assert_eq!(first.len(), 1);
        assert_eq!(src.current_index, 1);

        // Second: error (index does not advance)
        let err = src.next().expect_err("expected error");
        assert!(matches!(err, SourceError::Transport(_)));
        assert_eq!(src.current_index, 1);

        // Third: success (retry same index)
        let second = src.next().expect("next should succeed");
        assert_eq!(second.len(), 1);
        assert_eq!(src.current_index, 2);
    }

    #[test]
    fn infinite_source_from_receiver_drains_and_errors_on_close() {
        let (tx, rx) = std::sync::mpsc::channel::<TestPayload>();
        tx.send(TestPayload { n: 1 }).expect("send 1");
        tx.send(TestPayload { n: 2 }).expect("send 2");
        drop(tx);

        let mut src = InfiniteSourceTyped::from_receiver(rx, Some(100));
        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        let batch = src.next().expect("next should succeed");
        assert_eq!(batch.len(), 2);
        assert_eq!(src.current_index, 2);

        let err = src.next().expect_err("expected channel closed error");
        match err {
            SourceError::Other(msg) => assert_eq!(msg, "channel closed"),
            other => panic!("expected Other(\"channel closed\"), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn async_infinite_source_typed_emits_and_idles() {
        let mut src = AsyncInfiniteSourceTyped::new(|index| async move {
            if index < 2 {
                vec![TestPayload { n: index }]
            } else {
                Vec::new()
            }
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        let first = src.next().await.expect("next should succeed");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].writer_id, writer_id);
        assert_eq!(first[0].event_type(), TestPayload::versioned_event_type());
        assert_eq!(src.current_index, 1);

        let second = src.next().await.expect("next should succeed");
        assert_eq!(second.len(), 1);
        assert_eq!(src.current_index, 2);

        let idle = src.next().await.expect("next should succeed");
        assert!(idle.is_empty(), "expected idle poll");
        assert_eq!(src.current_index, 2);
    }

    #[tokio::test]
    async fn fallible_async_infinite_source_typed_propagates_errors() {
        let mut src = FallibleAsyncInfiniteSourceTyped::new(|index| async move {
            if index == 0 {
                Ok(vec![TestPayload { n: 0 }])
            } else {
                Err(SourceError::Timeout("boom".to_string()))
            }
        });

        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        let first = src.next().await.expect("next should succeed");
        assert_eq!(first.len(), 1);
        assert_eq!(src.current_index, 1);

        let err = src.next().await.expect_err("expected error");
        assert!(matches!(err, SourceError::Timeout(_)));
        assert_eq!(src.current_index, 1, "error should not advance index");
    }

    #[tokio::test]
    async fn async_infinite_source_from_receiver_errors_on_close() {
        let (tx, rx) = tokio::sync::mpsc::channel::<TestPayload>(16);
        tx.send(TestPayload { n: 1 }).await.expect("send 1");
        tx.send(TestPayload { n: 2 }).await.expect("send 2");
        drop(tx);

        let mut src = AsyncInfiniteSourceTyped::from_receiver(rx);
        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        let first = src.next().await.expect("next should succeed");
        assert_eq!(first.len(), 1);

        let second = src.next().await.expect("next should succeed");
        assert_eq!(second.len(), 1);

        let err = src.next().await.expect_err("expected channel closed error");
        match err {
            SourceError::Other(msg) => assert_eq!(msg, "channel closed"),
            other => panic!("expected Other(\"channel closed\"), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn async_infinite_source_from_stream_ends_as_error() {
        let stream = futures::stream::iter(vec![TestPayload { n: 1 }, TestPayload { n: 2 }]);
        let mut src = AsyncInfiniteSourceTyped::from_stream(stream);
        let writer_id = WriterId::from(StageId::new());
        src.bind_writer_id(writer_id);

        let first = src.next().await.expect("next should succeed");
        assert_eq!(first.len(), 1);

        let second = src.next().await.expect("next should succeed");
        assert_eq!(second.len(), 1);

        let err = src.next().await.expect_err("expected stream ended error");
        match err {
            SourceError::Other(msg) => assert_eq!(msg, "stream ended"),
            other => panic!("expected Other(\"stream ended\"), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn async_infinite_source_from_stream_clones_share_progress() {
        let stream = futures::stream::iter(vec![TestPayload { n: 1 }, TestPayload { n: 2 }]);
        let mut src1 = AsyncInfiniteSourceTyped::from_stream(stream);
        let writer_id = WriterId::from(StageId::new());
        src1.bind_writer_id(writer_id);

        let mut src2 = src1.clone();

        let first = src1.next().await.expect("next should succeed");
        assert_eq!(first.len(), 1);
        assert_eq!(
            TestPayload::from_event(&first[0]).expect("payload"),
            TestPayload { n: 1 }
        );

        let second = src2.next().await.expect("next should succeed");
        assert_eq!(second.len(), 1);
        assert_eq!(
            TestPayload::from_event(&second[0]).expect("payload"),
            TestPayload { n: 2 }
        );
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed source helpers
//!
//! These helpers return domain types and let the sealed source adapter own
//! envelope construction and writer identity.
//!
//! Notes:
//! - Finite sources: EOF is supervisor-owned. Typed sources signal completion via `Ok(None)`.
//! - Infinite sources: never complete naturally; they run until external shutdown.

use crate::stages::common::handlers::source::SourceError;
use crate::stages::common::handlers::{
    TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler,
};
use crate::typing::SourceTyping;
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use obzenflow_core::TypedPayload;
use serde::Serialize;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Mutex;

type BoxedFallibleBatchFuture<T> =
    Pin<Box<dyn Future<Output = Result<Option<Vec<T>>, SourceError>> + Send>>;

/// Typed finite source for synchronous producers (use with `source!`).
///
/// The producer returns:
/// - `Some(vec![...])` to emit one or more items
/// - `Some(vec![])` for an idle poll (no data ready yet)
/// - `None` to signal completion (supervisor emits EOF)
#[derive(Clone)]
pub struct FiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Option<Vec<T>> + Send + Sync,
{
    producer: F,
    current_index: usize,
    _phantom: PhantomData<T>,
}

impl<T, F> FiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Option<Vec<T>> + Send + Sync,
{
    /// Create from a batch producer.
    pub fn from_producer(producer: F) -> Self {
        Self {
            producer,
            current_index: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T> FiniteSourceTyped<T, fn(usize) -> Option<Vec<T>>>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
{
    /// Create from an iterator (primary constructor).
    ///
    /// Owns the input without calling `into_iter` or `next` during construction.
    /// Live polling creates the iterator once and moves one item per poll, without
    /// collecting the input. Strict replay leaves the input unconsumed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let events = vec![MyEvent { id: 1 }, MyEvent { id: 2 }];
    /// let source = FiniteSourceTyped::new(events);
    /// ```
    pub fn new<I>(
        iter: I,
    ) -> FiniteSourceTyped<T, impl FnMut(usize) -> Option<Vec<T>> + Send + Sync>
    where
        I: IntoIterator<Item = T> + Send + Sync,
        I::IntoIter: Send + Sync,
    {
        let mut items = std::iter::once_with(move || iter.into_iter())
            .flatten()
            .fuse();
        FiniteSourceTyped::from_producer(move |_| items.next().map(|item| vec![item]))
    }

    /// Convenience: create from a single-item producer.
    pub fn from_item_fn<G>(
        mut producer: G,
    ) -> FiniteSourceTyped<T, impl FnMut(usize) -> Option<Vec<T>> + Send + Sync>
    where
        G: FnMut(usize) -> Option<T> + Send + Sync,
    {
        FiniteSourceTyped::from_producer(move |index| producer(index).map(|item| vec![item]))
    }

    /// Create a fallible finite source from a batch producer.
    ///
    /// Returns `FallibleFiniteSourceTyped` for error-returning producers.
    pub fn fallible<F>(producer: F) -> FallibleFiniteSourceTyped<T, F>
    where
        F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync,
    {
        FallibleFiniteSourceTyped::new(producer)
    }

    /// Convenience: create from a fallible single-item producer.
    pub fn from_fallible_item_fn<G>(
        producer: G,
    ) -> FallibleFiniteSourceTyped<
        T,
        impl FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync,
    >
    where
        G: FnMut(usize) -> Result<Option<T>, SourceError> + Send + Sync,
    {
        FallibleFiniteSourceTyped::from_fallible_item_fn(producer)
    }
}

impl<T, F> std::fmt::Debug for FiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Option<Vec<T>> + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .finish()
    }
}

impl<T, F> TypedFiniteSourceHandler for FiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Option<Vec<T>> + Send + Sync,
{
    type Output = T;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        match (self.producer)(self.current_index) {
            Some(items) if items.is_empty() => Ok(Some(Vec::new())),
            Some(items) => {
                let item_count = items.len();
                self.current_index = self.current_index.saturating_add(item_count);
                Ok(Some(items))
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
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync,
{
    producer: F,
    current_index: usize,
    _phantom: PhantomData<T>,
}

impl<T, F> FallibleFiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync,
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
            _phantom: PhantomData,
        }
    }
}

impl<T> FallibleFiniteSourceTyped<T, fn(usize) -> Result<Option<Vec<T>>, SourceError>>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
{
    /// Convenience: create from a fallible single-item producer.
    ///
    /// Wraps the single-item producer into a batch producer internally.
    pub fn from_fallible_item_fn<G>(
        mut producer: G,
    ) -> FallibleFiniteSourceTyped<
        T,
        impl FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync,
    >
    where
        G: FnMut(usize) -> Result<Option<T>, SourceError> + Send + Sync,
    {
        FallibleFiniteSourceTyped::new(move |index| {
            producer(index).map(|opt| opt.map(|item| vec![item]))
        })
    }
}

impl<T, F> std::fmt::Debug for FallibleFiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallibleFiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .finish()
    }
}

impl<T, F> TypedFiniteSourceHandler for FallibleFiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync,
{
    type Output = T;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        // Propagate producer errors directly
        match (self.producer)(self.current_index)? {
            Some(items) if items.is_empty() => Ok(Some(Vec::new())),
            Some(items) => {
                let item_count = items.len();
                self.current_index = self.current_index.saturating_add(item_count);
                Ok(Some(items))
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
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    producer: F,
    current_index: usize,
    _phantom: PhantomData<fn() -> (T, Fut)>,
}

impl<T, F, Fut> Clone for AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            current_index: self.current_index,
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
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
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> std::fmt::Debug for AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncFiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .finish()
    }
}

#[async_trait]
impl<T, F, Fut> TypedAsyncFiniteSourceHandler for AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    type Output = T;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        match (self.producer)(self.current_index).await {
            Some(items) if items.is_empty() => Ok(Some(Vec::new())),
            Some(items) => {
                let item_count = items.len();
                self.current_index = self.current_index.saturating_add(item_count);
                Ok(Some(items))
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
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
{
    producer: F,
    current_index: usize,
    _phantom: PhantomData<fn() -> (T, Fut)>,
}

impl<T, F, Fut> Clone for FallibleAsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
{
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            current_index: self.current_index,
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> FallibleAsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
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
    T: Serialize + TypedPayload + Send + Sync + 'static,
{
    /// Convenience: create from a fallible async single-item producer.
    ///
    /// This internally wraps the item producer into a batch producer and boxes the future so
    /// callers can write `|i| async move { ... }` without additional adapters.
    pub fn from_fallible_async_item_fn<G, FutItem>(
        mut producer: G,
    ) -> FallibleAsyncFiniteSourceTyped<
        T,
        impl FnMut(usize) -> BoxedFallibleBatchFuture<T> + Send + Sync,
        BoxedFallibleBatchFuture<T>,
    >
    where
        G: FnMut(usize) -> FutItem + Send + Sync + 'static,
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
    T: Serialize + TypedPayload + Send + Sync + 'static,
{
    /// Create a fallible async source from a batch producer.
    ///
    /// Returns `FallibleAsyncFiniteSourceTyped` for error-returning producers.
    pub fn fallible<G, Fut>(producer: G) -> FallibleAsyncFiniteSourceTyped<T, G, Fut>
    where
        G: FnMut(usize) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
    {
        FallibleAsyncFiniteSourceTyped::new(producer)
    }

    /// Convenience: create from a fallible async single-item producer.
    pub fn from_fallible_async_item_fn<G, FutItem>(
        producer: G,
    ) -> FallibleAsyncFiniteSourceTyped<
        T,
        impl FnMut(usize) -> BoxedFallibleBatchFuture<T> + Send + Sync,
        BoxedFallibleBatchFuture<T>,
    >
    where
        G: FnMut(usize) -> FutItem + Send + Sync + 'static,
        FutItem: Future<Output = Result<Option<T>, SourceError>> + Send + 'static,
    {
        FallibleAsyncFiniteSourceTyped::from_fallible_async_item_fn(producer)
    }
}

impl<T, F, Fut> std::fmt::Debug for FallibleAsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallibleAsyncFiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .finish()
    }
}

#[async_trait]
impl<T, F, Fut> TypedAsyncFiniteSourceHandler for FallibleAsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
{
    type Output = T;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        // Propagate producer errors directly
        match (self.producer)(self.current_index).await? {
            Some(items) if items.is_empty() => Ok(Some(Vec::new())),
            Some(items) => {
                let item_count = items.len();
                self.current_index = self.current_index.saturating_add(item_count);
                Ok(Some(items))
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
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Vec<T> + Send + Sync,
{
    producer: F,
    current_index: usize,
    _phantom: PhantomData<T>,
}

impl<T, F> InfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Vec<T> + Send + Sync,
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
            _phantom: PhantomData,
        }
    }
}

impl<T> InfiniteSourceTyped<T, fn(usize) -> Vec<T>>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
{
    /// Create a fallible infinite source from a batch producer.
    ///
    /// Returns `FallibleInfiniteSourceTyped` for error-returning producers.
    pub fn fallible<G>(producer: G) -> FallibleInfiniteSourceTyped<T, G>
    where
        G: FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync,
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
        impl FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync,
    > {
        let cap = batch_cap
            .unwrap_or(DEFAULT_INFINITE_RECEIVER_BATCH_CAP)
            .max(1);
        let receiver = Mutex::new(receiver);

        FallibleInfiniteSourceTyped::new(move |_index| {
            let mut batch = Vec::new();
            let guard = receiver
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
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Vec<T> + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InfiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .finish()
    }
}

impl<T, F> TypedInfiniteSourceHandler for InfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Vec<T> + Send + Sync,
{
    type Output = T;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        let items = (self.producer)(self.current_index);
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let item_count = items.len();
        self.current_index = self.current_index.saturating_add(item_count);
        Ok(items)
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
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync,
{
    producer: F,
    current_index: usize,
    _phantom: PhantomData<T>,
}

impl<T, F> FallibleInfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync,
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
            _phantom: PhantomData,
        }
    }
}

impl<T, F> std::fmt::Debug for FallibleInfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallibleInfiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .finish()
    }
}

impl<T, F> TypedInfiniteSourceHandler for FallibleInfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync,
{
    type Output = T;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        let items = (self.producer)(self.current_index)?;
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let item_count = items.len();
        self.current_index = self.current_index.saturating_add(item_count);
        Ok(items)
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
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Vec<T>> + Send,
{
    producer: F,
    current_index: usize,
    _phantom: PhantomData<fn() -> (T, Fut)>,
}

impl<T, F, Fut> Clone for AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Vec<T>> + Send,
{
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            current_index: self.current_index,
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
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
            _phantom: PhantomData,
        }
    }
}

impl<T>
    AsyncInfiniteSourceTyped<T, fn(usize) -> std::future::Ready<Vec<T>>, std::future::Ready<Vec<T>>>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
{
    /// Create a fallible async infinite source from a batch producer.
    ///
    /// Returns `FallibleAsyncInfiniteSourceTyped` for error-returning producers.
    pub fn fallible<G, FutG>(producer: G) -> FallibleAsyncInfiniteSourceTyped<T, G, FutG>
    where
        G: FnMut(usize) -> FutG + Send + Sync,
        FutG: Future<Output = Result<Vec<T>, SourceError>> + Send,
    {
        FallibleAsyncInfiniteSourceTyped::new(producer)
    }

    /// Transfer an async stream into one source execution.
    /// Stream end remains a source error; infinite completion is runtime-owned.
    pub fn from_stream<S>(
        stream: S,
    ) -> impl TypedAsyncInfiniteSourceHandler<Output = T> + SourceTyping<Output = T>
    where
        S: Stream<Item = T> + Send + Unpin + 'static,
    {
        StreamSource {
            stream: Mutex::new(stream),
        }
    }

    /// Transfer a Tokio receiver into one source execution.
    pub fn from_receiver(
        receiver: tokio::sync::mpsc::Receiver<T>,
    ) -> impl TypedAsyncInfiniteSourceHandler<Output = T> + SourceTyping<Output = T> {
        ReceiverSource { receiver }
    }
}

impl<T, F, Fut> std::fmt::Debug for AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Vec<T>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncInfiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .finish()
    }
}

#[async_trait]
impl<T, F, Fut> TypedAsyncInfiniteSourceHandler for AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Vec<T>> + Send,
{
    type Output = T;

    async fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        let items = (self.producer)(self.current_index).await;
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let item_count = items.len();
        self.current_index = self.current_index.saturating_add(item_count);
        Ok(items)
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
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Vec<T>, SourceError>> + Send,
{
    producer: F,
    current_index: usize,
    _phantom: PhantomData<fn() -> (T, Fut)>,
}

impl<T, F, Fut> Clone for FallibleAsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<Vec<T>, SourceError>> + Send,
{
    fn clone(&self) -> Self {
        Self {
            producer: self.producer.clone(),
            current_index: self.current_index,
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> FallibleAsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
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
            _phantom: PhantomData,
        }
    }
}

impl<T, F, Fut> std::fmt::Debug for FallibleAsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Vec<T>, SourceError>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FallibleAsyncInfiniteSourceTyped")
            .field("item_type", &std::any::type_name::<T>())
            .field("current_index", &self.current_index)
            .finish()
    }
}

#[async_trait]
impl<T, F, Fut> TypedAsyncInfiniteSourceHandler for FallibleAsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Vec<T>, SourceError>> + Send,
{
    type Output = T;

    async fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        let items = (self.producer)(self.current_index).await?;
        if items.is_empty() {
            return Ok(Vec::new());
        }

        let item_count = items.len();
        self.current_index = self.current_index.saturating_add(item_count);
        Ok(items)
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

impl<T, F> SourceTyping for FiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Option<Vec<T>> + Send + Sync,
{
    type Output = T;
}

impl<T, F> SourceTyping for FallibleFiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Option<Vec<T>>, SourceError> + Send + Sync,
{
    type Output = T;
}

impl<T, F, Fut> SourceTyping for AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    type Output = T;
}

impl<T, F, Fut> SourceTyping for FallibleAsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Option<Vec<T>>, SourceError>> + Send,
{
    type Output = T;
}

impl<T, F> SourceTyping for InfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Vec<T> + Send + Sync,
{
    type Output = T;
}

impl<T, F> SourceTyping for FallibleInfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Result<Vec<T>, SourceError> + Send + Sync,
{
    type Output = T;
}

impl<T, F, Fut> SourceTyping for AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Vec<T>> + Send,
{
    type Output = T;
}

impl<T, F, Fut> SourceTyping for FallibleAsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Result<Vec<T>, SourceError>> + Send,
{
    type Output = T;
}

struct StreamSource<S> {
    stream: Mutex<S>,
}

impl<S: Stream> SourceTyping for StreamSource<S>
where
    S::Item: TypedPayload,
{
    type Output = S::Item;
}

#[async_trait]
impl<S, T> TypedAsyncInfiniteSourceHandler for StreamSource<S>
where
    S: Stream<Item = T> + Send + Unpin,
    T: TypedPayload + Send + Sync + 'static,
{
    type Output = T;
    async fn next(&mut self) -> Result<Vec<T>, SourceError> {
        self.stream
            .get_mut()
            .map_err(|_| SourceError::Other("stream lock poisoned".into()))?
            .next()
            .await
            .map(|item| vec![item])
            .ok_or_else(|| SourceError::Other("stream ended".into()))
    }
}

struct ReceiverSource<T> {
    receiver: tokio::sync::mpsc::Receiver<T>,
}

impl<T: TypedPayload> SourceTyping for ReceiverSource<T> {
    type Output = T;
}

#[async_trait]
impl<T: TypedPayload + Send + Sync + 'static> TypedAsyncInfiniteSourceHandler
    for ReceiverSource<T>
{
    type Output = T;
    async fn next(&mut self) -> Result<Vec<T>, SourceError> {
        self.receiver
            .recv()
            .await
            .map(|item| vec![item])
            .ok_or_else(|| SourceError::Other("channel closed".into()))
    }
    async fn drain(&mut self) -> Result<(), SourceError> {
        self.receiver.close();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

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

        let first = src
            .next()
            .expect("next should succeed")
            .expect("not complete");
        assert_eq!(first.len(), 1);

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

        let first = src
            .next()
            .await
            .expect("next should succeed")
            .expect("not complete");
        assert_eq!(first.len(), 1);

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

        let first = src
            .next()
            .expect("next should succeed")
            .expect("not complete");
        assert_eq!(first.len(), 1);

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

        let first = src
            .next()
            .await
            .expect("next should succeed")
            .expect("not complete");
        assert_eq!(first.len(), 1);

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

        let first = src
            .next()
            .await
            .expect("next should succeed")
            .expect("not complete");
        assert_eq!(first.len(), 1);

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

        let first = src.next().expect("next should succeed");
        assert_eq!(first.len(), 1);
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

        let first = src.next().await.expect("next should succeed");
        assert_eq!(first.len(), 1);
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
    async fn async_infinite_source_from_stream_preserves_owned_progress() {
        let stream = futures::stream::iter(vec![TestPayload { n: 1 }, TestPayload { n: 2 }]);
        let mut src1 = AsyncInfiniteSourceTyped::from_stream(stream);

        let first = src1.next().await.expect("next should succeed");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0], TestPayload { n: 1 });

        let second = src1.next().await.expect("next should succeed");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0], TestPayload { n: 2 });
    }
}

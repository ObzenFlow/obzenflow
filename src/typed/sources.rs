// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed source helper facades.

use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler,
};
use obzenflow_runtime::stages::source::{
    AsyncFiniteSourceTyped, AsyncInfiniteSourceTyped, FiniteSourceTyped, InfiniteSourceTyped,
};
use obzenflow_runtime::typing::SourceTyping;
use serde::Serialize;
use std::fmt::Debug;
use std::future::Future;

/// Create a finite typed source from an iterator (convenience wrapper).
pub fn finite<T, I>(
    iter: I,
) -> impl FiniteSourceHandler + SourceTyping<Output = T> + Clone + Debug + Send + Sync + 'static
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    I: IntoIterator<Item = T>,
{
    FiniteSourceTyped::new(iter)
}

/// Create an async finite typed source from an async batch producer.
pub fn async_finite<T, F, Fut>(producer: F) -> AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    AsyncFiniteSourceTyped::new(producer)
}

/// Create an infinite typed source from a batch producer.
pub fn infinite<T, F>(producer: F) -> InfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Vec<T> + Send + Sync + Clone,
{
    InfiniteSourceTyped::new(producer)
}

/// Create an async infinite typed source from an async batch producer.
pub fn async_infinite<T, F, Fut>(producer: F) -> AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Vec<T>> + Send,
{
    AsyncInfiniteSourceTyped::new(producer)
}

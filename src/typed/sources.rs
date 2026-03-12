// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed source helper facades.

use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::common::handlers::FiniteSourceHandler;
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
) -> impl FiniteSourceHandler + SourceTyping<Output = T> + Clone + Debug + 'static
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
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

/// Create a finite typed source from a per-item producer.
///
/// This is the facade equivalent of `FiniteSourceTyped::from_item_fn(...)`.
pub fn finite_from_fn<T, F>(
    producer: F,
) -> impl FiniteSourceHandler + SourceTyping<Output = T> + Clone + Debug + 'static
where
    T: Serialize + TypedPayload + Clone + Send + Sync + 'static,
    F: FnMut(usize) -> Option<T> + Send + Sync + Clone + 'static,
{
    let mut producer = producer;
    FiniteSourceTyped::from_producer(move |index| producer(index).map(|item| vec![item]))
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

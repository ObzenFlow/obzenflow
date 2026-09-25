// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! In-process source adapters constructed from values and functions.

use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler;
use obzenflow_runtime::stages::source::{
    AsyncFiniteSourceTyped, AsyncInfiniteSourceTyped, FiniteSourceTyped, InfiniteSourceTyped,
};
use obzenflow_runtime::typing::SourceTyping;
use serde::Serialize;
use std::fmt::Debug;
use std::future::Future;

/// Create a finite typed source that emits exactly one item, then EOF.
pub fn once<T>(
    item: T,
) -> impl TypedFiniteSourceHandler<Output = T> + SourceTyping<Output = T> + Debug + 'static
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
{
    finite(std::iter::once(item))
}

/// Create a finite typed source from an iterator.
pub fn finite<T, I>(
    iter: I,
) -> impl TypedFiniteSourceHandler<Output = T> + SourceTyping<Output = T> + Debug + 'static
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    I: IntoIterator<Item = T>,
{
    let mut items = iter.into_iter().collect::<Vec<T>>().into_iter();
    FiniteSourceTyped::from_producer(move |_| items.next().map(|item| vec![item]))
}

/// Create a finite typed source from a per-item producer.
pub fn finite_from_fn<T, F>(
    producer: F,
) -> impl TypedFiniteSourceHandler<Output = T> + SourceTyping<Output = T> + Debug + 'static
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Option<T> + Send + Sync + 'static,
{
    let mut producer = producer;
    FiniteSourceTyped::from_producer(move |index| producer(index).map(|item| vec![item]))
}

/// Create an async finite typed source from an async batch producer.
pub fn async_finite<T, F, Fut>(producer: F) -> AsyncFiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Option<Vec<T>>> + Send,
{
    AsyncFiniteSourceTyped::new(producer)
}

/// Create an infinite typed source from a batch producer.
pub fn infinite<T, F>(producer: F) -> InfiniteSourceTyped<T, F>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Vec<T> + Send + Sync,
{
    InfiniteSourceTyped::new(producer)
}

/// Create an async infinite typed source from an async batch producer.
pub fn async_infinite<T, F, Fut>(producer: F) -> AsyncInfiniteSourceTyped<T, F, Fut>
where
    T: Serialize + TypedPayload + Send + Sync + 'static,
    F: FnMut(usize) -> Fut + Send + Sync,
    Fut: Future<Output = Vec<T>> + Send,
{
    AsyncInfiniteSourceTyped::new(producer)
}

/// Create a finite source from a business producer. The closure is moved once
/// and called only by live execution; `None` signals exhaustion.
pub fn generate<T, F>(
    mut producer: F,
) -> impl TypedFiniteSourceHandler<Output = T> + SourceTyping<Output = T> + Debug
where
    T: TypedPayload + Send + Sync + 'static,
    F: FnMut() -> Option<T> + Send + Sync + 'static,
{
    FiniteSourceTyped::from_producer(move |_| producer().map(|item| vec![item]))
}

/// Transfer a Tokio receiver into one asynchronous infinite source execution.
/// Use topology fan-out to distribute its output to multiple downstream stages.
///
/// ```compile_fail
/// use obzenflow_adapters::sources::from_receiver;
/// use obzenflow_core::TypedPayload;
/// #[derive(serde::Serialize, serde::Deserialize)]
/// struct Row(u64);
/// impl TypedPayload for Row { const EVENT_TYPE: &'static str = "row"; }
/// let (_, receiver) = tokio::sync::mpsc::channel::<Row>(8);
/// let first = from_receiver(receiver);
/// let second = from_receiver(receiver); // receiver has moved
/// ```
pub fn from_receiver<T>(
    receiver: tokio::sync::mpsc::Receiver<T>,
) -> impl obzenflow_runtime::stages::source::TypedAsyncInfiniteSourceHandler<Output = T>
       + SourceTyping<Output = T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    AsyncInfiniteSourceTyped::from_receiver(receiver)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct Item(u64);

    impl TypedPayload for Item {
        const EVENT_TYPE: &'static str = "adapters.sources.once.item";
    }

    #[test]
    fn once_emits_one_item_then_eof() {
        let mut source = once(Item(7));
        assert_eq!(source.next().unwrap(), Some(vec![Item(7)]));
        assert_eq!(source.next().unwrap(), None);
    }
}

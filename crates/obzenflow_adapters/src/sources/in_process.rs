// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! In-process sources with uniquely owned values or a channel receiver.

use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::source::{
    SourceError, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
};
use obzenflow_runtime::typing::SourceTyping;
use std::fmt;
use std::iter::Fuse;
use tokio::sync::mpsc;

/// A finite source that transfers its values into one live execution.
/// Construction does not create or poll the iterator. Each live poll emits
/// one item; exhaustion is fused. Neither the input nor its items need Clone.
pub struct ValuesSource<I: IntoIterator> {
    state: ValuesState<I>,
}

enum ValuesState<I: IntoIterator> {
    Ready(I),
    Reading(Fuse<I::IntoIter>),
    Exhausted,
}

impl<I> ValuesSource<I>
where
    I: IntoIterator + Send + Sync + 'static,
    I::IntoIter: Send + Sync,
    I::Item: TypedPayload + Send + Sync + 'static,
{
    pub fn new(values: I) -> Self {
        Self {
            state: ValuesState::Ready(values),
        }
    }
}

impl<I: IntoIterator> fmt::Debug for ValuesSource<I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValuesSource").finish_non_exhaustive()
    }
}

impl<I: IntoIterator> SourceTyping for ValuesSource<I>
where
    I::Item: TypedPayload,
{
    type Output = I::Item;
}

impl<I> TypedFiniteSourceHandler for ValuesSource<I>
where
    I: IntoIterator + Send + Sync + 'static,
    I::IntoIter: Send + Sync,
    I::Item: TypedPayload + Send + Sync + 'static,
{
    type Output = I::Item;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        let mut iter = match std::mem::replace(&mut self.state, ValuesState::Exhausted) {
            ValuesState::Ready(values) => values.into_iter().fuse(),
            ValuesState::Reading(iter) => iter,
            ValuesState::Exhausted => return Ok(None),
        };
        let item = iter.next();
        if item.is_some() {
            self.state = ValuesState::Reading(iter);
        }
        Ok(item.map(|item| vec![item]))
    }
}

/// An infinite source owning one Tokio channel receiver.
/// A closed channel is a source error. Orderly drain closes the receiver;
/// use topology fan-out to distribute facts to multiple downstream stages.
pub struct ChannelSource<T> {
    receiver: mpsc::Receiver<T>,
}

impl<T> fmt::Debug for ChannelSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelSource").finish_non_exhaustive()
    }
}

impl<T: TypedPayload + Send + Sync + 'static> ChannelSource<T> {
    pub fn new(receiver: mpsc::Receiver<T>) -> Self {
        Self { receiver }
    }
}

impl<T: TypedPayload> SourceTyping for ChannelSource<T> {
    type Output = T;
}

#[async_trait]
impl<T: TypedPayload + Send + Sync + 'static> TypedAsyncInfiniteSourceHandler for ChannelSource<T> {
    type Output = T;

    async fn next(&mut self) -> Result<Vec<T>, SourceError> {
        self.receiver
            .recv()
            .await
            .map(|item| vec![item])
            .ok_or(SourceError::Other(
                obzenflow_core::event::SourceDiagnosticReason::InputClosed.into(),
            ))
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct Item(u64);

    impl TypedPayload for Item {
        const EVENT_TYPE: &'static str = "adapters.sources.once.item";
    }

    #[test]
    fn once_emits_one_item_then_eof() {
        let mut source = ValuesSource::new([Item(7)]);
        assert_eq!(source.next().unwrap(), Some(vec![Item(7)]));
        assert_eq!(source.next().unwrap(), None);
    }

    #[test]
    fn finite_defers_iterator_creation_and_consumes_one_item_per_poll() {
        struct Input {
            starts: Arc<AtomicUsize>,
            polls: Arc<AtomicUsize>,
        }

        struct Items(Arc<AtomicUsize>);

        impl Iterator for Items {
            type Item = Item;

            fn next(&mut self) -> Option<Item> {
                let index = self.0.fetch_add(1, Ordering::SeqCst);
                (index < 2).then_some(Item(index as u64))
            }
        }

        impl IntoIterator for Input {
            type Item = Item;
            type IntoIter = Items;

            fn into_iter(self) -> Items {
                self.starts.fetch_add(1, Ordering::SeqCst);
                Items(self.polls)
            }
        }

        let starts = Arc::new(AtomicUsize::new(0));
        let polls = Arc::new(AtomicUsize::new(0));
        let mut source = ValuesSource::new(Input {
            starts: starts.clone(),
            polls: polls.clone(),
        });
        assert!(format!("{source:?}").contains("ValuesSource"));
        assert_eq!(starts.load(Ordering::SeqCst), 0);
        assert_eq!(polls.load(Ordering::SeqCst), 0);

        assert_eq!(source.next().unwrap(), Some(vec![Item(0)]));
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        assert_eq!(source.next().unwrap(), Some(vec![Item(1)]));
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert_eq!(source.next().unwrap(), None);
        assert_eq!(source.next().unwrap(), None);
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 3);
    }
    #[tokio::test]
    async fn channel_source_drains_pending_items_and_errors_on_close() {
        let (tx, rx) = mpsc::channel(16);
        tx.send(Item(1)).await.unwrap();
        tx.send(Item(2)).await.unwrap();
        let mut source = ChannelSource::new(rx);
        assert_eq!(source.next().await.unwrap(), vec![Item(1)]);
        source.drain().await.unwrap();
        assert!(tx.send(Item(3)).await.is_err());
        assert_eq!(source.next().await.unwrap(), vec![Item(2)]);
        assert!(matches!(
            source.next().await,
            Err(SourceError::Other(diagnostic))
                if diagnostic.reason() == obzenflow_core::event::SourceDiagnosticReason::InputClosed
        ));
    }
}

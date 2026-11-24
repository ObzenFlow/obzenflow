// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::SystemName;
use obzenflow_runtime::queries::{QueryContext, QueryError, QueryPort, QueryRequest};
use obzenflow_runtime::stages::common::handlers::source::{
    Continuous, FiniteRead, SourceRead, SourceReceiver,
};
use obzenflow_runtime::stages::source::{
    SourceError, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
};
use obzenflow_runtime::typing::SourceTyping;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::Fuse;
use std::marker::PhantomData;
use std::sync::{Mutex, PoisonError};

const VALUES_PER_READ: usize = 1000;

/// `ValuesSource` is a finite source that transfers its values into one live
/// execution. Its `in-memory` system creates the iterator at the first live read
/// and answers each read with up to 1,000 items. A read at the position of the
/// previous read gets the same items again. After the last item, each read ends
/// the input. `ValuesSource` does not clone the input or its items.
pub struct ValuesSource<I: IntoIterator> {
    port: QueryPort<dyn ValueFeed>,
    values: PhantomData<fn() -> I>,
}

impl<I> ValuesSource<I>
where
    I: IntoIterator + Send + Sync + 'static,
    I::IntoIter: Send + Sync,
    I::Item: TypedPayload + Send + Sync + 'static,
{
    pub fn new(values: I) -> Self {
        let system = InMemory {
            feed: Mutex::new(Feed {
                values: ValuesState::Ready(values),
                taken: 0,
                last: None,
            }),
        };
        Self {
            port: QueryPort::to(system),
            values: PhantomData,
        }
    }
}

impl<I: IntoIterator> Clone for ValuesSource<I> {
    fn clone(&self) -> Self {
        Self {
            port: self.port.clone(),
            values: PhantomData,
        }
    }
}

/// `ValueFeed` is the port of an in-memory value feed.
#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait ValueFeed: Send + Sync {
    #[query(type = "obzenflow.in_memory.values_read")]
    async fn read(
        &self,
        from: &ValuesRead,
        ctx: &QueryContext,
    ) -> Result<FiniteRead<serde_json::Value, ValuesRead>, QueryError>;
}

/// `ValuesRead` is the argument of one read of an in-memory value feed. It holds
/// the position of the next item.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValuesRead {
    pub at: u64,
}

struct InMemory<I: IntoIterator> {
    feed: Mutex<Feed<I>>,
}

struct Feed<I: IntoIterator> {
    values: ValuesState<I>,
    taken: u64,
    last: Option<(u64, FiniteRead<serde_json::Value, ValuesRead>)>,
}

enum ValuesState<I: IntoIterator> {
    Ready(I),
    Reading(Fuse<I::IntoIter>),
    Exhausted,
}

impl<I: IntoIterator> SystemName for InMemory<I> {
    const NAME: &'static str = "in-memory";
}

impl<I> InMemory<I>
where
    I: IntoIterator,
    I::Item: TypedPayload,
{
    fn take(
        &self,
        from: &ValuesRead,
    ) -> Result<FiniteRead<serde_json::Value, ValuesRead>, QueryError> {
        let mut feed = self.feed.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((at, reply)) = &feed.last {
            if *at == from.at {
                return Ok(reply.clone());
            }
            if from.at < *at {
                return Err(QueryError::terminal(
                    SourcePollErrorKind::Validation,
                    SourceDiagnosticReason::InvalidValue,
                ));
            }
        }
        let mut iter = match std::mem::replace(&mut feed.values, ValuesState::Exhausted) {
            ValuesState::Ready(values) => values.into_iter().fuse(),
            ValuesState::Reading(iter) => iter,
            ValuesState::Exhausted => return Ok(FiniteRead::EndOfInput),
        };
        while feed.taken < from.at {
            if iter.next().is_none() {
                return Ok(FiniteRead::EndOfInput);
            }
            feed.taken += 1;
        }
        let mut records = Vec::new();
        for item in iter.by_ref().take(VALUES_PER_READ) {
            records.push(serde_json::to_value(&item).map_err(|_| {
                QueryError::terminal(
                    SourcePollErrorKind::Other,
                    SourceDiagnosticReason::UnrepresentableValue,
                )
            })?);
        }
        let read = records.len() as u64;
        feed.taken += read;
        let reply = if records.is_empty() {
            FiniteRead::EndOfInput
        } else {
            feed.values = ValuesState::Reading(iter);
            FiniteRead::Batch {
                records,
                next: ValuesRead { at: from.at + read },
            }
        };
        feed.last = Some((from.at, reply.clone()));
        Ok(reply)
    }
}

#[async_trait]
impl<I> ValueFeed for InMemory<I>
where
    I: IntoIterator + Send + Sync + 'static,
    I::IntoIter: Send + Sync,
    I::Item: TypedPayload + Send + Sync + 'static,
{
    async fn read(
        &self,
        from: &ValuesRead,
        _ctx: &QueryContext,
    ) -> Result<FiniteRead<serde_json::Value, ValuesRead>, QueryError> {
        self.take(from)
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

    fn read(&self) -> SourceRead<Self::Output> {
        SourceRead::finite(self.port.clone(), ValuesRead { at: 0 }, |value| {
            serde_json::from_value(value)
                .map_err(|_| SourceError::Validation(SourceDiagnosticReason::InvalidRecord.into()))
        })
        .in_process()
    }
}

#[derive(Clone)]
struct SelectedValues<R: QueryRequest, T> {
    port: QueryPort<R::Port>,
    first: R,
    output: PhantomData<fn() -> T>,
}
impl<R: QueryRequest, T: TypedPayload> SourceTyping for SelectedValues<R, T> {
    type Output = T;
}
impl<R, T> TypedFiniteSourceHandler for SelectedValues<R, T>
where
    T: TypedPayload + Send + Sync + 'static,
    R: QueryRequest<Answer = FiniteRead<T, R>>,
{
    type Output = T;
    fn read(&self) -> SourceRead<T> {
        SourceRead::finite(self.port.clone(), self.first.clone(), Ok)
    }
}
impl<T: TypedPayload + Send + Sync + 'static> ValuesSource<std::iter::Empty<T>> {
    pub fn through<R>(
        port: QueryPort<R::Port>,
        first: R,
    ) -> impl TypedFiniteSourceHandler<Output = T> + SourceTyping<Output = T>
    where
        R: QueryRequest<Answer = FiniteRead<T, R>>,
    {
        SelectedValues {
            port,
            first,
            output: PhantomData,
        }
    }
}

/// A source for sender-initiated events on a bounded in-process channel.
pub struct ChannelSource<T> {
    receiver: std::sync::Arc<SourceReceiver<T>>,
}
impl<T> fmt::Debug for ChannelSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelSource").finish_non_exhaustive()
    }
}
impl<T> ChannelSource<T> {
    pub fn new(receiver: SourceReceiver<T>) -> Self {
        Self {
            receiver: std::sync::Arc::new(receiver),
        }
    }
}
impl<T: TypedPayload> SourceTyping for ChannelSource<T> {
    type Output = T;
}
#[async_trait]
impl<T: TypedPayload + Send + Sync + 'static> TypedAsyncInfiniteSourceHandler for ChannelSource<T> {
    type Output = T;
    fn read(&self) -> SourceRead<T, Continuous> {
        SourceRead::channel(self.receiver.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_runtime::stages::common::handlers::source::source_channel;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
    struct Item(u64);

    impl TypedPayload for Item {
        const EVENT_TYPE: &'static str = "adapters.sources.once.item";
    }

    fn feed<I>(values: I) -> InMemory<I>
    where
        I: IntoIterator,
    {
        InMemory {
            feed: Mutex::new(Feed {
                values: ValuesState::Ready(values),
                taken: 0,
                last: None,
            }),
        }
    }

    fn items(
        read: FiniteRead<serde_json::Value, ValuesRead>,
    ) -> Option<(Vec<serde_json::Value>, u64)> {
        match read {
            FiniteRead::Batch { records, next } => Some((records, next.at)),
            FiniteRead::EndOfInput => None,
        }
    }

    #[test]
    fn once_emits_one_item_then_eof() {
        let feed = feed([Item(7)]);
        let (records, next) = items(feed.take(&ValuesRead { at: 0 }).unwrap()).unwrap();
        assert_eq!(records, [serde_json::json!(7)]);
        assert!(items(feed.take(&ValuesRead { at: next }).unwrap()).is_none());
    }

    #[test]
    fn value_feed_reads_bounded_batches() {
        let feed = feed((0..2_500).map(Item));
        let (first, next) = items(feed.take(&ValuesRead { at: 0 }).unwrap()).unwrap();
        assert_eq!((first.len(), next), (VALUES_PER_READ, 1_000));
        let (second, next) = items(feed.take(&ValuesRead { at: next }).unwrap()).unwrap();
        assert_eq!((second.len(), next), (VALUES_PER_READ, 2_000));
        assert_eq!(second[0], serde_json::json!(1_000));
        let (last, next) = items(feed.take(&ValuesRead { at: next }).unwrap()).unwrap();
        assert_eq!((last.len(), next), (500, 2_500));
        assert!(items(feed.take(&ValuesRead { at: next }).unwrap()).is_none());
    }

    #[test]
    fn value_feed_repeat_returns_same_batch() {
        let feed = feed((0..3).map(Item));
        let first = feed.take(&ValuesRead { at: 0 }).unwrap();
        let again = feed.take(&ValuesRead { at: 0 }).unwrap();
        assert_eq!(items(first.clone()), items(again));
        let (_, next) = items(first).unwrap();
        assert!(items(feed.take(&ValuesRead { at: next }).unwrap()).is_none());
        assert!(items(feed.take(&ValuesRead { at: next }).unwrap()).is_none());
        assert!(feed.take(&ValuesRead { at: 0 }).is_err());
    }

    #[test]
    fn iterator_creation_waits_for_first_read() {
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
        let source = ValuesSource::new(Input {
            starts: starts.clone(),
            polls: polls.clone(),
        });
        assert!(format!("{source:?}").contains("ValuesSource"));
        let feed = feed(Input {
            starts: starts.clone(),
            polls: polls.clone(),
        });
        assert_eq!(starts.load(Ordering::SeqCst), 0);
        assert_eq!(polls.load(Ordering::SeqCst), 0);

        let (first, next) = items(feed.take(&ValuesRead { at: 0 }).unwrap()).unwrap();
        assert_eq!(first, [serde_json::json!(0), serde_json::json!(1)]);
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 3);
        assert!(items(feed.take(&ValuesRead { at: next }).unwrap()).is_none());
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn channel_ready_leaves_items_queued() {
        let (tx, rx) = source_channel(16);
        tx.send(Item(1)).await.unwrap();
        tx.send(Item(2)).await.unwrap();
        rx.ready().await.unwrap();
        assert_eq!(rx.len(), 2, "readiness takes nothing");
        assert_eq!(tx.capacity(), 14);
        assert_eq!(rx.take(10), [Item(1), Item(2)]);
    }

    #[tokio::test]
    async fn closed_channel_readiness_returns_input_closed() {
        let (tx, rx) = source_channel::<Item>(1);
        drop(tx);
        assert!(rx.ready().await.is_err());
    }
}

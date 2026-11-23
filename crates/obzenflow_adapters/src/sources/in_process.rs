// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! In-process sources with uniquely owned values or a channel receiver.

use async_trait::async_trait;
use obzenflow_core::event::payloads::execution_payload::ReattachGuarantee;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{EffectContext, EffectError, Port, SystemName};
use obzenflow_runtime::stages::common::handlers::source::{ContinuousRead, FiniteRead, SourceRead};
use obzenflow_runtime::stages::source::{
    SourceError, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
};
use obzenflow_runtime::typing::SourceTyping;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::Fuse;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, PoisonError};
use tokio::sync::mpsc;

/// `ValuesSource` is a finite source that transfers its values into one live
/// execution. Its `in-memory` system creates the iterator at the first live read
/// and answers each read with one item. After the last item, each read ends the
/// input. `ValuesSource` does not clone the input or its items.
pub struct ValuesSource<I: IntoIterator> {
    port: Port<dyn ValueFeed>,
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
            }),
        };
        Self {
            port: Port::to(system).own_instance(),
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
    #[effect(type = "obzenflow.in_memory.values_read", exactly_once, records = reply)]
    async fn read(
        &self,
        from: &ValuesRead,
        ctx: &EffectContext,
    ) -> Result<FiniteRead<serde_json::Value, ValuesRead>, EffectError>;
}

/// `ValuesRead` is the argument of one read of an in-memory value feed. It holds
/// the position of the next item.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValuesRead {
    at: u64,
}

struct InMemory<I: IntoIterator> {
    feed: Mutex<Feed<I>>,
}

struct Feed<I: IntoIterator> {
    values: ValuesState<I>,
    taken: u64,
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
    ) -> Result<FiniteRead<serde_json::Value, ValuesRead>, EffectError> {
        let mut feed = self.feed.lock().unwrap_or_else(PoisonError::into_inner);
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
        let Some(item) = iter.next() else {
            return Ok(FiniteRead::EndOfInput);
        };
        feed.taken += 1;
        feed.values = ValuesState::Reading(iter);
        let value = serde_json::to_value(&item)
            .map_err(|error| EffectError::Serialization(error.to_string()))?;
        Ok(FiniteRead::Batch {
            records: vec![value],
            next: ValuesRead { at: from.at + 1 },
        })
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
        _ctx: &EffectContext,
    ) -> Result<FiniteRead<serde_json::Value, ValuesRead>, EffectError> {
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

/// `ChannelSource` is an infinite source that reads one Tokio channel receiver
/// through its `channel` system. Input still in the channel when a run stops is
/// lost, so a resumed run reattaches with `AtMostOnce`. A closed channel is a
/// source error. Use topology fan-out to send facts to more than one downstream
/// stage.
pub struct ChannelSource<T> {
    port: Port<dyn ChannelFeed>,
    state: Arc<ChannelState<T>>,
}

impl<T> fmt::Debug for ChannelSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChannelSource").finish_non_exhaustive()
    }
}

impl<T: TypedPayload + Send + Sync + 'static> ChannelSource<T> {
    pub fn new(receiver: mpsc::Receiver<T>) -> Self {
        let state = Arc::new(ChannelState {
            receiver: tokio::sync::Mutex::new(receiver),
            front: Mutex::new(None),
        });
        Self {
            port: Port::to(Channel {
                state: state.clone(),
            })
            .own_instance(),
            state,
        }
    }
}

/// `ChannelFeed` is the port of a channel feed. A read takes the items that the
/// channel holds.
#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait ChannelFeed: Send + Sync {
    #[effect(type = "obzenflow.channel.take", at_most_once, records = reply)]
    async fn read(
        &self,
        from: &ChannelRead,
        ctx: &EffectContext,
    ) -> Result<ContinuousRead<serde_json::Value, ChannelRead>, EffectError>;
}

/// `ChannelRead` is the argument of one read of a channel feed. It sets the
/// maximum number of items that the read takes.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChannelRead {
    batch: usize,
}

struct ChannelState<T> {
    receiver: tokio::sync::Mutex<mpsc::Receiver<T>>,
    front: Mutex<Option<T>>,
}

impl<T> ChannelState<T> {
    async fn ready(&self) -> Result<(), SourceError> {
        if self
            .front
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
        {
            return Ok(());
        }
        let first = self
            .receiver
            .lock()
            .await
            .recv()
            .await
            .ok_or(SourceError::Other(
                SourceDiagnosticReason::InputClosed.into(),
            ))?;
        *self.front.lock().unwrap_or_else(PoisonError::into_inner) = Some(first);
        Ok(())
    }
}

struct Channel<T> {
    state: Arc<ChannelState<T>>,
}

impl<T> SystemName for Channel<T> {
    const NAME: &'static str = "channel";
}

impl<T: TypedPayload> Channel<T> {
    async fn take(
        &self,
        from: &ChannelRead,
    ) -> Result<ContinuousRead<serde_json::Value, ChannelRead>, EffectError> {
        let mut items: Vec<T> = self
            .state
            .front
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .into_iter()
            .collect();
        {
            let mut receiver = self.state.receiver.lock().await;
            while items.len() < from.batch {
                match receiver.try_recv() {
                    Ok(item) => items.push(item),
                    Err(_) => break,
                }
            }
        }
        let records = items
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| EffectError::Serialization(error.to_string()))?;
        Ok(ContinuousRead {
            records,
            next: from.clone(),
        })
    }
}

#[async_trait]
impl<T: TypedPayload + Send + Sync + 'static> ChannelFeed for Channel<T> {
    async fn read(
        &self,
        from: &ChannelRead,
        _ctx: &EffectContext,
    ) -> Result<ContinuousRead<serde_json::Value, ChannelRead>, EffectError> {
        self.take(from).await
    }
}

impl<T: TypedPayload> SourceTyping for ChannelSource<T> {
    type Output = T;
}

#[async_trait]
impl<T: TypedPayload + Send + Sync + 'static> TypedAsyncInfiniteSourceHandler for ChannelSource<T> {
    type Output = T;

    fn read(&self) -> SourceRead<T> {
        let state = self.state.clone();
        SourceRead::continuous(
            self.port.clone(),
            ChannelRead { batch: 1000 },
            move || {
                let state = state.clone();
                Box::pin(async move { state.ready().await })
            },
            ReattachGuarantee::AtMostOnce,
            |value| {
                serde_json::from_value(value).map_err(|_| {
                    SourceError::Validation(SourceDiagnosticReason::InvalidRecord.into())
                })
            },
        )
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

    fn feed<I>(values: I) -> InMemory<I>
    where
        I: IntoIterator,
    {
        InMemory {
            feed: Mutex::new(Feed {
                values: ValuesState::Ready(values),
                taken: 0,
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
        assert_eq!(first, [serde_json::json!(0)]);
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let (second, next) = items(feed.take(&ValuesRead { at: next }).unwrap()).unwrap();
        assert_eq!(second, [serde_json::json!(1)]);
        assert_eq!(polls.load(Ordering::SeqCst), 2);
        assert!(items(feed.take(&ValuesRead { at: next }).unwrap()).is_none());
        assert!(items(feed.take(&ValuesRead { at: next }).unwrap()).is_none());
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 3);
    }
    #[tokio::test]
    async fn channel_read_takes_buffered_items() {
        let (tx, rx) = mpsc::channel(16);
        tx.send(Item(1)).await.unwrap();
        tx.send(Item(2)).await.unwrap();
        let source = ChannelSource::new(rx);
        source.state.ready().await.unwrap();
        let system = Channel {
            state: source.state.clone(),
        };
        let read = system.take(&ChannelRead { batch: 10 }).await.unwrap();
        assert_eq!(read.records, [serde_json::json!(1), serde_json::json!(2)]);
    }

    #[tokio::test]
    async fn closed_channel_readiness_returns_input_closed() {
        let (tx, rx) = mpsc::channel::<Item>(1);
        drop(tx);
        let source = ChannelSource::new(rx);
        assert!(matches!(
            source.state.ready().await,
            Err(SourceError::Other(diagnostic))
                if diagnostic.reason() == SourceDiagnosticReason::InputClosed
        ));
    }
}

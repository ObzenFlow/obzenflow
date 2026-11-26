// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::queries::{FiniteRead, QueryContext, QueryError};
use obzenflow_runtime::stages::common::handlers::source::SourceRead;
use obzenflow_runtime::stages::source::{SourceError, TypedFiniteSourceHandler};
use obzenflow_runtime::systems::Port;
use obzenflow_runtime::typing::SourceTyping;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::iter::Fuse;
use std::sync::{Arc, Mutex, PoisonError};

const VALUES_PER_READ: usize = 1000;

/// `ValuesSource` is a finite source that transfers its values into one live
/// execution. Its `in-memory` system creates the iterator at the first live read
/// and answers each read with up to 1,000 items, or up to the number that
/// `chunk_size` sets. A read at the position of the previous read gets the same
/// items again. After the last item, each read ends the input. `ValuesSource`
/// does not clone the input or its items.
pub struct ValuesSource<I: IntoIterator> {
    system: Arc<InMemory<I>>,
}

impl<I> ValuesSource<I>
where
    I: IntoIterator + Send + Sync + 'static,
    I::IntoIter: Send + Sync,
    I::Item: TypedPayload + Send + Sync + 'static,
{
    pub fn new(values: I) -> Self {
        Self {
            system: Arc::new(InMemory {
                feed: Mutex::new(Feed {
                    values: ValuesState::Ready(values),
                    taken: 0,
                    last: None,
                    per_read: VALUES_PER_READ,
                }),
            }),
        }
    }

    /// `chunk_size` sets the maximum number of items in one read. Zero gives one
    /// item. A rate limit on the source counts reads.
    pub fn chunk_size(self, chunk_size: usize) -> Self {
        self.system
            .feed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .per_read = chunk_size.max(1);
        self
    }
}

impl<I: IntoIterator> Clone for ValuesSource<I> {
    fn clone(&self) -> Self {
        Self {
            system: self.system.clone(),
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
    per_read: usize,
}

enum ValuesState<I: IntoIterator> {
    Ready(I),
    Reading(Fuse<I::IntoIter>),
    Exhausted,
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
        for item in iter.by_ref().take(feed.per_read) {
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
        let port = Port::<dyn ValueFeed>::to(self.system.clone());
        SourceRead::finite(port, ValuesRead { at: 0 }, |value| {
            serde_json::from_value(value)
                .map_err(|_| SourceError::Validation(SourceDiagnosticReason::InvalidRecord.into()))
        })
        .in_process()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};

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
                per_read: VALUES_PER_READ,
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
    fn chunk_size_bounds_items_per_read() {
        let source = ValuesSource::new((0..5).map(Item)).chunk_size(2);
        let (first, next) = items(source.system.take(&ValuesRead { at: 0 }).unwrap()).unwrap();
        assert_eq!((first.len(), next), (2, 2));
        let (second, next) = items(source.system.take(&ValuesRead { at: next }).unwrap()).unwrap();
        assert_eq!((second.len(), next), (2, 4));
        let (last, next) = items(source.system.take(&ValuesRead { at: next }).unwrap()).unwrap();
        assert_eq!((last, next), (vec![serde_json::json!(4)], 5));
        assert!(items(source.system.take(&ValuesRead { at: next }).unwrap()).is_none());
    }

    #[test]
    fn chunk_size_zero_reads_one_item() {
        let source = ValuesSource::new((0..2).map(Item)).chunk_size(0);
        let (first, next) = items(source.system.take(&ValuesRead { at: 0 }).unwrap()).unwrap();
        assert_eq!((first, next), (vec![serde_json::json!(0)], 1));
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
}

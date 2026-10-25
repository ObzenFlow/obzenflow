// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::{AiKey, Chunk, Partials, ReduceTrigger};
use obzenflow_runtime::ai::{AiKeyFn, AiRole};
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::Arc;

/// An `ai!` handler that makes one call per input with `role`.
pub fn infer<R, In>(role: R) -> Infer<R, In>
where
    R: AiRole<In>,
{
    Infer {
        role,
        _input: PhantomData,
    }
}

/// An `ai!` handler that makes one call per group of items with `role`.
/// Choose the group size with [`Map::chunks`].
pub fn map<R, T>(role: R) -> Map<R, T, Unkeyed>
where
    R: AiRole<Chunk<T>>,
{
    Map {
        role,
        key: Unkeyed,
        _items: PhantomData,
    }
}

/// A stage whose items share one key, so its groups and windows span the
/// whole input.
#[derive(Debug, Clone, Copy, Default)]
pub struct Unkeyed;

/// A stage that groups and reduces each key's items on its own.
pub struct Keyed<K, F> {
    key: F,
    _key: PhantomData<fn() -> K>,
}

impl<K, F: Clone> Clone for Keyed<K, F> {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            _key: PhantomData,
        }
    }
}

/// One call per input.
pub struct Infer<R, In> {
    role: R,
    _input: PhantomData<fn(In)>,
}

/// One call per group, once the group size is chosen with [`Map::chunks`].
pub struct Map<R, T, Key> {
    role: R,
    key: Key,
    _items: PhantomData<fn(T)>,
}

/// One call per group of at most `n` items.
pub struct Chunked<R, T, Key> {
    role: R,
    key: Key,
    chunk_size: usize,
    _items: PhantomData<fn(T)>,
}

/// One call per group and a reduce, once its trigger is chosen with
/// [`MapReduce::on_eof`] or [`MapReduce::every_n_chunks`].
pub struct MapReduce<R, S, T, Key> {
    map: R,
    reduce: S,
    key: Key,
    chunk_size: usize,
    _items: PhantomData<fn(T)>,
}

/// One call per group and one reduce per window.
pub struct TriggeredMapReduce<R, S, T, Key> {
    map: R,
    reduce: S,
    key: Key,
    chunk_size: usize,
    trigger: AiTrigger,
    _items: PhantomData<fn(T)>,
}

#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AiTrigger {
    OnEof,
    EveryNChunks(usize),
}

impl AiTrigger {
    pub fn resolve(self) -> Option<ReduceTrigger> {
        match self {
            Self::OnEof => Some(ReduceTrigger::OnEof),
            Self::EveryNChunks(chunks) => {
                NonZeroU64::new(chunks as u64).map(|chunks| ReduceTrigger::EveryNChunks { chunks })
            }
        }
    }
}

impl<R, T> Map<R, T, Unkeyed>
where
    R: AiRole<Chunk<T>>,
{
    /// Key each item with `key`, so groups and windows form within each key.
    pub fn key_by<K, F>(self, key: F) -> Map<R, T, Keyed<K, F>>
    where
        K: AiKey,
        F: Fn(&T) -> K + Send + Sync + 'static,
    {
        Map {
            role: self.role,
            key: Keyed {
                key,
                _key: PhantomData,
            },
            _items: PhantomData,
        }
    }
}

impl<R, T, Key> Map<R, T, Key>
where
    R: AiRole<Chunk<T>>,
{
    /// Close each group at `n` items, or at a natural end of input.
    pub fn chunks(self, n: usize) -> Chunked<R, T, Key> {
        Chunked {
            role: self.role,
            key: self.key,
            chunk_size: n,
            _items: PhantomData,
        }
    }
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`.key_by(..)` comes before `.chunks(n)`",
    label = "groups are already formed here",
    note = "write `ai::map(role).key_by(|item: &T| ..).chunks(n)`, so groups are formed within each key"
)]
pub trait KeyByBeforeChunks {}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`ai::infer` has no key",
    label = "`ai::infer` makes one call per input",
    note = "use `ai::map(role).key_by(|item: &T| ..).chunks(n)` to group items within each key"
)]
pub trait InferHasNoKey {}

impl<R, In> Infer<R, In> {
    #[doc(hidden)]
    pub fn key_by<F>(self, _: F) -> Self
    where
        Self: InferHasNoKey,
    {
        self
    }

    #[doc(hidden)]
    pub fn into_role(self) -> R {
        self.role
    }
}

impl<R, T, Key> Chunked<R, T, Key>
where
    R: AiRole<Chunk<T>>,
{
    #[doc(hidden)]
    pub fn key_by<F>(self, _: F) -> Self
    where
        Self: KeyByBeforeChunks,
    {
        self
    }

    /// Reduce each window's partials with `role`, once a trigger is chosen.
    pub fn reduce<S>(self, role: S) -> MapReduce<R, S, T, Key>
    where
        S: AiRole<Partials<T, <R as AiRole<Chunk<T>>>::Out>>,
    {
        MapReduce {
            map: self.role,
            reduce: role,
            key: self.key,
            chunk_size: self.chunk_size,
            _items: PhantomData,
        }
    }

    #[doc(hidden)]
    pub fn into_parts(self) -> (R, Key, usize) {
        (self.role, self.key, self.chunk_size)
    }
}

impl<R, S, T, Key> MapReduce<R, S, T, Key> {
    #[doc(hidden)]
    pub fn key_by<F>(self, _: F) -> Self
    where
        Self: KeyByBeforeChunks,
    {
        self
    }

    /// Reduce each key once, at a natural end of input.
    pub fn on_eof(self) -> TriggeredMapReduce<R, S, T, Key> {
        self.triggered(AiTrigger::OnEof)
    }

    /// Reduce each key once per `k` of its chunks, and the remainder at a
    /// natural end of input.
    pub fn every_n_chunks(self, k: usize) -> TriggeredMapReduce<R, S, T, Key> {
        self.triggered(AiTrigger::EveryNChunks(k))
    }

    fn triggered(self, trigger: AiTrigger) -> TriggeredMapReduce<R, S, T, Key> {
        TriggeredMapReduce {
            map: self.map,
            reduce: self.reduce,
            key: self.key,
            chunk_size: self.chunk_size,
            trigger,
            _items: PhantomData,
        }
    }
}

impl<R, S, T, Key> TriggeredMapReduce<R, S, T, Key> {
    #[doc(hidden)]
    pub fn key_by<F>(self, _: F) -> Self
    where
        Self: KeyByBeforeChunks,
    {
        self
    }

    #[doc(hidden)]
    pub fn into_parts(self) -> (R, S, Key, usize, AiTrigger) {
        (
            self.map,
            self.reduce,
            self.key,
            self.chunk_size,
            self.trigger,
        )
    }
}

#[doc(hidden)]
pub trait AiKeySpec<T>: Send + Sync + 'static {
    type Key: Ord + Clone + serde::Serialize + Send + Sync + 'static;

    fn key_fn(self) -> Option<AiKeyFn<T, Self::Key>>;
}

impl<T> AiKeySpec<T> for Unkeyed {
    type Key = ();

    fn key_fn(self) -> Option<AiKeyFn<T, Self::Key>> {
        None
    }
}

impl<T, K, F> AiKeySpec<T> for Keyed<K, F>
where
    K: AiKey,
    F: Fn(&T) -> K + Send + Sync + 'static,
{
    type Key = K;

    fn key_fn(self) -> Option<AiKeyFn<T, Self::Key>> {
        Some(Arc::new(self.key))
    }
}

macro_rules! opaque_debug {
    ($ty:ident < $($param:ident),+ >) => {
        impl<$($param),+> fmt::Debug for $ty<$($param),+> {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.debug_struct(stringify!($ty)).finish_non_exhaustive()
            }
        }
    };
}

opaque_debug!(Infer<R, In>);
opaque_debug!(Map<R, T, Key>);
opaque_debug!(Chunked<R, T, Key>);
opaque_debug!(MapReduce<R, S, T, Key>);
opaque_debug!(TriggeredMapReduce<R, S, T, Key>);
opaque_debug!(Keyed<K, F>);

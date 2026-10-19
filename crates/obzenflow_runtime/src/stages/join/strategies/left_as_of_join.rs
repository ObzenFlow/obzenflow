// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Point-in-time left join: one output per stream row from the version in force.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::{AsOf, AsOfJoin, AsOfReferenceView, TypedAsOfJoinHandler};
use obzenflow_core::{OneFactStageOutput, TypedPayload};
use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;

type LeftAsOfJoinBuild<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn, J> =
    AsOfJoin<LeftAsOfJoin<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn, J>>;

/// Builder state for a point-in-time join with its catalogue key and effective time set.
pub struct LeftJoinBuilderDslWithEffectiveAt<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn> {
    pub(super) catalog_key_fn: CatalogKeyFn,
    pub(super) effective_at_fn: EffectiveAtFn,
    pub(super) _phantom: PhantomData<(C, S, E, K, T)>,
}

impl<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn>
    LeftJoinBuilderDslWithEffectiveAt<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn>
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + Debug,
    T: Ord + Copy + Send + Sync + Debug + 'static,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone,
    EffectiveAtFn: Fn(&C) -> T + Send + Sync + Clone,
{
    /// Set the stream key extractor.
    pub fn stream_key<F>(
        self,
        key_fn: F,
    ) -> LeftJoinBuilderDslWithAsOfKeys<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, F>
    where
        F: Fn(&S) -> K + Send + Sync + Clone,
    {
        LeftJoinBuilderDslWithAsOfKeys {
            catalog_key_fn: self.catalog_key_fn,
            effective_at_fn: self.effective_at_fn,
            stream_key_fn: key_fn,
            _phantom: PhantomData,
        }
    }
}

/// Builder state for a point-in-time join with both keys set.
pub struct LeftJoinBuilderDslWithAsOfKeys<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn> {
    catalog_key_fn: CatalogKeyFn,
    effective_at_fn: EffectiveAtFn,
    stream_key_fn: StreamKeyFn,
    _phantom: PhantomData<(C, S, E, K, T)>,
}

impl<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn>
    LeftJoinBuilderDslWithAsOfKeys<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn>
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + Debug,
    T: Ord + Copy + Send + Sync + Debug + 'static,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone,
    EffectiveAtFn: Fn(&C) -> T + Send + Sync + Clone,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone,
{
    /// Set the time each stream row is joined as of.
    pub fn as_of<F>(
        self,
        as_of_fn: F,
    ) -> LeftJoinBuilderDslWithAsOf<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, F>
    where
        F: Fn(&S) -> T + Send + Sync + Clone,
    {
        LeftJoinBuilderDslWithAsOf {
            catalog_key_fn: self.catalog_key_fn,
            effective_at_fn: self.effective_at_fn,
            stream_key_fn: self.stream_key_fn,
            as_of_fn,
            _phantom: PhantomData,
        }
    }
}

/// Builder state for a point-in-time join with every accessor set.
pub struct LeftJoinBuilderDslWithAsOf<
    C,
    S,
    E,
    K,
    T,
    CatalogKeyFn,
    EffectiveAtFn,
    StreamKeyFn,
    AsOfFn,
> {
    catalog_key_fn: CatalogKeyFn,
    effective_at_fn: EffectiveAtFn,
    stream_key_fn: StreamKeyFn,
    as_of_fn: AsOfFn,
    _phantom: PhantomData<(C, S, E, K, T)>,
}

impl<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn>
    LeftJoinBuilderDslWithAsOf<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn>
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + Debug,
    T: Ord + Copy + Send + Sync + Debug + 'static,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone,
    EffectiveAtFn: Fn(&C) -> T + Send + Sync + Clone,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone,
    AsOfFn: Fn(&S) -> T + Send + Sync + Clone,
{
    /// Set the join function, which writes one output for each stream row, and
    /// build the point-in-time join.
    pub fn build<J>(
        self,
        join_fn: J,
    ) -> LeftAsOfJoinBuild<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn, J>
    where
        E: OneFactStageOutput + Send + Sync + 'static,
        J: Fn(AsOf<C>, S) -> E + Send + Sync + Clone,
    {
        AsOfJoin::new(LeftAsOfJoin {
            catalog_key_fn: self.catalog_key_fn,
            effective_at_fn: self.effective_at_fn,
            stream_key_fn: self.stream_key_fn,
            as_of_fn: self.as_of_fn,
            join_fn,
            _phantom: PhantomData,
        })
    }
}

/// Built-in point-in-time join that passes each stream row's [`AsOf`] selection
/// to its join function.
pub struct LeftAsOfJoin<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn, JoinFn> {
    catalog_key_fn: CatalogKeyFn,
    effective_at_fn: EffectiveAtFn,
    stream_key_fn: StreamKeyFn,
    as_of_fn: AsOfFn,
    join_fn: JoinFn,
    _phantom: PhantomData<(C, S, E, K, T)>,
}

impl<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn, JoinFn> Clone
    for LeftAsOfJoin<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn, JoinFn>
where
    CatalogKeyFn: Clone,
    EffectiveAtFn: Clone,
    StreamKeyFn: Clone,
    AsOfFn: Clone,
    JoinFn: Clone,
{
    fn clone(&self) -> Self {
        Self {
            catalog_key_fn: self.catalog_key_fn.clone(),
            effective_at_fn: self.effective_at_fn.clone(),
            stream_key_fn: self.stream_key_fn.clone(),
            as_of_fn: self.as_of_fn.clone(),
            join_fn: self.join_fn.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn, JoinFn> Debug
    for LeftAsOfJoin<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn, JoinFn>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeftAsOfJoin")
            .field("catalog", &std::any::type_name::<C>())
            .field("stream", &std::any::type_name::<S>())
            .field("output", &std::any::type_name::<E>())
            .finish_non_exhaustive()
    }
}

impl<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn, JoinFn> TypedAsOfJoinHandler
    for LeftAsOfJoin<C, S, E, K, T, CatalogKeyFn, EffectiveAtFn, StreamKeyFn, AsOfFn, JoinFn>
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    E: OneFactStageOutput + Send + Sync + 'static,
    K: Eq + Hash + Clone + Send + Sync + Debug,
    T: Ord + Copy + Send + Sync + Debug + 'static,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync,
    EffectiveAtFn: Fn(&C) -> T + Send + Sync,
    StreamKeyFn: Fn(&S) -> K + Send + Sync,
    AsOfFn: Fn(&S) -> T + Send + Sync,
    JoinFn: Fn(AsOf<C>, S) -> E + Send + Sync,
{
    type State = ();
    type ReferenceKey = K;
    type EffectiveTime = T;
    type Reference = C;
    type Stream = S;
    type Output = E;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(
        &self,
        reference: &Self::Reference,
    ) -> Result<(Self::ReferenceKey, Self::EffectiveTime), HandlerError> {
        Ok((
            (self.catalog_key_fn)(reference),
            (self.effective_at_fn)(reference),
        ))
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut AsOfReferenceView<
            '_,
            Self::ReferenceKey,
            Self::EffectiveTime,
            Self::Reference,
        >,
        stream: Self::Stream,
    ) -> Result<Vec<Self::Output>, HandlerError> {
        let key = (self.stream_key_fn)(&stream);
        let as_of = (self.as_of_fn)(&stream);
        let version = references.select_as_of(&key, &as_of);
        Ok(vec![(self.join_fn)(version, stream)])
    }
}

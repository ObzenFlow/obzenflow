// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Strict Join Strategy - trigger jonestown protocol on unmatched stream events
//!
//! Mission-critical systems where missing reference data indicates corruption

use super::common::{JoinStrategy, JoinStrategyValueOutput, JoinWithStrategy};
use crate::stages::common::stage_handle::StageHandle;
use crate::stages::join::config::JoinReferenceMode;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::StageId;
use obzenflow_core::TypedPayload;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

type StrictJoinBuildResult<C, S, E, K, CatalogKeyFn, StreamKeyFn, J> = (
    StageId,
    StrictJoin<C, S, E, K, CatalogKeyFn, StreamKeyFn, J>,
);

/// Builder for StrictJoin
/// Type parameters: <CatalogType, StreamType, EnrichedType>
pub struct StrictJoinBuilder<C, S, E> {
    _phantom: PhantomData<(C, S, E)>,
}

impl<C, S, E> Default for StrictJoinBuilder<C, S, E> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<C, S, E> StrictJoinBuilder<C, S, E>
where
    C: TypedPayload + Clone + Send + Sync + 'static,
    S: TypedPayload + Clone + Send + Sync + 'static,
    E: TypedPayload + Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the reference stage handle (for programmatic use)
    /// Returns a builder that will eventually produce (StageId, StrictJoin) for the DSL layer
    pub fn with_reference(
        self,
        reference_handle: Arc<dyn StageHandle>,
    ) -> StrictJoinBuilderWithReference<C, S, E> {
        StrictJoinBuilderWithReference {
            reference_stage_id: reference_handle.stage_id(),
            _phantom: PhantomData,
        }
    }

    /// Set the reference stage ID directly (for DSL use)
    pub fn with_reference_id(
        self,
        reference_stage_id: StageId,
    ) -> StrictJoinBuilderWithReference<C, S, E> {
        StrictJoinBuilderWithReference {
            reference_stage_id,
            _phantom: PhantomData,
        }
    }

    /// Set the catalog key extractor directly (for DSL use without with_reference)
    pub fn catalog_key<K, F>(self, key_fn: F) -> StrictJoinBuilderDslWithCatalogKey<C, S, E, K, F>
    where
        K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
        F: Fn(&C) -> K + Send + Sync + Clone,
    {
        StrictJoinBuilderDslWithCatalogKey {
            catalog_key_fn: key_fn,
            _phantom: PhantomData,
        }
    }
}

/// Builder state for DSL usage with catalog key set (no reference ID needed)
pub struct StrictJoinBuilderDslWithCatalogKey<C, S, E, K, CatalogKeyFn> {
    catalog_key_fn: CatalogKeyFn,
    _phantom: PhantomData<(C, S, E, K)>,
}

impl<C, S, E, K, CatalogKeyFn> StrictJoinBuilderDslWithCatalogKey<C, S, E, K, CatalogKeyFn>
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone,
{
    /// Set the stream key extractor
    pub fn stream_key<F>(
        self,
        key_fn: F,
    ) -> StrictJoinBuilderDslWithKeys<C, S, E, K, CatalogKeyFn, F>
    where
        F: Fn(&S) -> K + Send + Sync + Clone,
    {
        StrictJoinBuilderDslWithKeys {
            catalog_key_fn: self.catalog_key_fn,
            stream_key_fn: key_fn,
            reference_mode: JoinReferenceMode::FiniteEof,
            _phantom: PhantomData,
        }
    }
}

/// Builder state for DSL usage with both keys set (no reference ID needed)
pub struct StrictJoinBuilderDslWithKeys<C, S, E, K, CatalogKeyFn, StreamKeyFn> {
    catalog_key_fn: CatalogKeyFn,
    stream_key_fn: StreamKeyFn,
    reference_mode: JoinReferenceMode,
    _phantom: PhantomData<(C, S, E, K)>,
}

impl<C, S, E, K, CatalogKeyFn, StreamKeyFn>
    StrictJoinBuilderDslWithKeys<C, S, E, K, CatalogKeyFn, StreamKeyFn>
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone,
{
    /// Set the join function and build just the handler (for DSL use)
    pub fn live(mut self) -> Self {
        self.reference_mode = JoinReferenceMode::Live;
        self
    }

    pub fn build<J>(self, join_fn: J) -> StrictJoin<C, S, E, K, CatalogKeyFn, StreamKeyFn, J>
    where
        J: Fn(C, S) -> E + Send + Sync + Clone,
    {
        JoinWithStrategy {
            strategy: StrictJoinStrategy {
                join_fn,
                _phantom: PhantomData,
            },
            catalog_key_fn: self.catalog_key_fn,
            stream_key_fn: self.stream_key_fn,
            reference_mode: self.reference_mode,
            _phantom: PhantomData,
        }
    }
}

/// Builder state with reference stage set
pub struct StrictJoinBuilderWithReference<C, S, E> {
    reference_stage_id: StageId,
    _phantom: PhantomData<(C, S, E)>,
}

impl<C, S, E> StrictJoinBuilderWithReference<C, S, E>
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
{
    /// Set the catalog key extractor (extracts key from events being stored in catalog)
    pub fn catalog_key<K, F>(self, key_fn: F) -> StrictJoinBuilderWithCatalogKey<C, S, E, K, F>
    where
        K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
        F: Fn(&C) -> K + Send + Sync + Clone,
    {
        StrictJoinBuilderWithCatalogKey {
            reference_stage_id: self.reference_stage_id,
            catalog_key_fn: key_fn,
            _phantom: PhantomData,
        }
    }
}

/// Builder state with catalog key set
pub struct StrictJoinBuilderWithCatalogKey<C, S, E, K, CatalogKeyFn> {
    reference_stage_id: StageId,
    catalog_key_fn: CatalogKeyFn,
    _phantom: PhantomData<(C, S, E, K)>,
}

impl<C, S, E, K, CatalogKeyFn> StrictJoinBuilderWithCatalogKey<C, S, E, K, CatalogKeyFn>
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone,
{
    /// Set the stream key extractor (extracts key from streaming events to lookup in catalog)
    pub fn stream_key<F>(self, key_fn: F) -> StrictJoinBuilderWithKeys<C, S, E, K, CatalogKeyFn, F>
    where
        F: Fn(&S) -> K + Send + Sync + Clone,
    {
        StrictJoinBuilderWithKeys {
            reference_stage_id: self.reference_stage_id,
            catalog_key_fn: self.catalog_key_fn,
            stream_key_fn: key_fn,
            reference_mode: JoinReferenceMode::FiniteEof,
            _phantom: PhantomData,
        }
    }
}

/// Builder state with both keys set
pub struct StrictJoinBuilderWithKeys<C, S, E, K, CatalogKeyFn, StreamKeyFn> {
    reference_stage_id: StageId,
    catalog_key_fn: CatalogKeyFn,
    stream_key_fn: StreamKeyFn,
    reference_mode: JoinReferenceMode,
    _phantom: PhantomData<(C, S, E, K)>,
}

impl<C, S, E, K, CatalogKeyFn, StreamKeyFn>
    StrictJoinBuilderWithKeys<C, S, E, K, CatalogKeyFn, StreamKeyFn>
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone,
{
    /// Set the join function and build the handler
    pub fn live(mut self) -> Self {
        self.reference_mode = JoinReferenceMode::Live;
        self
    }

    pub fn join<J>(
        self,
        join_fn: J,
    ) -> StrictJoinBuildResult<C, S, E, K, CatalogKeyFn, StreamKeyFn, J>
    where
        J: Fn(C, S) -> E + Send + Sync + Clone,
    {
        (
            self.reference_stage_id,
            JoinWithStrategy {
                strategy: StrictJoinStrategy {
                    join_fn,
                    _phantom: PhantomData,
                },
                catalog_key_fn: self.catalog_key_fn,
                stream_key_fn: self.stream_key_fn,
                reference_mode: self.reference_mode,
                _phantom: PhantomData,
            },
        )
    }
}

#[derive(Clone)]
pub struct StrictJoinStrategy<C, S, E, K, J> {
    pub(crate) join_fn: J,
    pub(crate) _phantom: PhantomData<(C, S, E, K)>,
}

impl<C, S, E, K, J> JoinStrategy for StrictJoinStrategy<C, S, E, K, J>
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    J: Fn(C, S) -> E + Send + Sync + Clone,
{
    type CatalogType = C;
    type StreamType = S;
    type EnrichedType = E;
    type Key = K;

    fn match_reference(
        &self,
        reference: Option<Self::CatalogType>,
        stream_data: Self::StreamType,
        stream_key: Self::Key,
    ) -> JoinStrategyValueOutput<Self::EnrichedType> {
        match reference {
            Some(catalog_data) => {
                tracing::debug!("StrictJoin: Found match for key: {:?}", stream_key);
                let output = (self.join_fn)(catalog_data, stream_data);
                JoinStrategyValueOutput::facts(vec![output])
            }
            None => {
                tracing::error!(
                    "StrictJoin: Data integrity violation - stream key {:?} has no catalog match",
                    stream_key
                );

                tracing::error!("StrictJoin: emitting poison EOF for unmatched stream event");
                JoinStrategyValueOutput::framework_eof(EofKind::Poison)
            }
        }
    }
}

/// Type alias for wrapped StrictJoin strategy
pub type StrictJoin<C, S, E, K, CatalogKeyFn, StreamKeyFn, J> =
    JoinWithStrategy<StrictJoinStrategy<C, S, E, K, J>, CatalogKeyFn, StreamKeyFn>;

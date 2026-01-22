//! Inner Join Strategy - drop unmatched stream events
//!
//! SQL Equivalent: SELECT * FROM stream INNER JOIN reference ON ...

use super::common::{JoinStrategy, JoinWithStrategy};
use crate::stages::common::stage_handle::StageHandle;
use crate::stages::join::config::{JoinReferenceMode, DEFAULT_REFERENCE_BATCH_CAP};
use obzenflow_core::ChainEvent;
use obzenflow_core::TypedPayload;
use obzenflow_core::{StageId, WriterId};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::sync::Arc;

type InnerJoinBuildResult<C, S, E, K, CatalogKeyFn, StreamKeyFn, J> =
    (StageId, InnerJoin<C, S, E, K, CatalogKeyFn, StreamKeyFn, J>);

/// Builder for InnerJoin
/// Type parameters: <CatalogType, StreamType, EnrichedType>
pub struct InnerJoinBuilder<C, S, E> {
    _phantom: PhantomData<(C, S, E)>,
}

impl<C, S, E> Default for InnerJoinBuilder<C, S, E> {
    fn default() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<C, S, E> InnerJoinBuilder<C, S, E>
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
{
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the reference stage handle (for programmatic use)
    /// Returns a builder that will eventually produce (StageId, InnerJoin) for the DSL layer
    pub fn with_reference(
        self,
        reference_handle: Arc<dyn StageHandle>,
    ) -> InnerJoinBuilderWithReference<C, S, E> {
        InnerJoinBuilderWithReference {
            reference_stage_id: reference_handle.stage_id(),
            _phantom: PhantomData,
        }
    }

    /// Set the reference stage ID directly (for DSL use)
    pub fn with_reference_id(
        self,
        reference_stage_id: StageId,
    ) -> InnerJoinBuilderWithReference<C, S, E> {
        InnerJoinBuilderWithReference {
            reference_stage_id,
            _phantom: PhantomData,
        }
    }

    /// Set the catalog key extractor directly (for DSL use without with_reference)
    pub fn catalog_key<K, F>(self, key_fn: F) -> InnerJoinBuilderDslWithCatalogKey<C, S, E, K, F>
    where
        K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
        F: Fn(&C) -> K + Send + Sync + Clone,
    {
        InnerJoinBuilderDslWithCatalogKey {
            catalog_key_fn: key_fn,
            _phantom: PhantomData,
        }
    }
}

/// Builder state for DSL usage with catalog key set (no reference ID needed)
pub struct InnerJoinBuilderDslWithCatalogKey<C, S, E, K, CatalogKeyFn> {
    catalog_key_fn: CatalogKeyFn,
    _phantom: PhantomData<(C, S, E, K)>,
}

impl<C, S, E, K, CatalogKeyFn> InnerJoinBuilderDslWithCatalogKey<C, S, E, K, CatalogKeyFn>
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
    ) -> InnerJoinBuilderDslWithKeys<C, S, E, K, CatalogKeyFn, F>
    where
        F: Fn(&S) -> K + Send + Sync + Clone,
    {
        InnerJoinBuilderDslWithKeys {
            catalog_key_fn: self.catalog_key_fn,
            stream_key_fn: key_fn,
            reference_mode: JoinReferenceMode::FiniteEof,
            reference_batch_cap: Some(DEFAULT_REFERENCE_BATCH_CAP),
            _phantom: PhantomData,
        }
    }
}

/// Builder state for DSL usage with both keys set (no reference ID needed)
pub struct InnerJoinBuilderDslWithKeys<C, S, E, K, CatalogKeyFn, StreamKeyFn> {
    catalog_key_fn: CatalogKeyFn,
    stream_key_fn: StreamKeyFn,
    reference_mode: JoinReferenceMode,
    reference_batch_cap: Option<usize>,
    _phantom: PhantomData<(C, S, E, K)>,
}

impl<C, S, E, K, CatalogKeyFn, StreamKeyFn>
    InnerJoinBuilderDslWithKeys<C, S, E, K, CatalogKeyFn, StreamKeyFn>
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

    pub fn reference_batch_cap(mut self, cap: Option<usize>) -> Self {
        self.reference_batch_cap = cap;
        self
    }

    pub fn build<J>(self, join_fn: J) -> InnerJoin<C, S, E, K, CatalogKeyFn, StreamKeyFn, J>
    where
        J: Fn(C, S) -> E + Send + Sync + Clone,
    {
        JoinWithStrategy {
            strategy: InnerJoinStrategy {
                join_fn,
                _phantom: PhantomData,
            },
            catalog_key_fn: self.catalog_key_fn,
            stream_key_fn: self.stream_key_fn,
            reference_mode: self.reference_mode,
            reference_batch_cap: self.reference_batch_cap,
            _phantom: PhantomData,
        }
    }
}

/// Builder state with reference stage set
pub struct InnerJoinBuilderWithReference<C, S, E> {
    reference_stage_id: StageId,
    _phantom: PhantomData<(C, S, E)>,
}

impl<C, S, E> InnerJoinBuilderWithReference<C, S, E>
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
{
    /// Set the catalog key extractor (extracts key from events being stored in catalog)
    pub fn catalog_key<K, F>(self, key_fn: F) -> InnerJoinBuilderWithCatalogKey<C, S, E, K, F>
    where
        K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
        F: Fn(&C) -> K + Send + Sync + Clone,
    {
        InnerJoinBuilderWithCatalogKey {
            reference_stage_id: self.reference_stage_id,
            catalog_key_fn: key_fn,
            _phantom: PhantomData,
        }
    }
}

/// Builder state with catalog key set
pub struct InnerJoinBuilderWithCatalogKey<C, S, E, K, CatalogKeyFn> {
    reference_stage_id: StageId,
    catalog_key_fn: CatalogKeyFn,
    _phantom: PhantomData<(C, S, E, K)>,
}

impl<C, S, E, K, CatalogKeyFn> InnerJoinBuilderWithCatalogKey<C, S, E, K, CatalogKeyFn>
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone,
{
    /// Set the stream key extractor (extracts key from streaming events to lookup in catalog)
    pub fn stream_key<F>(self, key_fn: F) -> InnerJoinBuilderWithKeys<C, S, E, K, CatalogKeyFn, F>
    where
        F: Fn(&S) -> K + Send + Sync + Clone,
    {
        InnerJoinBuilderWithKeys {
            reference_stage_id: self.reference_stage_id,
            catalog_key_fn: self.catalog_key_fn,
            stream_key_fn: key_fn,
            reference_mode: JoinReferenceMode::FiniteEof,
            reference_batch_cap: Some(DEFAULT_REFERENCE_BATCH_CAP),
            _phantom: PhantomData,
        }
    }
}

/// Builder state with both keys set
pub struct InnerJoinBuilderWithKeys<C, S, E, K, CatalogKeyFn, StreamKeyFn> {
    reference_stage_id: StageId,
    catalog_key_fn: CatalogKeyFn,
    stream_key_fn: StreamKeyFn,
    reference_mode: JoinReferenceMode,
    reference_batch_cap: Option<usize>,
    _phantom: PhantomData<(C, S, E, K)>,
}

impl<C, S, E, K, CatalogKeyFn, StreamKeyFn>
    InnerJoinBuilderWithKeys<C, S, E, K, CatalogKeyFn, StreamKeyFn>
where
    C: TypedPayload + Clone + Send + Sync,
    S: TypedPayload + Clone + Send + Sync,
    E: TypedPayload + Clone + Send + Sync,
    K: Eq + Hash + Clone + Send + Sync + std::fmt::Debug,
    CatalogKeyFn: Fn(&C) -> K + Send + Sync + Clone,
    StreamKeyFn: Fn(&S) -> K + Send + Sync + Clone,
{
    /// Set the join function and build the handler (returns tuple for programmatic use)
    /// Returns (reference_stage_id, strategy) for the DSL layer to use
    pub fn live(mut self) -> Self {
        self.reference_mode = JoinReferenceMode::Live;
        self
    }

    pub fn reference_batch_cap(mut self, cap: Option<usize>) -> Self {
        self.reference_batch_cap = cap;
        self
    }

    pub fn join<J>(
        self,
        join_fn: J,
    ) -> InnerJoinBuildResult<C, S, E, K, CatalogKeyFn, StreamKeyFn, J>
    where
        J: Fn(C, S) -> E + Send + Sync + Clone,
    {
        (
            self.reference_stage_id,
            JoinWithStrategy {
                strategy: InnerJoinStrategy {
                    join_fn,
                    _phantom: PhantomData,
                },
                catalog_key_fn: self.catalog_key_fn,
                stream_key_fn: self.stream_key_fn,
                reference_mode: self.reference_mode,
                reference_batch_cap: self.reference_batch_cap,
                _phantom: PhantomData,
            },
        )
    }
}

/// Inner Join - drops stream events with no matching reference
///
/// The strategy doesn't know or care about reference stage IDs.
/// It just builds a catalog and matches stream events against it.
#[derive(Clone)]
pub struct InnerJoinStrategy<C, S, E, K, J> {
    pub(crate) join_fn: J,
    pub(crate) _phantom: PhantomData<(C, S, E, K)>,
}

impl<C, S, E, K, J> JoinStrategy for InnerJoinStrategy<C, S, E, K, J>
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

    fn match_stream_event(
        &self,
        catalog: &HashMap<Self::Key, Self::CatalogType>,
        stream_data: Self::StreamType,
        stream_key: Self::Key,
        writer_id: WriterId,
    ) -> Vec<ChainEvent> {
        match catalog.get(&stream_key) {
            Some(catalog_data) => {
                tracing::debug!("InnerJoin: Found match for key: {:?}", stream_key);
                let output = (self.join_fn)(catalog_data.clone(), stream_data);
                vec![output.to_event(writer_id)]
            }
            None => {
                tracing::debug!("InnerJoin: No match for key: {:?} (dropping)", stream_key);
                vec![]
            }
        }
    }
}

/// Type alias for the wrapped InnerJoin strategy
pub type InnerJoin<C, S, E, K, CatalogKeyFn, StreamKeyFn, J> =
    JoinWithStrategy<InnerJoinStrategy<C, S, E, K, J>, CatalogKeyFn, StreamKeyFn>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::JoinHandler;
    use obzenflow_core::TypedPayload;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct CatalogRow {
        key: String,
        value: String,
    }
    impl TypedPayload for CatalogRow {
        const EVENT_TYPE: &'static str = "test.catalog";
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct StreamRow {
        key: String,
    }
    impl TypedPayload for StreamRow {
        const EVENT_TYPE: &'static str = "test.stream";
    }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct Joined {
        source: String,
        stream: String,
    }
    impl TypedPayload for Joined {
        const EVENT_TYPE: &'static str = "test.joined";
    }

    #[test]
    fn inner_join_hits_and_drops_misses() {
        let handler = InnerJoinBuilder::<CatalogRow, StreamRow, Joined>::new()
            .catalog_key(|c| c.key.clone())
            .stream_key(|s| s.key.clone())
            .build(|c, s| Joined {
                source: c.value,
                stream: s.key,
            });

        let mut state = handler.initial_state();
        let writer = WriterId::from(StageId::new());

        // hydrate
        handler
            .process_event(
                &mut state,
                CatalogRow {
                    key: "k1".into(),
                    value: "v1".into(),
                }
                .to_event(writer),
                StageId::new(),
                writer,
            )
            .expect("process_event should succeed for inner join hydrate case");

        // hit
        let hit = handler
            .process_event(
                &mut state,
                StreamRow { key: "k1".into() }.to_event(writer),
                StageId::new(),
                writer,
            )
            .expect("process_event should succeed for inner join hit case");
        let joined =
            Joined::from_event(&hit[0]).expect("should deserialize joined row for inner join hit");
        assert_eq!(
            joined,
            Joined {
                source: "v1".into(),
                stream: "k1".into()
            }
        );

        // miss drops
        let miss = handler
            .process_event(
                &mut state,
                StreamRow {
                    key: "missing".into(),
                }
                .to_event(writer),
                StageId::new(),
                writer,
            )
            .expect("process_event should succeed for inner join miss case");
        assert!(miss.is_empty());
    }
}

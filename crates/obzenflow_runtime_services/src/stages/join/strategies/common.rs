//! Common types and utilities for join handlers

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::JoinHandler;
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_core::StageId;
use obzenflow_core::{ChainEvent, WriterId};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

/// Shared state structure for typed joins
///
/// Uses in-memory HashMap for fast lookups (suitable for <1GB per side).
/// For larger dimensions (>1GB), see FLOWIP-093d (CatalogStore) and FLOWIP-093 (Materialized Views).
#[derive(Clone, Debug)]
pub struct TypedJoinState<Ref, Stream, K>
where
    Ref: TypedPayload + Clone,
    Stream: TypedPayload + Clone,
    K: Eq + Hash + Clone,
{
    /// Reference catalog (dimension data)
    pub reference_catalog: HashMap<K, Ref>,

    /// Stream catalog (ONLY for Streaming strategy - stream-to-stream joins)
    pub stream_catalog: HashMap<K, Stream>,

    /// Buffered stream events (ONLY for WaitForReference strategy)
    pub stream_pending: Vec<Stream>,

    /// Reference EOF marker (for flow control)
    pub reference_complete: bool,

    /// Stream EOF marker
    pub stream_complete: bool,

    /// Deduplication for control signals to avoid repeated processing/log noise
    pub reference_eof_seen: bool,
    pub stream_eof_seen: bool,
}

impl<Ref, Stream, K> TypedJoinState<Ref, Stream, K>
where
    Ref: TypedPayload + Clone,
    Stream: TypedPayload + Clone,
    K: Eq + Hash + Clone,
{
    /// Create new empty join state
    pub fn new() -> Self {
        Self {
            reference_catalog: HashMap::new(),
            stream_catalog: HashMap::new(),
            stream_pending: Vec::new(),
            reference_complete: false,
            stream_complete: false,
            reference_eof_seen: false,
            stream_eof_seen: false,
        }
    }

    /// Check if both upstreams are complete
    pub fn is_fully_drained(&self) -> bool {
        self.reference_complete && self.stream_complete
    }
}

impl<Ref, Stream, K> Default for TypedJoinState<Ref, Stream, K>
where
    Ref: TypedPayload + Clone,
    Stream: TypedPayload + Clone,
    K: Eq + Hash + Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

/// Strategy trait that only defines how to match stream events against a catalog
pub trait JoinStrategy {
    type CatalogType: TypedPayload + Clone + Send + Sync;
    type StreamType: TypedPayload + Clone + Send + Sync;
    type EnrichedType: TypedPayload + Clone + Send + Sync;
    type Key: Eq + Hash + Clone + Send + Sync + std::fmt::Debug;

    fn match_stream_event(
        &self,
        catalog: &HashMap<Self::Key, Self::CatalogType>,
        stream_data: Self::StreamType,
        stream_key: Self::Key,
        writer_id: WriterId,
    ) -> Vec<ChainEvent>;
}

/// Wrapper that implements JoinHandler by delegating to a JoinStrategy
pub struct JoinWithStrategy<S, CatalogKeyFn, StreamKeyFn>
where
    S: JoinStrategy + Clone + Send + Sync,
{
    pub(crate) strategy: S,
    pub(crate) catalog_key_fn: CatalogKeyFn,
    pub(crate) stream_key_fn: StreamKeyFn,
    pub(crate) _phantom: PhantomData<S>,
}

impl<S, CatalogKeyFn, StreamKeyFn> Clone for JoinWithStrategy<S, CatalogKeyFn, StreamKeyFn>
where
    S: JoinStrategy + Clone + Send + Sync,
    CatalogKeyFn: Clone,
    StreamKeyFn: Clone,
{
    fn clone(&self) -> Self {
        Self {
            strategy: self.strategy.clone(),
            catalog_key_fn: self.catalog_key_fn.clone(),
            stream_key_fn: self.stream_key_fn.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<S, CatalogKeyFn, StreamKeyFn> std::fmt::Debug
    for JoinWithStrategy<S, CatalogKeyFn, StreamKeyFn>
where
    S: JoinStrategy + Clone + Send + Sync,
    CatalogKeyFn: Fn(&S::CatalogType) -> S::Key + Send + Sync + Clone,
    StreamKeyFn: Fn(&S::StreamType) -> S::Key + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinWithStrategy")
            .field("strategy", &std::any::type_name::<S>())
            .field("catalog_key_fn", &"<closure>")
            .field("stream_key_fn", &"<closure>")
            .finish()
    }
}

#[async_trait::async_trait]
impl<S, CatalogKeyFn, StreamKeyFn> JoinHandler for JoinWithStrategy<S, CatalogKeyFn, StreamKeyFn>
where
    S: JoinStrategy + Clone + Send + Sync,
    CatalogKeyFn: Fn(&S::CatalogType) -> S::Key + Send + Sync + Clone + 'static,
    StreamKeyFn: Fn(&S::StreamType) -> S::Key + Send + Sync + Clone + 'static,
{
    type State = TypedJoinState<S::CatalogType, S::StreamType, S::Key>;

    fn initial_state(&self) -> Self::State {
        TypedJoinState {
            reference_catalog: HashMap::new(),
            stream_catalog: HashMap::new(),
            stream_pending: Vec::new(),
            reference_complete: false,
            stream_complete: false,
            reference_eof_seen: false,
            stream_eof_seen: false,
        }
    }

    fn process_event(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        // Hydrating: catalog side
        if let Some(catalog_data) = S::CatalogType::from_event(&event) {
            let key = (self.catalog_key_fn)(&catalog_data);
            tracing::debug!(
                strategy = %std::any::type_name::<S>(),
                key = ?key,
                "JoinWithStrategy: Added to catalog"
            );
            state.reference_catalog.insert(key, catalog_data);
            return Ok(vec![]);
        }

        // Enriching: stream side
        if let Some(stream_data) = S::StreamType::from_event(&event) {
            let key = (self.stream_key_fn)(&stream_data);
            let events = self.strategy.match_stream_event(
                &state.reference_catalog,
                stream_data,
                key,
                writer_id,
            );
            return Ok(events);
        }

        Ok(vec![])
    }

    fn on_source_eof(
        &self,
        state: &mut Self::State,
        source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if state.reference_eof_seen && state.stream_eof_seen {
            tracing::debug!(
                strategy = %std::any::type_name::<S>(),
                "JoinWithStrategy: duplicate EOF ignored"
            );
            return Ok(vec![]);
        }

        // Heuristic: first EOF flips reference, second flips stream (order doesn’t matter here)
        if !state.reference_eof_seen {
            state.reference_eof_seen = true;
            state.reference_complete = true;
            tracing::info!(
                strategy = %std::any::type_name::<S>(),
                catalog_size = state.reference_catalog.len(),
                source = ?source_id,
                "JoinWithStrategy: reference EOF received"
            );
        } else if !state.stream_eof_seen {
            state.stream_eof_seen = true;
            state.stream_complete = true;
            tracing::info!(
                strategy = %std::any::type_name::<S>(),
                source = ?source_id,
                "JoinWithStrategy: stream EOF received"
            );
        }
        Ok(vec![])
    }

    async fn drain(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::schema::TypedPayload;
    use obzenflow_core::id::StageId;
    use obzenflow_core::WriterId;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct CatalogRow {
        key: String,
        value: String,
    }

    impl TypedPayload for CatalogRow {
        const EVENT_TYPE: &'static str = "test.catalog";
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct StreamRow {
        key: String,
    }

    impl TypedPayload for StreamRow {
        const EVENT_TYPE: &'static str = "test.stream";
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct JoinedRow {
        catalog_value: Option<String>,
        stream_key: String,
    }

    impl TypedPayload for JoinedRow {
        const EVENT_TYPE: &'static str = "test.joined";
    }

    #[derive(Clone)]
    struct TestJoinStrategy;

    impl JoinStrategy for TestJoinStrategy {
        type CatalogType = CatalogRow;
        type StreamType = StreamRow;
        type EnrichedType = JoinedRow;
        type Key = String;

        fn match_stream_event(
            &self,
            catalog: &HashMap<Self::Key, Self::CatalogType>,
            stream_data: Self::StreamType,
            stream_key: Self::Key,
            writer_id: WriterId,
        ) -> Vec<ChainEvent> {
            let joined = JoinedRow {
                catalog_value: catalog.get(&stream_key).map(|c| c.value.clone()),
                stream_key,
            };
            vec![joined.to_event(writer_id)]
        }
    }

    fn make_join() -> JoinWithStrategy<
        TestJoinStrategy,
        impl Fn(&CatalogRow) -> String + Send + Sync + Clone + 'static,
        impl Fn(&StreamRow) -> String + Send + Sync + Clone + 'static,
    > {
        JoinWithStrategy {
            strategy: TestJoinStrategy,
            catalog_key_fn: |c: &CatalogRow| c.key.clone(),
            stream_key_fn: |s: &StreamRow| s.key.clone(),
            _phantom: PhantomData,
        }
    }

    #[test]
    fn catalog_hydration_builds_reference_map() {
        let handler = make_join();
        let mut state = handler.initial_state();
        let writer = WriterId::from(StageId::new());

        let catalog_event = CatalogRow {
            key: "k1".into(),
            value: "v1".into(),
        }
        .to_event(writer.clone());

        let out = handler
            .process_event(&mut state, catalog_event, StageId::new(), writer)
            .expect("process_event should succeed in catalog_hydration_builds_reference_map");
        assert!(out.is_empty());
        assert_eq!(state.reference_catalog.len(), 1);
        assert!(state.reference_catalog.contains_key("k1"));
    }

    #[test]
    fn stream_hit_emits_joined_row() {
        let handler = make_join();
        let mut state = handler.initial_state();
        let writer = WriterId::from(StageId::new());

        // hydrate
        let catalog_event = CatalogRow {
            key: "k1".into(),
            value: "v1".into(),
        }
        .to_event(writer.clone());
        handler
            .process_event(&mut state, catalog_event, StageId::new(), writer.clone())
            .expect("process_event should succeed while hydrating catalog");

        // enrich
        let stream_event = StreamRow { key: "k1".into() }.to_event(WriterId::from(StageId::new()));
        let out = handler
            .process_event(&mut state, stream_event, StageId::new(), writer.clone())
            .expect("process_event should succeed in stream_hit_emits_joined_row");
        assert_eq!(out.len(), 1);

        let joined = JoinedRow::from_event(&out[0]).expect("should deserialize joined row");
        assert_eq!(
            joined,
            JoinedRow {
                catalog_value: Some("v1".into()),
                stream_key: "k1".into()
            }
        );
    }

    #[test]
    fn stream_miss_passes_none_catalog() {
        let handler = make_join();
        let mut state = handler.initial_state();
        let writer = WriterId::from(StageId::new());

        let stream_event = StreamRow {
            key: "missing".into(),
        }
        .to_event(WriterId::from(StageId::new()));
        let out = handler
            .process_event(&mut state, stream_event, StageId::new(), writer.clone())
            .expect("process_event should succeed in stream_miss_passes_none_catalog");
        assert_eq!(out.len(), 1);
        let joined = JoinedRow::from_event(&out[0]).expect("should deserialize joined row");
        assert_eq!(
            joined,
            JoinedRow {
                catalog_value: None,
                stream_key: "missing".into()
            }
        );
    }

    #[test]
    fn on_source_eof_marks_reference_complete() {
        let handler = make_join();
        let mut state = handler.initial_state();
        let writer = WriterId::from(StageId::new());

        handler.on_source_eof(&mut state, StageId::new(), writer.clone());
        assert!(state.reference_complete);

        state.stream_complete = true;
        assert!(state.is_fully_drained());
    }

    #[test]
    fn duplicate_eof_is_ignored() {
        let handler = make_join();
        let mut state = handler.initial_state();
        let writer = WriterId::from(StageId::new());

        // first EOF sets reference complete
        handler.on_source_eof(&mut state, StageId::new(), writer.clone());
        assert!(state.reference_eof_seen);
        assert!(state.reference_complete);

        // second EOF sets stream complete
        handler.on_source_eof(&mut state, StageId::new(), writer.clone());
        assert!(state.stream_eof_seen);
        assert!(state.stream_complete);

        // third EOF ignored (no panic/changes)
        handler.on_source_eof(&mut state, StageId::new(), writer.clone());
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Common types and utilities for join handlers

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::JoinHandler;
use crate::stages::join::config::JoinReferenceMode;
use crate::typing::JoinTyping;
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::context::CompositeActivationContext;
use obzenflow_core::event::schema::TypedPayload;
use obzenflow_core::StageId;
use obzenflow_core::{ChainEvent, WriterId};
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;

/// Shared state structure for typed joins
///
/// Uses in-memory HashMap for fast lookups (suitable for <1GB per side).
/// For larger dimensions (>1GB), see FLOWIP-094b (CatalogStore) and FLOWIP-094 (Materialized Views).
#[derive(Clone, Debug)]
pub struct TypedJoinState<Ref, Stream, K>
where
    Ref: TypedPayload + Clone,
    Stream: TypedPayload + Clone,
    K: Eq + Hash + Clone,
{
    /// Reference catalog (dimension data)
    pub reference_catalog: HashMap<K, Ref>,

    /// Exact activation provenance of the currently stored reference row for
    /// each key. Catalog replacement replaces provenance at the same key.
    pub reference_activations: HashMap<K, Vec<CompositeActivationContext>>,

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
            reference_activations: HashMap::new(),
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

/// Outputs from one stream match plus the exact reference catalog keys whose
/// values contributed to them. Requiring this declaration prevents the join
/// wrapper from silently dropping reference-side composite activations or
/// over-attributing every catalog row.
#[derive(Clone, Debug)]
pub struct JoinStrategyOutput<K> {
    pub events: Vec<ChainEvent>,
    pub contributing_reference_keys: Vec<K>,
}

impl<K> JoinStrategyOutput<K> {
    pub fn new(events: Vec<ChainEvent>, contributing_reference_keys: Vec<K>) -> Self {
        Self {
            events,
            contributing_reference_keys,
        }
    }

    pub fn without_reference(events: Vec<ChainEvent>) -> Self {
        Self::new(events, Vec::new())
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
    ) -> JoinStrategyOutput<Self::Key>;
}

/// Wrapper that implements JoinHandler by delegating to a JoinStrategy
pub struct JoinWithStrategy<S, CatalogKeyFn, StreamKeyFn>
where
    S: JoinStrategy + Clone + Send + Sync,
{
    pub(crate) strategy: S,
    pub(crate) catalog_key_fn: CatalogKeyFn,
    pub(crate) stream_key_fn: StreamKeyFn,
    pub(crate) reference_mode: JoinReferenceMode,
    pub(crate) reference_batch_cap: Option<usize>,
    /// FLOWIP-010 §7: build-resolved, installed by the stage builder;
    /// default until then so direct construction keeps working.
    pub(crate) lineage: obzenflow_core::config::LineagePolicy,
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
            reference_mode: self.reference_mode,
            reference_batch_cap: self.reference_batch_cap,
            lineage: self.lineage,
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
            .field("reference_mode", &self.reference_mode)
            .field("reference_batch_cap", &self.reference_batch_cap)
            .finish()
    }
}

impl<S, CatalogKeyFn, StreamKeyFn> JoinTyping for JoinWithStrategy<S, CatalogKeyFn, StreamKeyFn>
where
    S: JoinStrategy + Clone + Send + Sync,
{
    type Reference = S::CatalogType;
    type Stream = S::StreamType;
    type Output = S::EnrichedType;
}

#[async_trait::async_trait]
impl<S, CatalogKeyFn, StreamKeyFn> JoinHandler for JoinWithStrategy<S, CatalogKeyFn, StreamKeyFn>
where
    S: JoinStrategy + Clone + Send + Sync,
    CatalogKeyFn: Fn(&S::CatalogType) -> S::Key + Send + Sync + Clone + 'static,
    StreamKeyFn: Fn(&S::StreamType) -> S::Key + Send + Sync + Clone + 'static,
{
    type State = TypedJoinState<S::CatalogType, S::StreamType, S::Key>;

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        self.lineage = policy;
    }

    fn initial_state(&self) -> Self::State {
        TypedJoinState {
            reference_catalog: HashMap::new(),
            reference_activations: HashMap::new(),
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
            state
                .reference_activations
                .insert(key.clone(), event.composite_activations().to_vec());
            state.reference_catalog.insert(key, catalog_data);
            return Ok(vec![]);
        }

        // Enriching: stream side
        if let Some(stream_data) = S::StreamType::from_event(&event) {
            let key = (self.stream_key_fn)(&stream_data);
            let strategy_output = self.strategy.match_stream_event(
                &state.reference_catalog,
                stream_data,
                key,
                writer_id,
            );
            for reference_key in &strategy_output.contributing_reference_keys {
                if !state.reference_catalog.contains_key(reference_key) {
                    return Err(HandlerError::Validation(format!(
                        "join strategy declared missing contributing reference key {reference_key:?}"
                    )));
                }
            }
            let reference_activations: Vec<_> = strategy_output
                .contributing_reference_keys
                .iter()
                .filter_map(|key| state.reference_activations.get(key))
                .flatten()
                .cloned()
                .collect();
            let events = propagate_stream_lineage(&event, strategy_output.events, self.lineage)?;
            let events = events
                .into_iter()
                .map(|event| {
                    event
                        .try_with_composite_activations(reference_activations.clone())
                        .map_err(|error| HandlerError::Validation(error.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(events);
        }

        Ok(vec![])
    }

    fn reference_mode(&self) -> JoinReferenceMode {
        self.reference_mode
    }

    fn reference_batch_cap(&self) -> Option<usize> {
        self.reference_batch_cap
    }

    fn on_source_eof(
        &self,
        state: &mut Self::State,
        source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        // Live mode: the runtime invokes this hook for the *stream side only* (stream EOF is
        // authoritative for completion). Reference EOF is forwarded but ignored.
        if self.reference_mode == JoinReferenceMode::Live {
            if state.stream_eof_seen {
                tracing::debug!(
                    strategy = %std::any::type_name::<S>(),
                    "JoinWithStrategy: duplicate stream EOF ignored (Live)"
                );
                return Ok(vec![]);
            }

            state.stream_eof_seen = true;
            state.stream_complete = true;
            tracing::info!(
                strategy = %std::any::type_name::<S>(),
                source = ?source_id,
                "JoinWithStrategy: stream EOF received (Live)"
            );
            return Ok(vec![]);
        }

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

fn propagate_stream_lineage(
    parent: &ChainEvent,
    outputs: Vec<ChainEvent>,
    lineage: obzenflow_core::config::LineagePolicy,
) -> Result<Vec<ChainEvent>, HandlerError> {
    // The join strategies typically materialize new events via `TypedPayload::to_event`,
    // which does not preserve correlation/lineage. Since joins conceptually enrich a
    // stream event, we propagate correlation + causality from the stream parent.

    // Match `ChainEventFactory::derived_event`: the same build-resolved
    // policy, threaded as data (FLOWIP-010 §7).
    let max_depth = lineage.max_lineage_depth;

    outputs
        .into_iter()
        .map(|mut event| {
            if event.is_lifecycle() || event.is_control() {
                return Ok(event);
            }

            if event.correlation.is_none() {
                event.correlation = parent.correlation.clone();
            }

            if event.replay_context.is_none() {
                event.replay_context = parent.replay_context.clone();
            }

            event = event
                .try_with_composite_activations(parent.composite_activations().to_vec())
                .map_err(|error| HandlerError::Validation(error.to_string()))?;

            if event.causality.is_root() {
                let mut causality = CausalityContext::with_parent(parent.id);

                // Propagate ancestors up to depth limit (parent already counts as depth=1).
                let ancestors_to_add = parent
                    .causality
                    .parent_ids
                    .iter()
                    .take(max_depth.saturating_sub(1));

                for ancestor in ancestors_to_add {
                    causality = causality.add_parent(*ancestor);
                }

                event.causality = causality;
            }

            Ok(event)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::join::config::DEFAULT_REFERENCE_BATCH_CAP;
    use obzenflow_core::event::context::CompositeActivationContext;
    use obzenflow_core::event::schema::TypedPayload;
    use obzenflow_core::id::{CompositeId, StageId};
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
            _stream_data: Self::StreamType,
            stream_key: Self::Key,
            writer_id: WriterId,
        ) -> JoinStrategyOutput<Self::Key> {
            let contributing_reference_keys = catalog
                .contains_key(&stream_key)
                .then(|| stream_key.clone())
                .into_iter()
                .collect();
            let joined = JoinedRow {
                catalog_value: catalog.get(&stream_key).map(|c| c.value.clone()),
                stream_key,
            };
            JoinStrategyOutput::new(
                vec![joined.to_event(writer_id)],
                contributing_reference_keys,
            )
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
            reference_mode: JoinReferenceMode::FiniteEof,
            reference_batch_cap: Some(DEFAULT_REFERENCE_BATCH_CAP),
            lineage: obzenflow_core::config::LineagePolicy::default(),
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
        .to_event(writer);

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
        .to_event(writer);
        handler
            .process_event(&mut state, catalog_event, StageId::new(), writer)
            .expect("process_event should succeed while hydrating catalog");

        // enrich
        let stream_event = StreamRow { key: "k1".into() }.to_event(WriterId::from(StageId::new()));
        let out = handler
            .process_event(&mut state, stream_event, StageId::new(), writer)
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
    fn stream_hit_unions_only_the_matching_reference_activation() {
        let handler = make_join();
        let mut state = handler.initial_state();
        let writer = WriterId::from(StageId::new());

        for key in ["k1", "k2"] {
            let reference = CatalogRow {
                key: key.into(),
                value: format!("value-{key}"),
            }
            .to_event(writer);
            let reference_id = reference.id;
            let reference = reference
                .try_with_composite_activations(vec![CompositeActivationContext::new(
                    CompositeId::new("catalog:lookup"),
                    reference_id,
                    key,
                    100,
                )])
                .unwrap();
            handler
                .process_event(&mut state, reference, StageId::new(), writer)
                .unwrap();
        }

        let stream = StreamRow { key: "k1".into() }.to_event(writer);
        let stream_id = stream.id;
        let stream = stream
            .try_with_composite_activations(vec![CompositeActivationContext::new(
                CompositeId::new("saga:checkout"),
                stream_id,
                "commands",
                200,
            )])
            .unwrap();
        let output = handler
            .process_event(&mut state, stream, StageId::new(), writer)
            .unwrap();
        assert_eq!(output.len(), 1);
        let activations = output[0].composite_activations();
        assert_eq!(activations.len(), 2);
        assert!(activations.iter().any(|activation| {
            activation.composite_id == CompositeId::new("catalog:lookup")
                && activation.entry_port == "k1"
        }));
        assert!(!activations
            .iter()
            .any(|activation| activation.entry_port == "k2"));
        assert!(activations.iter().any(|activation| {
            activation.composite_id == CompositeId::new("saga:checkout")
                && activation.entry_port == "commands"
        }));
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
            .process_event(&mut state, stream_event, StageId::new(), writer)
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

        handler
            .on_source_eof(&mut state, StageId::new(), writer)
            .expect("on_source_eof should succeed in on_source_eof_marks_reference_complete");
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
        handler
            .on_source_eof(&mut state, StageId::new(), writer)
            .expect("on_source_eof should succeed while marking reference complete");
        assert!(state.reference_eof_seen);
        assert!(state.reference_complete);

        // second EOF sets stream complete
        handler
            .on_source_eof(&mut state, StageId::new(), writer)
            .expect("on_source_eof should succeed while marking stream complete");
        assert!(state.stream_eof_seen);
        assert!(state.stream_complete);

        // third EOF ignored (no panic/changes)
        handler
            .on_source_eof(&mut state, StageId::new(), writer)
            .expect("on_source_eof should succeed while ignoring duplicate EOF");
    }
}

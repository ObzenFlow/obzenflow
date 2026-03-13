// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI map-reduce composite descriptor (FLOWIP-086z-part-2).
//!
//! This is a DSL-level authoring surface that lowers into ordinary stages:
//! `<binding>__chunk`, `<binding>__map`, `<binding>__collect`, `<binding>__finalize`.

use super::{CompositeLowering, LoweringArtifacts, SubgraphInternalEdgeSpec, TopologySubgraphSpec};
use crate::dsl::stage_descriptor::{
    AsyncTransformDescriptor, StageDescriptor, StatefulDescriptor, TransformDescriptor,
    BINDING_DERIVED_NAME_SENTINEL,
};
use crate::dsl::typing::{
    wrap_typed_descriptor, BoundAsyncTransform, BoundTransform, StageTypingMetadata, TypeHint,
};
use obzenflow_adapters::middleware::ai::map_reduce::{
    AiMapReduceChunkManifestFactory, AiMapReduceMapFactory,
};
use obzenflow_adapters::middleware::MiddlewareFactory;
use obzenflow_core::topology::subgraphs::StageSubgraphMembership;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{AsyncTransformHandler, TransformHandler};
use obzenflow_runtime::stages::stateful::CollectByInput;
use obzenflow_topology::EdgeKind;
use std::any::type_name;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Clone)]
struct DynTransformHandler {
    inner: Arc<dyn TransformHandler>,
    inner_type: &'static str,
}

impl DynTransformHandler {
    fn new<H>(handler: H) -> Self
    where
        H: TransformHandler + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(handler),
            inner_type: type_name::<H>(),
        }
    }
}

impl fmt::Debug for DynTransformHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynTransformHandler")
            .field("inner_type", &self.inner_type)
            .finish()
    }
}

#[async_trait::async_trait]
impl TransformHandler for DynTransformHandler {
    fn process(
        &self,
        event: obzenflow_core::ChainEvent,
    ) -> Result<Vec<obzenflow_core::ChainEvent>, HandlerError> {
        self.inner.process(event)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            HandlerError::Validation("DynTransformHandler: drain requires unique Arc".to_string())
        })?;
        inner.drain().await
    }
}

#[derive(Clone)]
struct DynAsyncTransformHandler {
    inner: Arc<dyn AsyncTransformHandler>,
    inner_type: &'static str,
}

impl DynAsyncTransformHandler {
    fn new<H>(handler: H) -> Self
    where
        H: AsyncTransformHandler + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(handler),
            inner_type: type_name::<H>(),
        }
    }
}

impl fmt::Debug for DynAsyncTransformHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynAsyncTransformHandler")
            .field("inner_type", &self.inner_type)
            .finish()
    }
}

#[async_trait::async_trait]
impl AsyncTransformHandler for DynAsyncTransformHandler {
    async fn process(
        &self,
        event: obzenflow_core::ChainEvent,
    ) -> Result<Vec<obzenflow_core::ChainEvent>, HandlerError> {
        self.inner.process(event).await
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        let inner = Arc::get_mut(&mut self.inner).ok_or_else(|| {
            HandlerError::Validation(
                "DynAsyncTransformHandler: drain requires unique Arc".to_string(),
            )
        })?;
        inner.drain().await
    }
}

pub fn map_reduce<In, Chunk, Partial, Collected, Out>(
) -> AiMapReduceBuilder<In, Chunk, Partial, Collected, Out> {
    AiMapReduceBuilder::new()
}

pub struct AiMapReduceBuilder<In, Chunk, Partial, Collected, Out> {
    chunker: Option<DynTransformHandler>,
    map: Option<DynAsyncTransformHandler>,
    collect: Option<CollectByInput<Partial, Collected>>,
    finalize: Option<DynAsyncTransformHandler>,
    chunk_middleware: Vec<Box<dyn MiddlewareFactory>>,
    map_middleware: Vec<Box<dyn MiddlewareFactory>>,
    collect_middleware: Vec<Box<dyn MiddlewareFactory>>,
    finalize_middleware: Vec<Box<dyn MiddlewareFactory>>,
    _phantom: PhantomData<(In, Chunk, Partial, Collected, Out)>,
}

impl<In, Chunk, Partial, Collected, Out> AiMapReduceBuilder<In, Chunk, Partial, Collected, Out> {
    fn new() -> Self {
        Self {
            chunker: None,
            map: None,
            collect: None,
            finalize: None,
            chunk_middleware: Vec::new(),
            map_middleware: Vec::new(),
            collect_middleware: Vec::new(),
            finalize_middleware: Vec::new(),
            _phantom: PhantomData,
        }
    }

    pub fn chunker<H>(mut self, handler: H) -> Self
    where
        H: TransformHandler + Send + Sync + 'static,
    {
        self.chunker = Some(DynTransformHandler::new(handler));
        self
    }

    pub fn map<H>(mut self, handler: H) -> Self
    where
        H: AsyncTransformHandler + Send + Sync + 'static,
    {
        self.map = Some(DynAsyncTransformHandler::new(handler));
        self
    }

    pub fn collect(mut self, collector: CollectByInput<Partial, Collected>) -> Self {
        self.collect = Some(collector);
        self
    }

    pub fn finalize<H>(mut self, handler: H) -> Self
    where
        H: AsyncTransformHandler + Send + Sync + 'static,
    {
        self.finalize = Some(DynAsyncTransformHandler::new(handler));
        self
    }

    pub fn chunk_middleware<I, M>(mut self, middleware: I) -> Self
    where
        I: IntoIterator<Item = M>,
        M: MiddlewareFactory + 'static,
    {
        self.chunk_middleware.extend(
            middleware
                .into_iter()
                .map(|mw| Box::new(mw) as Box<dyn MiddlewareFactory>),
        );
        self
    }

    pub fn map_middleware<I, M>(mut self, middleware: I) -> Self
    where
        I: IntoIterator<Item = M>,
        M: MiddlewareFactory + 'static,
    {
        self.map_middleware.extend(
            middleware
                .into_iter()
                .map(|mw| Box::new(mw) as Box<dyn MiddlewareFactory>),
        );
        self
    }

    pub fn collect_middleware<I, M>(mut self, middleware: I) -> Self
    where
        I: IntoIterator<Item = M>,
        M: MiddlewareFactory + 'static,
    {
        self.collect_middleware.extend(
            middleware
                .into_iter()
                .map(|mw| Box::new(mw) as Box<dyn MiddlewareFactory>),
        );
        self
    }

    pub fn finalize_middleware<I, M>(mut self, middleware: I) -> Self
    where
        I: IntoIterator<Item = M>,
        M: MiddlewareFactory + 'static,
    {
        self.finalize_middleware.extend(
            middleware
                .into_iter()
                .map(|mw| Box::new(mw) as Box<dyn MiddlewareFactory>),
        );
        self
    }

    pub fn build(self) -> Box<dyn StageDescriptor>
    where
        In: Clone + Send + Sync + 'static,
        Chunk: Clone + TypedPayload + Send + Sync + 'static,
        Partial: TypedPayload + serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
        Collected:
            Clone + serde::Serialize + TypedPayload + std::fmt::Debug + Send + Sync + 'static,
        Out: Clone + Send + Sync + 'static,
    {
        Box::new(
            AiMapReduceCompositeDescriptor::<In, Chunk, Partial, Collected, Out> {
                name: BINDING_DERIVED_NAME_SENTINEL.to_string(),
                chunker: self.chunker.expect("ai::map_reduce: missing chunker(...)"),
                map: self.map.expect("ai::map_reduce: missing map(...)"),
                collect: self.collect.expect("ai::map_reduce: missing collect(...)"),
                finalize: self
                    .finalize
                    .expect("ai::map_reduce: missing finalize(...)"),
                chunk_middleware: self.chunk_middleware,
                map_middleware: self.map_middleware,
                collect_middleware: self.collect_middleware,
                finalize_middleware: self.finalize_middleware,
                _phantom: PhantomData,
            },
        )
    }
}

struct AiMapReduceCompositeDescriptor<In, Chunk, Partial, Collected, Out> {
    name: String,
    chunker: DynTransformHandler,
    map: DynAsyncTransformHandler,
    collect: CollectByInput<Partial, Collected>,
    finalize: DynAsyncTransformHandler,
    chunk_middleware: Vec<Box<dyn MiddlewareFactory>>,
    map_middleware: Vec<Box<dyn MiddlewareFactory>>,
    collect_middleware: Vec<Box<dyn MiddlewareFactory>>,
    finalize_middleware: Vec<Box<dyn MiddlewareFactory>>,
    _phantom: PhantomData<(In, Chunk, Partial, Collected, Out)>,
}

#[async_trait::async_trait]
impl<In, Chunk, Partial, Collected, Out> StageDescriptor
    for AiMapReduceCompositeDescriptor<In, Chunk, Partial, Collected, Out>
where
    In: Clone + Send + Sync + 'static,
    Chunk: Clone + TypedPayload + Send + Sync + 'static,
    Partial: TypedPayload + serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
    Collected: Clone + serde::Serialize + TypedPayload + std::fmt::Debug + Send + Sync + 'static,
    Out: Clone + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn stage_type(&self) -> obzenflow_core::event::context::StageType {
        obzenflow_core::event::context::StageType::Transform
    }

    fn is_composite(&self) -> bool {
        true
    }

    fn try_lower_composite(
        self: Box<Self>,
        binding: &str,
    ) -> Result<Option<CompositeLowering>, crate::dsl::FlowBuildError> {
        let chunk_stage = format!("{binding}__chunk");
        let map_stage = format!("{binding}__map");
        let collect_stage = format!("{binding}__collect");
        let finalize_stage = format!("{binding}__finalize");

        let subgraph_id = format!("ai_map_reduce:{binding}");

        // ---------------------------------------------------------------------
        // Chunk stage: In -> Chunk (+ manifest control event)
        // ---------------------------------------------------------------------
        let mut chunk_middleware = self.chunk_middleware;
        chunk_middleware.push(Box::new(AiMapReduceChunkManifestFactory::<Chunk>::new()));

        let chunk_handler = BoundTransform::<In, Chunk, _>::new(self.chunker);
        let chunk_descriptor = TransformDescriptor {
            name: chunk_stage.clone(),
            handler: chunk_handler,
            middleware: chunk_middleware,
        };
        let chunk_descriptor = wrap_typed_descriptor(
            Box::new(chunk_descriptor),
            StageTypingMetadata::transform(
                TypeHint::exact(type_name::<In>()),
                TypeHint::exact(type_name::<Chunk>()),
                false,
                None,
            ),
        );

        // ---------------------------------------------------------------------
        // Map stage: Chunk -> Partial (+ wrapper to drop manifests, tag partials, emit chunk_failed)
        // ---------------------------------------------------------------------
        let mut map_middleware: Vec<Box<dyn MiddlewareFactory>> = Vec::new();
        map_middleware.push(Box::new(AiMapReduceMapFactory::<Chunk, Partial>::new()));
        map_middleware.extend(self.map_middleware);

        let map_handler = BoundAsyncTransform::<Chunk, Partial, _>::new(self.map);
        let map_descriptor = AsyncTransformDescriptor {
            name: map_stage.clone(),
            handler: map_handler,
            middleware: map_middleware,
        };
        let map_descriptor = wrap_typed_descriptor(
            Box::new(map_descriptor),
            StageTypingMetadata::transform(
                TypeHint::exact(type_name::<Chunk>()),
                TypeHint::exact(type_name::<Partial>()),
                false,
                None,
            ),
        );

        // ---------------------------------------------------------------------
        // Collect stage: Partial -> Collected (but consumes internal manifest + tagged partials)
        // ---------------------------------------------------------------------
        let collect_descriptor = StatefulDescriptor {
            name: collect_stage.clone(),
            handler: self.collect,
            emit_interval: None,
            middleware: self.collect_middleware,
        };
        let collect_descriptor = wrap_typed_descriptor(
            Box::new(collect_descriptor),
            StageTypingMetadata::stateful(
                TypeHint::exact(type_name::<Partial>()),
                TypeHint::exact(type_name::<Collected>()),
                false,
                None,
            ),
        );

        // ---------------------------------------------------------------------
        // Finalise stage: Collected -> Out
        // ---------------------------------------------------------------------
        let finalize_handler = BoundAsyncTransform::<Collected, Out, _>::new(self.finalize);
        let finalize_descriptor = AsyncTransformDescriptor {
            name: finalize_stage.clone(),
            handler: finalize_handler,
            middleware: self.finalize_middleware,
        };
        let finalize_descriptor = wrap_typed_descriptor(
            Box::new(finalize_descriptor),
            StageTypingMetadata::transform(
                TypeHint::exact(type_name::<Collected>()),
                TypeHint::exact(type_name::<Out>()),
                false,
                None,
            ),
        );

        // ---------------------------------------------------------------------
        // Edges + topology metadata
        // ---------------------------------------------------------------------
        let edges = vec![
            (chunk_stage.clone(), map_stage.clone(), EdgeKind::Forward),
            (
                chunk_stage.clone(),
                collect_stage.clone(),
                EdgeKind::Forward,
            ),
            (map_stage.clone(), collect_stage.clone(), EdgeKind::Forward),
            (
                collect_stage.clone(),
                finalize_stage.clone(),
                EdgeKind::Forward,
            ),
        ];

        let mut stage_subgraphs = std::collections::HashMap::new();
        stage_subgraphs.insert(
            chunk_stage.clone(),
            StageSubgraphMembership {
                subgraph_id: subgraph_id.clone(),
                kind: "ai_map_reduce".to_string(),
                binding: binding.to_string(),
                role: "chunk".to_string(),
                order: 0,
                is_entry: true,
                is_exit: false,
            },
        );
        stage_subgraphs.insert(
            map_stage.clone(),
            StageSubgraphMembership {
                subgraph_id: subgraph_id.clone(),
                kind: "ai_map_reduce".to_string(),
                binding: binding.to_string(),
                role: "map".to_string(),
                order: 1,
                is_entry: false,
                is_exit: false,
            },
        );
        stage_subgraphs.insert(
            collect_stage.clone(),
            StageSubgraphMembership {
                subgraph_id: subgraph_id.clone(),
                kind: "ai_map_reduce".to_string(),
                binding: binding.to_string(),
                role: "collect".to_string(),
                order: 2,
                is_entry: false,
                is_exit: false,
            },
        );
        stage_subgraphs.insert(
            finalize_stage.clone(),
            StageSubgraphMembership {
                subgraph_id: subgraph_id.clone(),
                kind: "ai_map_reduce".to_string(),
                binding: binding.to_string(),
                role: "finalize".to_string(),
                order: 3,
                is_entry: false,
                is_exit: true,
            },
        );

        let subgraph = TopologySubgraphSpec {
            subgraph_id: subgraph_id.clone(),
            kind: "ai_map_reduce".to_string(),
            binding: binding.to_string(),
            label: binding.to_string(),
            member_stage_names: vec![
                chunk_stage.clone(),
                map_stage.clone(),
                collect_stage.clone(),
                finalize_stage.clone(),
            ],
            internal_edges: vec![
                SubgraphInternalEdgeSpec {
                    from_stage: chunk_stage.clone(),
                    to_stage: map_stage.clone(),
                    role: "data".to_string(),
                },
                SubgraphInternalEdgeSpec {
                    from_stage: chunk_stage.clone(),
                    to_stage: collect_stage.clone(),
                    role: "manifest".to_string(),
                },
                SubgraphInternalEdgeSpec {
                    from_stage: map_stage.clone(),
                    to_stage: collect_stage.clone(),
                    role: "data".to_string(),
                },
                SubgraphInternalEdgeSpec {
                    from_stage: collect_stage.clone(),
                    to_stage: finalize_stage.clone(),
                    role: "data".to_string(),
                },
            ],
            entry_stage_names: vec![chunk_stage.clone()],
            exit_stage_names: vec![finalize_stage.clone()],
            parent_subgraph_id: None,
            collapsible: true,
        };

        Ok(Some(CompositeLowering {
            stages: vec![
                (chunk_stage, chunk_descriptor),
                (map_stage, map_descriptor),
                (collect_stage, collect_descriptor),
                (finalize_stage, finalize_descriptor),
            ],
            edges,
            entry_stage: format!("{binding}__chunk"),
            exit_stage: format!("{binding}__finalize"),
            artifacts: LoweringArtifacts {
                stage_subgraphs,
                subgraphs: vec![subgraph],
            },
        }))
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        _config: obzenflow_runtime::pipeline::config::StageConfig,
        _resources: obzenflow_runtime::stages::StageResources,
        _flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        _control_middleware: Arc<
            obzenflow_adapters::middleware::control::ControlMiddlewareAggregator,
        >,
    ) -> Result<obzenflow_runtime::stages::common::stage_handle::BoxedStageHandle, String> {
        Err(
            "ai::map_reduce composite descriptors must be lowered during flow! materialisation"
                .to_string(),
        )
    }
}

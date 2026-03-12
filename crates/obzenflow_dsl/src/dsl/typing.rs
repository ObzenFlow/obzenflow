// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Types-first metadata and descriptor wrappers for the DSL layer.

use crate::dsl::stage_descriptor::StageDescriptor;
use async_trait::async_trait;
use obzenflow_adapters::middleware::{control::ControlMiddlewareAggregator, MiddlewareFactory};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::delivery_payload::DeliveryPayload;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, AsyncTransformHandler,
    FiniteSourceHandler, InfiniteSourceHandler, JoinHandler, SinkHandler, StatefulHandler,
    TransformHandler,
};
use obzenflow_runtime::stages::common::stage_handle::BoxedStageHandle;
use obzenflow_runtime::stages::StageResources;
use obzenflow_runtime::typing::{
    JoinTyping, SinkTyping, SourceTyping, StatefulTyping, TransformTyping,
};
use obzenflow_topology::{EdgeKind, Topology};
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

/// Three-way model for declared type positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeHint {
    Unspecified,
    Exact(String),
    Mixed,
}

impl TypeHint {
    pub fn exact(name: impl Into<String>) -> Self {
        Self::Exact(name.into())
    }
}

/// Stage-local typing metadata captured during macro expansion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageTypingMetadata {
    pub input_type: TypeHint,
    pub output_type: TypeHint,
    pub boundary_in_type: TypeHint,
    pub boundary_out_type: TypeHint,
    pub reference_type: TypeHint,
    pub stream_type: TypeHint,
    pub is_placeholder: bool,
    pub placeholder_message: Option<String>,
}

impl StageTypingMetadata {
    pub fn source(output_type: TypeHint, is_placeholder: bool, message: Option<String>) -> Self {
        Self {
            input_type: TypeHint::Unspecified,
            output_type,
            boundary_in_type: TypeHint::Unspecified,
            boundary_out_type: TypeHint::Unspecified,
            reference_type: TypeHint::Unspecified,
            stream_type: TypeHint::Unspecified,
            is_placeholder,
            placeholder_message: message,
        }
    }

    pub fn transform(
        input_type: TypeHint,
        output_type: TypeHint,
        is_placeholder: bool,
        message: Option<String>,
    ) -> Self {
        Self {
            input_type,
            output_type,
            boundary_in_type: TypeHint::Unspecified,
            boundary_out_type: TypeHint::Unspecified,
            reference_type: TypeHint::Unspecified,
            stream_type: TypeHint::Unspecified,
            is_placeholder,
            placeholder_message: message,
        }
    }

    pub fn stateful(
        input_type: TypeHint,
        output_type: TypeHint,
        is_placeholder: bool,
        message: Option<String>,
    ) -> Self {
        Self::transform(input_type, output_type, is_placeholder, message)
    }

    pub fn sink(input_type: TypeHint, is_placeholder: bool, message: Option<String>) -> Self {
        Self {
            input_type,
            output_type: TypeHint::Unspecified,
            boundary_in_type: TypeHint::Unspecified,
            boundary_out_type: TypeHint::Unspecified,
            reference_type: TypeHint::Unspecified,
            stream_type: TypeHint::Unspecified,
            is_placeholder,
            placeholder_message: message,
        }
    }

    pub fn join(
        reference_type: TypeHint,
        stream_type: TypeHint,
        output_type: TypeHint,
        is_placeholder: bool,
        message: Option<String>,
    ) -> Self {
        Self {
            input_type: TypeHint::Unspecified,
            output_type,
            boundary_in_type: TypeHint::Unspecified,
            boundary_out_type: TypeHint::Unspecified,
            reference_type,
            stream_type,
            is_placeholder,
            placeholder_message: message,
        }
    }
}

/// Which downstream input position was compared for an edge hint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeInputRole {
    Input,
    Reference,
    Stream,
}

impl EdgeInputRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Reference => "reference",
            Self::Stream => "stream",
        }
    }
}

/// Structured result from the edge compatibility checker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeWarning {
    pub upstream_stage: String,
    pub downstream_stage: String,
    pub upstream_type: String,
    pub expected_type: String,
    pub input_role: EdgeInputRole,
}

/// Descriptor wrapper that preserves typing metadata through type erasure.
pub struct TypedStageDescriptor {
    inner: Box<dyn StageDescriptor>,
    metadata: StageTypingMetadata,
}

impl TypedStageDescriptor {
    pub fn new(inner: Box<dyn StageDescriptor>, metadata: StageTypingMetadata) -> Self {
        Self { inner, metadata }
    }
}

pub fn wrap_typed_descriptor(
    inner: Box<dyn StageDescriptor>,
    metadata: StageTypingMetadata,
) -> Box<dyn StageDescriptor> {
    Box::new(TypedStageDescriptor::new(inner, metadata))
}

fn placeholder_message(stage_kind: &str, message: Option<&str>) -> String {
    match message {
        Some(message) => format!("{stage_kind} placeholder executed: {message}"),
        None => format!("{stage_kind} placeholder executed"),
    }
}

pub struct PlaceholderFiniteSource<T> {
    _phantom: PhantomData<T>,
    message: Option<&'static str>,
}

impl<T> PlaceholderFiniteSource<T> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
        }
    }
}

impl<T> Clone for PlaceholderFiniteSource<T> {
    fn clone(&self) -> Self {
        Self::new(self.message)
    }
}

impl<T> fmt::Debug for PlaceholderFiniteSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceholderFiniteSource")
            .field("message", &self.message)
            .finish()
    }
}

impl<T> SourceTyping for PlaceholderFiniteSource<T> {
    type Output = T;
}

impl<T> FiniteSourceHandler for PlaceholderFiniteSource<T>
where
    T: Send + Sync + 'static,
{
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        panic!("{}", placeholder_message("finite source", self.message));
    }
}

pub struct PlaceholderAsyncSource<T> {
    _phantom: PhantomData<T>,
    message: Option<&'static str>,
}

impl<T> PlaceholderAsyncSource<T> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
        }
    }
}

impl<T> Clone for PlaceholderAsyncSource<T> {
    fn clone(&self) -> Self {
        Self::new(self.message)
    }
}

impl<T> fmt::Debug for PlaceholderAsyncSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceholderAsyncSource")
            .field("message", &self.message)
            .finish()
    }
}

impl<T> SourceTyping for PlaceholderAsyncSource<T> {
    type Output = T;
}

#[async_trait]
impl<T> AsyncFiniteSourceHandler for PlaceholderAsyncSource<T>
where
    T: Send + Sync + 'static,
{
    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        panic!("{}", placeholder_message("async source", self.message));
    }
}

#[async_trait]
impl<T> AsyncInfiniteSourceHandler for PlaceholderAsyncSource<T>
where
    T: Send + Sync + 'static,
{
    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        panic!(
            "{}",
            placeholder_message("async infinite source", self.message)
        );
    }
}

pub struct PlaceholderInfiniteSource<T> {
    _phantom: PhantomData<T>,
    message: Option<&'static str>,
}

impl<T> PlaceholderInfiniteSource<T> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
        }
    }
}

impl<T> Clone for PlaceholderInfiniteSource<T> {
    fn clone(&self) -> Self {
        Self::new(self.message)
    }
}

impl<T> fmt::Debug for PlaceholderInfiniteSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceholderInfiniteSource")
            .field("message", &self.message)
            .finish()
    }
}

impl<T> SourceTyping for PlaceholderInfiniteSource<T> {
    type Output = T;
}

impl<T> InfiniteSourceHandler for PlaceholderInfiniteSource<T>
where
    T: Send + Sync + 'static,
{
    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        panic!("{}", placeholder_message("infinite source", self.message));
    }
}

pub struct PlaceholderTransform<In, Out> {
    _phantom: PhantomData<(In, Out)>,
    message: Option<&'static str>,
}

impl<In, Out> PlaceholderTransform<In, Out> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
        }
    }
}

impl<In, Out> Clone for PlaceholderTransform<In, Out> {
    fn clone(&self) -> Self {
        Self::new(self.message)
    }
}

impl<In, Out> fmt::Debug for PlaceholderTransform<In, Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceholderTransform")
            .field("message", &self.message)
            .finish()
    }
}

impl<In, Out> TransformTyping for PlaceholderTransform<In, Out> {
    type Input = In;
    type Output = Out;
}

#[async_trait]
impl<In, Out> TransformHandler for PlaceholderTransform<In, Out>
where
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        panic!("{}", placeholder_message("transform", self.message));
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        panic!("{}", placeholder_message("transform", self.message));
    }
}

pub struct PlaceholderAsyncTransform<In, Out> {
    _phantom: PhantomData<(In, Out)>,
    message: Option<&'static str>,
}

impl<In, Out> PlaceholderAsyncTransform<In, Out> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
        }
    }
}

impl<In, Out> Clone for PlaceholderAsyncTransform<In, Out> {
    fn clone(&self) -> Self {
        Self::new(self.message)
    }
}

impl<In, Out> fmt::Debug for PlaceholderAsyncTransform<In, Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceholderAsyncTransform")
            .field("message", &self.message)
            .finish()
    }
}

impl<In, Out> TransformTyping for PlaceholderAsyncTransform<In, Out> {
    type Input = In;
    type Output = Out;
}

#[async_trait]
impl<In, Out> AsyncTransformHandler for PlaceholderAsyncTransform<In, Out>
where
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    async fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        panic!("{}", placeholder_message("async transform", self.message));
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        panic!("{}", placeholder_message("async transform", self.message));
    }
}

pub struct PlaceholderStateful<In, Out> {
    _phantom: PhantomData<(In, Out)>,
    message: Option<&'static str>,
}

impl<In, Out> PlaceholderStateful<In, Out> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
        }
    }
}

impl<In, Out> Clone for PlaceholderStateful<In, Out> {
    fn clone(&self) -> Self {
        Self::new(self.message)
    }
}

impl<In, Out> fmt::Debug for PlaceholderStateful<In, Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceholderStateful")
            .field("message", &self.message)
            .finish()
    }
}

impl<In, Out> StatefulTyping for PlaceholderStateful<In, Out> {
    type Input = In;
    type Output = Out;
}

#[async_trait]
impl<In, Out> StatefulHandler for PlaceholderStateful<In, Out>
where
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    type State = ();

    fn accumulate(&mut self, _state: &mut Self::State, _event: ChainEvent) {
        panic!("{}", placeholder_message("stateful", self.message));
    }

    fn initial_state(&self) -> Self::State {}

    fn create_events(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        panic!("{}", placeholder_message("stateful", self.message));
    }

    async fn drain(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        panic!("{}", placeholder_message("stateful", self.message));
    }
}

pub struct PlaceholderSink<In> {
    _phantom: PhantomData<In>,
    message: Option<&'static str>,
}

impl<In> PlaceholderSink<In> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
        }
    }
}

impl<In> Clone for PlaceholderSink<In> {
    fn clone(&self) -> Self {
        Self::new(self.message)
    }
}

impl<In> fmt::Debug for PlaceholderSink<In> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceholderSink")
            .field("message", &self.message)
            .finish()
    }
}

impl<In> SinkTyping for PlaceholderSink<In> {
    type Input = In;
}

#[async_trait]
impl<In> SinkHandler for PlaceholderSink<In>
where
    In: Send + Sync + 'static,
{
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        panic!("{}", placeholder_message("sink", self.message));
    }
}

pub struct PlaceholderJoin<Ref, Stream, Out> {
    _phantom: PhantomData<(Ref, Stream, Out)>,
    message: Option<&'static str>,
}

impl<Ref, Stream, Out> PlaceholderJoin<Ref, Stream, Out> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
        }
    }
}

impl<Ref, Stream, Out> Clone for PlaceholderJoin<Ref, Stream, Out> {
    fn clone(&self) -> Self {
        Self::new(self.message)
    }
}

impl<Ref, Stream, Out> fmt::Debug for PlaceholderJoin<Ref, Stream, Out> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlaceholderJoin")
            .field("message", &self.message)
            .finish()
    }
}

impl<Ref, Stream, Out> JoinTyping for PlaceholderJoin<Ref, Stream, Out> {
    type Reference = Ref;
    type Stream = Stream;
    type Output = Out;
}

#[async_trait]
impl<Ref, Stream, Out> JoinHandler for PlaceholderJoin<Ref, Stream, Out>
where
    Ref: Send + Sync + 'static,
    Stream: Send + Sync + 'static,
    Out: Send + Sync + 'static,
{
    type State = ();

    fn initial_state(&self) -> Self::State {}

    fn process_event(
        &self,
        _state: &mut Self::State,
        _event: ChainEvent,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        panic!("{}", placeholder_message("join", self.message));
    }

    fn on_source_eof(
        &self,
        _state: &mut Self::State,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        panic!("{}", placeholder_message("join", self.message));
    }

    async fn drain(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        panic!("{}", placeholder_message("join", self.message));
    }
}

#[async_trait]
impl StageDescriptor for TypedStageDescriptor {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn set_name(&mut self, name: String) {
        self.inner.set_name(name);
    }

    fn stage_type(&self) -> StageType {
        self.inner.stage_type()
    }

    fn reference_stage_id(&self) -> Option<StageId> {
        self.inner.reference_stage_id()
    }

    fn reference_stage_name(&self) -> Option<&str> {
        self.inner.reference_stage_name()
    }

    fn set_reference_stage_id(&mut self, id: StageId) {
        self.inner.set_reference_stage_id(id);
    }

    async fn create_handle_with_flow_middleware(
        self: Box<Self>,
        config: StageConfig,
        resources: StageResources,
        flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Result<BoxedStageHandle, String> {
        self.inner
            .create_handle_with_flow_middleware(
                config,
                resources,
                flow_middleware,
                control_middleware,
            )
            .await
    }

    fn stage_middleware_names(&self) -> Vec<String> {
        self.inner.stage_middleware_names()
    }

    fn stage_middleware_factories(&self) -> &[Box<dyn MiddlewareFactory>] {
        self.inner.stage_middleware_factories()
    }

    fn debug_info(&self) -> String {
        self.inner.debug_info()
    }

    fn typing_metadata(&self) -> Option<&StageTypingMetadata> {
        Some(&self.metadata)
    }
}

fn select_downstream_input_hint<'a>(
    upstream_stage_id: StageId,
    downstream_descriptor: &'a dyn StageDescriptor,
    downstream_metadata: &'a StageTypingMetadata,
) -> (EdgeInputRole, &'a TypeHint) {
    if downstream_descriptor.stage_type() == StageType::Join {
        if downstream_descriptor.reference_stage_id() == Some(upstream_stage_id) {
            return (
                EdgeInputRole::Reference,
                &downstream_metadata.reference_type,
            );
        }

        return (EdgeInputRole::Stream, &downstream_metadata.stream_type);
    }

    (EdgeInputRole::Input, &downstream_metadata.input_type)
}

/// Collect non-blocking edge compatibility warnings for typed stages.
pub fn collect_edge_warnings(
    topology: &Topology,
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: &HashMap<String, StageId>,
) -> Vec<EdgeWarning> {
    let mut id_to_descriptor: HashMap<StageId, &dyn StageDescriptor> = HashMap::new();
    for (dsl_name, descriptor) in descriptors {
        if let Some(stage_id) = name_to_id.get(dsl_name) {
            id_to_descriptor.insert(*stage_id, descriptor.as_ref());
        }
    }

    let mut warnings = Vec::new();
    for edge in topology.edges() {
        if edge.kind != EdgeKind::Forward {
            continue;
        }

        let upstream_stage_id = StageId::from_ulid(edge.from.ulid());
        let downstream_stage_id = StageId::from_ulid(edge.to.ulid());

        let Some(upstream_descriptor) = id_to_descriptor.get(&upstream_stage_id) else {
            continue;
        };
        let Some(downstream_descriptor) = id_to_descriptor.get(&downstream_stage_id) else {
            continue;
        };

        let Some(upstream_metadata) = upstream_descriptor.typing_metadata() else {
            continue;
        };
        let Some(downstream_metadata) = downstream_descriptor.typing_metadata() else {
            continue;
        };

        let (input_role, downstream_input_hint) = select_downstream_input_hint(
            upstream_stage_id,
            *downstream_descriptor,
            downstream_metadata,
        );

        match (&upstream_metadata.output_type, downstream_input_hint) {
            (TypeHint::Exact(upstream_type), TypeHint::Exact(expected_type))
                if upstream_type != expected_type =>
            {
                warnings.push(EdgeWarning {
                    upstream_stage: upstream_descriptor.name().to_string(),
                    downstream_stage: downstream_descriptor.name().to_string(),
                    upstream_type: upstream_type.clone(),
                    expected_type: expected_type.clone(),
                    input_role,
                });
            }
            (_, TypeHint::Mixed) | (_, TypeHint::Unspecified) | (TypeHint::Unspecified, _) => {}
            (TypeHint::Mixed, _) => {}
            _ => {}
        }
    }

    warnings
}

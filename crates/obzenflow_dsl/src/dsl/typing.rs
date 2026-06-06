// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Types-first metadata and descriptor wrappers for the DSL layer.

use crate::dsl::stage_descriptor::StageDescriptor;
use crate::dsl::StageCreationResult;
use async_trait::async_trait;
use obzenflow_adapters::middleware::{control::ControlMiddlewareAggregator, MiddlewareFactory};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_runtime::feed_plan::{
    payload_key_from_type_hint, FactVisibility, FeedKey, FeedPlan, FeedRole, LogicalFeed,
    PayloadTypeDescriptor, StageOutputContract,
};
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
use obzenflow_topology::{EdgeKind, StageTypingInfo, Topology, TypeHintInfo};
use std::any::{type_name, TypeId};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Two-way model for declared type positions.
///
/// `Exact` carries a runtime fingerprint of the Rust type (`type_id`) plus a
/// canonical display name for rendering. Identity is by `type_id`; the
/// `display_name` is for error messages and topology rendering only. See
/// FLOWIP-114c "Canonical type identity" for the rationale. The earlier
/// `Mixed` variant was removed in PR D of FLOWIP-114c; the wire-side
/// `TypeHintInfo::Mixed` is kept for snapshot round-tripping but is no longer
/// emitted by the DSL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TypeHint {
    Unspecified,
    Exact {
        type_id: TypeId,
        display_name: String,
    },
}

impl TypeHint {
    /// Construct an `Exact` hint from a Rust type. Identity is by `TypeId`,
    /// which is canonical within one compilation; display name is the
    /// compiler-chosen `std::any::type_name`, which gives identical types
    /// identical rendered names regardless of how the caller spelled them.
    pub fn exact<T: 'static>() -> Self {
        Self::Exact {
            type_id: TypeId::of::<T>(),
            display_name: type_name::<T>().to_string(),
        }
    }
}

impl From<&TypeHint> for TypeHintInfo {
    fn from(value: &TypeHint) -> Self {
        match value {
            TypeHint::Unspecified => Self::Unspecified,
            TypeHint::Exact { display_name, .. } => Self::Exact {
                name: display_name.clone(),
            },
        }
    }
}

/// Stage-local typing metadata captured during macro expansion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StageTypingMetadata {
    pub input_type: TypeHint,
    pub output_type: TypeHint,
    pub output_contract: Vec<TypeHint>,
    pub boundary_in_type: TypeHint,
    pub boundary_out_type: TypeHint,
    pub reference_type: TypeHint,
    pub stream_type: TypeHint,
    pub is_placeholder: bool,
    pub placeholder_message: Option<String>,
}

impl StageTypingMetadata {
    pub fn source(output_type: TypeHint, is_placeholder: bool, message: Option<String>) -> Self {
        let output_contract = output_contract_from_output_type(&output_type);
        Self {
            input_type: TypeHint::Unspecified,
            output_contract,
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
        let output_contract = output_contract_from_output_type(&output_type);
        Self {
            input_type,
            output_contract,
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
            output_contract: Vec::new(),
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
        let output_contract = output_contract_from_output_type(&output_type);
        Self {
            input_type: TypeHint::Unspecified,
            output_contract,
            output_type,
            boundary_in_type: TypeHint::Unspecified,
            boundary_out_type: TypeHint::Unspecified,
            reference_type,
            stream_type,
            is_placeholder,
            placeholder_message: message,
        }
    }

    pub fn with_output_contract(mut self, output_contract: Vec<TypeHint>) -> Self {
        self.output_contract = output_contract;
        self
    }
}

fn output_contract_from_output_type(output_type: &TypeHint) -> Vec<TypeHint> {
    match output_type {
        TypeHint::Unspecified => Vec::new(),
        output_type => vec![output_type.clone()],
    }
}

impl From<&StageTypingMetadata> for StageTypingInfo {
    fn from(value: &StageTypingMetadata) -> Self {
        Self {
            input_type: (&value.input_type).into(),
            output_type: (&value.output_type).into(),
            boundary_in_type: (&value.boundary_in_type).into(),
            boundary_out_type: (&value.boundary_out_type).into(),
            reference_type: (&value.reference_type).into(),
            stream_type: (&value.stream_type).into(),
            is_placeholder: value.is_placeholder,
            placeholder_message: value.placeholder_message.clone(),
        }
    }
}

/// Collect runtime-owned stage typing metadata keyed by core stage ID.
pub fn collect_stage_typing_info(
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: &HashMap<String, StageId>,
) -> HashMap<StageId, StageTypingInfo> {
    let mut stage_typing = HashMap::new();

    for (name, descriptor) in descriptors {
        let Some(stage_id) = name_to_id.get(name).copied() else {
            continue;
        };
        let Some(metadata) = descriptor.typing_metadata() else {
            continue;
        };

        stage_typing.insert(stage_id, metadata.into());
    }

    stage_typing
}

/// Which downstream input position was compared for an edge hint.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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

impl fmt::Display for EdgeInputRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<EdgeInputRole> for FeedRole {
    fn from(value: EdgeInputRole) -> Self {
        match value {
            EdgeInputRole::Input => Self::Input,
            EdgeInputRole::Reference => Self::Reference,
            EdgeInputRole::Stream => Self::Stream,
        }
    }
}

/// Structured result from `validate_edge_typing`. Carries enough context to
/// build a `FlowBuildError::EdgeTypingMismatch`. See FLOWIP-114c.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EdgeError {
    pub upstream_stage: String,
    pub downstream_stage: String,
    pub upstream_type: String,
    pub expected_type: String,
    pub input_role: EdgeInputRole,
    pub kind: crate::dsl::error::EdgeTypingMismatchKind,
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
    warned: Arc<AtomicBool>,
}

impl<T> PlaceholderFiniteSource<T> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
            warned: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<T> Clone for PlaceholderFiniteSource<T> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
            message: self.message,
            warned: Arc::clone(&self.warned),
        }
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
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("finite source", self.message));
        }
        Ok(None)
    }
}

pub struct PlaceholderAsyncSource<T> {
    _phantom: PhantomData<T>,
    message: Option<&'static str>,
    warned: Arc<AtomicBool>,
}

impl<T> PlaceholderAsyncSource<T> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
            warned: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<T> Clone for PlaceholderAsyncSource<T> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
            message: self.message,
            warned: Arc::clone(&self.warned),
        }
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
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("async source", self.message));
        }
        Ok(None)
    }
}

#[async_trait]
impl<T> AsyncInfiniteSourceHandler for PlaceholderAsyncSource<T>
where
    T: Send + Sync + 'static,
{
    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!(
                "{}",
                placeholder_message("async infinite source", self.message)
            );
        }
        Ok(Vec::new())
    }
}

pub struct PlaceholderInfiniteSource<T> {
    _phantom: PhantomData<T>,
    message: Option<&'static str>,
    warned: Arc<AtomicBool>,
}

impl<T> PlaceholderInfiniteSource<T> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
            warned: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<T> Clone for PlaceholderInfiniteSource<T> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
            message: self.message,
            warned: Arc::clone(&self.warned),
        }
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
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("infinite source", self.message));
        }
        Ok(Vec::new())
    }
}

pub struct PlaceholderTransform<In, Out> {
    _phantom: PhantomData<(In, Out)>,
    message: Option<&'static str>,
    warned: Arc<AtomicBool>,
}

impl<In, Out> PlaceholderTransform<In, Out> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
            warned: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<In, Out> Clone for PlaceholderTransform<In, Out> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
            message: self.message,
            warned: Arc::clone(&self.warned),
        }
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
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("transform", self.message));
        }
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("transform", self.message));
        }
        Ok(())
    }
}

pub struct PlaceholderAsyncTransform<In, Out> {
    _phantom: PhantomData<(In, Out)>,
    message: Option<&'static str>,
    warned: Arc<AtomicBool>,
}

impl<In, Out> PlaceholderAsyncTransform<In, Out> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
            warned: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<In, Out> Clone for PlaceholderAsyncTransform<In, Out> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
            message: self.message,
            warned: Arc::clone(&self.warned),
        }
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
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("async transform", self.message));
        }
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("async transform", self.message));
        }
        Ok(())
    }
}

// ============================================================================
// Contract-bound handler wrappers (FLOWIP-086z)
// ============================================================================

/// Wrapper that binds a declared `In -> Out` contract to an arbitrary transform handler.
///
/// The handler only needs to implement [`TransformHandler`]. The typed stage macros use this
/// wrapper internally so application handlers do not have to implement `TransformTyping`.
#[doc(hidden)]
#[derive(Clone)]
pub struct BoundTransform<In, Out, H> {
    inner: H,
    _phantom: PhantomData<(In, Out)>,
}

impl<In, Out, H> BoundTransform<In, Out, H>
where
    In: 'static,
    Out: 'static,
{
    pub fn new(inner: H) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }
}

impl<In, Out, H> fmt::Debug for BoundTransform<In, Out, H>
where
    H: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundTransform")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<In: 'static, Out: 'static, H> TransformTyping for BoundTransform<In, Out, H> {
    type Input = In;
    type Output = Out;
}

#[async_trait]
impl<In, Out, H> TransformHandler for BoundTransform<In, Out, H>
where
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
    H: TransformHandler + Send + Sync,
{
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        self.inner.process(event)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        self.inner.drain().await
    }
}

/// Wrapper that binds a declared `In -> Out` contract to an arbitrary async transform handler.
#[doc(hidden)]
#[derive(Clone)]
pub struct BoundAsyncTransform<In, Out, H> {
    inner: H,
    _phantom: PhantomData<(In, Out)>,
}

impl<In, Out, H> BoundAsyncTransform<In, Out, H>
where
    In: 'static,
    Out: 'static,
{
    pub fn new(inner: H) -> Self {
        Self {
            inner,
            _phantom: PhantomData,
        }
    }
}

impl<In, Out, H> fmt::Debug for BoundAsyncTransform<In, Out, H>
where
    H: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundAsyncTransform")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<In: 'static, Out: 'static, H> TransformTyping for BoundAsyncTransform<In, Out, H> {
    type Input = In;
    type Output = Out;
}

#[async_trait]
impl<In, Out, H> AsyncTransformHandler for BoundAsyncTransform<In, Out, H>
where
    In: Send + Sync + 'static,
    Out: Send + Sync + 'static,
    H: AsyncTransformHandler + Send + Sync,
{
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        self.inner.process(event).await
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        self.inner.drain().await
    }
}

pub struct PlaceholderStateful<In, Out> {
    _phantom: PhantomData<(In, Out)>,
    message: Option<&'static str>,
    warned: Arc<AtomicBool>,
}

impl<In, Out> PlaceholderStateful<In, Out> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
            warned: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<In, Out> Clone for PlaceholderStateful<In, Out> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
            message: self.message,
            warned: Arc::clone(&self.warned),
        }
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
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("stateful", self.message));
        }
    }

    fn initial_state(&self) -> Self::State {}

    fn create_events(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("stateful", self.message));
        }
        Ok(Vec::new())
    }
}

pub struct PlaceholderSink<In> {
    _phantom: PhantomData<In>,
    message: Option<&'static str>,
    warned: Arc<AtomicBool>,
}

impl<In> PlaceholderSink<In> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
            warned: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<In> Clone for PlaceholderSink<In> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
            message: self.message,
            warned: Arc::clone(&self.warned),
        }
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
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("sink", self.message));
        }
        Ok(DeliveryPayload::success(
            "placeholder",
            DeliveryMethod::Noop,
            None,
        ))
    }

    async fn flush(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("sink", self.message));
        }
        Ok(None)
    }
}

pub struct PlaceholderJoin<Ref, Stream, Out> {
    _phantom: PhantomData<(Ref, Stream, Out)>,
    message: Option<&'static str>,
    warned: Arc<AtomicBool>,
}

impl<Ref, Stream, Out> PlaceholderJoin<Ref, Stream, Out> {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            _phantom: PhantomData,
            message,
            warned: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl<Ref, Stream, Out> Clone for PlaceholderJoin<Ref, Stream, Out> {
    fn clone(&self) -> Self {
        Self {
            _phantom: PhantomData,
            message: self.message,
            warned: Arc::clone(&self.warned),
        }
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
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("join", self.message));
        }
        Ok(Vec::new())
    }

    fn on_source_eof(
        &self,
        _state: &mut Self::State,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("join", self.message));
        }
        Ok(Vec::new())
    }

    async fn drain(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        if !self.warned.swap(true, Ordering::Relaxed) {
            tracing::warn!("{}", placeholder_message("join", self.message));
        }
        Ok(Vec::new())
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

    fn is_composite(&self) -> bool {
        self.inner.is_composite()
    }

    fn try_lower_composite(
        self: Box<Self>,
        binding: &str,
    ) -> Result<Option<crate::dsl::composites::CompositeLowering>, crate::dsl::FlowBuildError> {
        let TypedStageDescriptor { inner, metadata: _ } = *self;
        inner.try_lower_composite(binding)
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
    ) -> StageCreationResult<BoxedStageHandle> {
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

    fn is_effectful(&self) -> bool {
        self.inner.is_effectful()
    }

    fn is_deterministic_input_orderer(&self) -> bool {
        self.inner.is_deterministic_input_orderer()
    }

    fn stage_logic_version(&self) -> String {
        self.inner.stage_logic_version()
    }

    fn effect_declarations(&self) -> Vec<obzenflow_runtime::effects::EffectDeclaration> {
        self.inner.effect_declarations()
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

/// Derive the runtime-owned feed plan from DSL descriptors and forward edges.
///
/// This is the authoritative runtime projection for FLOWIP-120b. It deliberately
/// does not read `obzenflow-topology` edge typing annotations; those are
/// render-only metadata for clients.
pub fn derive_feed_plan(
    topology: &Topology,
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: &HashMap<String, StageId>,
) -> FeedPlan {
    let mut id_to_descriptor: HashMap<StageId, &dyn StageDescriptor> = HashMap::new();
    let mut stage_output_contracts: HashMap<StageId, StageOutputContract> = HashMap::new();

    for (dsl_name, descriptor) in descriptors {
        let Some(stage_id) = name_to_id.get(dsl_name).copied() else {
            continue;
        };
        id_to_descriptor.insert(stage_id, descriptor.as_ref());

        let Some(metadata) = descriptor.typing_metadata() else {
            continue;
        };
        stage_output_contracts.insert(stage_id, output_contract_from_metadata(metadata));
    }

    let mut feeds = Vec::new();
    for edge in topology.edges() {
        if edge.kind != EdgeKind::Forward {
            continue;
        }

        let upstream_stage_id = StageId::from_ulid(edge.from.ulid());
        let downstream_stage_id = StageId::from_ulid(edge.to.ulid());

        let Some(upstream_descriptor) = id_to_descriptor.get(&upstream_stage_id) else {
            continue;
        };
        if upstream_descriptor.typing_metadata().is_none() {
            continue;
        }

        let Some(downstream_descriptor) = id_to_descriptor.get(&downstream_stage_id) else {
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
        if matches!(downstream_input_hint, TypeHint::Unspecified) {
            continue;
        }

        let selected_type_hint = TypeHintInfo::from(downstream_input_hint);
        let selected_payload_key = payload_key_from_type_hint(&selected_type_hint);
        let selected_payload =
            PayloadTypeDescriptor::from_type_hint(selected_type_hint, FactVisibility::Routable);

        feeds.push(LogicalFeed {
            key: FeedKey::new(
                upstream_stage_id,
                downstream_stage_id,
                selected_payload_key,
                FeedRole::from(input_role),
            ),
            selected_payload,
        });
    }

    FeedPlan::new(stage_output_contracts, feeds)
}

fn output_contract_from_metadata(metadata: &StageTypingMetadata) -> StageOutputContract {
    let outputs: Vec<PayloadTypeDescriptor> = metadata
        .output_contract
        .iter()
        .filter(|output_type| !matches!(output_type, TypeHint::Unspecified))
        .map(|output_type| {
            PayloadTypeDescriptor::from_type_hint(
                TypeHintInfo::from(output_type),
                FactVisibility::Unrouted,
            )
        })
        .collect();

    if outputs.is_empty() {
        StageOutputContract::empty()
    } else {
        StageOutputContract { outputs }
    }
}

fn exact_output_contract_members(metadata: &StageTypingMetadata) -> Vec<(TypeId, String)> {
    metadata
        .output_contract
        .iter()
        .filter_map(|output_type| match output_type {
            TypeHint::Exact {
                type_id,
                display_name,
            } => Some((*type_id, display_name.clone())),
            TypeHint::Unspecified => None,
        })
        .collect()
}

fn output_contract_display(metadata: &StageTypingMetadata) -> String {
    let members = exact_output_contract_members(metadata);
    if members.is_empty() {
        return "unspecified".to_string();
    }

    members
        .into_iter()
        .map(|(_, display_name)| display_name)
        .collect::<Vec<_>>()
        .join(" | ")
}

fn selected_or_single_output_for_fan_in(
    metadata: &StageTypingMetadata,
    downstream_input_hint: &TypeHint,
) -> Option<(TypeId, String)> {
    let members = exact_output_contract_members(metadata);

    if let TypeHint::Exact {
        type_id: expected_id,
        display_name: expected_name,
    } = downstream_input_hint
    {
        if members.iter().any(|(type_id, _)| type_id == expected_id) {
            return Some((*expected_id, expected_name.clone()));
        }
    }

    if members.len() == 1 {
        return members.into_iter().next();
    }

    None
}

/// FLOWIP-114c per-stage typing-metadata check. Returns an error if any
/// descriptor lacks `typing_metadata()` or carries `Unspecified` on a slot
/// that is applicable for the descriptor's stage role.
///
/// Applicable slots by role:
/// - Source (any kind): `output_type`
/// - Transform / AsyncTransform / Stateful: `input_type`, `output_type`
/// - Sink: `input_type`
/// - Join: `reference_type`, `stream_type`, `output_type`
#[allow(clippy::result_large_err)]
pub fn validate_stage_typing_metadata(
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
) -> Result<(), crate::dsl::error::FlowBuildError> {
    use crate::dsl::error::FlowBuildError;

    for (name, descriptor) in descriptors {
        let Some(meta) = descriptor.typing_metadata() else {
            return Err(FlowBuildError::StageMissingTypingMetadata {
                stage_name: name.clone(),
            });
        };

        let stage_type = descriptor.stage_type();
        let unspecified = |hint: &TypeHint| matches!(hint, TypeHint::Unspecified);
        let invalid_output_contract =
            || meta.output_contract.is_empty() || meta.output_contract.iter().any(unspecified);

        match stage_type {
            StageType::FiniteSource | StageType::InfiniteSource => {
                if unspecified(&meta.output_type) || invalid_output_contract() {
                    return Err(FlowBuildError::UnspecifiedTypingOnApplicableSlot {
                        stage_name: name.clone(),
                        slot: "output".to_string(),
                    });
                }
            }
            StageType::Transform | StageType::Stateful => {
                if unspecified(&meta.input_type) {
                    return Err(FlowBuildError::UnspecifiedTypingOnApplicableSlot {
                        stage_name: name.clone(),
                        slot: "input".to_string(),
                    });
                }
                if unspecified(&meta.output_type) || invalid_output_contract() {
                    return Err(FlowBuildError::UnspecifiedTypingOnApplicableSlot {
                        stage_name: name.clone(),
                        slot: "output".to_string(),
                    });
                }
            }
            StageType::Sink => {
                if unspecified(&meta.input_type) {
                    return Err(FlowBuildError::UnspecifiedTypingOnApplicableSlot {
                        stage_name: name.clone(),
                        slot: "input".to_string(),
                    });
                }
            }
            StageType::Join => {
                if unspecified(&meta.reference_type) {
                    return Err(FlowBuildError::UnspecifiedTypingOnApplicableSlot {
                        stage_name: name.clone(),
                        slot: "reference".to_string(),
                    });
                }
                if unspecified(&meta.stream_type) {
                    return Err(FlowBuildError::UnspecifiedTypingOnApplicableSlot {
                        stage_name: name.clone(),
                        slot: "stream".to_string(),
                    });
                }
                if unspecified(&meta.output_type) || invalid_output_contract() {
                    return Err(FlowBuildError::UnspecifiedTypingOnApplicableSlot {
                        stage_name: name.clone(),
                        slot: "output".to_string(),
                    });
                }
            }
        }
    }

    Ok(())
}

/// Validate edge typing across forward edges in a built topology.
///
/// Returns `Ok(())` when every edge is compatible, or `Err(errors)` listing
/// every per-edge and per-slot mismatch. Composite-internal edges (both
/// endpoints sharing a `StageSubgraphMembership.subgraph_id`) are skipped:
/// the composite outer-boundary invariant is responsible for those, not the
/// per-edge validator. See FLOWIP-114c "Composite-internal edge handling."
///
/// Two passes:
/// - Per-edge `Exact` mismatch produces `EdgeTypingMismatchKind::SingleEdge`
///   when one upstream's `Exact` type disagrees with the downstream slot's
///   declared `Exact` type.
/// - Per-(downstream, slot) fan-in grouping produces
///   `EdgeTypingMismatchKind::HeterogeneousFanIn` when two or more upstreams
///   feeding the same slot emit different `Exact` types. Each leg of a join
///   is checked independently per Acceptance #4.
pub fn validate_edge_typing(
    topology: &Topology,
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: &HashMap<String, StageId>,
) -> Result<(), Vec<EdgeError>> {
    use crate::dsl::error::EdgeTypingMismatchKind;

    let mut id_to_descriptor: HashMap<StageId, &dyn StageDescriptor> = HashMap::new();
    for (dsl_name, descriptor) in descriptors {
        if let Some(stage_id) = name_to_id.get(dsl_name) {
            id_to_descriptor.insert(*stage_id, descriptor.as_ref());
        }
    }

    // Per-(downstream_id, role) fan-in collector. Holds (upstream_stage_name,
    // type_id, display_name, downstream_stage_name) for every Exact-typed
    // inbound edge.
    type IngressKey = (StageId, EdgeInputRole);
    let mut fan_in: HashMap<IngressKey, Vec<(String, std::any::TypeId, String, String)>> =
        HashMap::new();

    // SingleEdge errors collected during the per-edge pass, keyed by
    // (downstream_id, role) so the final-merge step can drop the ones subsumed
    // by a HeterogeneousFanIn on the same slot. FLOWIP-114c PR closing:
    // surfacing the richer kind matters more than reporting every individual
    // upstream that disagrees with the downstream contract on a heterogeneous
    // group.
    let mut single_edge_errors: HashMap<IngressKey, Vec<EdgeError>> = HashMap::new();

    for edge in topology.edges() {
        if edge.kind != EdgeKind::Forward {
            continue;
        }

        let upstream_stage_id = StageId::from_ulid(edge.from.ulid());
        let downstream_stage_id = StageId::from_ulid(edge.to.ulid());

        // Composite-internal-edge skip: if both endpoints belong to the same
        // composite expansion (same `subgraph_id`), the edge is internal to a
        // composite that was wired by composite-author code under composite
        // unit tests. Per-edge validation does not apply.
        let upstream_subgraph = topology
            .stage_info(edge.from)
            .and_then(|info| info.subgraph.as_ref().map(|m| m.subgraph_id.clone()));
        let downstream_subgraph = topology
            .stage_info(edge.to)
            .and_then(|info| info.subgraph.as_ref().map(|m| m.subgraph_id.clone()));
        if let (Some(u_sg), Some(d_sg)) = (&upstream_subgraph, &downstream_subgraph) {
            if u_sg == d_sg {
                continue;
            }
        }

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

        // (1) Per-edge SingleEdge mismatch. The downstream-selected type must
        // be a member of the upstream output contract.
        if let TypeHint::Exact {
            type_id: expected_id,
            display_name: expected_name,
        } = downstream_input_hint
        {
            let upstream_members = exact_output_contract_members(upstream_metadata);
            if !upstream_members
                .iter()
                .any(|(upstream_id, _)| upstream_id == expected_id)
            {
                single_edge_errors
                    .entry((downstream_stage_id, input_role))
                    .or_default()
                    .push(EdgeError {
                        upstream_stage: upstream_descriptor.name().to_string(),
                        downstream_stage: downstream_descriptor.name().to_string(),
                        upstream_type: output_contract_display(upstream_metadata),
                        expected_type: expected_name.clone(),
                        input_role,
                        kind: EdgeTypingMismatchKind::SingleEdge,
                    });
            }
        }

        // (2) Collect for fan-in grouping.
        if let Some((u_id, u_name)) =
            selected_or_single_output_for_fan_in(upstream_metadata, downstream_input_hint)
        {
            fan_in
                .entry((downstream_stage_id, input_role))
                .or_default()
                .push((
                    upstream_descriptor.name().to_string(),
                    u_id,
                    u_name,
                    downstream_descriptor.name().to_string(),
                ));
        }
    }

    // Per-slot HeterogeneousFanIn: when two or more upstreams in the same
    // (downstream, role) group emit different Exact types, emit one error per
    // offending slot. The error names the alphabetically-first upstream as the
    // focal point and lists every other upstream + actual type via `other_*`.
    // The sort is on upstream stage name so the focal element is stable across
    // changes to topology edge insertion order; without it, the regression
    // tests in Acceptance #25 would be flaky.
    let mut hetero_slots: Vec<IngressKey> = Vec::new();
    let mut hetero_errors: Vec<EdgeError> = Vec::new();
    let mut fan_in_keys: Vec<IngressKey> = fan_in.keys().copied().collect();
    fan_in_keys.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.as_str().cmp(b.1.as_str())));

    for key in fan_in_keys {
        let entries = fan_in.get_mut(&key).expect("key from keys iteration");
        if entries.len() < 2 {
            continue;
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let first_type_id = entries[0].1;
        let all_same = entries.iter().all(|(_, tid, _, _)| *tid == first_type_id);
        if all_same {
            continue;
        }

        let (focal_upstream, _focal_type_id, focal_display, downstream_stage_name) = &entries[0];
        let other_upstream_stages: Vec<String> = entries[1..]
            .iter()
            .map(|(name, _, _, _)| name.clone())
            .collect();
        let other_actual_types: Vec<String> = entries[1..]
            .iter()
            .map(|(_, _, dn, _)| dn.clone())
            .collect();
        // The "expected" for a HeterogeneousFanIn slot is the focal upstream's
        // type, which makes the error message read "the other upstreams should
        // align to me". The author resolves by aligning all branches to one
        // common envelope type.
        hetero_slots.push(key);
        hetero_errors.push(EdgeError {
            upstream_stage: focal_upstream.clone(),
            downstream_stage: downstream_stage_name.clone(),
            upstream_type: focal_display.clone(),
            expected_type: focal_display.clone(),
            input_role: key.1,
            kind: EdgeTypingMismatchKind::HeterogeneousFanIn {
                other_upstream_stages,
                other_actual_types,
            },
        });
    }

    // Final assembly: HeterogeneousFanIn first (the architecturally-meaningful
    // diagnosis), then SingleEdge errors that are NOT subsumed by a hetero
    // group on the same (downstream, role) slot. `build_typed_flow!` surfaces
    // the first error from the returned vector as the structured
    // `FlowBuildError::EdgeTypingMismatch`, so the ordering chosen here
    // determines which kind reaches the user.
    let mut errors: Vec<EdgeError> = hetero_errors;
    let mut surviving_single_keys: Vec<IngressKey> = single_edge_errors.keys().copied().collect();
    surviving_single_keys
        .sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.as_str().cmp(b.1.as_str())));
    for key in surviving_single_keys {
        if hetero_slots.contains(&key) {
            continue;
        }
        let mut bucket = single_edge_errors.remove(&key).unwrap_or_default();
        bucket.sort_by(|a, b| a.upstream_stage.cmp(&b.upstream_stage));
        errors.extend(bucket);
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

pub fn validate_effectful_deterministic_input_order(
    topology: &Topology,
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: &HashMap<String, StageId>,
) -> Result<(), Box<crate::dsl::FlowBuildError>> {
    let mut id_to_descriptor: HashMap<StageId, &dyn StageDescriptor> = HashMap::new();
    let mut id_to_name: HashMap<StageId, String> = HashMap::new();
    for (dsl_name, descriptor) in descriptors {
        if let Some(stage_id) = name_to_id.get(dsl_name) {
            id_to_descriptor.insert(*stage_id, descriptor.as_ref());
            id_to_name.insert(*stage_id, descriptor.name().to_string());
        }
    }

    let mut inbound: HashMap<StageId, Vec<StageId>> = HashMap::new();
    for edge in topology.edges() {
        if edge.kind != EdgeKind::Forward {
            continue;
        }
        inbound
            .entry(StageId::from_ulid(edge.to.ulid()))
            .or_default()
            .push(StageId::from_ulid(edge.from.ulid()));
    }

    fn deterministic_for_stage(
        stage_id: StageId,
        inbound: &HashMap<StageId, Vec<StageId>>,
        descriptors: &HashMap<StageId, &dyn StageDescriptor>,
        memo: &mut HashMap<StageId, bool>,
        visiting: &mut HashSet<StageId>,
    ) -> bool {
        if let Some(value) = memo.get(&stage_id) {
            return *value;
        }
        if !visiting.insert(stage_id) {
            memo.insert(stage_id, false);
            return false;
        }

        let upstreams = inbound.get(&stage_id).cloned().unwrap_or_default();
        let deterministic = if upstreams.is_empty() {
            true
        } else if upstreams.len() > 1 {
            descriptors
                .get(&stage_id)
                .map(|descriptor| descriptor.is_deterministic_input_orderer())
                .unwrap_or(false)
        } else {
            deterministic_for_stage(upstreams[0], inbound, descriptors, memo, visiting)
        };

        visiting.remove(&stage_id);
        memo.insert(stage_id, deterministic);
        deterministic
    }

    let mut memo = HashMap::new();
    let mut stage_ids: Vec<StageId> = id_to_descriptor.keys().copied().collect();
    stage_ids.sort();

    for stage_id in stage_ids {
        let Some(descriptor) = id_to_descriptor.get(&stage_id) else {
            continue;
        };
        if !descriptor.is_effectful() {
            continue;
        }

        let mut visiting = HashSet::new();
        if !deterministic_for_stage(
            stage_id,
            &inbound,
            &id_to_descriptor,
            &mut memo,
            &mut visiting,
        ) {
            return Err(Box::new(
                crate::dsl::FlowBuildError::EffectfulFanInRequiresDeterministicOrder {
                    stage_name: id_to_name
                        .get(&stage_id)
                        .cloned()
                        .unwrap_or_else(|| descriptor.name().to_string()),
                },
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod placeholder_warn_once_tests {
    use super::*;

    #[test]
    fn placeholder_stateful_clone_shares_warned_flag() {
        let handler = PlaceholderStateful::<u8, u16>::new(None);
        let cloned = handler.clone();

        assert!(Arc::ptr_eq(&handler.warned, &cloned.warned));
        assert!(!handler.warned.load(Ordering::Relaxed));

        assert!(!cloned.warned.swap(true, Ordering::Relaxed));
        assert!(handler.warned.load(Ordering::Relaxed));
        assert!(cloned.warned.swap(true, Ordering::Relaxed));
    }
}

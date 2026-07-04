// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Types-first metadata and descriptor wrappers for the DSL layer.

use crate::dsl::stage_descriptor::StageDescriptor;
use crate::dsl::StageCreationResult;
use async_trait::async_trait;
use obzenflow_adapters::middleware::{control::ControlMiddlewareAggregator, MiddlewareFactory};
use obzenflow_core::ai::{
    AiMapReduceChunkFailed, AiMapReducePlanningManifest, AiMapReduceTaggedPartial,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::TypedPayload;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_runtime::effects::SynthesizedOutcomeKind;
use obzenflow_runtime::feed_plan::{
    FactVisibility, FeedKey, FeedPlan, FeedRole, LogicalFeed, PayloadTypeDescriptor,
    StageOutputContract,
};
use obzenflow_runtime::id_conversions::StageIdExt;
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
        event_type: Option<String>,
        schema_version: Option<u32>,
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
            event_type: None,
            schema_version: None,
        }
    }

    pub fn exact_payload<T: TypedPayload + 'static>() -> Self {
        Self::Exact {
            type_id: TypeId::of::<T>(),
            display_name: type_name::<T>().to_string(),
            event_type: Some(T::versioned_event_type()),
            schema_version: Some(T::SCHEMA_VERSION),
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
    /// Legacy scalar output projection retained for the published
    /// `obzenflow-topology` 0.4 render model and scalar handler trait anchors.
    ///
    /// Runtime routing and edge validation must use `output_contract`, not this
    /// field. For `In -> { A, B, C }`, no member is semantically privileged.
    pub output_type: TypeHint,
    /// Unordered semantic set of fact types this stage may author. Stored as a
    /// Vec only to keep descriptor metadata simple and deterministic in tests.
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

    pub fn with_additional_output_contract(mut self, additional_members: Vec<TypeHint>) -> Self {
        let mut output_contract = if self.output_contract.is_empty() {
            output_contract_from_output_type(&self.output_type)
        } else {
            self.output_contract.clone()
        };
        for member in additional_members {
            if matches!(member, TypeHint::Unspecified) || output_contract.contains(&member) {
                continue;
            }
            output_contract.push(member);
        }
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

fn output_type_projection_for_topology(value: &StageTypingMetadata) -> TypeHintInfo {
    let mut members = value
        .output_contract
        .iter()
        .filter(|member| !matches!(member, TypeHint::Unspecified));
    let Some(first) = members.next() else {
        return (&value.output_type).into();
    };

    if members.next().is_some() {
        TypeHintInfo::Mixed
    } else {
        first.into()
    }
}

impl From<&StageTypingMetadata> for StageTypingInfo {
    fn from(value: &StageTypingMetadata) -> Self {
        Self {
            input_type: (&value.input_type).into(),
            output_type: output_type_projection_for_topology(value),
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
    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        self.inner.install_lineage_policy(policy)
    }

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
    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        self.inner.install_lineage_policy(policy)
    }

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
        Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
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

    fn backpressure_clause(
        &self,
    ) -> Option<&crate::dsl::backpressure_clause::BackpressureClause> {
        self.inner.backpressure_clause()
    }

    fn set_name(&mut self, name: String) {
        self.inner.set_name(name);
    }

    fn stage_type(&self) -> StageType {
        self.inner.stage_type()
    }

    fn policy_guard_surface(&self) -> crate::dsl::stage_descriptor::PolicyGuardSurface {
        self.inner.policy_guard_surface()
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

    fn sink_delivery_safety(&self) -> Option<obzenflow_runtime::effects::SinkDeliverySafety> {
        self.inner.sink_delivery_safety()
    }

    fn sink_delivery_type(&self) -> Option<&'static str> {
        self.inner.sink_delivery_type()
    }

    fn sink_canonical_destination(&self) -> Option<serde_json::Value> {
        self.inner.sink_canonical_destination()
    }

    fn effect_declarations(&self) -> Vec<obzenflow_runtime::effects::EffectDeclaration> {
        self.inner.effect_declarations()
    }

    fn synthesized_outcome_registrations(
        &self,
    ) -> Vec<obzenflow_runtime::effects::SynthesizedOutcomeRegistration> {
        self.inner.synthesized_outcome_registrations()
    }

    fn type_shaping_config_errors(&self) -> Vec<String> {
        self.inner.type_shaping_config_errors()
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

fn ai_map_reduce_internal_transport_feeds(
    topology: &Topology,
    edge: &obzenflow_topology::DirectedEdge,
) -> Option<Vec<TypeHint>> {
    let upstream = topology.stage_info(edge.from)?.subgraph.as_ref()?;
    let downstream = topology.stage_info(edge.to)?.subgraph.as_ref()?;

    if upstream.subgraph_id != downstream.subgraph_id
        || upstream.kind != "ai_map_reduce"
        || downstream.kind != "ai_map_reduce"
        || downstream.role != "collect"
    {
        return None;
    }

    match upstream.role.as_str() {
        "chunk" => Some(vec![
            TypeHint::exact_payload::<AiMapReducePlanningManifest>(),
        ]),
        "map" => Some(vec![
            TypeHint::exact_payload::<AiMapReduceTaggedPartial<serde_json::Value>>(),
            TypeHint::exact_payload::<AiMapReduceChunkFailed>(),
        ]),
        _ => None,
    }
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

        if let Some(transport_feeds) = ai_map_reduce_internal_transport_feeds(topology, edge) {
            for selected_hint in transport_feeds {
                if matches!(selected_hint, TypeHint::Unspecified) {
                    continue;
                }

                let selected_payload =
                    payload_descriptor_from_type_hint(&selected_hint, FactVisibility::Routable);
                let selected_payload_key = selected_payload.payload_key();

                feeds.push(LogicalFeed {
                    key: FeedKey::new(
                        upstream_stage_id,
                        downstream_stage_id,
                        selected_payload_key,
                        FeedRole::Input,
                    ),
                    selected_payload,
                });
            }

            continue;
        }

        let (input_role, downstream_input_hint) = select_downstream_input_hint(
            upstream_stage_id,
            *downstream_descriptor,
            downstream_metadata,
        );
        if matches!(downstream_input_hint, TypeHint::Unspecified) {
            continue;
        }

        let selected_payload =
            payload_descriptor_from_type_hint(downstream_input_hint, FactVisibility::Routable);
        let selected_payload_key = selected_payload.payload_key();

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
        .map(|output_type| payload_descriptor_from_type_hint(output_type, FactVisibility::Unrouted))
        .collect();

    if outputs.is_empty() {
        StageOutputContract::empty()
    } else {
        StageOutputContract { outputs }
    }
}

fn payload_descriptor_from_type_hint(
    output_type: &TypeHint,
    visibility: FactVisibility,
) -> PayloadTypeDescriptor {
    let mut descriptor =
        PayloadTypeDescriptor::from_type_hint(TypeHintInfo::from(output_type), visibility);
    if let TypeHint::Exact {
        event_type,
        schema_version,
        ..
    } = output_type
    {
        descriptor.event_type = event_type.clone();
        descriptor.schema_version = *schema_version;
    }
    descriptor
}

fn exact_output_contract_members(metadata: &StageTypingMetadata) -> Vec<(TypeId, String)> {
    metadata
        .output_contract
        .iter()
        .filter_map(|output_type| match output_type {
            TypeHint::Exact {
                type_id,
                display_name,
                ..
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
        ..
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

/// FLOWIP-120h: reject middleware that silently drops events at the effect
/// boundary (e.g. a circuit breaker configured with `OpenPolicy::Skip`) on any
/// effectful stage. The handler awaits a value from `fx.perform`, so transport
/// truncation has no coherent boundary behaviour; without this check the
/// rejection surfaces only at runtime, while the breaker is open.
#[allow(clippy::result_large_err)]
pub fn validate_effectful_middleware_compatibility(
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    flow_middleware: &[Box<dyn MiddlewareFactory>],
) -> Result<(), crate::dsl::error::FlowBuildError> {
    use crate::dsl::error::FlowBuildError;

    for (name, descriptor) in descriptors {
        if !descriptor.is_effectful() {
            continue;
        }
        let resolved = crate::middleware_resolution::resolve_middleware_view(
            flow_middleware,
            descriptor.stage_middleware_factories(),
            name,
        )
        .map_err(|e| FlowBuildError::PipelineBuildFailed(e.to_string()))?;
        for factory in resolved {
            if factory.hints().effect_boundary_truncates {
                return Err(
                    FlowBuildError::MiddlewarePolicyIncompatibleWithEffectfulStage {
                        stage_name: name.clone(),
                        middleware_label: factory.label().to_string(),
                        policy: "OpenPolicy::Skip".to_string(),
                    },
                );
            }
        }
    }
    Ok(())
}

/// FLOWIP-120h: validate `output_middleware:` lane contributions at build time.
///
/// Every branch fact a type-shaping middleware may author must be a member of
/// the arrow contract (corrected Option A: membership is enforced, never
/// inferred), must not collide with the guarded effect's own fact set (the
/// event type is the branch discriminator), and typed-outcome middleware is
/// limited to single-effect stages until FLOWIP-120c locks per-effect scope.
/// Producer-side effect-fact containment lives in
/// [`validate_effect_fact_containment`], which runs unconditionally
/// (FLOWIP-120m); this validator owns only the registration-gated checks.
#[allow(clippy::result_large_err)]
pub fn validate_type_shaping_contributions(
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
) -> Result<(), crate::dsl::error::FlowBuildError> {
    use crate::dsl::error::FlowBuildError;

    for (name, descriptor) in descriptors {
        if let Some(error) = descriptor.type_shaping_config_errors().first() {
            return Err(FlowBuildError::TypeShapingConfiguration {
                stage_name: name.clone(),
                message: error.clone(),
            });
        }

        let registrations = descriptor.synthesized_outcome_registrations();
        if registrations.is_empty() {
            continue;
        }

        let declarations = descriptor.effect_declarations();

        let Some(metadata) = descriptor.typing_metadata() else {
            return Err(FlowBuildError::StageMissingTypingMetadata {
                stage_name: name.clone(),
            });
        };
        let contract_type_ids: HashSet<TypeId> = exact_output_contract_members(metadata)
            .into_iter()
            .map(|(type_id, _)| type_id)
            .collect();

        // FLOWIP-120c H7: every registration names the effect its builder
        // guards (the `Effect with [...]` entry position stamps it), so the
        // checks run per named effect and multi-effect stages validate each
        // attachment against its own declaration.
        let mut branch_shaped_effects: HashSet<&str> = HashSet::new();
        let mut outcome_shaped_effects: HashSet<&str> = HashSet::new();

        for registration in &registrations {
            let target = match registration.effect_type.as_deref() {
                Some(target) => target,
                None if declarations.len() == 1 => declarations[0].effect_type,
                None => {
                    return Err(FlowBuildError::TypeShapingConfiguration {
                        stage_name: name.clone(),
                        message: format!(
                            "typed policy builder '{}' names no effect on a multi-effect \
                             stage; attach it to the effect it guards \
                             (`Effect with [...]`, FLOWIP-120c H7)",
                            registration.source_label,
                        ),
                    });
                }
            };
            let Some(declaration) = declarations
                .iter()
                .find(|declaration| declaration.effect_type == target)
            else {
                return Err(FlowBuildError::TypeShapingConfiguration {
                    stage_name: name.clone(),
                    message: format!(
                        "typed policy builder '{}' targets effect '{target}', which the \
                         stage does not declare",
                        registration.source_label,
                    ),
                });
            };

            // FLOWIP-120m, narrowed per effect under FLOWIP-120c: one
            // fallback shape per guarded effect. Branch-shaped uses the
            // Guarded carrier; outcome-shaped uses the plain perform; mixing
            // them on one effect has no coherent perform signature.
            match registration.kind {
                SynthesizedOutcomeKind::BranchShaped => {
                    branch_shaped_effects.insert(declaration.effect_type);
                }
                SynthesizedOutcomeKind::OutcomeShaped => {
                    outcome_shaped_effects.insert(declaration.effect_type);
                }
            }
            if branch_shaped_effects.contains(declaration.effect_type)
                && outcome_shaped_effects.contains(declaration.effect_type)
            {
                return Err(FlowBuildError::MixedFallbackShapesOnStage {
                    stage_name: name.clone(),
                });
            }

            match registration.kind {
                SynthesizedOutcomeKind::BranchShaped => {
                    for fact in &registration.fact_types {
                        if !contract_type_ids.contains(&fact.type_id) {
                            return Err(FlowBuildError::MiddlewareContributedFactNotInContract {
                                stage_name: name.clone(),
                                middleware_label: registration.source_label.clone(),
                                fact: fact.display_name.clone(),
                                contract: output_contract_display(metadata),
                            });
                        }
                        if declaration
                            .outcome_fact_types
                            .iter()
                            .any(|member| member.event_type == fact.event_type)
                        {
                            return Err(FlowBuildError::TypeShapingConfiguration {
                                stage_name: name.clone(),
                                message: format!(
                                    "branch fact '{}' collides with an effect outcome fact; \
                                     branch dispatch is by event type, so middleware branch \
                                     facts must be disjoint from the guarded effect's fact set",
                                    fact.event_type,
                                ),
                            });
                        }
                    }
                }
                SynthesizedOutcomeKind::OutcomeShaped => {
                    // The inverse rule (FLOWIP-120m): an outcome-shaped
                    // fallback produces the protected effect's own facts, so
                    // its fact types must be contained in the target
                    // declaration's outcome fact set.
                    for fact in &registration.fact_types {
                        if !declaration
                            .outcome_fact_types
                            .iter()
                            .any(|member| member.event_type == fact.event_type)
                        {
                            return Err(FlowBuildError::OutcomeFallbackFactNotInEffectFactSet {
                                stage_name: name.clone(),
                                middleware_label: registration.source_label.clone(),
                                fact: fact.display_name.clone(),
                                effect_type: declaration.effect_type.to_string(),
                            });
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// FLOWIP-120m: unconditional producer-side effect-fact containment.
///
/// Every declared effect's fact set must be contained in the stage's arrow
/// contract, whether or not the stage configures an `output_middleware:`
/// lane. Before this validator the check lived behind that lane's
/// registration gate in [`validate_type_shaping_contributions`], so an
/// effectful stage without the lane surfaced an undeclared effect fact only
/// at commit time, after live I/O.
#[allow(clippy::result_large_err)]
pub fn validate_effect_fact_containment(
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
) -> Result<(), crate::dsl::error::FlowBuildError> {
    use crate::dsl::error::FlowBuildError;

    for (name, descriptor) in descriptors {
        // Sinks have no output contract for effect facts to be members of;
        // an effectful sink is rejected at materialisation by the FLOWIP-120b
        // retirement path with the message that explains the policy.
        if descriptor.stage_type() == StageType::Sink {
            continue;
        }

        let declarations = descriptor.effect_declarations();
        if declarations.is_empty() {
            continue;
        }

        let Some(metadata) = descriptor.typing_metadata() else {
            return Err(FlowBuildError::StageMissingTypingMetadata {
                stage_name: name.clone(),
            });
        };
        let contract_type_ids: HashSet<TypeId> = exact_output_contract_members(metadata)
            .into_iter()
            .map(|(type_id, _)| type_id)
            .collect();

        for declaration in &declarations {
            // Duplicate event types across carrier members cannot be seen by
            // the effect_outcome! macro (a macro compares tokens, not
            // EVENT_TYPE values), so the collision is rejected here, before
            // any I/O. Dispatch is by event type, so a collision would make
            // carrier reconstruction ambiguous.
            let mut members_by_event_type: HashMap<String, String> = HashMap::new();
            for fact in &declaration.outcome_fact_types {
                if let Some(first_member) = members_by_event_type
                    .insert(fact.event_type.to_string(), fact.display_name.clone())
                {
                    return Err(FlowBuildError::DuplicateEffectOutcomeFactEventType {
                        stage_name: name.clone(),
                        effect_type: declaration.effect_type.to_string(),
                        event_type: fact.event_type.to_string(),
                        first_member,
                        second_member: fact.display_name.clone(),
                    });
                }
                if !contract_type_ids.contains(&fact.type_id) {
                    return Err(FlowBuildError::EffectFactNotInContract {
                        stage_name: name.clone(),
                        effect_type: declaration.effect_type.to_string(),
                        fact: fact.display_name.clone(),
                        contract: output_contract_display(metadata),
                    });
                }
            }
        }
    }

    Ok(())
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

/// FLOWIP-120n F16 archive sink delivery-safety gate, covering both archive
/// verbs since FLOWIP-120v: deterministic replay re-consumes the recorded
/// stream and resume re-delivers the recorded prefix during catch-up, so
/// every sink must be classified. `IdempotentProjection` passes;
/// `NonIdempotentExternal` and undeclared sinks refuse fail-loud unless the
/// operator passed `allow_duplicate_sink_delivery`, which lets both through
/// with a warning naming the stage. Live runs never call this.
#[allow(clippy::result_large_err)]
pub fn validate_archive_sink_delivery_safety(
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    verb: obzenflow_runtime::bootstrap::ReplayVerb,
    allow_duplicate_sink_delivery: bool,
) -> Result<(), crate::dsl::error::FlowBuildError> {
    use crate::dsl::error::FlowBuildError;
    use obzenflow_runtime::bootstrap::ReplayVerb;
    use obzenflow_runtime::effects::SinkDeliverySafety;

    let verb_phrase = match verb {
        ReplayVerb::Replay => "replay will re-perform",
        ReplayVerb::Resume => "catch-up will duplicate",
    };

    // Sorted so the refusal names a deterministic stage across builds.
    let mut names: Vec<&String> = descriptors.keys().collect();
    names.sort();

    for name in names {
        let descriptor = &descriptors[name];
        // Only plain sinks exist (FLOWIP-120v removed the effectful surface).
        if descriptor.stage_type() != StageType::Sink {
            continue;
        }

        match descriptor.sink_delivery_safety() {
            Some(SinkDeliverySafety::IdempotentProjection) => {}
            Some(SinkDeliverySafety::NonIdempotentExternal) => {
                if !allow_duplicate_sink_delivery {
                    return Err(FlowBuildError::ArchiveRefusedNonIdempotentSink {
                        stage: name.clone(),
                        verb,
                    });
                }
                tracing::warn!(
                    stage = %name,
                    "allow_duplicate_sink_delivery: proceeding past non-idempotent \
                     sink; {verb_phrase} its external writes"
                );
            }
            None => {
                if !allow_duplicate_sink_delivery {
                    return Err(FlowBuildError::ArchiveRefusedUndeclaredSink {
                        stage: name.clone(),
                        verb,
                    });
                }
                tracing::warn!(
                    stage = %name,
                    "allow_duplicate_sink_delivery: proceeding past sink with \
                     undeclared delivery safety; {verb_phrase} its deliveries"
                );
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
            ..
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

pub fn validate_order_observer_deterministic_input_order(
    topology: &Topology,
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: &HashMap<String, StageId>,
    observers: &HashSet<StageId>,
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
            // An orderer makes the merge deterministic only over stable input
            // streams, so the stability induction needs both halves: the stage
            // orders its inputs AND every input stream is itself
            // deterministic. Stopping at the orderer would let a cycle, or a
            // fan-in the enablement walk never marked, sit above it unexamined.
            descriptors
                .get(&stage_id)
                .map(|descriptor| descriptor.is_deterministic_input_orderer())
                .unwrap_or(false)
                && upstreams.iter().all(|&upstream| {
                    deterministic_for_stage(upstream, inbound, descriptors, memo, visiting)
                })
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
        // FLOWIP-095m: seed from the same observer set the marking walk used
        // (effects, stateful folds, live/symmetric joins), not effects alone.
        if !observers.contains(&stage_id) {
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
            let stage_name = id_to_name
                .get(&stage_id)
                .cloned()
                .unwrap_or_else(|| descriptor.name().to_string());
            // FLOWIP-095m: effects keep their specific diagnostic; a stateful or
            // live-join observer gets the order-observer variant.
            let err = if descriptor.is_effectful() {
                crate::dsl::FlowBuildError::EffectfulFanInRequiresDeterministicOrder { stage_name }
            } else {
                crate::dsl::FlowBuildError::OrderObserverFanInRequiresDeterministicOrder {
                    stage_name,
                }
            };
            return Err(Box::new(err));
        }
    }

    Ok(())
}

/// FLOWIP-095m: does this stage's reconstruction read fan-in delivery order?
/// Effects (095d), stateful folds, and live/symmetric joins do; a hydrating join
/// is a structural orderer, and sources, transforms, and sinks are carriers.
///
/// Must be evaluated on the UNWRAPPED descriptor: the join arm reads
/// `is_deterministic_input_orderer()`, which `wrap_deterministic_orderers` forces
/// to true on any marked stage, making a marked live join look hydrating.
fn is_order_observer(descriptor: &dyn StageDescriptor) -> bool {
    if descriptor.is_effectful() {
        return true;
    }
    match descriptor.stage_type() {
        StageType::Stateful => true,
        StageType::Join => !descriptor.is_deterministic_input_orderer(),
        _ => false,
    }
}

/// FLOWIP-095m: the order-observer stages as ids, frozen pre-wrap for the guard.
/// The marking walk applies [`is_order_observer`] inline (it runs before the
/// wrap), but the guard runs after the wrap, where the join arm would misread, so
/// it consumes this set instead.
pub fn order_observer_stage_ids(
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: &HashMap<String, StageId>,
) -> HashSet<StageId> {
    let mut observers = HashSet::new();
    for (dsl_name, descriptor) in descriptors {
        if is_order_observer(descriptor.as_ref()) {
            if let Some(stage_id) = name_to_id.get(dsl_name) {
                observers.insert(*stage_id);
            }
        }
    }
    observers
}

/// FLOWIP-095m: the build-time enablement walk, the inverse of
/// `validate_order_observer_deterministic_input_order`.
///
/// Marks every multi-inbound stage on a path above an order observer (an
/// effect, a stateful fold, or a live/symmetric join; see
/// [`order_observer_stage_ids`]). Those stages run the canonical deterministic
/// merge on their input subscriptions and report `is_deterministic_input_orderer()`
/// true, so the guard accepts the observers below them.
///
/// The marking is transitive on purpose: the stability induction the guard
/// encodes requires every fan-in above an ordered stage to be ordered too,
/// and the guard verifies it by recursing through an orderer's own inputs.
/// Cycle members are never marked; the merge is never enabled on a cycle
/// edge, and the guard remains the safety net that rejects observers below
/// cycles, including a cycle that feeds an ordered fan-in from above.
pub fn derive_deterministic_fan_in_stages(
    topology: &Topology,
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: &HashMap<String, StageId>,
) -> HashSet<StageId> {
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

    let mut marked = HashSet::new();
    for (dsl_name, descriptor) in descriptors {
        if !is_order_observer(descriptor.as_ref()) {
            continue;
        }
        let Some(stage_id) = name_to_id.get(dsl_name) else {
            continue;
        };
        let mut stack = vec![*stage_id];
        let mut visited = HashSet::new();
        while let Some(current) = stack.pop() {
            if !visited.insert(current) {
                continue;
            }
            let upstreams = inbound.get(&current).cloned().unwrap_or_default();
            if upstreams.len() > 1 && !topology.is_in_cycle(current.to_topology_id()) {
                marked.insert(current);
            }
            stack.extend(upstreams);
        }
    }
    marked
}

/// FLOWIP-120n F18: ordered fan-ins whose every forward inbound edge comes
/// from a source stage. Their inputs are re-admitted verbatim on replay, so
/// the recorded `admission_seq` is a stable within-generation comparator and
/// the merge needs no Kahn wait. Derived-fed ordered fan-ins stay Kahn: their
/// inputs are re-authored on replay with fresh sequences.
pub fn derive_seq_ordered_fan_ins(
    topology: &Topology,
    deterministic_fan_in_stages: &HashSet<StageId>,
) -> HashSet<StageId> {
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

    deterministic_fan_in_stages
        .iter()
        .copied()
        .filter(|fan_in| {
            inbound.get(fan_in).is_some_and(|upstreams| {
                !upstreams.is_empty()
                    && upstreams
                        .iter()
                        .all(|upstream| inbound.get(upstream).is_none_or(|edges| edges.is_empty()))
            })
        })
        .collect()
}

/// FLOWIP-095j: per-stage delivery metadata recorded in the run manifest.
///
/// For each stage: the upstream stage keys delivering into it over forward
/// edges (sorted, deduplicated), and whether its input delivery order is
/// deterministic. A stage delivers deterministically when it has at most one
/// forward inbound edge and is outside any cycle, or when its descriptor
/// reports `is_deterministic_input_orderer()` (an FLOWIP-095d marked fan-in,
/// already wrapped by `wrap_deterministic_orderers` when this runs, or a
/// structural orderer such as the hydrating join). Cycle members are always
/// non-deterministic: backflow arrivals interleave by timing, and the 095d
/// merge is never enabled on a cycle edge.
///
/// Called after `wrap_deterministic_orderers` so the descriptor answer is the
/// same one the runtime acts on. The verifier (FLOWIP-095j) computes
/// order-certification as the transitive closure over these recorded fields,
/// never by re-deriving from a topology it does not have.
pub fn derive_manifest_delivery_metadata(
    topology: &Topology,
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: &HashMap<String, StageId>,
) -> HashMap<StageId, (Vec<String>, bool)> {
    let mut id_to_stage_key: HashMap<StageId, String> = HashMap::new();
    for (dsl_name, stage_id) in name_to_id {
        if let Some(descriptor) = descriptors.get(dsl_name.as_str()) {
            id_to_stage_key.insert(*stage_id, descriptor.name().to_string());
        }
    }

    let mut inbound_ids: HashMap<StageId, Vec<StageId>> = HashMap::new();
    for edge in topology.edges() {
        if edge.kind != EdgeKind::Forward {
            continue;
        }
        inbound_ids
            .entry(StageId::from_ulid(edge.to.ulid()))
            .or_default()
            .push(StageId::from_ulid(edge.from.ulid()));
    }

    let mut metadata = HashMap::new();
    for (dsl_name, stage_id) in name_to_id {
        let Some(descriptor) = descriptors.get(dsl_name.as_str()) else {
            continue;
        };
        let mut inbound: Vec<String> = inbound_ids
            .get(stage_id)
            .map(|ups| {
                ups.iter()
                    .filter_map(|up| id_to_stage_key.get(up).cloned())
                    .collect()
            })
            .unwrap_or_default();
        inbound.sort();
        inbound.dedup();

        let ordered_delivery = if topology.is_in_cycle(stage_id.to_topology_id()) {
            false
        } else {
            inbound.len() <= 1 || descriptor.is_deterministic_input_orderer()
        };
        metadata.insert(*stage_id, (inbound, ordered_delivery));
    }
    metadata
}

/// FLOWIP-095d: build-time orderer override.
///
/// Wraps a descriptor the enablement walk marked so it reports
/// `is_deterministic_input_orderer()` true. Everything else delegates to the
/// wrapped descriptor; in particular `typing_metadata()` must pass through, or
/// feed-plan derivation and edge typing silently degrade. The runtime half
/// (the canonical merge on the stage's subscription) is threaded separately
/// through `StageResourcesBuilder::with_deterministic_fan_in_stages`.
pub struct DeterministicOrdererOverride {
    inner: Box<dyn StageDescriptor>,
}

#[async_trait]
impl StageDescriptor for DeterministicOrdererOverride {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn backpressure_clause(
        &self,
    ) -> Option<&crate::dsl::backpressure_clause::BackpressureClause> {
        self.inner.backpressure_clause()
    }

    fn set_name(&mut self, name: String) {
        self.inner.set_name(name);
    }

    fn stage_type(&self) -> StageType {
        self.inner.stage_type()
    }

    fn policy_guard_surface(&self) -> crate::dsl::stage_descriptor::PolicyGuardSurface {
        self.inner.policy_guard_surface()
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
        self.inner.typing_metadata()
    }

    fn is_effectful(&self) -> bool {
        self.inner.is_effectful()
    }

    fn is_deterministic_input_orderer(&self) -> bool {
        true
    }

    fn stage_logic_version(&self) -> String {
        self.inner.stage_logic_version()
    }

    fn sink_delivery_safety(&self) -> Option<obzenflow_runtime::effects::SinkDeliverySafety> {
        self.inner.sink_delivery_safety()
    }

    fn sink_delivery_type(&self) -> Option<&'static str> {
        self.inner.sink_delivery_type()
    }

    fn sink_canonical_destination(&self) -> Option<serde_json::Value> {
        self.inner.sink_canonical_destination()
    }

    fn effect_declarations(&self) -> Vec<obzenflow_runtime::effects::EffectDeclaration> {
        self.inner.effect_declarations()
    }

    fn synthesized_outcome_registrations(
        &self,
    ) -> Vec<obzenflow_runtime::effects::SynthesizedOutcomeRegistration> {
        self.inner.synthesized_outcome_registrations()
    }

    fn type_shaping_config_errors(&self) -> Vec<String> {
        self.inner.type_shaping_config_errors()
    }

    fn is_composite(&self) -> bool {
        self.inner.is_composite()
    }

    #[allow(clippy::result_large_err)]
    fn try_lower_composite(
        self: Box<Self>,
        binding: &str,
    ) -> Result<Option<crate::dsl::composites::CompositeLowering>, crate::dsl::FlowBuildError> {
        self.inner.try_lower_composite(binding)
    }
}

/// FLOWIP-095d: apply the orderer override to every marked descriptor that is
/// not already a structural orderer (hydrating joins stay unwrapped).
pub fn wrap_deterministic_orderers(
    descriptors: &mut HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: &HashMap<String, StageId>,
    marked: &HashSet<StageId>,
) {
    let to_wrap: Vec<String> = descriptors
        .iter()
        .filter(|(dsl_name, descriptor)| {
            name_to_id
                .get(dsl_name.as_str())
                .is_some_and(|stage_id| marked.contains(stage_id))
                && !descriptor.is_deterministic_input_orderer()
        })
        .map(|(dsl_name, _)| dsl_name.clone())
        .collect();

    for dsl_name in to_wrap {
        if let Some(inner) = descriptors.remove(&dsl_name) {
            descriptors.insert(dsl_name, Box::new(DeterministicOrdererOverride { inner }));
        }
    }
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

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Common types and utilities for join handlers

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::join::{
    JoinReferenceView, TypedJoinHandler, TypedJoinInvocation,
};
use crate::stages::join::config::JoinReferenceMode;
use obzenflow_core::event::schema::TypedPayload;
use std::hash::Hash;
use std::marker::PhantomData;

/// Value-only result of applying a built-in join strategy. Protocol control is
/// carried separately and remains available only to the sealed typed adapter.
pub struct JoinStrategyValueOutput<E> {
    pub outputs: Vec<E>,
    pub framework_eof: Option<obzenflow_core::event::payloads::flow_control_payload::EofKind>,
}

impl<E> JoinStrategyValueOutput<E> {
    pub fn facts(outputs: Vec<E>) -> Self {
        Self {
            outputs,
            framework_eof: None,
        }
    }

    pub fn framework_eof(
        kind: obzenflow_core::event::payloads::flow_control_payload::EofKind,
    ) -> Self {
        Self {
            outputs: Vec::new(),
            framework_eof: Some(kind),
        }
    }
}

/// Strategy trait that only defines how to match stream events against a catalog
pub trait JoinStrategy {
    type CatalogType: TypedPayload + Clone + Send + Sync;
    type StreamType: TypedPayload + Clone + Send + Sync;
    type EnrichedType: TypedPayload + Clone + Send + Sync;
    type Key: Eq + Hash + Clone + Send + Sync + std::fmt::Debug;

    fn match_reference(
        &self,
        reference: Option<Self::CatalogType>,
        stream_data: Self::StreamType,
        stream_key: Self::Key,
    ) -> JoinStrategyValueOutput<Self::EnrichedType>;
}

/// Built-in typed join wrapper delegating matching policy to a strategy.
pub struct JoinWithStrategy<S, CatalogKeyFn, StreamKeyFn> {
    pub(crate) strategy: S,
    pub(crate) catalog_key_fn: CatalogKeyFn,
    pub(crate) stream_key_fn: StreamKeyFn,
    pub(crate) reference_mode: JoinReferenceMode,
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
            .finish()
    }
}

impl<S, CatalogKeyFn, StreamKeyFn> TypedJoinHandler
    for JoinWithStrategy<S, CatalogKeyFn, StreamKeyFn>
where
    S: JoinStrategy + Clone + Send + Sync,
    S::CatalogType: 'static,
    S::StreamType: 'static,
    S::EnrichedType: 'static,
    CatalogKeyFn: Fn(&S::CatalogType) -> S::Key + Send + Sync + Clone + 'static,
    StreamKeyFn: Fn(&S::StreamType) -> S::Key + Send + Sync + Clone + 'static,
{
    type State = ();
    type ReferenceKey = S::Key;
    type Reference = S::CatalogType;
    type Stream = S::StreamType;
    type Output = S::EnrichedType;

    fn initial_state(&self) -> Self::State {}

    fn reference_mode(&self) -> JoinReferenceMode {
        self.reference_mode
    }

    fn admit_reference(
        &self,
        reference: &Self::Reference,
    ) -> Result<Self::ReferenceKey, HandlerError> {
        Ok((self.catalog_key_fn)(reference))
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
        stream: Self::Stream,
    ) -> Result<Vec<Self::Output>, HandlerError> {
        let key = (self.stream_key_fn)(&stream);
        let reference = references.select(&key);
        Ok(self
            .strategy
            .match_reference(reference, stream, key)
            .outputs)
    }

    fn process_stream_invocation(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, Self::ReferenceKey, Self::Reference>,
        stream: Self::Stream,
    ) -> Result<TypedJoinInvocation<Self::Output>, HandlerError> {
        let key = (self.stream_key_fn)(&stream);
        let reference = references.select(&key);
        let output = self.strategy.match_reference(reference, stream, key);
        Ok(match output.framework_eof {
            Some(kind) => TypedJoinInvocation::with_framework_eof(output.outputs, kind),
            None => TypedJoinInvocation::facts_only_for_framework(output.outputs),
        })
    }
}

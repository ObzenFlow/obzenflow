// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Async Map helper for transform stages.
//!
//! Mirrors the sync `Map` / `MapTyped` helpers, but implements `AsyncTransformHandler`
//! so it can be used with `async_transform!` without bespoke handler structs.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::AsyncTransformHandler;
use crate::typing::TransformTyping;
use async_trait::async_trait;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, TypedPayload};
use serde::{de::DeserializeOwned, Serialize};
use std::future::Future;
use std::marker::PhantomData;

type AsyncMapTypedPhantom<T, O, Fut> = PhantomData<(T, O, fn() -> Fut)>;

/// Async Map helper for `ChainEvent` mode.
///
/// Wraps an async function `Fn(ChainEvent) -> Future<Output = ChainEvent>`.
#[derive(Clone)]
pub struct AsyncMap<F, Fut>
where
    F: Fn(ChainEvent) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ChainEvent> + Send,
{
    mapper: F,
    _phantom: PhantomData<fn() -> Fut>,
}

impl<F, Fut> AsyncMap<F, Fut>
where
    F: Fn(ChainEvent) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ChainEvent> + Send,
{
    pub fn new(mapper: F) -> Self {
        Self {
            mapper,
            _phantom: PhantomData,
        }
    }
}

impl<F, Fut> std::fmt::Debug for AsyncMap<F, Fut>
where
    F: Fn(ChainEvent) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = ChainEvent> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncMap")
            .field("mapper", &"<closure>")
            .finish()
    }
}

#[async_trait]
impl<F, Fut> AsyncTransformHandler for AsyncMap<F, Fut>
where
    F: Fn(ChainEvent) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = ChainEvent> + Send + 'static,
{
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let out = (self.mapper)(event).await;
        Ok(vec![out])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Typed async Map helper.
///
/// Deserialises `T` from the input event payload, calls an async mapper, and
/// emits a derived event of type `O::versioned_event_type()`.
#[derive(Clone)]
pub struct AsyncMapTyped<T, O, F, Fut>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = O> + Send,
{
    mapper: F,
    _phantom: AsyncMapTypedPhantom<T, O, Fut>,
}

impl<T, O, F, Fut> AsyncMapTyped<T, O, F, Fut>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = O> + Send,
{
    pub fn new(mapper: F) -> Self {
        Self {
            mapper,
            _phantom: PhantomData,
        }
    }
}

impl<T, O, F, Fut> std::fmt::Debug for AsyncMapTyped<T, O, F, Fut>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = O> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncMapTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("output_type", &std::any::type_name::<O>())
            .finish()
    }
}

impl<T, O, F, Fut> TransformTyping for AsyncMapTyped<T, O, F, Fut>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = O> + Send,
{
    type Input = T;
    type Output = O;
}

#[async_trait]
impl<T, O, F, Fut> AsyncTransformHandler for AsyncMapTyped<T, O, F, Fut>
where
    T: DeserializeOwned + Send + Sync + 'static,
    O: Serialize + Send + Sync + TypedPayload + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = O> + Send + 'static,
{
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let input: T = match serde_json::from_value(event.payload().clone()) {
            Ok(v) => v,
            Err(e) => {
                panic!(
                    "AsyncMapTyped: Failed to deserialize payload into {}: {}. \
                     Use AsyncTryMapWithTyped if deserialization can fail.",
                    std::any::type_name::<T>(),
                    e
                );
            }
        };

        let output_value = (self.mapper)(input).await;

        let payload = match serde_json::to_value(&output_value) {
            Ok(p) => p,
            Err(e) => {
                panic!(
                    "AsyncMapTyped: Failed to serialize {} into payload: {}",
                    std::any::type_name::<O>(),
                    e
                );
            }
        };

        let event_type = O::versioned_event_type();
        Ok(vec![ChainEventFactory::derived_data_event(
            event.writer_id,
            &event,
            &event_type,
            payload,
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

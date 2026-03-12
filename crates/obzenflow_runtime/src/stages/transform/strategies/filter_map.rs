// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FilterMap helper for transform stages.
//!
//! Like [`Map`](super::map::Map), but allows dropping events by returning `None`.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::TransformHandler;
use crate::typing::TransformTyping;
use async_trait::async_trait;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::ChainEvent;
use obzenflow_core::TypedPayload;
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;

#[derive(Clone)]
pub struct FilterMap<F>
where
    F: Fn(ChainEvent) -> Option<ChainEvent> + Send + Sync + Clone,
{
    mapper: F,
}

impl<F> FilterMap<F>
where
    F: Fn(ChainEvent) -> Option<ChainEvent> + Send + Sync + Clone,
{
    pub fn new(mapper: F) -> Self {
        Self { mapper }
    }
}

impl<F> std::fmt::Debug for FilterMap<F>
where
    F: Fn(ChainEvent) -> Option<ChainEvent> + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterMap")
            .field("mapper", &"<closure>")
            .finish()
    }
}

#[async_trait]
impl<F> TransformHandler for FilterMap<F>
where
    F: Fn(ChainEvent) -> Option<ChainEvent> + Send + Sync + Clone + 'static,
{
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(match (self.mapper)(event) {
            Some(event) => vec![event],
            None => vec![],
        })
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct FilterMapTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Option<O> + Send + Sync + Clone,
{
    mapper: F,
    _phantom: PhantomData<(T, O)>,
}

impl<T, O, F> FilterMapTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Option<O> + Send + Sync + Clone,
{
    pub fn new(mapper: F) -> Self {
        Self {
            mapper,
            _phantom: PhantomData,
        }
    }
}

impl<T, O, F> std::fmt::Debug for FilterMapTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Option<O> + Send + Sync + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterMapTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("output_type", &std::any::type_name::<O>())
            .finish()
    }
}

impl<T, O, F> TransformTyping for FilterMapTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Option<O> + Send + Sync + Clone,
{
    type Input = T;
    type Output = O;
}

#[async_trait]
impl<T, O, F> TransformHandler for FilterMapTyped<T, O, F>
where
    T: DeserializeOwned + Send + Sync + 'static,
    O: Serialize + Send + Sync + TypedPayload + 'static,
    F: Fn(T) -> Option<O> + Send + Sync + Clone + 'static,
{
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let input_value: T = match serde_json::from_value(event.payload().clone()) {
            Ok(v) => v,
            Err(e) => {
                panic!(
                    "FilterMapTyped: Failed to deserialize payload into {}: {}. \
                     Use TryMapWithTyped if deserialization can fail.",
                    std::any::type_name::<T>(),
                    e
                );
            }
        };

        let Some(output_value) = (self.mapper)(input_value) else {
            return Ok(vec![]);
        };

        let payload = match serde_json::to_value(&output_value) {
            Ok(p) => p,
            Err(e) => {
                panic!(
                    "FilterMapTyped: Failed to serialize {} into payload: {}",
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

impl<F> FilterMap<F>
where
    F: Fn(ChainEvent) -> Option<ChainEvent> + Send + Sync + Clone,
{
    pub fn typed<T, O, G>(mapper: G) -> FilterMapTyped<T, O, G>
    where
        T: DeserializeOwned + Send + Sync,
        O: Serialize + Send + Sync + TypedPayload,
        G: Fn(T) -> Option<O> + Send + Sync + Clone,
    {
        FilterMapTyped::new(mapper)
    }
}

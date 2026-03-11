// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Async TryMapWith helper for transform stages.
//!
//! Mirrors the sync `TryMapWith` / `TryMapWithTyped` helpers, but implements
//! `AsyncTransformHandler` so it can be used with `async_transform!` without
//! bespoke handler structs.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::AsyncTransformHandler;
use crate::typing::TransformTyping;
use async_trait::async_trait;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::{ChainEvent, TypedPayload};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::json;
use std::future::Future;
use std::marker::PhantomData;

use super::try_map_with::ErrorStrategy;

/// Async TryMapWith helper for `ChainEvent` mode.
///
/// The converter returns `Result<ChainEvent, String>`, and errors are handled
/// according to the configured `ErrorStrategy`.
#[derive(Clone)]
pub struct AsyncTryMapWith<F, Fut>
where
    F: Fn(ChainEvent) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<ChainEvent, String>> + Send,
{
    converter: F,
    error_strategy: ErrorStrategy,
    _phantom: PhantomData<fn() -> Fut>,
}

impl<F, Fut> AsyncTryMapWith<F, Fut>
where
    F: Fn(ChainEvent) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<ChainEvent, String>> + Send,
{
    pub fn new(converter: F) -> Self {
        Self {
            converter,
            error_strategy: ErrorStrategy::ToErrorJournal,
            _phantom: PhantomData,
        }
    }

    pub fn on_error_journal(mut self) -> Self {
        self.error_strategy = ErrorStrategy::ToErrorJournal;
        self
    }

    pub fn on_error_emit(mut self, event_type: impl Into<String>) -> Self {
        self.error_strategy = ErrorStrategy::ToEventType(event_type.into());
        self
    }

    pub fn on_error_emit_with<P>(mut self, event_type: impl Into<String>, payload_fn: P) -> Self
    where
        P: Fn(&ChainEvent, &str) -> serde_json::Value + Send + Sync + 'static,
    {
        self.error_strategy =
            ErrorStrategy::ToEventTypeWith(event_type.into(), std::sync::Arc::new(payload_fn));
        self
    }

    pub fn on_error_drop(mut self) -> Self {
        self.error_strategy = ErrorStrategy::Drop;
        self
    }

    pub fn on_error_with<H>(mut self, handler: H) -> Self
    where
        H: Fn(ChainEvent, String) -> Option<ChainEvent> + Send + Sync + 'static,
    {
        self.error_strategy = ErrorStrategy::Custom(std::sync::Arc::new(handler));
        self
    }

    fn handle_error(&self, event: ChainEvent, error_msg: String) -> Vec<ChainEvent> {
        match &self.error_strategy {
            ErrorStrategy::Drop => vec![],
            ErrorStrategy::ToErrorJournal => {
                let mut error_event = event;
                error_event.processing_info.status =
                    ProcessingStatus::error(format!("Conversion failed: {error_msg}"));
                vec![error_event]
            }
            ErrorStrategy::ToEventType(event_type) => {
                let mut payload = event.payload();
                payload["validation_error"] = json!(error_msg);
                vec![ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    event_type,
                    payload,
                )]
            }
            ErrorStrategy::ToEventTypeWith(event_type, payload_fn) => {
                let mut payload = event.payload();
                let error_value = payload_fn(&event, &error_msg);
                if let serde_json::Value::Object(error_map) = error_value {
                    for (key, value) in error_map {
                        payload[key] = value;
                    }
                }
                vec![ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    event_type,
                    payload,
                )]
            }
            ErrorStrategy::Custom(handler) => handler(event, error_msg).into_iter().collect(),
        }
    }
}

impl<F, Fut> std::fmt::Debug for AsyncTryMapWith<F, Fut>
where
    F: Fn(ChainEvent) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<ChainEvent, String>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncTryMapWith")
            .field("converter", &"<closure>")
            .field("error_strategy", &self.error_strategy)
            .finish()
    }
}

#[async_trait]
impl<F, Fut> AsyncTransformHandler for AsyncTryMapWith<F, Fut>
where
    F: Fn(ChainEvent) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<ChainEvent, String>> + Send + 'static,
{
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let original = event.clone();
        match (self.converter)(event).await {
            Ok(converted) => Ok(vec![converted]),
            Err(error_msg) => Ok(self.handle_error(original, error_msg)),
        }
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Typed async TryMapWith helper.
///
/// Deserialises `T`, calls an async converter, and emits `O` on success. Errors
/// follow the same `ErrorStrategy` semantics as the sync typed helper.
#[derive(Clone)]
pub struct AsyncTryMapWithTyped<T, O, F, Fut>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<O, String>> + Send,
{
    converter: F,
    error_strategy: ErrorStrategy,
    _phantom: PhantomData<(T, O, fn() -> Fut)>,
}

impl<T, O, F, Fut> AsyncTryMapWithTyped<T, O, F, Fut>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<O, String>> + Send,
{
    pub fn new(converter: F) -> Self {
        Self {
            converter,
            error_strategy: ErrorStrategy::ToErrorJournal,
            _phantom: PhantomData,
        }
    }

    pub fn on_error_journal(mut self) -> Self {
        self.error_strategy = ErrorStrategy::ToErrorJournal;
        self
    }

    pub fn on_error_emit(mut self, event_type: impl Into<String>) -> Self {
        self.error_strategy = ErrorStrategy::ToEventType(event_type.into());
        self
    }

    pub fn on_error_emit_with<P>(mut self, event_type: impl Into<String>, payload_fn: P) -> Self
    where
        P: Fn(&ChainEvent, &str) -> serde_json::Value + Send + Sync + 'static,
    {
        self.error_strategy =
            ErrorStrategy::ToEventTypeWith(event_type.into(), std::sync::Arc::new(payload_fn));
        self
    }

    pub fn on_error_drop(mut self) -> Self {
        self.error_strategy = ErrorStrategy::Drop;
        self
    }

    pub fn on_error_with<H>(mut self, handler: H) -> Self
    where
        H: Fn(ChainEvent, String) -> Option<ChainEvent> + Send + Sync + 'static,
    {
        self.error_strategy = ErrorStrategy::Custom(std::sync::Arc::new(handler));
        self
    }

    fn handle_error(&self, event: ChainEvent, error_msg: String) -> Vec<ChainEvent> {
        match &self.error_strategy {
            ErrorStrategy::Drop => vec![],
            ErrorStrategy::ToErrorJournal => {
                let mut error_event = event;
                error_event.processing_info.status = ProcessingStatus::error(error_msg);
                vec![error_event]
            }
            ErrorStrategy::ToEventType(event_type) => {
                let mut payload = event.payload();
                payload["validation_error"] = json!(error_msg);
                vec![ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    event_type,
                    payload,
                )]
            }
            ErrorStrategy::ToEventTypeWith(event_type, payload_fn) => {
                let mut payload = event.payload();
                let error_value = payload_fn(&event, &error_msg);
                if let serde_json::Value::Object(error_map) = error_value {
                    for (key, value) in error_map {
                        payload[key] = value;
                    }
                }
                vec![ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    event_type,
                    payload,
                )]
            }
            ErrorStrategy::Custom(handler) => handler(event, error_msg).into_iter().collect(),
        }
    }
}

impl<T, O, F, Fut> std::fmt::Debug for AsyncTryMapWithTyped<T, O, F, Fut>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<O, String>> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncTryMapWithTyped")
            .field("input_type", &std::any::type_name::<T>())
            .field("output_type", &std::any::type_name::<O>())
            .field("error_strategy", &self.error_strategy)
            .finish()
    }
}

impl<T, O, F, Fut> TransformTyping for AsyncTryMapWithTyped<T, O, F, Fut>
where
    T: DeserializeOwned + Send + Sync,
    O: Serialize + Send + Sync + TypedPayload,
    F: Fn(T) -> Fut + Send + Sync + Clone,
    Fut: Future<Output = Result<O, String>> + Send,
{
    type Input = T;
    type Output = O;
}

#[async_trait]
impl<T, O, F, Fut> AsyncTransformHandler for AsyncTryMapWithTyped<T, O, F, Fut>
where
    T: DeserializeOwned + Send + Sync + 'static,
    O: Serialize + Send + Sync + TypedPayload + 'static,
    F: Fn(T) -> Fut + Send + Sync + Clone + 'static,
    Fut: Future<Output = Result<O, String>> + Send + 'static,
{
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let input: T = match serde_json::from_value(event.payload().clone()) {
            Ok(v) => v,
            Err(e) => {
                let error_msg = format!(
                    "Failed to deserialize payload into {}: {}",
                    std::any::type_name::<T>(),
                    e
                );
                return Ok(self.handle_error(event, error_msg));
            }
        };

        match (self.converter)(input).await {
            Ok(output_value) => {
                match serde_json::to_value(&output_value) {
                    Ok(payload) => {
                        let event_type = O::versioned_event_type();
                        Ok(vec![ChainEventFactory::derived_data_event(
                            event.writer_id,
                            &event,
                            &event_type,
                            payload,
                        )])
                    }
                    Err(e) => {
                        let error_msg = format!(
                            "Failed to serialize {} into payload: {}",
                            std::any::type_name::<O>(),
                            e
                        );
                        Ok(self.handle_error(event, error_msg))
                    }
                }
            }
            Err(error_msg) => Ok(self.handle_error(event, error_msg)),
        }
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}


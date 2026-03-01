// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler trait for stateless transform stages
//!
//! Examples: Data enrichers, filters, mappers, routers

use async_trait::async_trait;
use obzenflow_core::ChainEvent;

/// Handler for stateless transform stages
///
/// Transforms are the workhorses of the pipeline - they:
/// - Start processing immediately (no waiting)
/// - Process events one at a time
/// - Can filter (0 outputs), pass through (1 output), or expand (N outputs)
///
/// # Example
/// ```ignore
/// use obzenflow_runtime::stages::common::handlers::TransformHandler;
/// use obzenflow_core::ChainEvent;
/// use obzenflow_core::event::ChainEventContent;
/// use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// use std::collections::HashMap;
/// use serde_json::{json, Value};
/// use async_trait::async_trait;
///
/// type Result<T> = std::result::Result<T, HandlerError>;
///
/// struct DataEnricher {
///     cache: HashMap<String, Value>,
/// }
///
/// #[async_trait]
/// impl TransformHandler for DataEnricher {
///     fn process(&self, mut event: ChainEvent) -> Result<Vec<ChainEvent>> {
///         // Enrich event with cached metadata
///         if let Some(metadata) = self.cache.get(&event.event_type()) {
///             if let ChainEventContent::Data { ref mut payload, .. } = event.content {
///                 payload["metadata"] = metadata.clone();
///             }
///         }
///         Ok(vec![event])
///     }
///     
///     // Stateless transform has no special drain logic
///     async fn drain(&mut self) -> Result<()> {
///         Ok(())
///     }
/// }
/// ```
use crate::stages::common::handler_error::HandlerError;

#[async_trait]
pub trait TransformHandler: Send + Sync {
    /// Process an event, potentially producing multiple outputs
    ///
    /// This is a pure function - same input always produces same output.
    ///
    /// `Ok(outputs)` means the handler succeeded:
    /// - `outputs.len() == 0` → no outputs / filter
    /// - `outputs.len() >= 1` → emitted events
    ///
    /// `Err(HandlerError)` means a per-record failure occurred while
    /// processing this event (e.g. remote timeout, decode failure). The
    /// supervisor will convert this into an error-marked event and route
    /// it using ErrorKind.
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    /// Perform any cleanup during shutdown
    ///
    /// For stateless transforms, this is typically a no-op.
    /// For stateful transforms, this might flush caches or close connections.
    async fn drain(&mut self) -> std::result::Result<(), HandlerError>;
}

/// Async handler for stateless transform stages.
///
/// This is intended for IO-bound transforms that need to `await` (HTTP, DB, LLM calls).
#[async_trait]
pub trait AsyncTransformHandler: Send + Sync {
    /// Process an event asynchronously, potentially producing multiple outputs.
    async fn process(
        &self,
        event: ChainEvent,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    /// Perform any cleanup during shutdown.
    async fn drain(&mut self) -> std::result::Result<(), HandlerError>;
}

/// Unified async handler surface used by the transform stage supervisor.
///
/// This allows the supervisor to always `await` handler processing while preserving
/// the existing sync `TransformHandler` API. Both sync and async handlers implement
/// this trait (sync via blanket impl, async via `AsyncTransformHandlerAdapter` wrapper).
#[async_trait]
pub(crate) trait UnifiedTransformHandler: Send + Sync {
    async fn process(
        &self,
        event: ChainEvent,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    async fn drain(&mut self) -> std::result::Result<(), HandlerError>;
}

#[async_trait]
impl<T: TransformHandler + Send + Sync> UnifiedTransformHandler for T {
    async fn process(
        &self,
        event: ChainEvent,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        TransformHandler::process(self, event)
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        TransformHandler::drain(self).await
    }
}

/// Adapter wrapper that allows `AsyncTransformHandler` to implement `UnifiedTransformHandler`.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct AsyncTransformHandlerAdapter<T>(pub T);

#[async_trait]
impl<T: AsyncTransformHandler + Send + Sync> UnifiedTransformHandler
    for AsyncTransformHandlerAdapter<T>
{
    async fn process(
        &self,
        event: ChainEvent,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        AsyncTransformHandler::process(&self.0, event).await
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        AsyncTransformHandler::drain(&mut self.0).await
    }
}

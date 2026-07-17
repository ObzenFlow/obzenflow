// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler trait for stateless transform stages
//!
//! Examples: Data enrichers, filters, mappers, routers

use crate::effects::{EffectInvocationContext, Effects};
use crate::typing::TransformTyping;
use async_trait::async_trait;
use obzenflow_core::event::schema::TypedPayload;
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

    /// FLOWIP-010 §7: called once at stage build with the build-resolved
    /// lineage policy. Handlers that create derived events store it; the
    /// default ignores it.
    fn install_lineage_policy(&mut self, _policy: obzenflow_core::config::LineagePolicy) {}
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

    /// FLOWIP-010 §7: called once at stage build with the build-resolved
    /// lineage policy. Handlers that create derived events store it; the
    /// default ignores it.
    fn install_lineage_policy(&mut self, _policy: obzenflow_core::config::LineagePolicy) {}
}

/// Unified async handler surface used by the transform stage supervisor.
///
/// This allows the supervisor to always `await` handler processing while preserving
/// the existing sync `TransformHandler` API. Both sync and async handlers implement
/// this trait (sync via blanket impl, async via `AsyncTransformHandlerAdapter` wrapper).
#[doc(hidden)]
#[async_trait]
pub trait UnifiedTransformHandler: Send + Sync {
    /// Process one event. `scope` is the per-event middleware execution
    /// scope computed by the supervisor at dispatch (FLOWIP-120c H3);
    /// handlers without middleware ignore it.
    async fn process(
        &self,
        event: ChainEvent,
        effect_context: Option<EffectInvocationContext>,
        scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError>;

    async fn drain(&mut self) -> std::result::Result<(), HandlerError>;

    fn stage_logic_version(&self) -> &str {
        "1"
    }

    /// FLOWIP-010 §7: forwarded to the wrapped handler at stage build.
    fn install_lineage_policy(&mut self, _policy: obzenflow_core::config::LineagePolicy) {}
}

#[async_trait]
impl<T: TransformHandler + Send + Sync> UnifiedTransformHandler for T {
    async fn process(
        &self,
        event: ChainEvent,
        _effect_context: Option<EffectInvocationContext>,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        TransformHandler::process(self, event)
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        TransformHandler::drain(self).await
    }

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        TransformHandler::install_lineage_policy(self, policy)
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
        _effect_context: Option<EffectInvocationContext>,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        AsyncTransformHandler::process(&self.0, event).await
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        AsyncTransformHandler::drain(&mut self.0).await
    }

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        self.0.install_lineage_policy(policy)
    }
}

/// Async transform surface for replay-safe effects.
///
/// The stage arrow and `effects:` clause are the canonical operator-facing
/// contract. `Output` and `AllowedEffects` mirror those declarations so Rust
/// can reject an undeclared [`Effects::emit`] or [`Effects::perform`] inside
/// this handler before the handler is erased for execution. They carry no
/// runtime metadata and are never journalled.
///
/// A single output fact may be named directly. For a multi-fact arrow, use
/// [`obzenflow_core::stage_fact_set!`]. Always mirror effect types with
/// [`crate::effect_set!`]; middleware and policy values remain only in the
/// stage's `effects:` clause.
///
/// ```ignore
/// # use async_trait::async_trait;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # struct GatewayTransform;
/// # struct ValidatedOrder;
/// # struct PaymentAuthorized;
/// # struct PaymentDeclined;
/// # struct OrderCancelled;
/// # struct PaymentAuthorizationUnavailable;
/// # struct AuthorizePayment;
/// #[async_trait]
/// impl EffectfulTransformHandler for GatewayTransform {
///     type Input = ValidatedOrder;
///     type Output = obzenflow_core::stage_fact_set![
///         PaymentAuthorized,
///         PaymentDeclined,
///         OrderCancelled,
///         PaymentAuthorizationUnavailable,
///     ];
///     type AllowedEffects = obzenflow_runtime::effect_set![AuthorizePayment];
///
///     // `process` mirrors this contract through
///     // `Effects<Self::Output, Self::AllowedEffects>`.
///     # async fn process(
///     #     &self,
///     #     _input: Self::Input,
///     #     _fx: &mut obzenflow_runtime::effects::Effects<
///     #         Self::Output,
///     #         Self::AllowedEffects,
///     #     >,
///     # ) -> Result<
///     #     obzenflow_runtime::effects::StageCompletion<Self::Output>,
///     #     obzenflow_runtime::stages::common::handler_error::HandlerError,
///     # > {
///     #     unimplemented!()
///     # }
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not satisfy `EffectfulTransformHandler` for this stage",
    label = "this handler does not match the effectful transform contract",
    note = "implement `EffectfulTransformHandler` with `Input`, `Output`, `AllowedEffects`, \
            and `process`; `Input` must match the arrow input, while `Output` and \
            `AllowedEffects` mirror the canonical arrow and `effects:` clause (FLOWIP-120z B9)"
)]
#[async_trait]
pub trait EffectfulTransformHandler: Send + Sync {
    type Input: TypedPayload + Send + Sync + 'static;
    type Output: obzenflow_core::StageFactSet;
    type AllowedEffects: crate::effects::EffectSet;

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> std::result::Result<crate::effects::StageCompletion<Self::Output>, HandlerError>;

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "1"
    }
}

#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct EffectfulTransformHandlerAdapter<H>(pub H);

impl<H> TransformTyping for EffectfulTransformHandlerAdapter<H>
where
    H: EffectfulTransformHandler,
{
    type Input = H::Input;
    type Output = H::Output;
}

#[async_trait]
impl<H> UnifiedTransformHandler for EffectfulTransformHandlerAdapter<H>
where
    H: EffectfulTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    async fn process(
        &self,
        event: ChainEvent,
        effect_context: Option<EffectInvocationContext>,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let input = H::Input::try_from_event(&event)
            .map_err(|e| HandlerError::Deserialization(e.to_string()))?;
        let effect_context = effect_context.ok_or_else(|| {
            HandlerError::Other("effectful transform invoked without effect context".to_string())
        })?;
        let mut fx = Effects::<H::Output, H::AllowedEffects>::new(effect_context);
        let _completion = self.0.process(input, &mut fx).await?;
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        self.0.drain().await
    }

    fn stage_logic_version(&self) -> &str {
        self.0.stage_logic_version()
    }
}

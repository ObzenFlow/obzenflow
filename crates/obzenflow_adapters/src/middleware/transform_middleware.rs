// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Middleware adapter for TransformHandler
//!
//! This module provides the ability to wrap TransformHandler implementations
//! with middleware for cross-cutting concerns like logging, monitoring, and retry logic.

use super::{
    context_keys::{
        CircuitBreakerAttempt, CircuitBreakerRetryAfterMs, CircuitBreakerRetryDelayMs,
        CircuitBreakerShouldRetry, CircuitBreakerTotalRetryWallMs,
    },
    Middleware, MiddlewareAction, MiddlewareContext,
};
use async_trait::async_trait;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::ChainEvent;
use obzenflow_core::MiddlewareExecutionScope;
use obzenflow_runtime::effects::{
    EffectBoundaryAction, EffectBoundaryContext, EffectBoundaryMiddleware, EffectBoundaryStart,
    EffectInvocationContext,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use obzenflow_runtime::stages::common::handlers::{AsyncTransformHandler, TransformHandler};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A TransformHandler wrapper that applies middleware to transform operations
#[derive(Clone)]
pub struct MiddlewareTransform<H: TransformHandler> {
    inner: H,
    middleware_chain: Arc<Vec<Arc<dyn Middleware>>>,
    /// Replay execution scope for this stage (FLOWIP-120a).
    execution_scope: MiddlewareExecutionScope,
}

impl<H: TransformHandler> std::fmt::Debug for MiddlewareTransform<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiddlewareTransform")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .finish()
    }
}

impl<H: TransformHandler> MiddlewareTransform<H> {
    /// Create a new middleware-wrapped transform handler
    pub fn new(inner: H) -> Self {
        Self {
            inner,
            middleware_chain: Arc::new(Vec::new()),
            execution_scope: MiddlewareExecutionScope::LiveHandler,
        }
    }

    /// Add middleware to the chain
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        Arc::make_mut(&mut self.middleware_chain).push(Arc::from(middleware));
        self
    }

    /// Bind the replay execution scope for this stage (FLOWIP-120a).
    pub fn with_execution_scope(mut self, scope: MiddlewareExecutionScope) -> Self {
        self.execution_scope = scope;
        self
    }

    /// Apply the middleware chain to a transform operation
    fn apply_middleware<F>(
        &self,
        event: ChainEvent,
        transform_fn: F,
    ) -> Result<Vec<ChainEvent>, HandlerError>
    where
        F: FnOnce(ChainEvent) -> Result<Vec<ChainEvent>, HandlerError>,
    {
        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::with_scope(self.execution_scope);

        // Pre-processing phase
        for middleware in self.middleware_chain.iter() {
            match middleware.pre_handle(&event, &mut ctx) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(mut results) => {
                    // Pre-write phase for skip results
                    for result in &mut results {
                        for mw in self.middleware_chain.iter() {
                            mw.pre_write(result, &ctx);
                        }
                    }

                    // Check for control events even on skip
                    if !ctx.control_events().is_empty() {
                        tracing::trace!(
                            "Appending {} control events from middleware (skip path)",
                            ctx.control_events().len()
                        );
                    }
                    // Pre-write on control events - take ownership to avoid borrow issues
                    let mut control_events = ctx.take_control_events();
                    for control_event in &mut control_events {
                        for mw in self.middleware_chain.iter() {
                            mw.pre_write(control_event, &ctx);
                        }
                    }
                    results.extend(control_events);
                    return Ok(results);
                }
                MiddlewareAction::Abort => {
                    let mut err = event.clone();
                    err.processing_info.status = ProcessingStatus::error("aborted by middleware");
                    return Ok(vec![err]);
                }
            }
        }

        // Execute the transform.
        //
        // If the handler returns `Err(HandlerError)`, convert it into an
        // error-marked event so middleware (e.g., circuit breaker) can still
        // observe the ErrorKind-driven outcome.
        // TODO(051c): When sync retry is added, extract `retry_after` from
        // `HandlerError::RateLimited { retry_after, .. }` into the typed middleware context so the
        // circuit breaker can honour provider back-off hints (mirrors the async path).
        //
        // TODO(D2/051c): The sync transform path catches `HandlerError` and converts
        // it to an error-marked event, but never calls `Middleware::on_error` on the
        // middleware chain.  This means any middleware relying on `on_error` (e.g.,
        // for cleanup or observability) will not fire in the sync path.  The circuit
        // breaker currently observes failures via `post_handle` + `ErrorKind` on the
        // marked event, so this is not a correctness issue today, but it violates the
        // middleware contract and should be reconciled when the sync retry loop is added.
        let mut results = match transform_fn(event.clone()) {
            Ok(results) => results,
            Err(err) => {
                let reason = format!("Transform handler error: {err:?}");
                vec![event.clone().mark_as_error(reason, err.kind())]
            }
        };

        // Post-processing phase (reverse order)
        for middleware in self.middleware_chain.iter().rev() {
            middleware.post_handle(&event, &results, &mut ctx);
        }

        // Pre-write phase: allow middleware to enrich each result event
        for result in &mut results {
            for middleware in self.middleware_chain.iter() {
                middleware.pre_write(result, &ctx);
            }
        }

        // Append control events after all middleware runs
        if !ctx.control_events().is_empty() {
            tracing::trace!(
                "Appending {} control events from middleware",
                ctx.control_events().len()
            );
        }
        // Pre-write on control events - take ownership to avoid borrow issues
        let mut control_events = ctx.take_control_events();
        for control_event in &mut control_events {
            for middleware in self.middleware_chain.iter() {
                middleware.pre_write(control_event, &ctx);
            }
        }
        results.extend(control_events);

        Ok(results)
    }
}

#[async_trait]
impl<H: TransformHandler> TransformHandler for MiddlewareTransform<H> {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        // Short-circuit if event already has Error status
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            tracing::debug!(
                "MiddlewareTransform: Skipping event with Error status: {:?}",
                event.processing_info.status
            );
            return Ok(vec![event]);
        }
        self.apply_middleware(event, |e| self.inner.process(e))
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        // Drain is not intercepted by middleware - it's an infrastructure concern
        self.inner.drain().await
    }
}

/// Extension trait to add middleware capabilities to any TransformHandler
pub trait TransformHandlerExt: TransformHandler + Sized {
    /// Start building a middleware chain for this handler
    fn middleware(self) -> TransformMiddlewareBuilder<Self> {
        TransformMiddlewareBuilder::new(self)
    }
}

// Implement for all TransformHandlers
impl<T: TransformHandler> TransformHandlerExt for T {}

/// Builder for constructing middleware chains around transform handlers
pub struct TransformMiddlewareBuilder<H: TransformHandler> {
    handler: MiddlewareTransform<H>,
}

impl<H: TransformHandler> TransformMiddlewareBuilder<H> {
    fn new(inner: H) -> Self {
        Self {
            handler: MiddlewareTransform::new(inner),
        }
    }

    /// Add a middleware to the chain
    pub fn with<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.handler = self.handler.with_middleware(Box::new(middleware));
        self
    }

    /// Build the final middleware-wrapped handler
    pub fn build(self) -> MiddlewareTransform<H> {
        self.handler
    }
}

/// An AsyncTransformHandler wrapper that applies middleware to async transform operations.
#[derive(Clone)]
pub struct AsyncMiddlewareTransform<H: AsyncTransformHandler> {
    inner: H,
    middleware_chain: Arc<Vec<Arc<dyn Middleware>>>,
    /// Replay execution scope for this stage (FLOWIP-120a).
    execution_scope: MiddlewareExecutionScope,
}

impl<H: AsyncTransformHandler> std::fmt::Debug for AsyncMiddlewareTransform<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncMiddlewareTransform")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .finish()
    }
}

impl<H: AsyncTransformHandler> AsyncMiddlewareTransform<H> {
    /// Create a new middleware-wrapped async transform handler.
    pub fn new(inner: H) -> Self {
        Self {
            inner,
            middleware_chain: Arc::new(Vec::new()),
            execution_scope: MiddlewareExecutionScope::LiveHandler,
        }
    }

    /// Add middleware to the chain.
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        Arc::make_mut(&mut self.middleware_chain).push(Arc::from(middleware));
        self
    }

    /// Bind the replay execution scope for this stage (FLOWIP-120a).
    pub fn with_execution_scope(mut self, scope: MiddlewareExecutionScope) -> Self {
        self.execution_scope = scope;
        self
    }

    async fn apply_middleware<F, Fut>(
        &self,
        event: ChainEvent,
        mut transform_fn: F,
    ) -> Result<Vec<ChainEvent>, HandlerError>
    where
        F: FnMut(ChainEvent) -> Fut,
        Fut: Future<Output = Result<Vec<ChainEvent>, HandlerError>>,
    {
        let original = event.clone();
        let retry_start = Instant::now();
        let mut attempt: u32 = 0;
        let mut accumulated_control_events: Vec<ChainEvent> = Vec::new();

        loop {
            // Create a fresh ephemeral context per attempt so timing/observability middleware
            // can record per-attempt processing metadata cleanly.
            let mut ctx = MiddlewareContext::with_scope(self.execution_scope);
            ctx.insert::<CircuitBreakerAttempt>(attempt);

            // Pre-processing phase
            let mut short_circuit: Option<Vec<ChainEvent>> = None;
            for middleware in self.middleware_chain.iter() {
                match middleware.pre_handle(&original, &mut ctx) {
                    MiddlewareAction::Continue => continue,
                    MiddlewareAction::Skip(mut results) => {
                        accumulated_control_events.extend(ctx.take_control_events());
                        ctx.insert::<CircuitBreakerTotalRetryWallMs>(
                            retry_start.elapsed().as_millis() as u64,
                        );

                        for result in &mut results {
                            for mw in self.middleware_chain.iter() {
                                mw.pre_write(result, &ctx);
                            }
                        }
                        for control_event in &mut accumulated_control_events {
                            for mw in self.middleware_chain.iter() {
                                mw.pre_write(control_event, &ctx);
                            }
                        }
                        results.append(&mut accumulated_control_events);
                        short_circuit = Some(results);
                        break;
                    }
                    MiddlewareAction::Abort => {
                        accumulated_control_events.extend(ctx.take_control_events());
                        ctx.insert::<CircuitBreakerTotalRetryWallMs>(
                            retry_start.elapsed().as_millis() as u64,
                        );

                        let mut err = original.clone();
                        err.processing_info.status =
                            ProcessingStatus::error("aborted by middleware");
                        let mut results = vec![err];
                        for result in &mut results {
                            for mw in self.middleware_chain.iter() {
                                mw.pre_write(result, &ctx);
                            }
                        }
                        for control_event in &mut accumulated_control_events {
                            for mw in self.middleware_chain.iter() {
                                mw.pre_write(control_event, &ctx);
                            }
                        }
                        results.append(&mut accumulated_control_events);
                        short_circuit = Some(results);
                        break;
                    }
                }
            }

            if let Some(results) = short_circuit {
                return Ok(results);
            }

            // Execute the transform (async).
            //
            // If the handler returns `Err(HandlerError)`, convert it into an
            // error-marked event so middleware (e.g., circuit breaker) can still
            // observe the ErrorKind-driven outcome.
            let mut results = match transform_fn(original.clone()).await {
                Ok(results) => results,
                Err(err) => {
                    if let HandlerError::RateLimited {
                        retry_after: Some(wait),
                        ..
                    } = &err
                    {
                        ctx.insert::<CircuitBreakerRetryAfterMs>(wait.as_millis() as u64);
                    }
                    let reason = format!("Transform handler error: {err:?}");
                    vec![original.clone().mark_as_error(reason, err.kind())]
                }
            };

            // Post-processing phase (reverse order)
            for middleware in self.middleware_chain.iter().rev() {
                middleware.post_handle(&original, &results, &mut ctx);
            }

            accumulated_control_events.extend(ctx.take_control_events());

            // Ask middleware (circuit breaker) if we should retry.
            let should_retry = ctx
                .get::<CircuitBreakerShouldRetry>()
                .copied()
                .unwrap_or(false);

            if should_retry {
                let delay_ms = ctx
                    .get::<CircuitBreakerRetryDelayMs>()
                    .copied()
                    .unwrap_or(0);
                attempt = attempt.saturating_add(1);
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
                continue;
            }

            // Final attempt: record total wall time across all attempts (including backoff).
            ctx.insert::<CircuitBreakerTotalRetryWallMs>(retry_start.elapsed().as_millis() as u64);

            // Pre-write phase: allow middleware to enrich each result event.
            for result in &mut results {
                for middleware in self.middleware_chain.iter() {
                    middleware.pre_write(result, &ctx);
                }
            }

            // Pre-write on accumulated control events and append them.
            for control_event in &mut accumulated_control_events {
                for middleware in self.middleware_chain.iter() {
                    middleware.pre_write(control_event, &ctx);
                }
            }
            results.append(&mut accumulated_control_events);

            return Ok(results);
        }
    }
}

#[async_trait]
impl<H: AsyncTransformHandler> AsyncTransformHandler for AsyncMiddlewareTransform<H> {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        // Short-circuit if event already has Error status
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            tracing::debug!(
                "AsyncMiddlewareTransform: Skipping event with Error status: {:?}",
                event.processing_info.status
            );
            return Ok(vec![event]);
        }

        self.apply_middleware(event, |e| self.inner.process(e))
            .await
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        // Drain is not intercepted by middleware - it's an infrastructure concern
        self.inner.drain().await
    }
}

/// Extension trait to add middleware capabilities to any AsyncTransformHandler.
pub trait AsyncTransformHandlerExt: AsyncTransformHandler + Sized {
    /// Start building a middleware chain for this handler.
    fn middleware(self) -> AsyncTransformMiddlewareBuilder<Self> {
        AsyncTransformMiddlewareBuilder::new(self)
    }
}

impl<T: AsyncTransformHandler> AsyncTransformHandlerExt for T {}

/// Builder for constructing middleware chains around async transform handlers.
pub struct AsyncTransformMiddlewareBuilder<H: AsyncTransformHandler> {
    handler: AsyncMiddlewareTransform<H>,
}

impl<H: AsyncTransformHandler> AsyncTransformMiddlewareBuilder<H> {
    fn new(inner: H) -> Self {
        Self {
            handler: AsyncMiddlewareTransform::new(inner),
        }
    }

    pub fn with<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.handler = self.handler.with_middleware(Box::new(middleware));
        self
    }

    pub fn build(self) -> AsyncMiddlewareTransform<H> {
        self.handler
    }
}

/// A unified transform wrapper that applies existing middleware while preserving
/// the effect invocation context required by replay-safe effectful transforms.
///
/// This intentionally does not run the async transform retry loop. Retrying
/// around `fx.perform` would re-enter the same effect cursor and append duplicate
/// failed effect records. 120a's v1 effect boundary supports pre-gating,
/// fallback, post-observation, and enrichment; effect retry policy needs a
/// cursor-aware retry mechanism.
#[derive(Clone)]
#[doc(hidden)]
pub struct UnifiedMiddlewareTransform<H: UnifiedTransformHandler> {
    inner: H,
    middleware_chain: Arc<Vec<Arc<dyn Middleware>>>,
    /// Replay execution scope for this stage (FLOWIP-120a). Used for the
    /// non-effect-boundary handler path. Effect-boundary observation always runs
    /// under `LiveEffectBoundary`, since the boundary is only consulted when an
    /// effect executes live (replay returns the recorded outcome first).
    execution_scope: MiddlewareExecutionScope,
}

#[derive(Clone)]
struct MiddlewareEffectBoundary {
    middleware_chain: Arc<Vec<Arc<dyn Middleware>>>,
}

impl MiddlewareEffectBoundary {
    fn drain_control_events(&self, ctx: &mut MiddlewareContext) -> Vec<ChainEvent> {
        let mut control_events = ctx.take_control_events();
        for control_event in &mut control_events {
            for middleware in self.middleware_chain.iter() {
                middleware.pre_write(control_event, ctx);
            }
        }
        control_events
    }
}

impl EffectBoundaryMiddleware for MiddlewareEffectBoundary {
    fn before_effect(&self, event: &ChainEvent) -> EffectBoundaryStart {
        // The effect boundary is reached only when the effect is executing live
        // (strict replay returns the recorded outcome before consulting it), so
        // control middleware here must run and protect the live call.
        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);

        for middleware in self.middleware_chain.iter() {
            match middleware.pre_handle(event, &mut ctx) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(results) => {
                    let control_events = self.drain_control_events(&mut ctx);
                    return EffectBoundaryStart {
                        action: EffectBoundaryAction::Skip(results),
                        context: EffectBoundaryContext::new(ctx),
                        control_events,
                    };
                }
                MiddlewareAction::Abort => {
                    let control_events = self.drain_control_events(&mut ctx);
                    return EffectBoundaryStart {
                        action: EffectBoundaryAction::Abort,
                        context: EffectBoundaryContext::new(ctx),
                        control_events,
                    };
                }
            }
        }

        EffectBoundaryStart {
            action: EffectBoundaryAction::Continue,
            context: EffectBoundaryContext::new(ctx),
            control_events: Vec::new(),
        }
    }

    fn after_effect(
        &self,
        context: EffectBoundaryContext,
        event: &ChainEvent,
        outputs: &[ChainEvent],
    ) -> Vec<ChainEvent> {
        let Ok(mut ctx) = context.downcast::<MiddlewareContext>() else {
            return Vec::new();
        };

        for middleware in self.middleware_chain.iter().rev() {
            middleware.post_handle(event, outputs, &mut ctx);
        }

        self.drain_control_events(&mut ctx)
    }
}

impl<H: UnifiedTransformHandler> std::fmt::Debug for UnifiedMiddlewareTransform<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnifiedMiddlewareTransform")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .finish()
    }
}

impl<H: UnifiedTransformHandler> UnifiedMiddlewareTransform<H> {
    /// Create a new middleware-wrapped unified transform handler.
    pub fn new(inner: H) -> Self {
        Self {
            inner,
            middleware_chain: Arc::new(Vec::new()),
            execution_scope: MiddlewareExecutionScope::LiveHandler,
        }
    }

    /// Add middleware to the chain.
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        Arc::make_mut(&mut self.middleware_chain).push(Arc::from(middleware));
        self
    }

    /// Bind the replay execution scope for this stage (FLOWIP-120a).
    pub fn with_execution_scope(mut self, scope: MiddlewareExecutionScope) -> Self {
        self.execution_scope = scope;
        self
    }

    async fn apply_middleware(
        &self,
        event: ChainEvent,
        effect_context: Option<EffectInvocationContext>,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if let Some(mut effect_context) = effect_context {
            let boundary_control_events = effect_context.boundary_control_events.clone();
            effect_context.effect_boundary = Some(Arc::new(MiddlewareEffectBoundary {
                middleware_chain: self.middleware_chain.clone(),
            }));

            let mut results = match self
                .inner
                .process(event.clone(), Some(effect_context))
                .await
            {
                Ok(results) => results,
                Err(err) => {
                    let reason = format!("Transform handler error: {err:?}");
                    vec![event.clone().mark_as_error(reason, err.kind())]
                }
            };

            let ctx = MiddlewareContext::new();
            for result in &mut results {
                for middleware in self.middleware_chain.iter() {
                    middleware.pre_write(result, &ctx);
                }
            }

            let mut boundary_events = {
                let mut buffer = boundary_control_events
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                std::mem::take(&mut *buffer)
            };
            results.append(&mut boundary_events);

            return Ok(results);
        }

        let mut ctx = MiddlewareContext::with_scope(self.execution_scope);

        for middleware in self.middleware_chain.iter() {
            match middleware.pre_handle(&event, &mut ctx) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(mut results) => {
                    for result in &mut results {
                        for mw in self.middleware_chain.iter() {
                            mw.pre_write(result, &ctx);
                        }
                    }

                    let mut control_events = ctx.take_control_events();
                    for control_event in &mut control_events {
                        for mw in self.middleware_chain.iter() {
                            mw.pre_write(control_event, &ctx);
                        }
                    }
                    results.extend(control_events);
                    return Ok(results);
                }
                MiddlewareAction::Abort => {
                    let mut err = event.clone();
                    err.processing_info.status = ProcessingStatus::error("aborted by middleware");
                    return Ok(vec![err]);
                }
            }
        }

        let mut results = match self.inner.process(event.clone(), effect_context).await {
            Ok(results) => results,
            Err(err) => {
                let reason = format!("Transform handler error: {err:?}");
                vec![event.clone().mark_as_error(reason, err.kind())]
            }
        };

        for middleware in self.middleware_chain.iter().rev() {
            middleware.post_handle(&event, &results, &mut ctx);
        }

        for result in &mut results {
            for middleware in self.middleware_chain.iter() {
                middleware.pre_write(result, &ctx);
            }
        }

        let mut control_events = ctx.take_control_events();
        for control_event in &mut control_events {
            for middleware in self.middleware_chain.iter() {
                middleware.pre_write(control_event, &ctx);
            }
        }
        results.extend(control_events);

        Ok(results)
    }
}

#[async_trait]
impl<H> UnifiedTransformHandler for UnifiedMiddlewareTransform<H>
where
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    async fn process(
        &self,
        event: ChainEvent,
        effect_context: Option<EffectInvocationContext>,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            tracing::debug!(
                "UnifiedMiddlewareTransform: Skipping event with Error status: {:?}",
                event.processing_info.status
            );
            return Ok(vec![event]);
        }

        self.apply_middleware(event, effect_context).await
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        self.inner.drain().await
    }

    fn stage_logic_version(&self) -> &str {
        self.inner.stage_logic_version()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::ChainEventFactory;
    use serde_json::json;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration as StdDuration;

    struct TestTransform;

    #[async_trait]
    impl TransformHandler for TestTransform {
        fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            let mut payload = event.payload().clone();
            payload["processed"] = json!(true);

            let mut new_event =
                ChainEventFactory::data_event(event.writer_id, event.event_type(), payload);
            // Copy over metadata
            new_event.flow_context = event.flow_context.clone();
            new_event.processing_info = event.processing_info.clone();
            new_event.causality = event.causality.clone();

            Ok(vec![new_event])
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    struct TestMiddleware {
        tag: String,
    }

    impl Middleware for TestMiddleware {
        fn label(&self) -> &'static str {
            "test.middleware"
        }

        fn source_phase(&self) -> crate::middleware::SourceMiddlewarePhase {
            crate::middleware::SourceMiddlewarePhase::Ordinary
        }

        fn pre_handle(&self, _event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
            println!("Pre-handle: {}", self.tag);
            ctx.emit_ephemeral_event(ChainEventFactory::metrics_state_snapshot(
                obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
                json!({"phase": "pre", "tag": self.tag}),
            ));
            MiddlewareAction::Continue
        }

        fn post_handle(
            &self,
            _event: &ChainEvent,
            results: &[ChainEvent],
            ctx: &mut MiddlewareContext,
        ) {
            println!("Post-handle: {} - {} results", self.tag, results.len());
            // Check if we can see events from earlier middleware
            let events_count = ctx.ephemeral_events().len();
            println!("Context has {events_count} events");

            // Emit a post-processing event
            ctx.emit_ephemeral_event(ChainEventFactory::metrics_state_snapshot(
                obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
                json!({"phase": "post", "tag": self.tag, "results": results.len()}),
            ));
        }
    }

    #[test]
    fn test_transform_middleware_chain() {
        let handler = TestTransform
            .middleware()
            .with(TestMiddleware {
                tag: "first".to_string(),
            })
            .with(TestMiddleware {
                tag: "second".to_string(),
            })
            .build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test",
            json!({}),
        );

        let results = TransformHandler::process(&handler, event)
            .expect("TestTransform in middleware tests should not fail");

        // Middleware can't modify events anymore, just verify the transform worked
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].payload()["processed"], json!(true));

        // The middleware would have emitted events through context,
        // but we can't verify that here without access to the context
    }

    struct ErrorObservingMiddleware {
        saw_timeout: Arc<AtomicBool>,
    }

    impl Middleware for ErrorObservingMiddleware {
        fn label(&self) -> &'static str {
            "test.error_observing_middleware"
        }

        fn source_phase(&self) -> crate::middleware::SourceMiddlewarePhase {
            crate::middleware::SourceMiddlewarePhase::Ordinary
        }

        fn post_handle(
            &self,
            _event: &ChainEvent,
            results: &[ChainEvent],
            _ctx: &mut MiddlewareContext,
        ) {
            let saw_timeout = results.iter().any(|result| {
                matches!(
                    result.processing_info.status,
                    ProcessingStatus::Error {
                        kind: Some(
                            obzenflow_core::event::status::processing_status::ErrorKind::Timeout
                        ),
                        ..
                    }
                )
            });

            if saw_timeout {
                self.saw_timeout.store(true, Ordering::SeqCst);
            }
        }
    }

    struct ErrorTransform;

    #[async_trait]
    impl TransformHandler for ErrorTransform {
        fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            Err(HandlerError::Timeout("simulated_timeout".to_string()))
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[test]
    fn test_transform_middleware_converts_handler_error_to_error_event() {
        let saw_timeout = Arc::new(AtomicBool::new(false));
        let handler = ErrorTransform
            .middleware()
            .with(ErrorObservingMiddleware {
                saw_timeout: saw_timeout.clone(),
            })
            .build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test",
            json!({}),
        );

        let results = TransformHandler::process(&handler, event)
            .expect("handler errors should be converted to error events");

        assert_eq!(results.len(), 1);
        assert!(matches!(
            results[0].processing_info.status,
            ProcessingStatus::Error {
                kind: Some(obzenflow_core::event::status::processing_status::ErrorKind::Timeout),
                ..
            }
        ));
        assert!(
            saw_timeout.load(Ordering::SeqCst),
            "middleware post_handle should observe Timeout error event"
        );
    }

    struct AsyncErrorTransform;

    #[async_trait]
    impl AsyncTransformHandler for AsyncErrorTransform {
        async fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            Err(HandlerError::Timeout("simulated_timeout".to_string()))
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_async_transform_middleware_converts_handler_error_to_error_event() {
        let saw_timeout = Arc::new(AtomicBool::new(false));
        let handler = AsyncErrorTransform
            .middleware()
            .with(ErrorObservingMiddleware {
                saw_timeout: saw_timeout.clone(),
            })
            .build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test",
            json!({}),
        );

        let results = handler
            .process(event)
            .await
            .expect("handler errors should be converted to error events");

        assert_eq!(results.len(), 1);
        assert!(matches!(
            results[0].processing_info.status,
            ProcessingStatus::Error {
                kind: Some(obzenflow_core::event::status::processing_status::ErrorKind::Timeout),
                ..
            }
        ));
        assert!(
            saw_timeout.load(Ordering::SeqCst),
            "middleware post_handle should observe Timeout error event"
        );
    }

    struct ControlEventMiddleware;

    impl Middleware for ControlEventMiddleware {
        fn label(&self) -> &'static str {
            "test.control_event_middleware"
        }

        fn source_phase(&self) -> crate::middleware::SourceMiddlewarePhase {
            crate::middleware::SourceMiddlewarePhase::Ordinary
        }

        fn pre_handle(&self, _event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
            // Emit a control event
            let writer_id = obzenflow_core::WriterId::from(obzenflow_core::StageId::new());
            ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
                writer_id,
                json!({
                    "middleware": "test_middleware",
                    "state_transition": {
                        "from": "inactive",
                        "to": "active",
                        "reason": "test"
                    }
                }),
            ));
            MiddlewareAction::Continue
        }

        fn post_handle(
            &self,
            _event: &ChainEvent,
            _results: &[ChainEvent],
            ctx: &mut MiddlewareContext,
        ) {
            // Emit another control event in post phase
            let writer_id = obzenflow_core::WriterId::from(obzenflow_core::StageId::new());
            ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
                writer_id,
                json!({
                    "queue_depth": 10,
                    "in_flight": 3
                }),
            ));
        }
    }

    #[test]
    fn test_control_events_appended() {
        let handler = TestTransform
            .middleware()
            .with(ControlEventMiddleware)
            .build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test",
            json!({}),
        );

        let results = TransformHandler::process(&handler, event)
            .expect("TestTransform in middleware tests should not fail");

        // Should have 3 events: 1 from handler + 2 control events
        assert_eq!(results.len(), 3);

        // First event is the transformed data event
        assert_eq!(results[0].payload()["processed"], json!(true));
        assert!(!results[0].is_control());

        // Second event is the control event from pre_handle
        assert!(results[1].is_lifecycle());
        assert_eq!(results[1].event_type(), "lifecycle.metrics.state");

        // Third event is the control event from post_handle
        assert!(results[2].is_lifecycle());
        assert_eq!(results[2].event_type(), "lifecycle.metrics.state");
    }

    struct SkipWithControlMiddleware;

    impl Middleware for SkipWithControlMiddleware {
        fn label(&self) -> &'static str {
            "test.skip_with_control_middleware"
        }

        fn source_phase(&self) -> crate::middleware::SourceMiddlewarePhase {
            crate::middleware::SourceMiddlewarePhase::Ordinary
        }

        fn pre_handle(&self, _event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
            // Write control event then skip
            let writer_id = obzenflow_core::WriterId::from(obzenflow_core::StageId::new());
            ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
                writer_id,
                json!({
                    "middleware": "skip_test",
                    "action": "skipped"
                }),
            ));
            MiddlewareAction::Skip(vec![ChainEventFactory::data_event(
                writer_id,
                "skipped",
                json!({"skipped": true}),
            )])
        }
    }

    #[test]
    fn test_control_events_collected_on_skip() {
        let handler = TestTransform
            .middleware()
            .with(SkipWithControlMiddleware)
            .build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test",
            json!({}),
        );

        let results = TransformHandler::process(&handler, event)
            .expect("TestTransform in middleware tests should not fail");

        // Should have 2 events: 1 skip result + 1 control event
        assert_eq!(results.len(), 2);

        // First is the skip result
        assert_eq!(results[0].event_type(), "skipped");
        assert!(!results[0].is_control());

        // Second is the control event
        assert!(results[1].is_lifecycle());
        assert_eq!(results[1].event_type(), "lifecycle.metrics.state");
    }

    struct EffectBoundaryPostControlMiddleware;

    impl Middleware for EffectBoundaryPostControlMiddleware {
        fn label(&self) -> &'static str {
            "test.effect_boundary_post_control"
        }

        fn source_phase(&self) -> crate::middleware::SourceMiddlewarePhase {
            crate::middleware::SourceMiddlewarePhase::Ordinary
        }

        fn post_handle(
            &self,
            _event: &ChainEvent,
            _results: &[ChainEvent],
            ctx: &mut MiddlewareContext,
        ) {
            ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
                obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
                json!({"boundary": "post"}),
            ));
        }
    }

    struct EffectBoundarySkipControlMiddleware;

    impl Middleware for EffectBoundarySkipControlMiddleware {
        fn label(&self) -> &'static str {
            "test.effect_boundary_skip_control"
        }

        fn source_phase(&self) -> crate::middleware::SourceMiddlewarePhase {
            crate::middleware::SourceMiddlewarePhase::Ordinary
        }

        fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
            ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
                obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
                json!({"boundary": "skip"}),
            ));
            MiddlewareAction::Skip(vec![ChainEventFactory::data_event(
                event.writer_id,
                "test.effect_fallback",
                json!({"effect_value": 42}),
            )])
        }
    }

    #[test]
    fn effect_boundary_after_effect_returns_control_events() {
        let middleware: Arc<dyn Middleware> = Arc::new(EffectBoundaryPostControlMiddleware);
        let boundary = MiddlewareEffectBoundary {
            middleware_chain: Arc::new(vec![middleware]),
        };
        let writer_id = obzenflow_core::WriterId::from(obzenflow_core::StageId::new());
        let event = ChainEventFactory::data_event(writer_id, "test.input", json!({}));
        let output = ChainEventFactory::data_event(writer_id, "test.output", json!({}));

        let start = boundary.before_effect(&event);
        assert_eq!(start.control_events.len(), 0);
        assert!(matches!(start.action, EffectBoundaryAction::Continue));

        let control_events = boundary.after_effect(start.context, &event, &[output]);
        assert_eq!(control_events.len(), 1);
        assert!(control_events[0].is_lifecycle());
        assert_eq!(control_events[0].event_type(), "lifecycle.metrics.state");
    }

    #[test]
    fn effect_boundary_skip_returns_control_events() {
        let middleware: Arc<dyn Middleware> = Arc::new(EffectBoundarySkipControlMiddleware);
        let boundary = MiddlewareEffectBoundary {
            middleware_chain: Arc::new(vec![middleware]),
        };
        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test.input",
            json!({}),
        );

        let start = boundary.before_effect(&event);
        match start.action {
            EffectBoundaryAction::Skip(results) => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].payload()["effect_value"], json!(42));
            }
            _ => panic!("boundary middleware should skip with fallback"),
        }
        assert_eq!(start.control_events.len(), 1);
        assert!(start.control_events[0].is_lifecycle());
        assert_eq!(
            start.control_events[0].event_type(),
            "lifecycle.metrics.state"
        );
    }

    #[derive(Clone, Debug)]
    struct FlakyAsyncTransform {
        calls: Arc<AtomicUsize>,
        fail_times: usize,
    }

    #[async_trait]
    impl AsyncTransformHandler for FlakyAsyncTransform {
        async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n < self.fail_times {
                Err(HandlerError::Remote("transient".to_string()))
            } else {
                Ok(vec![event])
            }
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn async_middleware_transform_retries_via_circuit_breaker_context() {
        use crate::middleware::control::{CircuitBreakerBuilder, ControlMiddlewareAggregator};
        use crate::middleware::MiddlewareFactory;
        use obzenflow_core::StageId;
        use obzenflow_runtime::pipeline::config::StageConfig;

        let calls = Arc::new(AtomicUsize::new(0));
        let handler = FlakyAsyncTransform {
            calls: calls.clone(),
            fail_times: 2,
        };

        let factory = CircuitBreakerBuilder::new(5)
            .with_retry_fixed(StdDuration::from_millis(1), 3)
            .build();

        let config = StageConfig {
            stage_id: StageId::new(),
            name: "test".to_string(),
            flow_name: "test".to_string(),
            cycle_guard: None,
        };
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let cb = factory
            .create(&config, control)
            .expect("circuit breaker should materialize for async transform retry test");

        let wrapped = handler.middleware().with(cb).build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(StageId::new()),
            "test",
            json!({}),
        );

        let results = wrapped
            .process(event)
            .await
            .expect("handler errors should be retried and eventually succeed");

        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let data_events = results.iter().filter(|e| e.is_data()).count();
        let control_events = results.iter().filter(|e| e.is_lifecycle()).count();
        assert_eq!(data_events, 1);
        assert_eq!(control_events, 3);
    }

    #[tokio::test]
    async fn async_middleware_transform_exhausts_retries_and_returns_error_event() {
        use crate::middleware::control::{CircuitBreakerBuilder, ControlMiddlewareAggregator};
        use crate::middleware::MiddlewareFactory;
        use obzenflow_core::StageId;
        use obzenflow_runtime::pipeline::config::StageConfig;

        let calls = Arc::new(AtomicUsize::new(0));
        let handler = FlakyAsyncTransform {
            calls: calls.clone(),
            fail_times: usize::MAX,
        };

        let factory = CircuitBreakerBuilder::new(5)
            .with_retry_fixed(StdDuration::from_millis(1), 3)
            .build();

        let config = StageConfig {
            stage_id: StageId::new(),
            name: "test".to_string(),
            flow_name: "test".to_string(),
            cycle_guard: None,
        };
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let cb = factory
            .create(&config, control)
            .expect("circuit breaker should materialize for async transform exhaustion test");

        let wrapped = handler.middleware().with(cb).build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(StageId::new()),
            "test",
            json!({}),
        );

        let results = wrapped
            .process(event)
            .await
            .expect("handler errors should be converted into error events");

        assert_eq!(calls.load(Ordering::SeqCst), 3);

        let error_events = results
            .iter()
            .filter(|e| matches!(e.processing_info.status, ProcessingStatus::Error { .. }))
            .count();
        let control_events = results.iter().filter(|e| e.is_lifecycle()).count();
        assert_eq!(error_events, 1);
        assert_eq!(control_events, 3);
    }
}

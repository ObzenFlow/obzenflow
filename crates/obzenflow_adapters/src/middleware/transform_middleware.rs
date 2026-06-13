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
    observation_short_circuit, Middleware, MiddlewareAction, MiddlewareContext,
};
use async_trait::async_trait;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::{
    EffectFailureCause, EffectFailureCode, EffectFailureSource, RetryDisposition,
};
use obzenflow_core::ChainEvent;
use obzenflow_core::MiddlewareExecutionScope;
use obzenflow_runtime::effects::{
    EffectAbortReason, EffectBoundaryMiddleware, EffectBoundaryOutcome, EffectBoundaryReport,
    EffectExecution, EffectIdentity, EffectInvocationContext,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use obzenflow_runtime::stages::common::handlers::{AsyncTransformHandler, TransformHandler};
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A TransformHandler wrapper that applies middleware to transform operations.
///
/// Implements `UnifiedTransformHandler` directly (not `TransformHandler`, whose
/// blanket impl would conflict) so the supervisor's per-event execution scope
/// reaches the middleware context (FLOWIP-120c H3).
#[derive(Clone)]
pub struct MiddlewareTransform<H: TransformHandler> {
    inner: H,
    middleware_chain: Arc<Vec<Arc<dyn Middleware>>>,
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
        }
    }

    /// Add middleware to the chain
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        Arc::make_mut(&mut self.middleware_chain).push(Arc::from(middleware));
        self
    }

    /// Apply the middleware chain to a transform operation
    fn apply_middleware<F>(
        &self,
        event: ChainEvent,
        scope: MiddlewareExecutionScope,
        transform_fn: F,
    ) -> Result<Vec<ChainEvent>, HandlerError>
    where
        F: FnOnce(ChainEvent) -> Result<Vec<ChainEvent>, HandlerError>,
    {
        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::with_scope(scope);

        // Pre-processing phase
        for middleware in self.middleware_chain.iter() {
            let action = middleware.pre_handle(&event, &mut ctx);
            if let Some(message) = observation_short_circuit(middleware.as_ref(), &action) {
                return Err(HandlerError::Other(message));
            }
            match action {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip { mut results, .. } => {
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
                MiddlewareAction::Abort { .. } => {
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
impl<H: TransformHandler> UnifiedTransformHandler for MiddlewareTransform<H> {
    async fn process(
        &self,
        event: ChainEvent,
        _effect_context: Option<EffectInvocationContext>,
        scope: MiddlewareExecutionScope,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        // Short-circuit if event already has Error status
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            tracing::debug!(
                "MiddlewareTransform: Skipping event with Error status: {:?}",
                event.processing_info.status
            );
            return Ok(vec![event]);
        }
        self.apply_middleware(event, scope, |e| self.inner.process(e))
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
        }
    }

    /// Add middleware to the chain.
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        Arc::make_mut(&mut self.middleware_chain).push(Arc::from(middleware));
        self
    }

    async fn apply_middleware<F, Fut>(
        &self,
        event: ChainEvent,
        scope: MiddlewareExecutionScope,
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
            let mut ctx = MiddlewareContext::with_scope(scope);
            ctx.insert::<CircuitBreakerAttempt>(attempt);

            // Pre-processing phase
            let mut short_circuit: Option<Vec<ChainEvent>> = None;
            for (visited, middleware) in self.middleware_chain.iter().enumerate() {
                let action = middleware.pre_handle(&original, &mut ctx);
                if let Some(message) = observation_short_circuit(middleware.as_ref(), &action) {
                    return Err(HandlerError::Other(message));
                }
                match action {
                    MiddlewareAction::Continue => continue,
                    MiddlewareAction::Skip { mut results, cause } => {
                        // FLOWIP-114q transitional hook: notify the visited
                        // prefix before control events are drained so hook
                        // emissions ride the same context.
                        for mw in self.middleware_chain[..visited].iter() {
                            mw.on_rejected(&original, cause.as_ref(), &mut ctx);
                        }
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
                    MiddlewareAction::Abort { cause } => {
                        for mw in self.middleware_chain[..visited].iter() {
                            mw.on_rejected(&original, cause.as_ref(), &mut ctx);
                        }
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
impl<H: AsyncTransformHandler> UnifiedTransformHandler for AsyncMiddlewareTransform<H> {
    async fn process(
        &self,
        event: ChainEvent,
        _effect_context: Option<EffectInvocationContext>,
        scope: MiddlewareExecutionScope,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        // Short-circuit if event already has Error status
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            tracing::debug!(
                "AsyncMiddlewareTransform: Skipping event with Error status: {:?}",
                event.processing_info.status
            );
            return Ok(vec![event]);
        }

        self.apply_middleware(event, scope, |e| self.inner.process(e))
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

#[async_trait]
impl EffectBoundaryMiddleware for MiddlewareEffectBoundary {
    /// Transitional whole-chain boundary (FLOWIP-120a shape on the FLOWIP-120c
    /// seam): the stage's full middleware chain runs `pre_handle` as admission
    /// and `post_handle` as observation around the execution future. Retired
    /// by the placement split, which installs per-effect policy chains
    /// instead. Legacy semantics are preserved deliberately: a short-circuit
    /// does not finalize the visited prefix, and a rate limiter's delay blocks
    /// this future the way it blocked the worker before the seam.
    async fn around_effect(
        &self,
        _identity: &EffectIdentity,
        event: &ChainEvent,
        execute: EffectExecution,
    ) -> EffectBoundaryReport {
        // The effect boundary is reached only when the effect is executing live
        // (strict replay returns the recorded outcome before consulting it), so
        // control middleware here must run and protect the live call.
        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);

        for middleware in self.middleware_chain.iter() {
            let action = middleware.pre_handle(event, &mut ctx);
            if let Some(message) = observation_short_circuit(middleware.as_ref(), &action) {
                // A misclassified short-circuit at the boundary would author
                // synthesized outcome facts from arbitrary user code; reject
                // it as a recorded failure so replay reproduces the error.
                let control_events = self.drain_control_events(&mut ctx);
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Aborted(EffectAbortReason {
                        cause: EffectFailureCause {
                            source: EffectFailureSource::new(middleware.label()),
                            code: EffectFailureCode::new("observation_short_circuit"),
                        },
                        message,
                        retry: RetryDisposition::NotRetryable,
                    }),
                    control_events,
                };
            }
            match action {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip { results, .. } => {
                    let control_events = self.drain_control_events(&mut ctx);
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Skipped {
                            results,
                            source: Some(middleware.label().to_string()),
                        },
                        control_events,
                    };
                }
                MiddlewareAction::Abort { cause } => {
                    let control_events = self.drain_control_events(&mut ctx);
                    let reason = match cause {
                        Some(cause) => EffectAbortReason {
                            cause: EffectFailureCause {
                                source: cause.source,
                                code: cause.code,
                            },
                            message: cause.message,
                            retry: cause.retry,
                        },
                        None => EffectAbortReason {
                            cause: EffectFailureCause {
                                source: EffectFailureSource::new(middleware.label()),
                                code: EffectFailureCode::new("aborted"),
                            },
                            message: format!(
                                "middleware '{}' aborted effect execution",
                                middleware.label()
                            ),
                            retry: RetryDisposition::NotRetryable,
                        },
                    };
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Aborted(reason),
                        control_events,
                    };
                }
            }
        }

        let result = execute.await;
        match &result {
            Ok(outputs) => {
                for middleware in self.middleware_chain.iter().rev() {
                    middleware.post_handle(event, outputs, &mut ctx);
                }
            }
            Err(err) => {
                let error_event = event.clone().mark_as_error(
                    err.to_string(),
                    obzenflow_core::event::status::processing_status::ErrorKind::Remote,
                );
                for middleware in self.middleware_chain.iter().rev() {
                    middleware.post_handle(event, std::slice::from_ref(&error_event), &mut ctx);
                }
            }
        }
        let control_events = self.drain_control_events(&mut ctx);
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(result),
            control_events,
        }
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
        }
    }

    /// Add middleware to the chain.
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        Arc::make_mut(&mut self.middleware_chain).push(Arc::from(middleware));
        self
    }

    async fn apply_middleware(
        &self,
        event: ChainEvent,
        effect_context: Option<EffectInvocationContext>,
        scope: MiddlewareExecutionScope,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if let Some(mut effect_context) = effect_context {
            let boundary_control_events = effect_context.boundary_control_events.clone();
            effect_context.effect_boundary = Some(Arc::new(MiddlewareEffectBoundary {
                middleware_chain: self.middleware_chain.clone(),
            }));

            let mut results = match self
                .inner
                .process(event.clone(), Some(effect_context), scope)
                .await
            {
                Ok(results) => results,
                Err(err) => {
                    let reason = format!("Transform handler error: {err:?}");
                    vec![event.clone().mark_as_error(reason, err.kind())]
                }
            };

            let ctx = MiddlewareContext::with_scope(scope);
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

        let mut ctx = MiddlewareContext::with_scope(scope);

        for middleware in self.middleware_chain.iter() {
            let action = middleware.pre_handle(&event, &mut ctx);
            if let Some(message) = observation_short_circuit(middleware.as_ref(), &action) {
                return Err(HandlerError::Other(message));
            }
            match action {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip { mut results, .. } => {
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
                MiddlewareAction::Abort { .. } => {
                    let mut err = event.clone();
                    err.processing_info.status = ProcessingStatus::error("aborted by middleware");
                    return Ok(vec![err]);
                }
            }
        }

        let mut results = match self.inner.process(event.clone(), effect_context, scope).await {
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
        scope: MiddlewareExecutionScope,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            tracing::debug!(
                "UnifiedMiddlewareTransform: Skipping event with Error status: {:?}",
                event.processing_info.status
            );
            return Ok(vec![event]);
        }

        self.apply_middleware(event, effect_context, scope).await
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
    use crate::middleware::MiddlewareAbortCause;
    use async_trait::async_trait;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::StageId;
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

    #[tokio::test]
    async fn test_transform_middleware_chain() {
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

        let results = UnifiedTransformHandler::process(
            &handler,
            event,
            None,
            MiddlewareExecutionScope::LiveHandler,
        )
        .await
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

    #[tokio::test]
    async fn test_transform_middleware_converts_handler_error_to_error_event() {
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

        let results = UnifiedTransformHandler::process(
            &handler,
            event,
            None,
            MiddlewareExecutionScope::LiveHandler,
        )
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
            .process(event, None, MiddlewareExecutionScope::LiveHandler)
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

    #[tokio::test]
    async fn test_control_events_appended() {
        let handler = TestTransform
            .middleware()
            .with(ControlEventMiddleware)
            .build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test",
            json!({}),
        );

        let results = UnifiedTransformHandler::process(
            &handler,
            event,
            None,
            MiddlewareExecutionScope::LiveHandler,
        )
        .await
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

        fn kind(&self) -> crate::middleware::MiddlewareKind {
            // Exercises Skip mechanics, so it declares the policy kind.
            crate::middleware::MiddlewareKind::Policy
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
            MiddlewareAction::Skip {
                results: vec![ChainEventFactory::data_event(
                    writer_id,
                    "skipped",
                    json!({"skipped": true}),
                )],
                cause: None,
            }
        }
    }

    #[tokio::test]
    async fn test_control_events_collected_on_skip() {
        let handler = TestTransform
            .middleware()
            .with(SkipWithControlMiddleware)
            .build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test",
            json!({}),
        );

        let results = UnifiedTransformHandler::process(
            &handler,
            event,
            None,
            MiddlewareExecutionScope::LiveHandler,
        )
        .await
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

        fn kind(&self) -> crate::middleware::MiddlewareKind {
            // Exercises boundary Skip mechanics, so it declares the policy kind.
            crate::middleware::MiddlewareKind::Policy
        }

        fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
            ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
                obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
                json!({"boundary": "skip"}),
            ));
            MiddlewareAction::Skip {
                results: vec![ChainEventFactory::data_event(
                    event.writer_id,
                    "test.effect_fallback",
                    json!({"effect_value": 42}),
                )],
                cause: None,
            }
        }
    }

    fn test_effect_identity() -> EffectIdentity {
        EffectIdentity {
            effect_type: "test.effect",
            safety: obzenflow_runtime::effects::EffectSafety::Idempotent,
            cursor: obzenflow_runtime::effects::EffectCursor::new(
                "test_flow",
                "test_stage",
                1,
                0,
            ),
            idempotency_key: None,
        }
    }

    #[tokio::test]
    async fn effect_boundary_executed_returns_post_handle_control_events() {
        let middleware: Arc<dyn Middleware> = Arc::new(EffectBoundaryPostControlMiddleware);
        let boundary = MiddlewareEffectBoundary {
            middleware_chain: Arc::new(vec![middleware]),
        };
        let writer_id = obzenflow_core::WriterId::from(StageId::new());
        let event = ChainEventFactory::data_event(writer_id, "test.input", json!({}));
        let output = ChainEventFactory::data_event(writer_id, "test.output", json!({}));

        let execute: EffectExecution = Box::pin(async move { Ok(vec![output]) });
        let report = boundary
            .around_effect(&test_effect_identity(), &event, execute)
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
        assert_eq!(report.control_events.len(), 1);
        assert!(report.control_events[0].is_lifecycle());
        assert_eq!(
            report.control_events[0].event_type(),
            "lifecycle.metrics.state"
        );
    }

    #[tokio::test]
    async fn effect_boundary_skip_returns_control_events_without_polling_execution() {
        let middleware: Arc<dyn Middleware> = Arc::new(EffectBoundarySkipControlMiddleware);
        let boundary = MiddlewareEffectBoundary {
            middleware_chain: Arc::new(vec![middleware]),
        };
        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(StageId::new()),
            "test.input",
            json!({}),
        );

        let polled = Arc::new(AtomicBool::new(false));
        let polled_probe = polled.clone();
        let execute: EffectExecution = Box::pin(async move {
            polled_probe.store(true, Ordering::SeqCst);
            Ok(Vec::new())
        });

        let report = boundary
            .around_effect(&test_effect_identity(), &event, execute)
            .await;

        match report.outcome {
            EffectBoundaryOutcome::Skipped { results, source } => {
                assert_eq!(results.len(), 1);
                assert_eq!(results[0].payload()["effect_value"], json!(42));
                assert_eq!(source.as_deref(), Some("test.effect_boundary_skip_control"));
            }
            _ => panic!("boundary middleware should skip with fallback"),
        }
        assert!(
            !polled.load(Ordering::SeqCst),
            "a skip must short-circuit without polling the execution future"
        );
        assert_eq!(report.control_events.len(), 1);
        assert!(report.control_events[0].is_lifecycle());
        assert_eq!(
            report.control_events[0].event_type(),
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
            .process(event, None, MiddlewareExecutionScope::LiveHandler)
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
            .process(event, None, MiddlewareExecutionScope::LiveHandler)
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

    /// FLOWIP-114q transitional hook: middleware that returned `Continue`
    /// before a later short-circuit must see `on_rejected`; the rejecting
    /// middleware and everything after it must not.
    struct RejectionRecorder {
        label: &'static str,
        rejected: Arc<AtomicUsize>,
        saw_cause: Arc<AtomicBool>,
    }

    impl Middleware for RejectionRecorder {
        fn label(&self) -> &'static str {
            self.label
        }

        fn source_phase(&self) -> crate::middleware::SourceMiddlewarePhase {
            crate::middleware::SourceMiddlewarePhase::Ordinary
        }

        fn on_rejected(
            &self,
            _event: &ChainEvent,
            cause: Option<&MiddlewareAbortCause>,
            _ctx: &mut MiddlewareContext,
        ) {
            self.rejected.fetch_add(1, Ordering::SeqCst);
            if cause.is_some() {
                self.saw_cause.store(true, Ordering::SeqCst);
            }
        }
    }

    struct AbortingMiddleware;

    impl Middleware for AbortingMiddleware {
        fn label(&self) -> &'static str {
            "test.aborting"
        }

        fn source_phase(&self) -> crate::middleware::SourceMiddlewarePhase {
            crate::middleware::SourceMiddlewarePhase::Ordinary
        }

        fn kind(&self) -> crate::middleware::MiddlewareKind {
            // Exercises Abort mechanics, so it declares the policy kind.
            crate::middleware::MiddlewareKind::Policy
        }

        fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
            MiddlewareAction::Abort {
                cause: Some(MiddlewareAbortCause {
                    source: EffectFailureSource::new("test.aborting"),
                    code: EffectFailureCode::new("aborted"),
                    message: "test abort".to_string(),
                    retry: RetryDisposition::NotRetryable,
                    event: None,
                }),
            }
        }
    }

    struct AsyncEcho;

    #[async_trait]
    impl AsyncTransformHandler for AsyncEcho {
        async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![event])
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn on_rejected_fires_on_visited_prefix_only() {
        let visited_count = Arc::new(AtomicUsize::new(0));
        let visited_cause = Arc::new(AtomicBool::new(false));
        let unvisited_count = Arc::new(AtomicUsize::new(0));

        let wrapped = AsyncMiddlewareTransform::new(AsyncEcho)
            .with_middleware(Box::new(RejectionRecorder {
                label: "test.visited",
                rejected: visited_count.clone(),
                saw_cause: visited_cause.clone(),
            }))
            .with_middleware(Box::new(AbortingMiddleware))
            .with_middleware(Box::new(RejectionRecorder {
                label: "test.unvisited",
                rejected: unvisited_count.clone(),
                saw_cause: Arc::new(AtomicBool::new(false)),
            }));

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(StageId::new()),
            "test",
            json!({}),
        );

        wrapped
            .process(event, None, MiddlewareExecutionScope::LiveHandler)
            .await
            .expect("abort short-circuit returns an error-marked event, not Err");

        assert_eq!(
            visited_count.load(Ordering::SeqCst),
            1,
            "middleware before the rejecting one sees on_rejected exactly once"
        );
        assert!(
            visited_cause.load(Ordering::SeqCst),
            "the abort cause reaches the visited prefix"
        );
        assert_eq!(
            unvisited_count.load(Ordering::SeqCst),
            0,
            "middleware after the rejecting one never sees on_rejected"
        );
    }

    /// G6 regression (FLOWIP-120c): the effectful results path builds its
    /// `pre_write` context with the stage execution scope, not the
    /// `LiveHandler` default.
    mod effectful_pre_write_scope {
        use super::*;
        use obzenflow_core::event::EventEnvelope;
        use obzenflow_core::journal::{Journal, JournalError, JournalReader};
        use obzenflow_core::{EventId, JournalId, JournalOwner, JournalWriterId};
        use obzenflow_runtime::backpressure::BackpressureWriter;
        use obzenflow_runtime::effects::{EffectPortRegistry, EffectRuntimeMode};
        use obzenflow_runtime::feed_plan::StageOutputContract;
        use obzenflow_runtime::messaging::upstream_subscription::StageInputPosition;
        use obzenflow_runtime::stages::common::handlers::transform::traits::UnifiedTransformHandler;
        use std::sync::Mutex;

        struct MemoryJournal {
            id: JournalId,
            owner: Option<JournalOwner>,
            events: Mutex<Vec<EventEnvelope<ChainEvent>>>,
        }

        impl MemoryJournal {
            fn new(owner: JournalOwner) -> Self {
                Self {
                    id: JournalId::new(),
                    owner: Some(owner),
                    events: Mutex::new(Vec::new()),
                }
            }
        }

        struct EmptyReader;

        #[async_trait]
        impl JournalReader<ChainEvent> for EmptyReader {
            async fn next(&mut self) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
                Ok(None)
            }

            async fn skip(&mut self, _n: u64) -> Result<u64, JournalError> {
                Ok(0)
            }

            fn position(&self) -> u64 {
                0
            }
        }

        #[async_trait]
        impl Journal<ChainEvent> for MemoryJournal {
            fn id(&self) -> &JournalId {
                &self.id
            }

            fn owner(&self) -> Option<&JournalOwner> {
                self.owner.as_ref()
            }

            async fn append(
                &self,
                event: ChainEvent,
                _parent: Option<&EventEnvelope<ChainEvent>>,
            ) -> Result<EventEnvelope<ChainEvent>, JournalError> {
                let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
                self.events
                    .lock()
                    .expect("events lock poisoned")
                    .push(envelope.clone());
                Ok(envelope)
            }

            async fn read_causally_ordered(
                &self,
            ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
                Ok(self.events.lock().expect("events lock poisoned").clone())
            }

            async fn read_causally_after(
                &self,
                _after_event_id: &EventId,
            ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
                Ok(Vec::new())
            }

            async fn read_event(
                &self,
                _event_id: &EventId,
            ) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
                Ok(None)
            }

            async fn reader(&self) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
                Ok(Box::new(EmptyReader))
            }

            async fn reader_from(
                &self,
                _position: u64,
            ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
                Ok(Box::new(EmptyReader))
            }

            async fn read_last_n(
                &self,
                _count: usize,
            ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
                Ok(Vec::new())
            }
        }

        struct ScopeProbe {
            seen: Arc<Mutex<Vec<MiddlewareExecutionScope>>>,
        }

        impl Middleware for ScopeProbe {
            fn label(&self) -> &'static str {
                "test.scope_probe"
            }

            fn source_phase(&self) -> crate::middleware::SourceMiddlewarePhase {
                crate::middleware::SourceMiddlewarePhase::Ordinary
            }

            fn pre_write(&self, _event: &mut ChainEvent, ctx: &MiddlewareContext) {
                self.seen
                    .lock()
                    .expect("seen lock poisoned")
                    .push(ctx.execution_scope());
            }
        }

        #[derive(Clone, Debug)]
        struct UnifiedEcho;

        #[async_trait]
        impl UnifiedTransformHandler for UnifiedEcho {
            async fn process(
                &self,
                event: ChainEvent,
                _effect_context: Option<EffectInvocationContext>,
                _scope: MiddlewareExecutionScope,
            ) -> Result<Vec<ChainEvent>, HandlerError> {
                Ok(vec![event])
            }

            async fn drain(&mut self) -> Result<(), HandlerError> {
                Ok(())
            }
        }

        fn effect_context(journal: Arc<MemoryJournal>) -> EffectInvocationContext {
            let stage_id = StageId::new();
            let writer_id = obzenflow_core::WriterId::from(stage_id);
            let parent = EventEnvelope::new(
                JournalWriterId::from(*journal.id()),
                ChainEventFactory::data_event(writer_id, "test.parent", json!({})),
            );
            EffectInvocationContext {
                flow_id: obzenflow_core::FlowId::new(),
                stage_id,
                stage_key: "effect_stage".to_string(),
                writer_id,
                input_seq: StageInputPosition(1),
                stage_logic_version: "test-v1".to_string(),
                data_journal: journal,
                flow_context: None,
                system_journal: None,
                instrumentation: None,
                heartbeat_state: None,
                parent,
                effect_history: None,
                effect_runtime_mode: EffectRuntimeMode::ReplayStrict,
                effect_ports: EffectPortRegistry::new(),
                effect_declarations: Vec::new(),
                synthesized_outcomes: Vec::new(),
                output_contract: StageOutputContract::empty(),
                backpressure_writer: BackpressureWriter::disabled(),
                emit_enabled: false,
                effect_boundary: None,
                boundary_control_events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// FLOWIP-120c H3: the scope is a per-call decision, so consecutive
        /// events through one wrapper can carry different scopes. This is
        /// the property FLOWIP-120n's resume phase predicate relies on.
        #[tokio::test]
        async fn consecutive_events_carry_independent_scopes() {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let wrapped = UnifiedMiddlewareTransform::new(UnifiedEcho)
                .with_middleware(Box::new(ScopeProbe { seen: seen.clone() }));

            let make_event = || {
                ChainEventFactory::data_event(
                    obzenflow_core::WriterId::from(StageId::new()),
                    "test",
                    json!({}),
                )
            };

            wrapped
                .process(
                    make_event(),
                    None,
                    MiddlewareExecutionScope::StrictReplayHandler,
                )
                .await
                .expect("echo handler should not fail");
            wrapped
                .process(make_event(), None, MiddlewareExecutionScope::LiveHandler)
                .await
                .expect("echo handler should not fail");

            let scopes = seen.lock().expect("seen lock poisoned").clone();
            assert_eq!(
                scopes,
                vec![
                    MiddlewareExecutionScope::StrictReplayHandler,
                    MiddlewareExecutionScope::LiveHandler,
                ],
                "each dispatch scopes its context independently"
            );
        }

        #[tokio::test]
        async fn effectful_results_pre_write_carries_stage_scope() {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let wrapped = UnifiedMiddlewareTransform::new(UnifiedEcho)
                .with_middleware(Box::new(ScopeProbe { seen: seen.clone() }));

            let journal = Arc::new(MemoryJournal::new(JournalOwner::stage(StageId::new())));
            let event = ChainEventFactory::data_event(
                obzenflow_core::WriterId::from(StageId::new()),
                "test",
                json!({}),
            );

            wrapped
                .process(
                    event,
                    Some(effect_context(journal)),
                    MiddlewareExecutionScope::StrictReplayHandler,
                )
                .await
                .expect("echo handler should not fail");

            let scopes = seen.lock().expect("seen lock poisoned").clone();
            assert!(
                !scopes.is_empty(),
                "pre_write probe should observe at least one result event"
            );
            assert!(
                scopes
                    .iter()
                    .all(|s| *s == MiddlewareExecutionScope::StrictReplayHandler),
                "effectful pre_write context must carry the stage scope, got {scopes:?}"
            );
        }
    }
}

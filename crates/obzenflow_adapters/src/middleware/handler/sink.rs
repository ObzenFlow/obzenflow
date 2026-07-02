// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Middleware adapter for SinkHandler
//!
//! This module provides middleware capabilities for SinkHandler implementations,
//! with special focus on error handling and retry logic.

use crate::middleware::{ErrorAction, Middleware, MiddlewareAction, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::ChainEvent;
use obzenflow_core::MiddlewareExecutionScope;
use obzenflow_runtime::effects::EffectInvocationContext;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkConsumeReport, SinkHandler, SinkLifecycleReport, UnifiedSinkHandler,
};
use std::sync::Arc;

/// A SinkHandler wrapper that applies middleware.
///
/// Implements `UnifiedSinkHandler` directly (not `SinkHandler`, whose blanket
/// impl would conflict) so the supervisor's per-event execution scope reaches
/// the middleware context (FLOWIP-120c H3).
#[derive(Clone)]
pub struct MiddlewareSink<H: SinkHandler> {
    inner: H,
    middleware_chain: Arc<Vec<Arc<dyn Middleware>>>,
}

impl<H: SinkHandler> std::fmt::Debug for MiddlewareSink<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiddlewareSink")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .finish()
    }
}

impl<H: SinkHandler> MiddlewareSink<H> {
    /// Create a new middleware-wrapped sink handler
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

    /// Apply middleware with error handling
    async fn apply_middleware_with_error_handling(
        &mut self,
        event: ChainEvent,
        scope: MiddlewareExecutionScope,
    ) -> Result<SinkConsumeReport, HandlerError> {
        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::with_scope(scope);

        // Pre-processing phase
        for middleware in self.middleware_chain.iter() {
            let action = middleware.pre_handle(&event, &mut ctx);
            if let Some(message) =
                crate::middleware::observation_short_circuit(middleware.as_ref(), &action)
            {
                return Err(HandlerError::Other(message));
            }
            match action {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip { .. } => {
                    // Skip means don't consume - return success with no delivery
                    return Ok(SinkConsumeReport::new(DeliveryPayload::success(
                        DeliveryMethod::Noop,
                        None, // bytes_processed
                    )));
                }
                MiddlewareAction::Abort { .. } => {
                    // Abort is also treated as success but with a different message
                    return Ok(SinkConsumeReport::new(DeliveryPayload::success(
                        DeliveryMethod::Noop,
                        None, // bytes_processed
                    )));
                }
            }
        }

        // Consume with inner handler
        match self.inner.consume_report(event.clone()).await {
            Ok(report) => {
                // Post-processing phase - sinks don't produce output events
                let empty = vec![];
                for middleware in self.middleware_chain.iter() {
                    middleware.post_handle(&event, &empty, &mut ctx);
                }

                Ok(report)
            }
            Err(e) => {
                // Give each middleware a chance to handle the error
                for middleware in self.middleware_chain.iter() {
                    match middleware.on_error(&event, &mut ctx) {
                        ErrorAction::Propagate => continue,
                        ErrorAction::Recover(_) => {
                            // Recovery means success
                            return Ok(SinkConsumeReport::new(DeliveryPayload::success(
                                DeliveryMethod::Noop,
                                None, // bytes_processed
                            )));
                        }
                        ErrorAction::Retry => {
                            // Simple retry once - in production, might want exponential backoff
                            return self.inner.consume_report(event).await;
                        }
                    }
                }

                // If no middleware handled it, propagate the error
                Err(e)
            }
        }
    }
}

#[async_trait]
impl<H: SinkHandler> UnifiedSinkHandler for MiddlewareSink<H> {
    async fn consume_report(
        &mut self,
        event: ChainEvent,
        _effect_context: Option<EffectInvocationContext>,
        scope: MiddlewareExecutionScope,
    ) -> Result<SinkConsumeReport, HandlerError> {
        self.apply_middleware_with_error_handling(event, scope)
            .await
    }

    async fn flush_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        // Flush is not intercepted by middleware - it's an infrastructure concern
        self.inner.flush_report().await
    }

    async fn drain_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        // Drain is not intercepted by middleware - it's an infrastructure concern
        self.inner.drain_report().await
    }

    // No declaration hooks exist on `UnifiedSinkHandler` to forward:
    // declarations are descriptor-snapshotted pre-wrap (FLOWIP-120s), so a
    // wrapper cannot attenuate them.
}

impl<H: SinkHandler> MiddlewareSink<H> {
    /// Test-only method to access the inner handler
    #[cfg(test)]
    pub fn inner(&self) -> &H {
        &self.inner
    }
}

/// Extension trait to add middleware capabilities to any SinkHandler
pub trait SinkHandlerExt: SinkHandler + Sized {
    /// Start building a middleware chain for this handler
    fn middleware(self) -> SinkMiddlewareBuilder<Self> {
        SinkMiddlewareBuilder::new(self)
    }
}

// Implement for all SinkHandlers
impl<T: SinkHandler> SinkHandlerExt for T {}

/// Builder for constructing middleware chains around sink handlers
pub struct SinkMiddlewareBuilder<H: SinkHandler> {
    handler: MiddlewareSink<H>,
}

impl<H: SinkHandler> SinkMiddlewareBuilder<H> {
    fn new(inner: H) -> Self {
        Self {
            handler: MiddlewareSink::new(inner),
        }
    }

    /// Add a middleware to the chain
    pub fn with<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.handler = self.handler.with_middleware(Box::new(middleware));
        self
    }

    /// Build the final middleware-wrapped handler
    pub fn build(self) -> MiddlewareSink<H> {
        self.handler
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::delivery_payload::DeliveryPayload;
    use obzenflow_core::event::ChainEventFactory;
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    struct TestSink {
        consumed: Arc<Mutex<Vec<ChainEvent>>>,
        fail_count: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl SinkHandler for TestSink {
        async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
            let mut fail_count = self.fail_count.lock().unwrap();
            if *fail_count > 0 {
                *fail_count -= 1;
                return Err(HandlerError::other("Simulated failure"));
            }

            self.consumed.lock().unwrap().push(event);
            Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
        }
    }

    struct RetryMiddleware;

    impl Middleware for RetryMiddleware {
        fn label(&self) -> &'static str {
            "test.retry_middleware"
        }

        fn source_phase(&self) -> crate::middleware::SourceMiddlewarePhase {
            crate::middleware::SourceMiddlewarePhase::Ordinary
        }

        fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
            ErrorAction::Retry
        }
    }

    #[tokio::test]
    async fn test_sink_retry_middleware() {
        let consumed = Arc::new(Mutex::new(Vec::new()));
        let fail_count = Arc::new(Mutex::new(1)); // Fail once

        let mut handler = TestSink {
            consumed: consumed.clone(),
            fail_count: fail_count.clone(),
        }
        .middleware()
        .with(RetryMiddleware)
        .build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test",
            json!({"data": "test"}),
        );

        // Should succeed after retry
        handler
            .consume_report(event.clone(), None, MiddlewareExecutionScope::LiveHandler)
            .await
            .expect("retry middleware should recover the failed consume");
        assert_eq!(consumed.lock().unwrap().len(), 1);
    }
}

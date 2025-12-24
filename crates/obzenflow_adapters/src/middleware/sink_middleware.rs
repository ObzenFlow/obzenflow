//! Middleware adapter for SinkHandler
//!
//! This module provides middleware capabilities for SinkHandler implementations,
//! with special focus on error handling and retry logic.

use super::{ErrorAction, Middleware, MiddlewareAction, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{
    DeliveryMethod, DeliveryPayload, DeliveryResult,
};
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::ChainEvent;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
use std::sync::Arc;

/// A SinkHandler wrapper that applies middleware
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
    ) -> Result<DeliveryPayload, HandlerError> {
        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::new();

        // Pre-processing phase
        for middleware in self.middleware_chain.iter() {
            match middleware.pre_handle(&event, &mut ctx) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(_) => {
                    // Skip means don't consume - return success with no delivery
                    return Ok(DeliveryPayload::success(
                        "middleware_sink",
                        DeliveryMethod::Noop,
                        None, // bytes_processed
                    ));
                }
                MiddlewareAction::Abort => {
                    // Abort is also treated as success but with a different message
                    return Ok(DeliveryPayload::success(
                        "middleware_sink",
                        DeliveryMethod::Noop,
                        None, // bytes_processed
                    ));
                }
            }
        }

        // Consume with inner handler
        match self.inner.consume(event.clone()).await {
            Ok(payload) => {
                // Post-processing phase - sinks don't produce output events
                let empty = vec![];
                for middleware in self.middleware_chain.iter() {
                    middleware.post_handle(&event, &empty, &mut ctx);
                }

                // Extract timing from context if available (set by TimingMiddleware)
                let enhanced_payload = if let Some(start_value) =
                    ctx.get_baggage("processing_start_nanos")
                {
                    if let Some(start_nanos) = start_value.as_u64() {
                        let now_nanos = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as u64;
                        let duration_nanos = now_nanos - start_nanos;
                        let duration = MetricsDuration::from_nanos(duration_nanos);

                        tracing::debug!(
                            "SinkMiddleware: Enriching delivery payload with processing_duration={}",
                            duration
                        );

                        payload.with_processing_duration(duration)
                    } else {
                        payload
                    }
                } else {
                    payload
                };

                Ok(enhanced_payload)
            }
            Err(e) => {
                // Give each middleware a chance to handle the error
                for middleware in self.middleware_chain.iter() {
                    match middleware.on_error(&event, &mut ctx) {
                        ErrorAction::Propagate => continue,
                        ErrorAction::Recover(_) => {
                            // Recovery means success
                            return Ok(DeliveryPayload::success(
                                "middleware_sink",
                                DeliveryMethod::Noop,
                                None, // bytes_processed
                            ));
                        }
                        ErrorAction::Retry => {
                            // Simple retry once - in production, might want exponential backoff
                            return self.inner.consume(event).await;
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
impl<H: SinkHandler> SinkHandler for MiddlewareSink<H> {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        self.apply_middleware_with_error_handling(event).await
    }

    async fn flush(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        // Flush is not intercepted by middleware - it's an infrastructure concern
        self.inner.flush().await
    }

    async fn drain(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        // Drain is not intercepted by middleware - it's an infrastructure concern
        self.inner.drain().await
    }
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
            Ok(DeliveryPayload::success(
                "test_sink",
                DeliveryMethod::Noop,
                None,
            ))
        }
    }

    struct RetryMiddleware {
        max_retries: usize,
    }

    impl Middleware for RetryMiddleware {
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
        .with(RetryMiddleware { max_retries: 2 })
        .build();

        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test",
            json!({"data": "test"}),
        );

        // Should succeed after retry
        handler.consume(event.clone()).await.unwrap();
        assert_eq!(consumed.lock().unwrap().len(), 1);
    }
}

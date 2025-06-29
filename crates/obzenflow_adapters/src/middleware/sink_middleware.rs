//! Middleware adapter for SinkHandler
//!
//! This module provides middleware capabilities for SinkHandler implementations,
//! with special focus on error handling and retry logic.

use obzenflow_core::{ChainEvent, Result};
use obzenflow_runtime_services::control_plane::stages::handler_traits::SinkHandler;
use super::{Middleware, MiddlewareAction, ErrorAction, StepError};
use async_trait::async_trait;

/// A SinkHandler wrapper that applies middleware
pub struct MiddlewareSink<H: SinkHandler> {
    inner: H,
    middleware_chain: Vec<Box<dyn Middleware>>,
}

impl<H: SinkHandler> MiddlewareSink<H> {
    /// Create a new middleware-wrapped sink handler
    pub fn new(inner: H) -> Self {
        Self {
            inner,
            middleware_chain: Vec::new(),
        }
    }
    
    /// Add middleware to the chain
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        self.middleware_chain.push(middleware);
        self
    }
    
    /// Apply middleware with error handling
    fn apply_middleware_with_error_handling(
        &mut self,
        event: ChainEvent,
    ) -> Result<()> {
        // Pre-processing phase
        for middleware in &self.middleware_chain {
            match middleware.pre_handle(&event) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(_) => return Ok(()), // Skip means don't consume
                MiddlewareAction::Abort => return Ok(()),   // Abort is also a no-op for sinks
            }
        }
        
        // Consume with inner handler
        match self.inner.consume(event.clone()) {
            Ok(()) => {
                // Post-processing phase - sinks don't produce output events
                let mut empty = vec![];
                for middleware in &self.middleware_chain {
                    middleware.post_handle(&event, &mut empty);
                }
                Ok(())
            }
            Err(e) => {
                // Convert to StepError for middleware
                let error_string = format!("{:?}", e);
                let step_error: StepError = Box::new(
                    std::io::Error::new(std::io::ErrorKind::Other, error_string.clone())
                );
                
                // Give each middleware a chance to handle the error
                for middleware in &self.middleware_chain {
                    match middleware.on_error(&event, &step_error) {
                        ErrorAction::Propagate => continue,
                        ErrorAction::Recover(_) => return Ok(()), // Recovery means success
                        ErrorAction::Retry => {
                            // Simple retry once - in production, might want exponential backoff
                            return self.inner.consume(event);
                        }
                    }
                }
                
                // If no middleware handled it, propagate the error
                Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("Sink error: {}", error_string)
                )))
            }
        }
    }
}

#[async_trait]
impl<H: SinkHandler> SinkHandler for MiddlewareSink<H> {
    fn consume(&mut self, event: ChainEvent) -> Result<()> {
        self.apply_middleware_with_error_handling(event)
    }
    
    fn flush(&mut self) -> Result<()> {
        // Flush is not intercepted by middleware - it's an infrastructure concern
        self.inner.flush()
    }
    
    async fn drain(&mut self) -> Result<()> {
        // Drain is not intercepted by middleware - it's an infrastructure concern
        self.inner.drain().await
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
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    
    struct TestSink {
        consumed: Arc<Mutex<Vec<ChainEvent>>>,
        fail_count: Arc<Mutex<usize>>,
    }
    
    impl SinkHandler for TestSink {
        fn consume(&mut self, event: ChainEvent) -> Result<()> {
            let mut fail_count = self.fail_count.lock().unwrap();
            if *fail_count > 0 {
                *fail_count -= 1;
                return Err(anyhow::anyhow!("Simulated failure").into());
            }
            
            self.consumed.lock().unwrap().push(event);
            Ok(())
        }
    }
    
    struct RetryMiddleware {
        max_retries: usize,
    }
    
    impl Middleware for RetryMiddleware {
        fn on_error(&self, _event: &ChainEvent, _error: &StepError) -> ErrorAction {
            ErrorAction::Retry
        }
    }
    
    #[test]
    fn test_sink_retry_middleware() {
        let consumed = Arc::new(Mutex::new(Vec::new()));
        let fail_count = Arc::new(Mutex::new(1)); // Fail once
        
        let mut handler = TestSink {
            consumed: consumed.clone(),
            fail_count: fail_count.clone(),
        }
        .middleware()
        .with(RetryMiddleware { max_retries: 2 })
        .build();
        
        let event = ChainEvent::new(
            obzenflow_core::EventId::new(),
            obzenflow_core::WriterId::new(),
            "test",
            json!({"data": "test"})
        );
        
        // Should succeed after retry
        assert!(handler.consume(event.clone()).is_ok());
        assert_eq!(consumed.lock().unwrap().len(), 1);
    }
}
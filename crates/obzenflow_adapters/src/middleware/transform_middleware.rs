//! Middleware adapter for TransformHandler
//!
//! This module provides the ability to wrap TransformHandler implementations
//! with middleware for cross-cutting concerns like logging, monitoring, and retry logic.

use obzenflow_core::{ChainEvent, Result};
use obzenflow_runtime_services::control_plane::stages::handler_traits::TransformHandler;
use super::{Middleware, MiddlewareAction};
use async_trait::async_trait;

/// A TransformHandler wrapper that applies middleware to transform operations
pub struct MiddlewareTransform<H: TransformHandler> {
    inner: H,
    middleware_chain: Vec<Box<dyn Middleware>>,
}

impl<H: TransformHandler> MiddlewareTransform<H> {
    /// Create a new middleware-wrapped transform handler
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
    
    /// Apply the middleware chain to a transform operation
    fn apply_middleware<F>(&self, event: ChainEvent, transform_fn: F) -> Vec<ChainEvent>
    where
        F: FnOnce(ChainEvent) -> Vec<ChainEvent>,
    {
        // Pre-processing phase
        for middleware in &self.middleware_chain {
            match middleware.pre_handle(&event) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(results) => return results,
                MiddlewareAction::Abort => return vec![],
            }
        }
        
        // Execute the transform
        let mut results = transform_fn(event.clone());
        
        // Post-processing phase
        for middleware in &self.middleware_chain {
            middleware.post_handle(&event, &mut results);
        }
        
        results
    }
}

#[async_trait]
impl<H: TransformHandler> TransformHandler for MiddlewareTransform<H> {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        self.apply_middleware(event, |e| self.inner.process(e))
    }
    
    async fn drain(&mut self) -> Result<()> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    struct TestTransform;
    
    impl TransformHandler for TestTransform {
        fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
            event.payload["processed"] = json!(true);
            vec![event]
        }
    }
    
    struct TestMiddleware {
        tag: String,
    }
    
    impl Middleware for TestMiddleware {
        fn pre_handle(&self, _event: &ChainEvent) -> MiddlewareAction {
            println!("Pre-handle: {}", self.tag);
            MiddlewareAction::Continue
        }
        
        fn post_handle(&self, _event: &ChainEvent, results: &mut Vec<ChainEvent>) {
            println!("Post-handle: {} - {} results", self.tag, results.len());
            for result in results {
                result.payload[&self.tag] = json!(true);
            }
        }
    }
    
    #[test]
    fn test_transform_middleware_chain() {
        let handler = TestTransform
            .middleware()
            .with(TestMiddleware { tag: "first".to_string() })
            .with(TestMiddleware { tag: "second".to_string() })
            .build();
            
        let event = ChainEvent::new(
            obzenflow_core::EventId::new(),
            obzenflow_core::WriterId::new(),
            "test",
            json!({})
        );
        
        let results = handler.process(event);
        
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].payload["processed"], json!(true));
        assert_eq!(results[0].payload["first"], json!(true));
        assert_eq!(results[0].payload["second"], json!(true));
    }
}
//! Middleware adapter for Source handlers
//!
//! This module provides middleware capabilities for both FiniteSourceHandler 
//! and InfiniteSourceHandler implementations.

use obzenflow_core::{ChainEvent, EventId, WriterId};
use obzenflow_runtime_services::control_plane::stages::handler_traits::{
    FiniteSourceHandler, InfiniteSourceHandler
};
use super::{Middleware, MiddlewareAction, MiddlewareContext};

/// A FiniteSourceHandler wrapper that applies middleware
pub struct MiddlewareFiniteSource<H: FiniteSourceHandler> {
    inner: H,
    middleware_chain: Vec<Box<dyn Middleware>>,
    writer_id: WriterId, // Sources need a writer ID for synthetic events
}

impl<H: FiniteSourceHandler> MiddlewareFiniteSource<H> {
    /// Create a new middleware-wrapped finite source handler
    pub fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            inner,
            middleware_chain: Vec::new(),
            writer_id,
        }
    }
    
    /// Add middleware to the chain
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        self.middleware_chain.push(middleware);
        self
    }
}

impl<H: FiniteSourceHandler> FiniteSourceHandler for MiddlewareFiniteSource<H> {
    fn next(&mut self) -> Option<ChainEvent> {
        // Create a synthetic event for middleware to process
        let synthetic_event = ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "system.source.next",
            serde_json::json!({
                "source_type": "finite",
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            })
        );
        
        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::new();
        
        // Pre-processing phase
        for middleware in &self.middleware_chain {
            match middleware.pre_handle(&synthetic_event, &mut ctx) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(mut results) => {
                    // If middleware provides results, return the first one
                    return results.pop();
                },
                MiddlewareAction::Abort => return None,
            }
        }
        
        // Get next from inner source
        let result = self.inner.next();
        
        // Post-processing phase (only if we got an event)
        if let Some(ref event) = result {
            let results = vec![event.clone()];
            for middleware in &self.middleware_chain {
                middleware.post_handle(&synthetic_event, &results, &mut ctx);
            }
            // Return the original event (middleware can't modify it)
            Some(event.clone())
        } else {
            // Let middleware know we got no event
            let empty = vec![];
            for middleware in &self.middleware_chain {
                middleware.post_handle(&synthetic_event, &empty, &mut ctx);
            }
            None
        }
    }
    
    fn is_complete(&self) -> bool {
        // Completion check is not intercepted by middleware
        self.inner.is_complete()
    }
}

/// An InfiniteSourceHandler wrapper that applies middleware
pub struct MiddlewareInfiniteSource<H: InfiniteSourceHandler> {
    inner: H,
    middleware_chain: Vec<Box<dyn Middleware>>,
    writer_id: WriterId,
}

impl<H: InfiniteSourceHandler> MiddlewareInfiniteSource<H> {
    /// Create a new middleware-wrapped infinite source handler
    pub fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            inner,
            middleware_chain: Vec::new(),
            writer_id,
        }
    }
    
    /// Add middleware to the chain
    pub fn with_middleware(mut self, middleware: Box<dyn Middleware>) -> Self {
        self.middleware_chain.push(middleware);
        self
    }
}

impl<H: InfiniteSourceHandler> InfiniteSourceHandler for MiddlewareInfiniteSource<H> {
    fn next(&mut self) -> Option<ChainEvent> {
        // Create a synthetic event for middleware to process
        let synthetic_event = ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "system.source.next",
            serde_json::json!({
                "source_type": "infinite",
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            })
        );
        
        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::new();
        
        // Pre-processing phase
        for middleware in &self.middleware_chain {
            match middleware.pre_handle(&synthetic_event, &mut ctx) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(mut results) => {
                    // If middleware provides results, return the first one
                    return results.pop();
                },
                MiddlewareAction::Abort => return None,
            }
        }
        
        // Get next from inner source
        let result = self.inner.next();
        
        // Post-processing phase (only if we got an event)
        if let Some(ref event) = result {
            let results = vec![event.clone()];
            for middleware in &self.middleware_chain {
                middleware.post_handle(&synthetic_event, &results, &mut ctx);
            }
            // Return the original event (middleware can't modify it)
            Some(event.clone())
        } else {
            // Let middleware know we got no event
            let empty = vec![];
            for middleware in &self.middleware_chain {
                middleware.post_handle(&synthetic_event, &empty, &mut ctx);
            }
            None
        }
    }
}

/// Extension trait for finite sources
pub trait FiniteSourceHandlerExt: FiniteSourceHandler + Sized {
    /// Start building a middleware chain for this handler
    fn middleware(self, writer_id: WriterId) -> FiniteSourceMiddlewareBuilder<Self> {
        FiniteSourceMiddlewareBuilder::new(self, writer_id)
    }
}

impl<T: FiniteSourceHandler> FiniteSourceHandlerExt for T {}

/// Extension trait for infinite sources
pub trait InfiniteSourceHandlerExt: InfiniteSourceHandler + Sized {
    /// Start building a middleware chain for this handler
    fn middleware(self, writer_id: WriterId) -> InfiniteSourceMiddlewareBuilder<Self> {
        InfiniteSourceMiddlewareBuilder::new(self, writer_id)
    }
}

impl<T: InfiniteSourceHandler> InfiniteSourceHandlerExt for T {}

/// Builder for finite source middleware chains
pub struct FiniteSourceMiddlewareBuilder<H: FiniteSourceHandler> {
    handler: MiddlewareFiniteSource<H>,
}

impl<H: FiniteSourceHandler> FiniteSourceMiddlewareBuilder<H> {
    fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            handler: MiddlewareFiniteSource::new(inner, writer_id),
        }
    }
    
    /// Add a middleware to the chain
    pub fn with<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.handler = self.handler.with_middleware(Box::new(middleware));
        self
    }
    
    /// Build the final middleware-wrapped handler
    pub fn build(self) -> MiddlewareFiniteSource<H> {
        self.handler
    }
}

/// Builder for infinite source middleware chains
pub struct InfiniteSourceMiddlewareBuilder<H: InfiniteSourceHandler> {
    handler: MiddlewareInfiniteSource<H>,
}

impl<H: InfiniteSourceHandler> InfiniteSourceMiddlewareBuilder<H> {
    fn new(inner: H, writer_id: WriterId) -> Self {
        Self {
            handler: MiddlewareInfiniteSource::new(inner, writer_id),
        }
    }
    
    /// Add a middleware to the chain
    pub fn with<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.handler = self.handler.with_middleware(Box::new(middleware));
        self
    }
    
    /// Build the final middleware-wrapped handler
    pub fn build(self) -> MiddlewareInfiniteSource<H> {
        self.handler
    }
}
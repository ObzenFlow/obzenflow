//! Middleware adapter for Source handlers
//!
//! This module provides middleware capabilities for both FiniteSourceHandler
//! and InfiniteSourceHandler implementations.

use super::{Middleware, MiddlewareAction, MiddlewareContext};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, EventId, StageId, WriterId};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, InfiniteSourceHandler,
};

/// A FiniteSourceHandler wrapper that applies middleware
pub struct MiddlewareFiniteSource<H: FiniteSourceHandler> {
    inner: H,
    middleware_chain: Vec<Box<dyn Middleware>>,
    writer_id: WriterId, // Sources need a writer ID for synthetic events
}

// Manual Clone implementation that clones the handler but creates empty middleware chain
impl<H: FiniteSourceHandler + Clone> Clone for MiddlewareFiniteSource<H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            middleware_chain: Vec::new(), // Don't clone middleware, start fresh
            writer_id: self.writer_id.clone(),
        }
    }
}

impl<H: FiniteSourceHandler> std::fmt::Debug for MiddlewareFiniteSource<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiddlewareFiniteSource")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .field("writer_id", &self.writer_id)
            .finish()
    }
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
        let synthetic_event = ChainEventFactory::data_event(
            self.writer_id.clone(),
            "system.source.next",
            serde_json::json!({
                "source_type": "finite",
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            }),
        );

        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::new();

        // Pre-processing phase
        for middleware in &self.middleware_chain {
            match middleware.pre_handle(&synthetic_event, &mut ctx) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(mut results) => {
                    // If middleware provides results, enrich them before returning
                    if let Some(mut event) = results.pop() {
                        // Call pre_write to enrich the skipped event
                        for mw in &self.middleware_chain {
                            mw.pre_write(&mut event, &ctx);
                        }
                        return Some(event);
                    }
                    return None;
                }
                MiddlewareAction::Abort => return None,
            }
        }

        // Get next from inner source
        let result = self.inner.next();

        // Post-processing phase (only if we got an event)
        if let Some(mut event) = result {
            let results = vec![event.clone()];

            // Call post_handle for observation
            for middleware in &self.middleware_chain {
                middleware.post_handle(&synthetic_event, &results, &mut ctx);
            }

            // Now call pre_write to enrich the event before returning
            for middleware in &self.middleware_chain {
                middleware.pre_write(&mut event, &ctx);
            }

            // Return the enriched event
            Some(event)
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

// Manual Clone implementation that clones the handler but creates empty middleware chain
impl<H: InfiniteSourceHandler + Clone> Clone for MiddlewareInfiniteSource<H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            middleware_chain: Vec::new(), // Don't clone middleware, start fresh
            writer_id: self.writer_id.clone(),
        }
    }
}

impl<H: InfiniteSourceHandler> std::fmt::Debug for MiddlewareInfiniteSource<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiddlewareInfiniteSource")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .field("writer_id", &self.writer_id)
            .finish()
    }
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
        let synthetic_event = ChainEventFactory::data_event(
            self.writer_id.clone(),
            "system.source.next",
            serde_json::json!({
                "source_type": "infinite",
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            }),
        );

        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::new();

        // Pre-processing phase
        for middleware in &self.middleware_chain {
            match middleware.pre_handle(&synthetic_event, &mut ctx) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(mut results) => {
                    // If middleware provides results, enrich them before returning
                    if let Some(mut event) = results.pop() {
                        // Call pre_write to enrich the skipped event
                        for mw in &self.middleware_chain {
                            mw.pre_write(&mut event, &ctx);
                        }
                        return Some(event);
                    }
                    return None;
                }
                MiddlewareAction::Abort => return None,
            }
        }

        // Get next from inner source
        let result = self.inner.next();

        // Post-processing phase (only if we got an event)
        if let Some(mut event) = result {
            let results = vec![event.clone()];

            // Call post_handle for observation
            for middleware in &self.middleware_chain {
                middleware.post_handle(&synthetic_event, &results, &mut ctx);
            }

            // Now call pre_write to enrich the event before returning
            for middleware in &self.middleware_chain {
                middleware.pre_write(&mut event, &ctx);
            }

            // Return the enriched event
            Some(event)
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

//! Middleware adapter for TransformHandler
//!
//! This module provides the ability to wrap TransformHandler implementations
//! with middleware for cross-cutting concerns like logging, monitoring, and retry logic.

use obzenflow_core::{ChainEvent, Result};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_runtime_services::stages::common::handlers::TransformHandler;
use super::{Middleware, MiddlewareAction, MiddlewareContext};
use async_trait::async_trait;

/// A TransformHandler wrapper that applies middleware to transform operations
pub struct MiddlewareTransform<H: TransformHandler> {
    inner: H,
    middleware_chain: Vec<Box<dyn Middleware>>,
}

// Manual Clone implementation that clones the handler but creates empty middleware chain
impl<H: TransformHandler + Clone> Clone for MiddlewareTransform<H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            middleware_chain: Vec::new(), // Don't clone middleware, start fresh
        }
    }
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
        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::new();
        
        // Pre-processing phase
        for middleware in &self.middleware_chain {
            match middleware.pre_handle(&event, &mut ctx) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(mut results) => {
                    // Pre-write phase for skip results
                    for result in &mut results {
                        for mw in &self.middleware_chain {
                            mw.pre_write(result, &ctx);
                        }
                    }
                    
                    // Check for control events even on skip
                    if !ctx.control_events.is_empty() {
                        tracing::trace!(
                            "Appending {} control events from middleware (skip path)", 
                            ctx.control_events.len()
                        );
                    }
                    // Pre-write on control events - take ownership to avoid borrow issues
                    let mut control_events = std::mem::take(&mut ctx.control_events);
                    for control_event in &mut control_events {
                        for mw in &self.middleware_chain {
                            mw.pre_write(control_event, &ctx);
                        }
                    }
                    results.extend(control_events);
                    return results;
                },
                MiddlewareAction::Abort => {
                    let mut err = event.clone();
                    err.processing_info.status = ProcessingStatus::Error("aborted by middleware".into());
                    return vec![err];
                },
            }
        }
        
        // Execute the transform
        let mut results = transform_fn(event.clone());
        
        // Post-processing phase (reverse order)
        for middleware in self.middleware_chain.iter().rev() {
            middleware.post_handle(&event, &results, &mut ctx);
        }
        
        // Pre-write phase: allow middleware to enrich each result event
        for result in &mut results {
            for middleware in &self.middleware_chain {
                middleware.pre_write(result, &ctx);
            }
        }
        
        // Append control events after all middleware runs
        if !ctx.control_events.is_empty() {
            tracing::trace!(
                "Appending {} control events from middleware", 
                ctx.control_events.len()
            );
        }
        // Pre-write on control events - take ownership to avoid borrow issues
        let mut control_events = std::mem::take(&mut ctx.control_events);
        for control_event in &mut control_events {
            for middleware in &self.middleware_chain {
                middleware.pre_write(control_event, &ctx);
            }
        }
        results.extend(control_events);
        
        results
    }
}

#[async_trait]
impl<H: TransformHandler> TransformHandler for MiddlewareTransform<H> {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Short-circuit if event already has Error status
        if matches!(event.processing_info.status, ProcessingStatus::Error(_)) {
            tracing::debug!("MiddlewareTransform: Skipping event with Error status: {:?}", event.processing_info.status);
            return vec![event];
        }
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
    use async_trait::async_trait;
    
    struct TestTransform;
    
    #[async_trait]
    impl TransformHandler for TestTransform {
        fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
            let mut payload = event.payload().clone();
            payload["processed"] = json!(true);
            
            let mut new_event = ChainEventFactory::data_event(
                event.writer_id.clone(),
                event.event_type(),
                payload,
            );
            // Copy over metadata
            new_event.flow_context = event.flow_context.clone();
            new_event.processing_info = event.processing_info.clone();
            new_event.causality = event.causality.clone();
            
            vec![new_event]
        }
        
        async fn drain(&mut self) -> Result<()> {
            Ok(())
        }
    }
    
    struct TestMiddleware {
        tag: String,
    }
    
    impl Middleware for TestMiddleware {
        fn pre_handle(&self, _event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
            println!("Pre-handle: {}", self.tag);
            ctx.emit_event("test", &self.tag, json!({"phase": "pre"}));
            MiddlewareAction::Continue
        }
        
        fn post_handle(&self, _event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext) {
            println!("Post-handle: {} - {} results", self.tag, results.len());
            // Check if we can see events from earlier middleware
            let events_count = ctx.events.len();
            println!("Context has {} events", events_count);
            
            // Emit a post-processing event
            ctx.emit_event("test", &self.tag, json!({"phase": "post", "results": results.len()}));
        }
    }
    
    #[test]
    fn test_transform_middleware_chain() {
        let handler = TestTransform
            .middleware()
            .with(TestMiddleware { tag: "first".to_string() })
            .with(TestMiddleware { tag: "second".to_string() })
            .build();
            
        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test",
            json!({})
        );
        
        let results = handler.process(event);
        
        // Middleware can't modify events anymore, just verify the transform worked
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].payload()["processed"], json!(true));
        
        // The middleware would have emitted events through context,
        // but we can't verify that here without access to the context
    }
    
    struct ControlEventMiddleware;
    
    impl Middleware for ControlEventMiddleware {
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
                })
            ));
            MiddlewareAction::Continue
        }
        
        fn post_handle(&self, _event: &ChainEvent, _results: &[ChainEvent], ctx: &mut MiddlewareContext) {
            // Emit another control event in post phase
            let writer_id = obzenflow_core::WriterId::from(obzenflow_core::StageId::new());
            ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
                writer_id,
                json!({
                    "queue_depth": 10,
                    "in_flight": 3
                })
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
            json!({})
        );
        
        let results = handler.process(event);
        
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
        fn pre_handle(&self, _event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
            // Write control event then skip
            let writer_id = obzenflow_core::WriterId::from(obzenflow_core::StageId::new());
            ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
                writer_id.clone(),
                json!({
                    "middleware": "skip_test",
                    "action": "skipped"
                })
            ));
            MiddlewareAction::Skip(vec![ChainEventFactory::data_event(
                writer_id,
                "skipped",
                json!({"skipped": true})
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
            json!({})
        );
        
        let results = handler.process(event);
        
        // Should have 2 events: 1 skip result + 1 control event
        assert_eq!(results.len(), 2);
        
        // First is the skip result
        assert_eq!(results[0].event_type(), "skipped");
        assert!(!results[0].is_control());
        
        // Second is the control event
        assert!(results[1].is_lifecycle());
        assert_eq!(results[1].event_type(), "lifecycle.metrics.state");
    }
}
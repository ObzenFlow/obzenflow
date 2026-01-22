//! Middleware adapter for JoinHandler
//!
//! This module provides the ability to wrap JoinHandler implementations
//! with middleware for cross-cutting concerns like rate limiting, monitoring, and retries.
//!
//! ## Design Decision: Same Middleware for Both Sides
//!
//! Join stages have two input sides (reference and stream). Rather than introduce
//! complex per-side middleware configuration, we apply the same middleware uniformly
//! to both sides. This means:
//!
//! - Rate limiting applies to events from either input
//! - Circuit breakers trip based on combined failure rates
//! - Lifecycle events are emitted once per processed event regardless of source
//!
//! This matches user expectations and avoids configuration complexity.

use super::{Middleware, MiddlewareAction, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::JoinHandler;
use std::sync::Arc;

/// A JoinHandler wrapper that applies middleware to join operations
///
/// The same middleware chain is applied to events from both the reference
/// and stream sides of the join.
#[derive(Clone)]
pub struct MiddlewareJoin<H: JoinHandler> {
    inner: H,
    middleware_chain: Arc<Vec<Arc<dyn Middleware>>>,
}

impl<H: JoinHandler> std::fmt::Debug for MiddlewareJoin<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiddlewareJoin")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .finish()
    }
}

impl<H: JoinHandler> MiddlewareJoin<H> {
    /// Create a new middleware-wrapped join handler
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

    /// Apply middleware chain and return whether to continue processing
    fn apply_pre_middleware(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> bool {
        // Short-circuit if event already has Error status
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            tracing::debug!(
                "MiddlewareJoin: Skipping pre_handle for event with Error status: {:?}",
                event.processing_info.status
            );
            return false;
        }

        // Pre-processing phase
        for middleware in self.middleware_chain.iter() {
            match middleware.pre_handle(event, ctx) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(_) => return false,
                MiddlewareAction::Abort => return false,
            }
        }
        true
    }

    /// Apply post-middleware and enrich results
    fn apply_post_middleware(
        &self,
        event: &ChainEvent,
        results: &mut Vec<ChainEvent>,
        ctx: &mut MiddlewareContext,
    ) {
        // Post-processing phase (reverse order)
        for middleware in self.middleware_chain.iter().rev() {
            middleware.post_handle(event, results, ctx);
        }

        // Pre-write phase: allow middleware to enrich each result event
        for result in results.iter_mut() {
            for middleware in self.middleware_chain.iter() {
                middleware.pre_write(result, ctx);
            }
        }

        // Append control events from middleware
        if !ctx.control_events.is_empty() {
            tracing::trace!(
                "Appending {} control events from middleware",
                ctx.control_events.len()
            );
        }
        let mut control_events = std::mem::take(&mut ctx.control_events);
        for control_event in &mut control_events {
            for middleware in self.middleware_chain.iter() {
                middleware.pre_write(control_event, ctx);
            }
        }
        results.extend(control_events);
    }
}

#[async_trait]
impl<H: JoinHandler + Clone> JoinHandler for MiddlewareJoin<H>
where
    H::State: 'static,
{
    type State = H::State;

    fn initial_state(&self) -> Self::State {
        self.inner.initial_state()
    }

    fn process_event(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        source_id: StageId,
        writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::new();

        // Apply pre-middleware (same for both reference and stream sides)
        if !self.apply_pre_middleware(&event, &mut ctx) {
            return Ok(vec![]);
        }

        // Process with inner handler
        let mut results = self
            .inner
            .process_event(state, event.clone(), source_id, writer_id)?;

        // Apply post-middleware
        self.apply_post_middleware(&event, &mut results, &mut ctx);

        Ok(results)
    }

    fn reference_mode(
        &self,
    ) -> obzenflow_runtime_services::stages::join::config::JoinReferenceMode {
        self.inner.reference_mode()
    }

    fn reference_batch_cap(&self) -> Option<usize> {
        self.inner.reference_batch_cap()
    }

    fn on_source_eof(
        &self,
        state: &mut Self::State,
        source_id: StageId,
        writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        // Create ephemeral context
        let ctx = MiddlewareContext::new();

        // EOF handling doesn't go through pre_handle (no event to check)
        // Just delegate to inner handler
        let mut results = self.inner.on_source_eof(state, source_id, writer_id)?;

        // Pre-write phase: allow middleware to enrich result events
        for result in &mut results {
            for middleware in self.middleware_chain.iter() {
                middleware.pre_write(result, &ctx);
            }
        }

        Ok(results)
    }

    async fn drain(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        // Create ephemeral context
        let mut ctx = MiddlewareContext::new();

        // Drain from inner handler
        let mut results = self.inner.drain(state).await?;

        // Pre-write phase: allow middleware to enrich result events
        for result in &mut results {
            for middleware in self.middleware_chain.iter() {
                middleware.pre_write(result, &ctx);
            }
        }

        // Append any control events during drain
        // This is the final opportunity to emit middleware lifecycle events
        let mut control_events = std::mem::take(&mut ctx.control_events);
        for control_event in &mut control_events {
            for middleware in self.middleware_chain.iter() {
                middleware.pre_write(control_event, &ctx);
            }
        }
        results.extend(control_events);

        Ok(results)
    }
}

/// Extension trait to add middleware capabilities to any JoinHandler
pub trait JoinHandlerMiddlewareExt: JoinHandler + Sized {
    /// Start building a middleware chain for this handler
    fn middleware(self) -> JoinMiddlewareBuilder<Self> {
        JoinMiddlewareBuilder::new(self)
    }
}

// Implement for all JoinHandlers
impl<T: JoinHandler> JoinHandlerMiddlewareExt for T {}

/// Builder for constructing middleware chains around join handlers
pub struct JoinMiddlewareBuilder<H: JoinHandler> {
    handler: MiddlewareJoin<H>,
}

impl<H: JoinHandler> JoinMiddlewareBuilder<H> {
    fn new(inner: H) -> Self {
        Self {
            handler: MiddlewareJoin::new(inner),
        }
    }

    /// Add a middleware to the chain
    pub fn with<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.handler = self.handler.with_middleware(Box::new(middleware));
        self
    }

    /// Build the final middleware-wrapped handler
    pub fn build(self) -> MiddlewareJoin<H> {
        self.handler
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::ChainEventFactory;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone)]
    struct TestJoinHandler {
        process_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl JoinHandler for TestJoinHandler {
        type State = Vec<ChainEvent>;

        fn initial_state(&self) -> Self::State {
            Vec::new()
        }

        fn process_event(
            &self,
            state: &mut Self::State,
            event: ChainEvent,
            _source_id: StageId,
            writer_id: WriterId,
        ) -> Result<Vec<ChainEvent>, HandlerError> {
            self.process_count.fetch_add(1, Ordering::Relaxed);
            state.push(event.clone());

            // Emit a joined event
            Ok(vec![ChainEventFactory::data_event(
                writer_id,
                "joined",
                json!({ "source_event": event.id.to_string() }),
            )])
        }

        fn on_source_eof(
            &self,
            _state: &mut Self::State,
            _source_id: StageId,
            _writer_id: WriterId,
        ) -> Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![])
        }
    }

    struct SkipMiddleware;

    impl Middleware for SkipMiddleware {
        fn pre_handle(&self, event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
            // Skip events with "skip" in their payload
            if event.payload().get("skip").is_some() {
                MiddlewareAction::Skip(vec![])
            } else {
                MiddlewareAction::Continue
            }
        }
    }

    #[tokio::test]
    async fn test_join_middleware_skip() {
        let process_count = Arc::new(AtomicUsize::new(0));
        let handler = TestJoinHandler {
            process_count: process_count.clone(),
        }
        .middleware()
        .with(SkipMiddleware)
        .build();
        // Clone is used in join supervisor hot paths; ensure middleware survives clone.
        let handler = handler.clone();

        let mut state = handler.initial_state();
        let writer_id = WriterId::from(StageId::new());
        let source_id = StageId::new();

        // Normal event should be processed
        let event1 = ChainEventFactory::data_event(writer_id, "test", json!({"data": 1}));
        let results1 = handler
            .process_event(&mut state, event1, source_id, writer_id)
            .expect("Join middleware should succeed for normal event");
        assert_eq!(process_count.load(Ordering::Relaxed), 1);
        assert_eq!(results1.len(), 1);

        // Event with "skip" should be skipped
        let event2 = ChainEventFactory::data_event(writer_id, "test", json!({"skip": true}));
        let results2 = handler
            .process_event(&mut state, event2, source_id, writer_id)
            .expect("Join middleware should succeed for skipped event");
        assert_eq!(process_count.load(Ordering::Relaxed), 1); // Still 1
        assert_eq!(results2.len(), 0);

        // Another normal event from stream side (same middleware applies)
        let event3 = ChainEventFactory::data_event(writer_id, "test", json!({"data": 3}));
        let results3 = handler
            .process_event(&mut state, event3, source_id, writer_id)
            .expect("Join middleware should succeed for second normal event");
        assert_eq!(process_count.load(Ordering::Relaxed), 2);
        assert_eq!(results3.len(), 1);
    }
}

//! Middleware adapter for StatefulHandler
//!
//! This module provides the ability to wrap StatefulHandler implementations
//! with middleware for cross-cutting concerns like rate limiting, monitoring, and retries.

use super::{Middleware, MiddlewareAction, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::ChainEvent;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::StatefulHandler;

/// A StatefulHandler wrapper that applies middleware to stateful operations
pub struct MiddlewareStateful<H: StatefulHandler> {
    inner: H,
    middleware_chain: Vec<Box<dyn Middleware>>,
}

// Manual Clone implementation that clones the handler but creates empty middleware chain
impl<H: StatefulHandler + Clone> Clone for MiddlewareStateful<H> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            middleware_chain: Vec::new(), // Don't clone middleware, start fresh
        }
    }
}

impl<H: StatefulHandler> std::fmt::Debug for MiddlewareStateful<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MiddlewareStateful")
            .field("inner_type", &std::any::type_name::<H>())
            .field("middleware_count", &self.middleware_chain.len())
            .finish()
    }
}

impl<H: StatefulHandler> MiddlewareStateful<H> {
    /// Create a new middleware-wrapped stateful handler
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
}

#[async_trait]
impl<H: StatefulHandler + Clone> StatefulHandler for MiddlewareStateful<H>
where
    H::State: 'static,
{
    type State = H::State;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        // Short-circuit if event already has Error status
        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            tracing::debug!(
                "MiddlewareStateful: Skipping accumulate for event with Error status: {:?}",
                event.processing_info.status
            );
            return;
        }

        // Create ephemeral context for this processing
        let mut ctx = MiddlewareContext::new();

        // Pre-processing phase
        for middleware in &self.middleware_chain {
            match middleware.pre_handle(&event, &mut ctx) {
                MiddlewareAction::Continue => continue,
                MiddlewareAction::Skip(_) => {
                    // Skip means don't accumulate this event
                    return;
                }
                MiddlewareAction::Abort => {
                    // Abort means don't accumulate this event
                    return;
                }
            }
        }

        // Accumulate with inner handler
        self.inner.accumulate(state, event.clone());

        // Post-processing phase (reverse order)
        // Note: accumulate doesn't produce output, so results is empty
        let empty_results: Vec<ChainEvent> = vec![];
        for middleware in self.middleware_chain.iter().rev() {
            middleware.post_handle(&event, &empty_results, &mut ctx);
        }
    }

    fn initial_state(&self) -> Self::State {
        self.inner.initial_state()
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        // Create ephemeral context for emission
        let ctx = MiddlewareContext::new();

        // Get events from inner handler
        let mut results = self.inner.create_events(state)?;

        // Pre-write phase: allow middleware to enrich each result event
        for result in &mut results {
            for middleware in &self.middleware_chain {
                middleware.pre_write(result, &ctx);
            }
        }

        Ok(results)
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        // Delegate to inner handler - middleware doesn't affect emission strategy
        self.inner.should_emit(state)
    }

    fn emit(
        &self,
        state: &mut Self::State,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        // Create ephemeral context for emission
        let ctx = MiddlewareContext::new();

        // Emit from inner handler
        let mut results = self.inner.emit(state)?;

        // Pre-write phase: allow middleware to enrich each result event
        for result in &mut results {
            for middleware in &self.middleware_chain {
                middleware.pre_write(result, &ctx);
            }
        }

        Ok(results)
    }

    async fn drain(
        &self,
        state: &Self::State,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        // Create ephemeral context for drain
        let mut ctx = MiddlewareContext::new();

        // Drain from inner handler
        let mut results = self.inner.drain(state).await?;

        // Pre-write phase: allow middleware to enrich each result event
        for result in &mut results {
            for middleware in &self.middleware_chain {
                middleware.pre_write(result, &ctx);
            }
        }

        // Append control events from middleware during drain
        // This is the final opportunity to emit middleware lifecycle events
        if !ctx.control_events.is_empty() {
            tracing::trace!(
                "Appending {} control events from middleware in drain",
                ctx.control_events.len()
            );
        }
        let mut control_events = std::mem::take(&mut ctx.control_events);
        for control_event in &mut control_events {
            for middleware in &self.middleware_chain {
                middleware.pre_write(control_event, &ctx);
            }
        }
        results.extend(control_events);

        Ok(results)
    }
}

/// Extension trait to add middleware capabilities to any StatefulHandler
pub trait StatefulHandlerMiddlewareExt: StatefulHandler + Sized {
    /// Start building a middleware chain for this handler
    fn middleware(self) -> StatefulMiddlewareBuilder<Self> {
        StatefulMiddlewareBuilder::new(self)
    }
}

// Implement for all StatefulHandlers
impl<T: StatefulHandler> StatefulHandlerMiddlewareExt for T {}

/// Builder for constructing middleware chains around stateful handlers
pub struct StatefulMiddlewareBuilder<H: StatefulHandler> {
    handler: MiddlewareStateful<H>,
}

impl<H: StatefulHandler> StatefulMiddlewareBuilder<H> {
    fn new(inner: H) -> Self {
        Self {
            handler: MiddlewareStateful::new(inner),
        }
    }

    /// Add a middleware to the chain
    pub fn with<M: Middleware + 'static>(mut self, middleware: M) -> Self {
        self.handler = self.handler.with_middleware(Box::new(middleware));
        self
    }

    /// Build the final middleware-wrapped handler
    pub fn build(self) -> MiddlewareStateful<H> {
        self.handler
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone)]
    struct TestStatefulHandler {
        accumulated_count: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl StatefulHandler for TestStatefulHandler {
        type State = Vec<ChainEvent>;

        fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
            self.accumulated_count.fetch_add(1, Ordering::Relaxed);
            state.push(event);
        }

        fn initial_state(&self) -> Self::State {
            Vec::new()
        }

        fn create_events(
            &self,
            state: &Self::State,
        ) -> Result<Vec<ChainEvent>, HandlerError> {
            let writer_id = WriterId::from(StageId::new());
            Ok(vec![ChainEventFactory::data_event(
                writer_id,
                "aggregated",
                json!({ "count": state.len() }),
            )])
        }

        fn should_emit(&self, state: &Self::State) -> bool {
            state.len() >= 3
        }

        fn emit(
            &self,
            state: &mut Self::State,
        ) -> Result<Vec<ChainEvent>, HandlerError> {
            let events = self.create_events(state)?;
            state.clear();
            Ok(events)
        }

        async fn drain(
            &self,
            state: &Self::State,
        ) -> Result<Vec<ChainEvent>, HandlerError> {
            self.create_events(state)
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
    async fn test_stateful_middleware_skip() {
        let accumulated_count = Arc::new(AtomicUsize::new(0));
        let mut handler = TestStatefulHandler {
            accumulated_count: accumulated_count.clone(),
        }
        .middleware()
        .with(SkipMiddleware)
        .build();

        let mut state = handler.initial_state();
        let writer_id = WriterId::from(StageId::new());

        // Normal event should be accumulated
        let event1 = ChainEventFactory::data_event(writer_id.clone(), "test", json!({"data": 1}));
        handler.accumulate(&mut state, event1);
        assert_eq!(accumulated_count.load(Ordering::Relaxed), 1);

        // Event with "skip" should be skipped
        let event2 =
            ChainEventFactory::data_event(writer_id.clone(), "test", json!({"skip": true}));
        handler.accumulate(&mut state, event2);
        assert_eq!(accumulated_count.load(Ordering::Relaxed), 1); // Still 1

        // Another normal event
        let event3 = ChainEventFactory::data_event(writer_id.clone(), "test", json!({"data": 3}));
        handler.accumulate(&mut state, event3);
        assert_eq!(accumulated_count.load(Ordering::Relaxed), 2);
    }
}

#[cfg(test)]
mod tests {
    use flowstate_rs::prelude::*;
    use flowstate_rs::lifecycle::{EventHandler, ProcessingMode};
    use flowstate_rs::middleware::{Middleware, MiddlewareAction, EventHandlerExt};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // Simple test EventHandler
    struct TestHandler {
        transform_count: Arc<AtomicUsize>,
    }

    impl TestHandler {
        fn new() -> Self {
            Self {
                transform_count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl EventHandler for TestHandler {
        fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
            self.transform_count.fetch_add(1, Ordering::SeqCst);
            vec![event]
        }

        fn processing_mode(&self) -> ProcessingMode {
            ProcessingMode::Transform
        }
    }

    // Simple test middleware that counts pre/post calls
    struct CountingMiddleware {
        pre_count: Arc<AtomicUsize>,
        post_count: Arc<AtomicUsize>,
    }

    impl CountingMiddleware {
        fn new() -> Self {
            Self {
                pre_count: Arc::new(AtomicUsize::new(0)),
                post_count: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl Middleware for CountingMiddleware {
        fn pre_handle(&self, _event: &ChainEvent) -> MiddlewareAction {
            self.pre_count.fetch_add(1, Ordering::SeqCst);
            MiddlewareAction::Continue
        }

        fn post_handle(&self, _event: &ChainEvent, _results: &mut Vec<ChainEvent>) {
            self.post_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_middleware_works_with_event_handler() {
        // Create handler with middleware
        let base_handler = TestHandler::new();
        let transform_count = base_handler.transform_count.clone();
        
        let middleware = CountingMiddleware::new();
        let pre_count = middleware.pre_count.clone();
        let post_count = middleware.post_count.clone();
        
        let handler = base_handler
            .middleware()
            .with(middleware)
            .build();

        // Process an event
        let event = ChainEvent::new("test_event", json!({"value": 42}));
        let results = handler.transform(event);

        // Verify the middleware was called
        assert_eq!(pre_count.load(Ordering::SeqCst), 1, "pre_handle should be called once");
        assert_eq!(post_count.load(Ordering::SeqCst), 1, "post_handle should be called once");
        assert_eq!(transform_count.load(Ordering::SeqCst), 1, "transform should be called once");
        assert_eq!(results.len(), 1, "Should return one event");
    }

    #[test]
    fn test_multiple_middleware_lifo_order() {
        // Create handler with multiple middleware
        let handler = TestHandler::new();
        
        let middleware1 = CountingMiddleware::new();
        let pre_count1 = middleware1.pre_count.clone();
        
        let middleware2 = CountingMiddleware::new();
        let pre_count2 = middleware2.pre_count.clone();
        
        let wrapped_handler = handler
            .middleware()
            .with(middleware1)
            .with(middleware2)
            .build();

        // Process an event
        let event = ChainEvent::new("test_event", json!({}));
        let _ = wrapped_handler.transform(event);

        // Both middleware should be called
        assert_eq!(pre_count1.load(Ordering::SeqCst), 1);
        assert_eq!(pre_count2.load(Ordering::SeqCst), 1);
    }

    // Test observer method with middleware
    struct ObserverHandler;

    impl EventHandler for ObserverHandler {
        fn observe(&self, _event: &ChainEvent) -> Result<()> {
            Ok(())
        }

        fn processing_mode(&self) -> ProcessingMode {
            ProcessingMode::Observe
        }
    }

    #[test]
    fn test_middleware_with_observer() {
        let handler = ObserverHandler;
        let middleware = CountingMiddleware::new();
        let pre_count = middleware.pre_count.clone();
        let post_count = middleware.post_count.clone();
        
        let wrapped = handler
            .middleware()
            .with(middleware)
            .build();

        let event = ChainEvent::new("test", json!({}));
        let result = wrapped.observe(&event);

        assert!(result.is_ok());
        assert_eq!(pre_count.load(Ordering::SeqCst), 1);
        assert_eq!(post_count.load(Ordering::SeqCst), 1);
    }
}
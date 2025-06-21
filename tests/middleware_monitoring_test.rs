//! Test the new middleware-based monitoring implementation

use flowstate_rs::monitoring::{RED, USE};
use flowstate_rs::middleware::{StepExt, MonitoringMiddleware, Middleware, MiddlewareAction};
use flowstate_rs::chain_event::ChainEvent;
use flowstate_rs::step::{Step, StepType};
use flowstate_rs::stage;

struct TestProcessor;

impl Step for TestProcessor {
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event]
    }
}

#[test]
fn test_monitoring_middleware_creation() {
    // Direct middleware application
    let _step = TestProcessor
        .middleware()
        .with(MonitoringMiddleware::<RED>::new("test_processor"))
        .build();
    
    // Using the stage! macro
    let _step2 = stage!(TestProcessor, USE);
}

#[test]
fn test_middleware_chaining() {
    
    // Custom middleware for testing
    struct LoggingMiddleware;
    
    impl Middleware for LoggingMiddleware {
        fn pre_handle(&self, _event: &ChainEvent) -> MiddlewareAction {
            println!("Logging event");
            MiddlewareAction::Continue
        }
    }
    
    // Chain multiple middleware
    let _step = TestProcessor
        .middleware()
        .with(LoggingMiddleware)
        .with(MonitoringMiddleware::<RED>::new("test"))
        .build();
}
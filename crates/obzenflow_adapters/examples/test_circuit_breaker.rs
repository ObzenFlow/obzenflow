// Test circuit breaker event emission

use obzenflow_adapters::middleware::{CircuitBreakerMiddleware, Middleware, MiddlewareContext, MiddlewareAction};
use obzenflow_core::{ChainEvent, EventId, WriterId};
use serde_json::json;

fn main() {
    println!("=== Testing Circuit Breaker ===\n");
    
    let cb = CircuitBreakerMiddleware::new(3);
    let writer_id = WriterId::default();
    
    // Test 1: Normal operation (success)
    println!("Test 1: Normal operation");
    let event = ChainEvent::new(
        EventId::new(),
        writer_id,
        "test",
        json!({"test": 1}),
    );
    let mut ctx = MiddlewareContext::new();
    
    match cb.pre_handle(&event, &mut ctx) {
        MiddlewareAction::Continue => println!("Pre-handle: Continue"),
        MiddlewareAction::Skip(_) => println!("Pre-handle: Skip"),
        MiddlewareAction::Abort => println!("Pre-handle: Abort"),
    }
    
    println!("Context after pre-handle: {} events", ctx.events.len());
    for event in &ctx.events {
        println!("  Event: {} - {}", event.source, event.event_type);
    }
    
    // Simulate success
    let results = vec![event.clone()];
    cb.post_handle(&event, &results, &mut ctx);
    
    println!("Context after post-handle: {} events", ctx.events.len());
    for event in &ctx.events {
        println!("  Event: {} - {}", event.source, event.event_type);
    }
    
    // Test 2: Failures to trigger circuit opening
    println!("\n\nTest 2: Triggering circuit breaker");
    for i in 0..5 {
        println!("\nFailure #{}", i + 1);
        let event = ChainEvent::new(
            EventId::new(),
            writer_id,
            "test",
            json!({"test": i + 2}),
        );
        let mut ctx = MiddlewareContext::new();
        
        match cb.pre_handle(&event, &mut ctx) {
            MiddlewareAction::Continue => println!("Pre-handle: Continue"),
            MiddlewareAction::Skip(_) => println!("Pre-handle: Skip (circuit open!)"),
            MiddlewareAction::Abort => println!("Pre-handle: Abort"),
        }
        
        if ctx.events.len() > 0 {
            println!("Events emitted:");
            for event in &ctx.events {
                println!("  {} - {}: {:?}", event.source, event.event_type, event.data);
            }
        }
        
        // Simulate failure by empty results
        if matches!(cb.pre_handle(&event, &mut ctx), MiddlewareAction::Continue) {
            cb.post_handle(&event, &vec![], &mut ctx);
        }
    }
}
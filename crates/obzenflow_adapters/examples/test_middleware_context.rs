// Simple test to verify middleware context is preserved across layers

use obzenflow_adapters::middleware::{Middleware, MiddlewareContext, MiddlewareAction};
use obzenflow_core::{ChainEvent, EventId, WriterId};
use serde_json::json;

struct TestMiddleware {
    name: String,
}

impl TestMiddleware {
    fn new(name: &str) -> Self {
        Self { name: name.to_string() }
    }
}

impl Middleware for TestMiddleware {
    fn pre_handle(&self, _event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        println!("[{}] Pre-handle: {} events in context", self.name, ctx.events.len());
        
        // Emit an event
        ctx.emit_event(&self.name, "pre_handle", json!({
            "message": format!("{} was here", self.name)
        }));
        
        println!("[{}] Pre-handle: After emit, {} events in context", self.name, ctx.events.len());
        
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, _event: &ChainEvent, _results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        println!("[{}] Post-handle: {} events in context", self.name, ctx.events.len());
        
        // List all events
        for (i, event) in ctx.events.iter().enumerate() {
            println!("  Event #{}: {} - {}", i, event.source, event.event_type);
        }
        
        // Emit another event
        ctx.emit_event(&self.name, "post_handle", json!({
            "message": format!("{} finished", self.name)
        }));
        
        println!("[{}] Post-handle: After emit, {} events in context", self.name, ctx.events.len());
    }
}

fn main() {
    println!("=== Testing Middleware Context ===\n");
    
    // Create middleware stack
    let mw1 = TestMiddleware::new("MW1");
    let mw2 = TestMiddleware::new("MW2");
    let mw3 = TestMiddleware::new("MW3");
    
    let middleware: Vec<&dyn Middleware> = vec![&mw1, &mw2, &mw3];
    
    // Create test event
    let event = ChainEvent::new(
        EventId::new(),
        WriterId::default(),
        "test",
        json!({"value": 42}),
    );
    
    // Create context
    let mut ctx = MiddlewareContext::new();
    println!("Initial context has {} events\n", ctx.events.len());
    
    // Pre-handle phase
    println!("=== PRE-HANDLE PHASE ===");
    for mw in &middleware {
        let _ = mw.pre_handle(&event, &mut ctx);
        println!();
    }
    
    println!("\nContext after pre-handle has {} events", ctx.events.len());
    
    // Simulate processing
    let results = vec![event.clone()];
    
    // Post-handle phase (reverse order)
    println!("\n=== POST-HANDLE PHASE (reverse order) ===");
    for mw in middleware.iter().rev() {
        mw.post_handle(&event, &results, &mut ctx);
        println!();
    }
    
    println!("\n=== FINAL CONTEXT ===");
    println!("Total events: {}", ctx.events.len());
    for (i, event) in ctx.events.iter().enumerate() {
        println!("Event #{}: {} - {} - {:?}", i, event.source, event.event_type, event.data);
    }
}
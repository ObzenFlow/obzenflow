//! Demo of the blocking rate limiter implementation
//!
//! This demonstrates that the rate limiter now blocks instead of dropping events.

use obzenflow_adapters::middleware::{
    Middleware, MiddlewareAction, MiddlewareContext,
    RateLimiterFactory, MiddlewareFactory,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_runtime_services::pipeline::config::StageConfig;
use obzenflow_core::StageId;
use std::time::Instant;
use std::thread;
use std::sync::{Arc, Mutex};
use serde_json::json;

fn create_test_event(index: u32) -> ChainEvent {
    ChainEvent::new(
        EventId::new(),
        WriterId::new(),
        "test.event",
        json!({ "index": index }),
    )
}

fn main() {
    // Test blocking rate limiter - 2 events per second
    println!("🚀 Testing Blocking Rate Limiter (2 events/second)");
    println!("================================================");
    
    let factory = RateLimiterFactory::new(2.0);
    let config = StageConfig {
        stage_id: StageId::new(),
        name: "rate_limit_demo".to_string(),
        flow_name: "demo_flow".to_string(),
    };
    let middleware = factory.create(&config);
    
    // Track processed events
    let processed = Arc::new(Mutex::new(Vec::new()));
    let processed_clone = processed.clone();
    
    // Spawn thread to process events
    let handle = thread::spawn(move || {
        let mut ctx = MiddlewareContext::new();
        let start = Instant::now();
        
        for i in 0..10 {
            println!("\n⏳ Processing event {}...", i);
            let event = create_test_event(i);
            
            // This will block when out of tokens
            let before = Instant::now();
            let action = middleware.pre_handle(&event, &mut ctx);
            let blocked_time = before.elapsed();
            
            match action {
                MiddlewareAction::Continue => {
                    let elapsed = start.elapsed();
                    println!("✅ Event {} processed at {:.2}s (blocked for {:.2}s)", 
                        i, 
                        elapsed.as_secs_f64(),
                        blocked_time.as_secs_f64()
                    );
                    processed_clone.lock().unwrap().push((i, elapsed));
                }
                _ => {
                    println!("❌ Event {} was not continued!", i);
                }
            }
        }
        
        start.elapsed()
    });
    
    // Wait for processing to complete
    let total_time = handle.join().unwrap();
    
    // Verify results
    println!("\n📊 Results:");
    println!("===========");
    
    let events = processed.lock().unwrap();
    println!("Events processed: {}/10", events.len());
    println!("Total time: {:.2}s", total_time.as_secs_f64());
    println!("Expected time: ~{:.2}s (10 events at 2/sec)", 10.0 / 2.0);
    
    // Verify no events were lost
    if events.len() == 10 {
        println!("\n✅ SUCCESS: All events were processed!");
        println!("   The rate limiter blocks instead of dropping events.");
    } else {
        println!("\n❌ FAILURE: Some events were lost!");
    }
    
    // Show timing distribution
    println!("\nTiming distribution:");
    for (i, (idx, time)) in events.iter().enumerate() {
        let expected_time = (i as f64) * 0.5; // 2 events/sec = 0.5s per event
        println!("  Event {}: {:.2}s (expected: ~{:.2}s)", 
            idx, time.as_secs_f64(), expected_time);
    }
}
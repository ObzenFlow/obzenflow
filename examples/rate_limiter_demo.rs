//! Interactive demo of the rate limiter middleware
//!
//! This demo shows the rate limiter in action with real timing and visual output.

use obzenflow_adapters::middleware::{
    Middleware, MiddlewareAction, MiddlewareContext,
    RateLimiterMiddleware,
};
use obzenflow_adapters::middleware::common;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::journal::writer_id::WriterId;
use std::time::{Duration, Instant};
use std::thread;
use serde_json::json;

fn create_test_event(index: u32) -> ChainEvent {
    ChainEvent::new(
        EventId::new(),
        WriterId::new(),
        "test.event",
        json!({ "index": index }),
    )
}

fn print_separator() {
    println!("\n{}", "=".repeat(60));
}

fn demo_basic_rate_limiting() {
    print_separator();
    println!("📊 Demo 1: Basic Rate Limiting (5 events/second)");
    print_separator();
    
    let factory = RateLimiterFactory::new(5.0);
    let middleware = factory.create(&Default::default());
    let mut ctx = MiddlewareContext::new();
    
    println!("\nSending 10 events rapidly...");
    let start = Instant::now();
    
    for i in 0..10 {
        let event = create_test_event(i);
        let action = middleware.pre_handle(&event, &mut ctx);
        
        match action {
            MiddlewareAction::Continue => {
                println!("✅ Event {} allowed at {:>4}ms", 
                    i, start.elapsed().as_millis());
            }
            MiddlewareAction::Skip(_) => {
                println!("⏸️  Event {} delayed at {:>4}ms", 
                    i, start.elapsed().as_millis());
            }
            _ => {}
        }
        
        // Small delay to see the effect
        thread::sleep(Duration::from_millis(50));
    }
    
    println!("\nSummary: First 5 events pass, next 5 are delayed");
}

fn demo_burst_capacity() {
    print_separator();
    println!("📊 Demo 2: Burst Capacity (10/sec rate, 20 burst)");
    print_separator();
    
    let factory = RateLimiterFactory::new(10.0).with_burst(20.0);
    let middleware = factory.create(&Default::default());
    let mut ctx = MiddlewareContext::new();
    
    println!("\nSending 25 events in rapid burst...");
    let start = Instant::now();
    
    for i in 0..25 {
        let event = create_test_event(i);
        let action = middleware.pre_handle(&event, &mut ctx);
        
        match action {
            MiddlewareAction::Continue => {
                println!("✅ Event {:>2} allowed (burst) at {:>4}ms", 
                    i, start.elapsed().as_millis());
            }
            MiddlewareAction::Skip(_) => {
                println!("⏸️  Event {:>2} delayed at {:>4}ms", 
                    i, start.elapsed().as_millis());
            }
            _ => {}
        }
    }
    
    println!("\nSummary: First 20 events use burst capacity, 5 are delayed");
}

fn demo_token_refill() {
    print_separator();
    println!("📊 Demo 3: Token Refill Over Time (2/sec rate)");
    print_separator();
    
    let factory = RateLimiterFactory::new(2.0).with_burst(4.0);
    let middleware = factory.create(&Default::default());
    let mut ctx = MiddlewareContext::new();
    
    println!("\nPhase 1: Use up burst capacity...");
    for i in 0..4 {
        let event = create_test_event(i);
        middleware.pre_handle(&event, &mut ctx);
        println!("✅ Event {} sent (using burst)", i);
    }
    
    println!("\nPhase 2: Wait 1 second for refill...");
    thread::sleep(Duration::from_secs(1));
    println!("⏰ 2 tokens should have refilled");
    
    println!("\nPhase 3: Send 3 more events...");
    for i in 4..7 {
        let event = create_test_event(i);
        let action = middleware.pre_handle(&event, &mut ctx);
        
        match action {
            MiddlewareAction::Continue => {
                println!("✅ Event {} allowed (refilled token)", i);
            }
            MiddlewareAction::Skip(_) => {
                println!("⏸️  Event {} delayed (no tokens)", i);
            }
            _ => {}
        }
    }
    
    println!("\nSummary: 2 events use refilled tokens, 1 is delayed");
}

fn demo_control_events() {
    print_separator();
    println!("📊 Demo 4: Control Events Always Pass");
    print_separator();
    
    let factory = RateLimiterFactory::new(1.0); // Very restrictive
    let middleware = factory.create(&Default::default());
    let mut ctx = MiddlewareContext::new();
    
    println!("\nExhausting the single token...");
    let data_event = create_test_event(0);
    middleware.pre_handle(&data_event, &mut ctx);
    println!("✅ Data event sent (used the token)");
    
    println!("\nSending EOF control event...");
    let eof = ChainEvent::eof(EventId::new(), WriterId::new(), true);
    let action = middleware.pre_handle(&eof, &mut ctx);
    
    match action {
        MiddlewareAction::Continue => {
            println!("✅ EOF passed through (control events always pass)");
        }
        _ => {
            println!("❌ Unexpected behavior");
        }
    }
    
    println!("\nSending another data event...");
    let data_event2 = create_test_event(1);
    let action = middleware.pre_handle(&data_event2, &mut ctx);
    
    match action {
        MiddlewareAction::Skip(_) => {
            println!("⏸️  Data event delayed (no tokens available)");
        }
        _ => {
            println!("❌ Unexpected behavior");
        }
    }
}

fn demo_weighted_events() {
    print_separator();
    println!("📊 Demo 5: Weighted Events (100 tokens/sec, 5 cost/event)");
    print_separator();
    
    let factory = RateLimiterFactory::new(100.0)
        .with_burst(100.0)
        .with_cost(5.0);
    let middleware = factory.create(&Default::default());
    let mut ctx = MiddlewareContext::new();
    
    println!("\nEffective rate: 100/5 = 20 events/second");
    println!("Burst capacity: 100/5 = 20 events\n");
    
    for i in 0..25 {
        let event = create_test_event(i);
        let action = middleware.pre_handle(&event, &mut ctx);
        
        match action {
            MiddlewareAction::Continue => {
                println!("✅ Event {:>2} allowed (cost: 5 tokens)", i);
            }
            MiddlewareAction::Skip(_) => {
                println!("⏸️  Event {:>2} delayed (insufficient tokens)", i);
                break; // Stop at first delay for clarity
            }
            _ => {}
        }
    }
    
    println!("\nSummary: 20 events allowed (100 tokens / 5 per event)");
}

fn main() {
    println!("🚀 Rate Limiter Interactive Demo");
    println!("================================");
    println!("\nThis demo shows the rate limiter behavior with real timing.");
    println!("Watch how events are allowed or delayed based on token availability.\n");
    
    println!("Press Enter to start Demo 1...");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
    
    demo_basic_rate_limiting();
    
    println!("\n\nPress Enter for Demo 2...");
    std::io::stdin().read_line(&mut input).unwrap();
    
    demo_burst_capacity();
    
    println!("\n\nPress Enter for Demo 3...");
    std::io::stdin().read_line(&mut input).unwrap();
    
    demo_token_refill();
    
    println!("\n\nPress Enter for Demo 4...");
    std::io::stdin().read_line(&mut input).unwrap();
    
    demo_control_events();
    
    println!("\n\nPress Enter for Demo 5...");
    std::io::stdin().read_line(&mut input).unwrap();
    
    demo_weighted_events();
    
    print_separator();
    println!("✨ Demo Complete!");
    println!("\nKey Takeaways:");
    println!("- Token bucket provides smooth rate limiting");
    println!("- Burst capacity handles traffic spikes");
    println!("- Tokens refill at configured rate");
    println!("- Control events always pass through");
    println!("- Events are delayed, never dropped");
    print_separator();
}
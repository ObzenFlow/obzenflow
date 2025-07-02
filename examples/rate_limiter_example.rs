//! Example demonstrating rate limiting middleware with control strategies
//!
//! This example shows how rate limiting uses delay strategies to ensure
//! delayed events are flushed before EOF.

use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_runtime_services::stages::common::handlers::TransformHandler;
use obzenflow_adapters::middleware::{common, RateLimiterFactory};
use std::time::Duration;
use serde_json::json;

/// A simple transform that adds sequence numbers
struct SequenceTransform {
    counter: std::sync::atomic::AtomicU64,
}

impl SequenceTransform {
    fn new() -> Self {
        Self {
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl TransformHandler for SequenceTransform {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        let seq = self.counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(obj) = event.payload.as_object_mut() {
            obj.insert("sequence".to_string(), json!(seq));
        }
        vec![event]
    }
}

fn main() {
    println!("=== Rate Limiter Middleware Example ===\n");
    
    // Create rate limiter that allows 10 events per second with burst of 20
    let rate_limiter = common::rate_limit_with_burst(10.0, 20.0);
    
    println!("Created rate limiter:");
    println!("- Rate: 10 events/second");
    println!("- Burst capacity: 20 events");
    println!("- Uses token bucket algorithm");
    
    // Check the control strategy requirement
    if let Some(requirement) = rate_limiter.required_control_strategy() {
        match requirement {
            obzenflow_adapters::middleware::ControlStrategyRequirement::Windowing { window_duration } => {
                println!("- Control strategy: Delay EOF by {}s to flush delayed events", 
                    window_duration.as_secs_f64());
            }
            _ => {}
        }
    }
    
    println!("\nHow it works:");
    println!("1. Token bucket starts with 20 tokens (burst capacity)");
    println!("2. Each event consumes 1 token");
    println!("3. Tokens refill at 10/second");
    println!("4. When no tokens available, events are delayed");
    println!("5. Control events (like EOF) always pass through");
    println!("6. EOF strategy delays to ensure delayed events are flushed");
    
    println!("\nBehavior under different scenarios:");
    
    println!("\n📊 Scenario 1: Burst of 30 events");
    println!("- First 20 events: Pass immediately (using burst capacity)");
    println!("- Next 10 events: Delayed");
    println!("- Over next second: 10 delayed events released as tokens refill");
    
    println!("\n📊 Scenario 2: Steady rate of 15 events/second");
    println!("- First second: 15 events pass (using 15 tokens)");
    println!("- Remaining capacity: 5 tokens");
    println!("- Second second: First 10 pass, 5 delayed");
    println!("- Steady state: 10 pass, 5 delayed each second");
    
    println!("\n📊 Scenario 3: EOF arrives with delayed events");
    println!("- Delayed queue has 8 events");
    println!("- EOF arrives → Strategy delays by 0.8 seconds");
    println!("- During delay: Tokens refill, delayed events released");
    println!("- After delay: EOF forwarded, stage terminates cleanly");
    
    // Mathematical guarantees
    println!("\n🔢 Mathematical Guarantees:");
    println!("- Average rate: Exactly 10 events/second over time");
    println!("- Max burst: 20 events instantly");
    println!("- Max delay: burst_capacity / rate = 20/10 = 2 seconds");
    println!("- No events lost: All delayed events eventually processed");
    
    // Advanced features
    println!("\n🚀 Advanced Features:");
    
    // Weighted rate limiting
    let weighted_limiter = RateLimiterFactory::new(100.0)
        .with_burst(200.0)
        .with_cost(5.0); // Each event costs 5 tokens
    
    println!("\nWeighted rate limiting:");
    println!("- Token rate: 100/second");
    println!("- Event cost: 5 tokens");
    println!("- Effective rate: 20 events/second");
    println!("- Use case: Different event types have different costs");
    
    // Usage in DSL
    println!("\nUsage in DSL:");
    println!(r#"
    transform!("api_processor" => SequenceTransform::new(), [
        common::rate_limit_with_burst(100.0, 500.0),
        common::logging(tracing::Level::INFO),
    ]);
    "#);
    
    println!("\nBenefits:");
    println!("✓ Protects downstream services from overload");
    println!("✓ Smooths out traffic bursts");
    println!("✓ No data loss - events are delayed, not dropped");
    println!("✓ Clean shutdown with all delayed events processed");
    println!("✓ Control events maintain pipeline coordination");
    
    println!("\n🔍 Token Bucket vs Other Algorithms:");
    println!("- Token Bucket: Allows bursts, smooth refill");
    println!("- Leaky Bucket: Fixed output rate, no bursts");
    println!("- Fixed Window: Can have 2x spikes at window boundaries");
    println!("- Sliding Window: More memory intensive");
    println!("→ Token bucket chosen for flexibility and efficiency");
}
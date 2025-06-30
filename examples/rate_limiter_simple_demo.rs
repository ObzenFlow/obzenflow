//! Simple demonstration of rate limiting behavior
//!
//! Run this to see rate limiting in action!

use std::time::{Duration, Instant};
use std::thread;

fn simulate_rate_limiting(rate: f64, burst: f64, event_count: usize) {
    println!("\n🚀 Rate Limiting Simulation");
    println!("   Rate: {} events/sec", rate);
    println!("   Burst: {} events", burst);
    println!("   Sending: {} events\n", event_count);
    
    let mut tokens = burst;
    let refill_per_sec = rate;
    let mut last_refill = Instant::now();
    let start = Instant::now();
    
    let mut allowed = 0;
    let mut delayed = 0;
    
    for i in 0..event_count {
        // Refill tokens based on elapsed time
        let now = Instant::now();
        let elapsed = now.duration_since(last_refill).as_secs_f64();
        tokens = (tokens + elapsed * refill_per_sec).min(burst);
        last_refill = now;
        
        // Try to consume a token
        if tokens >= 1.0 {
            tokens -= 1.0;
            allowed += 1;
            println!("✅ Event {:>2} allowed at {:>4}ms (tokens: {:.1})", 
                i, start.elapsed().as_millis(), tokens);
        } else {
            delayed += 1;
            println!("⏸️  Event {:>2} delayed at {:>4}ms (tokens: {:.1})", 
                i, start.elapsed().as_millis(), tokens);
        }
        
        // Small delay between events
        thread::sleep(Duration::from_millis(50));
    }
    
    let elapsed = start.elapsed().as_secs_f64();
    println!("\n📊 Summary:");
    println!("   Duration: {:.2}s", elapsed);
    println!("   Allowed: {} events", allowed);
    println!("   Delayed: {} events", delayed);
    println!("   Effective rate: {:.1} events/sec", allowed as f64 / elapsed);
}

fn main() {
    println!("=== Rate Limiter Behavior Demo ===");
    println!("\nThis demonstrates how token bucket rate limiting works.\n");
    
    // Demo 1: Basic rate limiting
    println!("Demo 1: Basic Rate Limiting");
    println!("------------------------");
    simulate_rate_limiting(5.0, 5.0, 15);
    
    println!("\nPress Enter to continue...");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
    
    // Demo 2: Burst capacity
    println!("\nDemo 2: Burst Capacity");
    println!("---------------------");
    simulate_rate_limiting(5.0, 20.0, 25);
    
    println!("\nPress Enter to continue...");
    std::io::stdin().read_line(&mut input).unwrap();
    
    // Demo 3: High rate
    println!("\nDemo 3: High Rate");
    println!("----------------");
    simulate_rate_limiting(20.0, 20.0, 30);
    
    println!("\n✨ Demo Complete!");
    println!("\nIn FlowState, the rate limiter middleware:");
    println!("- Uses this token bucket algorithm");
    println!("- Delays events instead of dropping them");
    println!("- Ensures all delayed events are flushed before EOF");
    println!("- Integrates with control strategies for clean shutdown");
}
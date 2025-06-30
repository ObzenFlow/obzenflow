//! Example demonstrating control strategy composition
//!
//! This example shows how multiple middleware can contribute control strategies
//! that work together with clear precedence rules.

use std::time::Duration;

fn main() {
    println!("=== Control Strategy Composition Example ===\n");
    
    println!("Scenario: A transform stage with both windowing and circuit breaker middleware\n");
    
    // In DSL, this would look like:
    println!("Pipeline configuration:");
    println!(r#"
    transform!("processor" => DataProcessor, [
        windowing::count(Duration::from_secs(60)),    // 60-second windows
        circuit_breaker::new(10, Duration::from_secs(30)), // Opens after 10 failures
    ]);
    "#);
    
    println!("\nStrategy Requirements:");
    println!("1. Windowing middleware requires: Delay strategy (60s)");
    println!("2. Circuit breaker requires: Retry strategy (3 attempts, exponential backoff)");
    
    println!("\nComposed Strategy Behavior:");
    println!("When EOF arrives, the composite strategy applies precedence rules:\n");
    
    println!("Case 1: Normal operation (no failures)");
    println!("  - Windowing: 'Delay 45s' (window has 45s remaining)");
    println!("  - Circuit Breaker: 'Forward' (healthy, no retries needed)");
    println!("  - Result: Delay wins → Wait 45s for window to complete");
    
    println!("\nCase 2: Under failure (circuit breaker open)");
    println!("  - Windowing: 'Delay 30s' (window has 30s remaining)");
    println!("  - Circuit Breaker: 'Retry' (unhealthy, needs recovery time)");
    println!("  - Result: Delay wins → Wait 30s first");
    println!("  - After delay, if still unhealthy → Retry");
    
    println!("\nCase 3: Window complete but failures exist");
    println!("  - Windowing: 'Forward' (window complete)");
    println!("  - Circuit Breaker: 'Retry' (still recovering)");
    println!("  - Result: Retry wins → Don't terminate yet");
    
    println!("\nPrecedence Rules:");
    println!("1. Delay > Retry > Skip > Forward");
    println!("2. Most restrictive action wins");
    println!("3. Safety first principle");
    
    println!("\nBenefits of Strategy Composition:");
    println!("✓ Complex behaviors emerge from simple rules");
    println!("✓ Middleware remain focused on their concerns");
    println!("✓ Predictable, testable outcomes");
    println!("✓ No coordination needed between middleware");
    
    println!("\nOther Composition Examples:");
    
    println!("\n1. Batching + Rate Limiting:");
    println!("   - Batching: Delay until batch size reached");
    println!("   - Rate Limiter: Forward (within rate)");
    println!("   - Result: Wait for batch to fill");
    
    println!("\n2. Retry + Timeout:");
    println!("   - Retry: Retry on failures");
    println!("   - Timeout: Forward (no special EOF handling)");
    println!("   - Result: Retry with timeout constraints");
    
    println!("\n3. Multiple Windowing (e.g., 1min + 5min aggregations):");
    println!("   - Window1: Delay 45s");
    println!("   - Window2: Delay 3m 20s");
    println!("   - Result: Delay 3m 20s (longest delay)");
}
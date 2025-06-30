// Example demonstrating the complete 5-layer middleware architecture from FLOWIP-054a
//
// This example shows:
// - Layer 1: Operational middleware (circuit breaker, retry, rate limiter)
// - Layer 2: SLI computation (availability, success rate, latency)
// - Layer 3: Monitoring taxonomies (RED metrics)
// - Layer 4: SLO tracking (99.9% availability target)
// - Layer 5: Debug (logging all events)

use obzenflow_adapters::middleware::{
    // Core traits
    Middleware, MiddlewareContext, MiddlewareAction,
    
    // Layer 1: Operational middleware
    CircuitBreakerMiddleware, RetryMiddleware,
    
    // Layer 2: SLI middleware
    sli::{CircuitBreakerSLI, RetrySLI, LatencySLI, SLOTracker, SLODefinition, AlertConfig},
};
use obzenflow_core::{ChainEvent, EventId, WriterId};
use serde_json::json;
use std::time::{Duration, Instant};

/// Simple payment processor that demonstrates event processing
fn process_payment(event: &ChainEvent) -> Vec<ChainEvent> {
    println!("Processing payment: {:?}", event.payload);
    
    // Simulate some processing logic
    if let Some(amount) = event.payload["amount"].as_f64() {
        if amount > 1000.0 {
            // Large payments always fail to demonstrate circuit breaker
            println!("❌ Payment processing failed for large amount: {}", amount);
            return vec![]; // Failure
        }
        
        // Simulate processing delay
        std::thread::sleep(Duration::from_millis((amount * 0.1) as u64));
        
        // Success - return processed event
        vec![ChainEvent::new(
            EventId::new(),
            event.writer_id,
            "payment.processed",
            json!({
                "original_id": event.id.to_string(),
                "amount": amount,
                "status": "completed",
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }),
        )]
    } else {
        println!("Invalid payment event - missing amount");
        vec![]
    }
}

/// Debug middleware that logs all events in the context
struct DebugMiddleware {
    stage_name: String,
}

impl DebugMiddleware {
    fn new(stage_name: String) -> Self {
        Self { stage_name }
    }
}

impl Middleware for DebugMiddleware {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        println!("[{}] Pre-handle: Processing event {}", self.stage_name, event.id);
        println!("[{}] Context has {} events at pre-handle", self.stage_name, ctx.events.len());
        
        // Log any middleware events so far
        for (i, mw_event) in ctx.events.iter().enumerate() {
            println!(
                "[{}] Pre-handle event #{}: {} - {} - {:?}",
                self.stage_name, i, mw_event.source, mw_event.event_type, mw_event.data
            );
        }
        
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        println!(
            "[{}] Post-handle: Event {} produced {} results",
            self.stage_name,
            event.id,
            results.len()
        );
        println!("[{}] Context has {} events at post-handle", self.stage_name, ctx.events.len());
        
        // Log all middleware events
        for (i, mw_event) in ctx.events.iter().enumerate() {
            println!(
                "[{}] Post-handle event #{}: {} - {}",
                self.stage_name, i, mw_event.source, mw_event.event_type
            );
            
            match (mw_event.source.as_str(), mw_event.event_type.as_str()) {
                // Operational events
                ("circuit_breaker", event_type) => {
                    println!("[{}] ⚡ Circuit breaker: {} - {:?}", self.stage_name, event_type, mw_event.data);
                }
                ("retry", event_type) => {
                    println!("[{}] 🔄 Retry: {} - {:?}", self.stage_name, event_type, mw_event.data);
                }
                
                // SLI events
                ("sli", "availability") => {
                    if let Some(value) = mw_event.data["value"].as_f64() {
                        println!("[{}] 📊 Availability SLI: {:.2}%", self.stage_name, value * 100.0);
                    }
                }
                ("sli", "overall_success_rate") => {
                    if let Some(value) = mw_event.data["value"].as_f64() {
                        println!("[{}] 📊 Success rate SLI: {:.2}%", self.stage_name, value * 100.0);
                    }
                }
                ("sli", "latency_percentiles") => {
                    if let Some(p99) = mw_event.data["p99_ms"].as_u64() {
                        println!("[{}] 📊 Latency P99: {}ms", self.stage_name, p99);
                    }
                }
                
                // SLO events
                ("slo", "burn_rate_alert") => {
                    println!("[{}] 🚨 SLO burn rate alert: {:?}", self.stage_name, mw_event.data);
                }
                ("slo", "budget_exhausted") => {
                    println!("[{}] ❌ SLO budget exhausted: {:?}", self.stage_name, mw_event.data);
                }
                ("slo", "compliance_update") => {
                    let remaining = mw_event.data["budget_remaining_percentage"].as_f64().unwrap_or(0.0);
                    if remaining < 10.0 {
                        println!("[{}] ⚠️  SLO budget low: {:.2}% remaining", self.stage_name, remaining);
                    }
                }
                
                _ => {}
            }
        }
    }
}

fn main() {
    println!("=== Layered Middleware Example ===\n");
    
    let stage_name = "payment_processor";
    
    // Define SLOs
    let slos = vec![
        SLODefinition {
            name: "payment_availability".to_string(),
            indicator: format!("{}_availability", stage_name),
            target: 0.999, // 99.9% availability
            window: Duration::from_secs(3600), // 1 hour window for demo
            lower_is_better: false,
            alert_config: AlertConfig {
                fast_burn_threshold: 0.05,
                fast_burn_window: Duration::from_secs(60), // 1 minute
                slow_burn_threshold: 0.5,
                slow_burn_window: Duration::from_secs(600), // 10 minutes
            },
        },
        SLODefinition {
            name: "payment_success_rate".to_string(),
            indicator: format!("{}_overall_success", stage_name),
            target: 0.95, // 95% success rate
            window: Duration::from_secs(3600),
            lower_is_better: false,
            alert_config: AlertConfig::default(),
        },
    ];
    
    // Create middleware instances for all 5 layers
    let circuit_breaker = CircuitBreakerMiddleware::new(5);
    let retry = RetryMiddleware::exponential(3);
    let cb_sli = CircuitBreakerSLI::new(stage_name.to_string());
    let retry_sli = RetrySLI::new(stage_name.to_string());
    let latency_sli = LatencySLI::new(stage_name.to_string());
    let slo_tracker = SLOTracker::with_objectives(stage_name.to_string(), slos);
    let debug = DebugMiddleware::new(stage_name.to_string());
    
    // Create the middleware stack
    let middleware: Vec<&dyn Middleware> = vec![
        &circuit_breaker,   // Layer 1 - Circuit breaker (first to check)
        &retry,            // Layer 1 - Retry (after CB)
        &cb_sli,           // Layer 2 - Circuit breaker SLI
        &retry_sli,        // Layer 2 - Retry SLI
        &latency_sli,      // Layer 2 - Latency SLI
        &slo_tracker,      // Layer 4 - SLO tracking
        &debug,            // Layer 5 - Debug (last)
    ];
    
    println!("Created {} middleware layers", middleware.len());
    println!("\nProcessing payment events...\n");
    
    // Create test events
    let writer_id = WriterId::default();
    let events = vec![
        // Start with large payments to trigger circuit breaker immediately
        ChainEvent::new(
            EventId::new(),
            writer_id,
            "payment.requested",
            json!({
                "amount": 5000.0,
                "currency": "USD",
                "customer_id": "cust_456",
            }),
        ),
        ChainEvent::new(
            EventId::new(),
            writer_id,
            "payment.requested",
            json!({
                "amount": 3000.0,
                "currency": "USD",
                "customer_id": "cust_457",
            }),
        ),
        ChainEvent::new(
            EventId::new(),
            writer_id,
            "payment.requested",
            json!({
                "amount": 2500.0,
                "currency": "USD",
                "customer_id": "cust_458",
            }),
        ),
        ChainEvent::new(
            EventId::new(),
            writer_id,
            "payment.requested",
            json!({
                "amount": 4000.0,
                "currency": "USD",
                "customer_id": "cust_459",
            }),
        ),
        ChainEvent::new(
            EventId::new(),
            writer_id,
            "payment.requested",
            json!({
                "amount": 3500.0,
                "currency": "USD",
                "customer_id": "cust_460",
            }),
        ),
        // Normal payments (should be rejected if circuit is open)
        ChainEvent::new(
            EventId::new(),
            writer_id,
            "payment.requested",
            json!({
                "amount": 100.0,
                "currency": "USD",
                "customer_id": "cust_123",
            }),
        ),
        ChainEvent::new(
            EventId::new(),
            writer_id,
            "payment.requested",
            json!({
                "amount": 250.0,
                "currency": "USD",
                "customer_id": "cust_124",
            }),
        ),
        // Invalid payment
        ChainEvent::new(
            EventId::new(),
            writer_id,
            "payment.requested",
            json!({
                "currency": "USD",
                "customer_id": "cust_789",
                // Missing amount!
            }),
        ),
        // More normal payments to see recovery
        ChainEvent::new(
            EventId::new(),
            writer_id,
            "payment.requested",
            json!({
                "amount": 50.0,
                "currency": "USD",
                "customer_id": "cust_125",
            }),
        ),
        ChainEvent::new(
            EventId::new(),
            writer_id,
            "payment.requested",
            json!({
                "amount": 75.0,
                "currency": "USD",
                "customer_id": "cust_126",
            }),
        ),
    ];
    
    // Process events through the middleware stack
    for (i, event) in events.iter().enumerate() {
        println!("\n--- Event {} (amount: {:?}) ---", i + 1, event.payload["amount"]);
        
        let mut ctx = MiddlewareContext::new();
        let start = Instant::now();
        
        // Pre-handle phase
        let mut should_continue = true;
        for (idx, mw) in middleware.iter().enumerate() {
            println!("\n🔹 Running pre-handle for middleware #{}", idx);
            match mw.pre_handle(event, &mut ctx) {
                MiddlewareAction::Continue => {
                    println!("  → Continue to next middleware");
                    continue;
                }
                MiddlewareAction::Skip(results) => {
                    println!("⏭️  Middleware #{} skipped processing, returning {} results", idx, results.len());
                    should_continue = false;
                    break;
                }
                MiddlewareAction::Abort => {
                    println!("🛑 Middleware #{} aborted processing", idx);
                    should_continue = false;
                    break;
                }
            }
        }
        
        // Process if not skipped/aborted
        let results = if should_continue {
            process_payment(event)
        } else {
            vec![]
        };
        
        // Check context state before post-handle
        println!("\n📊 Context state before post-handle: {} events", ctx.events.len());
        for (i, mw_event) in ctx.events.iter().enumerate() {
            println!("  Event #{}: {} - {}", i, mw_event.source, mw_event.event_type);
        }
        
        let duration = start.elapsed();
        
        // Post-handle phase (in reverse order)
        println!("\n🔶 Starting post-handle phase (reverse order):");
        for (idx, mw) in middleware.iter().rev().enumerate() {
            println!("🔸 Running post-handle for middleware #{} (reverse idx: {})", middleware.len() - idx - 1, idx);
            mw.post_handle(event, &results, &mut ctx);
        }
        
        println!(
            "✅ Event {} completed in {:?} with {} results",
            event.id,
            duration,
            results.len()
        );
        
        // Summary of all events emitted
        if i == 0 || i == 4 || i == 9 {  // Show details for first event, 5th (after CB opens), and last
            println!("\n📋 Event Summary ({} total middleware events):", ctx.events.len());
            
            // Group events by source
            let mut by_source: std::collections::HashMap<&str, Vec<&obzenflow_adapters::middleware::MiddlewareEvent>> = std::collections::HashMap::new();
            for event in &ctx.events {
                by_source.entry(&event.source).or_insert_with(Vec::new).push(event);
            }
            
            // Show events by layer
            if let Some(cb_events) = by_source.get("circuit_breaker") {
                println!("  Layer 1 - Circuit Breaker: {} events", cb_events.len());
                for e in cb_events {
                    println!("    ⚡ {}", e.event_type);
                }
            }
            
            if let Some(retry_events) = by_source.get("retry") {
                println!("  Layer 1 - Retry: {} events", retry_events.len());
                for e in retry_events {
                    println!("    🔄 {}", e.event_type);
                }
            }
            
            if let Some(sli_events) = by_source.get("sli") {
                println!("  Layer 2 - SLI: {} events", sli_events.len());
                for e in sli_events {
                    match e.event_type.as_str() {
                        "availability" => {
                            if let Some(value) = e.data["value"].as_f64() {
                                println!("    📊 availability: {:.2}%", value * 100.0);
                            }
                        }
                        "latency_percentiles" => {
                            if let Some(p99) = e.data["p99_ms"].as_u64() {
                                println!("    ⏱️  latency p99: {}ms", p99);
                            }
                        }
                        _ => println!("    📊 {}", e.event_type)
                    }
                }
            }
            
            if let Some(slo_events) = by_source.get("slo") {
                println!("  Layer 4 - SLO: {} events", slo_events.len());
                for e in slo_events {
                    match e.event_type.as_str() {
                        "compliance_update" => {
                            if let Some(remaining) = e.data["budget_remaining_percentage"].as_f64() {
                                if remaining < 50.0 {
                                    println!("    ⚠️  compliance: {:.1}% budget remaining", remaining);
                                }
                            }
                        }
                        "burn_rate_alert" => println!("    🚨 {}", e.event_type),
                        _ => {}
                    }
                }
            }
        }
    }
    
    println!("\n=== Example Complete ===");
    println!("\nThis example demonstrated:");
    println!("- Layer 1: Circuit breaker and retry handling failures");
    println!("- Layer 2: SLI computation for availability, success rate, and latency");
    println!("- Layer 3: Monitoring (RED metrics - shown via SLI events)");
    println!("- Layer 4: SLO tracking with burn rate alerts");
    println!("- Layer 5: Debug logging of all middleware events");
}

// Required for the example
#[allow(dead_code)]
fn main_required_dependencies() {
    // These ensure the example's dependencies are available
    let _ = chrono::Utc::now();
    let _ = rand::random::<f64>();
}
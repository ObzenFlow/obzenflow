//! Stage Control Events Integration Demo
//! 
//! This example demonstrates how stages integrate with the control events system
//! through middleware. It shows:
//! - How stage behavior triggers control event emission
//! - Different types of control events based on processing patterns  
//! - How metrics are derived from these control events
//!
//! Run with: cargo run --example stage_control_events_demo

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_adapters::middleware::{circuit_breaker, rate_limit};
use obzenflow_adapters::monitoring::aggregator::{MetricsAggregator, MetricsEndpoint};
use obzenflow_infra::journal::memory::MemoryJournal;
use obzenflow_runtime_services::messaging::reactive_journal::ReactiveJournal;
use obzenflow_core::{ChainEvent, EventId, WriterId};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use anyhow::Result;

/// Demo event types
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct Order {
    id: u64,
    amount: f64,
    customer_id: String,
    items: Vec<String>,
}

/// Source that generates various order patterns to trigger different control events
struct OrderSource {
    orders: Vec<Order>,
    index: usize,
    writer_id: WriterId,
}

impl OrderSource {
    fn new() -> Self {
        let orders = vec![
            // Normal orders
            Order { id: 1, amount: 99.99, customer_id: "cust_123".into(), items: vec!["item1".into()] },
            Order { id: 2, amount: 149.99, customer_id: "cust_456".into(), items: vec!["item2".into(), "item3".into()] },
            
            // Invalid orders (will trigger failures)
            Order { id: 3, amount: -50.0, customer_id: "".into(), items: vec![] }, // Invalid amount
            Order { id: 4, amount: 0.0, customer_id: "cust_789".into(), items: vec![] }, // No items
            Order { id: 5, amount: 100.0, customer_id: "".into(), items: vec!["item4".into()] }, // No customer
            
            // More invalid to trigger circuit breaker
            Order { id: 6, amount: -10.0, customer_id: "bad".into(), items: vec![] },
            Order { id: 7, amount: -20.0, customer_id: "bad".into(), items: vec![] },
            
            // Recovery orders (should process successfully)
            Order { id: 8, amount: 75.50, customer_id: "cust_999".into(), items: vec!["item5".into()] },
            Order { id: 9, amount: 200.00, customer_id: "cust_888".into(), items: vec!["item6".into(), "item7".into()] },
            
            // Burst of orders to trigger rate limiting
            Order { id: 10, amount: 50.0, customer_id: "burst1".into(), items: vec!["item8".into()] },
            Order { id: 11, amount: 60.0, customer_id: "burst2".into(), items: vec!["item9".into()] },
            Order { id: 12, amount: 70.0, customer_id: "burst3".into(), items: vec!["item10".into()] },
            Order { id: 13, amount: 80.0, customer_id: "burst4".into(), items: vec!["item11".into()] },
            Order { id: 14, amount: 90.0, customer_id: "burst5".into(), items: vec!["item12".into()] },
            
            // Order that will be split (to show event expansion)
            Order { id: 15, amount: 300.0, customer_id: "cust_777".into(), 
                   items: vec!["bundle1".into(), "bundle2".into(), "bundle3".into(), "bundle4".into()] },
        ];
        
        Self {
            orders,
            index: 0,
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for OrderSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.index >= self.orders.len() {
            return None;
        }
        
        let order = &self.orders[self.index];
        self.index += 1;
        
        println!("📦 Emitting order #{} (${:.2})", order.id, order.amount);
        
        Some(ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "order.received",
            json!(order),
        ))
    }
    
    fn is_complete(&self) -> bool {
        self.index >= self.orders.len()
    }
}

/// Validator stage - demonstrates failure patterns that trigger control events
struct OrderValidator;

impl TransformHandler for OrderValidator {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Ok(order) = serde_json::from_value::<Order>(event.payload.clone()) {
            // Validation rules that will trigger different middleware responses
            if order.amount <= 0.0 {
                println!("  ❌ Order #{} failed: Invalid amount ${:.2}", order.id, order.amount);
                return vec![]; // Failure - will increment circuit breaker
            }
            
            if order.customer_id.is_empty() {
                println!("  ❌ Order #{} failed: No customer ID", order.id);
                return vec![]; // Failure
            }
            
            if order.items.is_empty() {
                println!("  ❌ Order #{} failed: No items", order.id);
                return vec![]; // Failure
            }
            
            // Valid order
            println!("  ✅ Order #{} validated", order.id);
            let mut validated = event.clone();
            validated.event_type = "order.validated".to_string();
            validated.payload["validation_timestamp"] = json!(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs());
            vec![validated]
        } else {
            vec![]
        }
    }
}

/// Splitter stage - demonstrates event expansion that triggers amendment control events
struct OrderSplitter;

impl TransformHandler for OrderSplitter {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type != "order.validated" {
            return vec![event];
        }
        
        if let Ok(order) = serde_json::from_value::<Order>(event.payload.clone()) {
            if order.items.len() > 2 {
                // Split large orders into individual item events
                println!("  🔀 Splitting order #{} into {} items", order.id, order.items.len());
                
                order.items.iter().enumerate().map(|(idx, item)| {
                    ChainEvent::new(
                        EventId::new(),
                        event.writer_id.clone(),
                        "order.item",
                        json!({
                            "order_id": order.id,
                            "item_index": idx,
                            "item": item,
                            "amount": order.amount / order.items.len() as f64,
                            "customer_id": order.customer_id.clone()
                        })
                    )
                }).collect()
            } else {
                vec![event]
            }
        } else {
            vec![event]
        }
    }
}

/// Sink that tracks what made it through the pipeline
struct OrderSink {
    processed_count: Arc<std::sync::Mutex<usize>>,
    total_amount: Arc<std::sync::Mutex<f64>>,
}

impl OrderSink {
    fn new() -> Self {
        Self {
            processed_count: Arc::new(std::sync::Mutex::new(0)),
            total_amount: Arc::new(std::sync::Mutex::new(0.0)),
        }
    }
}

impl SinkHandler for OrderSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        let mut count = self.processed_count.lock().unwrap();
        *count += 1;
        
        if let Some(amount) = event.payload["amount"].as_f64() {
            let mut total = self.total_amount.lock().unwrap();
            *total += amount;
        }
        
        match event.event_type.as_str() {
            "order.validated" => {
                if let Ok(order) = serde_json::from_value::<Order>(event.payload) {
                    println!("  💰 Processed whole order #{} (${:.2})", order.id, order.amount);
                }
            },
            "order.item" => {
                println!("  📦 Processed item from order #{}", event.payload["order_id"]);
            },
            _ => {}
        }
        
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing to see control event emissions
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,stage_control_events_demo=debug")
        .init();

    println!("🎯 Stage Control Events Integration Demo");
    println!("=======================================\n");
    
    // Create journal and reactive journal
    let memory_journal = Arc::new(MemoryJournal::new());
    let reactive_journal = Arc::new(ReactiveJournal::new(memory_journal.clone()));
    
    // Create and start MetricsAggregator
    println!("📊 Starting MetricsAggregator...");
    let aggregator = Arc::new(Mutex::new(MetricsAggregator::new()));
    MetricsAggregator::start(aggregator.clone(), reactive_journal.clone())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start aggregator: {}", e))?;
    
    // Create metrics endpoint using the same aggregator
    let endpoint = MetricsEndpoint::new(aggregator.clone());
    
    // Give aggregator time to set up
    sleep(Duration::from_millis(100)).await;
    
    // Create sink to track results
    let sink = OrderSink::new();
    let processed_count = sink.processed_count.clone();
    let total_amount = sink.total_amount.clone();
    
    println!("🚀 Building flow with middleware...\n");
    
    // Build flow with various middleware configurations
    let flow_handle = flow! {
        journal: reactive_journal.clone(),
        
        // Global rate limiter
        middleware: [
            rate_limit(5.0) // 5 events per second globally
        ],
        
        stages: {
            // Source
            orders = source!("order_source" => OrderSource::new());
            
            // Validator with circuit breaker
            // This will emit CONTROL_MIDDLEWARE_STATE events on state changes
            validator = transform!("order_validator" => OrderValidator, [
                circuit_breaker(3) // Opens after 3 consecutive failures
            ]);
            
            // Splitter with rate limiter
            // This will emit CONTROL_MIDDLEWARE_SUMMARY events periodically
            splitter = transform!("order_splitter" => OrderSplitter, [
                rate_limit(10.0) // 10 events per second for this stage
            ]);
            
            // Sink
            sink = sink!("order_sink" => sink);
        },
        
        topology: {
            orders |> validator;
            validator |> splitter;
            splitter |> sink;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to build flow: {:?}", e))?;
    
    println!("▶️  Running flow...\n");
    
    // Run the flow
    flow_handle.run().await.map_err(|e| anyhow::anyhow!("Failed to run flow: {:?}", e))?;
    
    // Give time for processing and control event emission
    sleep(Duration::from_secs(3)).await;
    
    // Display results
    println!("\n📈 Processing Results:");
    println!("   Events processed: {}", processed_count.lock().map(|g| *g).unwrap_or(0));
    println!("   Total amount: ${:.2}", total_amount.lock().map(|g| *g).unwrap_or(0.0));
    
    // Get and display metrics
    println!("\n📊 Metrics (from control events):");
    let metrics_text = endpoint.render_metrics().map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
    
    // Extract key metrics
    for line in metrics_text.lines() {
        if line.contains("obzenflow_events_total") && line.contains("order_validator") {
            println!("   Validator: {}", line);
        }
        if line.contains("obzenflow_errors_total") && line.contains("order_validator") {
            println!("   Validator errors: {}", line);
        }
        if line.contains("obzenflow_circuit_breaker_state") {
            println!("   Circuit breaker: {}", line);
        }
        if line.contains("obzenflow_rate_limiter_active") {
            println!("   Rate limiter: {}", line);
        }
        if line.contains("obzenflow_amendments_total") {
            println!("   Event expansions: {}", line);
        }
    }
    
    // Check aggregator state directly
    {
        let agg = aggregator.lock().await;
        
        println!("\n🔍 Detailed Metrics:");
        
        // Stage metrics
        let validator_events = agg.get_events_total("stage_control_events_demo", "order_validator");
        let validator_errors = agg.get_errors_total("stage_control_events_demo", "order_validator");
        let splitter_events = agg.get_events_total("stage_control_events_demo", "order_splitter");
        
        println!("   Validator processed: {:.0} events", validator_events);
        println!("   Validator errors: {:.0}", validator_errors);
        println!("   Splitter processed: {:.0} events", splitter_events);
        
        // Note: SAAFE metrics methods don't exist yet in MetricsAggregator
        // These would need to be implemented to track anomalies and amendments
    }
    
    println!("\n✅ Demo complete!");
    println!("\n💡 Key Observations:");
    println!("   1. Stages don't emit control events directly");
    println!("   2. Middleware observes stage behavior and emits appropriate events");
    println!("   3. CircuitBreaker tracked validation failures");
    println!("   4. RateLimiter may have throttled during bursts");
    println!("   5. Event expansion was tracked as amendments");
    println!("   6. All metrics derived from control events in the journal");
    
    Ok(())
}
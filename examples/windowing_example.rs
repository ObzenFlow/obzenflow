//! Example demonstrating windowing middleware with control strategies
//!
//! This example shows how the windowing middleware uses the delay strategy
//! to ensure time windows complete before accepting EOF.

use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_runtime_services::stages::common::handlers::TransformHandler;
use obzenflow_adapters::middleware::common;
use std::time::Duration;
use serde_json::json;

/// A simple transform that adds timestamps to events
struct TimestampTransform;

impl TransformHandler for TimestampTransform {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        // Add processing timestamp
        if let Some(obj) = event.payload.as_object_mut() {
            obj.insert(
                "processed_at".to_string(),
                json!(std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()),
            );
        }
        vec![event]
    }
}

fn main() {
    println!("=== Windowing Middleware Example ===\n");
    
    // Create windowing middleware that counts events in 5-second windows
    let windowing_factory = common::windowing::count(Duration::from_secs(5));
    
    println!("Created windowing middleware factory:");
    println!("- Name: {}", windowing_factory.name());
    println!("- Window duration: 5 seconds");
    
    // Check the control strategy requirement
    if let Some(requirement) = windowing_factory.required_control_strategy() {
        match requirement {
            obzenflow_adapters::middleware::ControlStrategyRequirement::Windowing { window_duration } => {
                println!("- Control strategy: Windowing with {}s delay", window_duration.as_secs());
            }
            _ => {}
        }
    }
    
    println!("\nHow it works:");
    println!("1. Data events are buffered until the window completes");
    println!("2. When the window duration elapses, an aggregated event is emitted");
    println!("3. When EOF arrives, the strategy delays it to allow final window to complete");
    println!("4. This ensures no data is lost at pipeline shutdown");
    
    // Example: Sum aggregation on a numeric field
    let sum_factory = common::windowing::sum(Duration::from_secs(10), "value");
    println!("\nSum aggregation example:");
    println!("- Sums the 'value' field over 10-second windows");
    
    // Example: Custom aggregation
    let custom_factory = common::windowing::custom(
        Duration::from_secs(60),
        |events| {
            let event_types: std::collections::HashMap<String, usize> = 
                events.iter()
                    .map(|e| e.event_type.clone())
                    .fold(std::collections::HashMap::new(), |mut acc, t| {
                        *acc.entry(t).or_insert(0) += 1;
                        acc
                    });
            
            let mut result = ChainEvent::new(
                EventId::new(),
                WriterId::new(),
                "windowing.type_counts",
                json!({
                    "type_counts": event_types,
                    "total_events": events.len(),
                }),
            );
            result
        }
    );
    
    println!("\nCustom aggregation example:");
    println!("- Counts events by type over 60-second windows");
    
    // In a real pipeline, this would be used like:
    println!("\nUsage in DSL:");
    println!(r#"
    transform!("aggregator" => TimestampTransform, [
        common::windowing::count(Duration::from_secs(30)),
        common::logging(tracing::Level::INFO),
    ]);
    "#);
    
    println!("\nBenefits:");
    println!("✓ Time-based aggregation without data loss");
    println!("✓ Automatic EOF delay ensures final window completes");
    println!("✓ Composable with other middleware");
    println!("✓ Clean separation of concerns");
}
//! Example demonstrating the new middleware-based monitoring approach
//! 
//! This shows how FLOWIP-050a replaces the old Monitor::step approach
//! with composable middleware.

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// Simple processor step
struct DataProcessor;

impl Step for DataProcessor {
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Transform the event
        let mut result = event.clone();
        result.payload["processed"] = json!(true);
        vec![result]
    }
}

// Source that generates events
struct EventGenerator {
    count: Arc<AtomicU64>,
    max_events: u64,
}

impl Step for EventGenerator {
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.count.fetch_add(1, Ordering::Relaxed);
        if current < self.max_events {
            vec![ChainEvent::new("generated", json!({
                "value": current,
                "timestamp": chrono::Utc::now().to_string()
            }))]
        } else {
            vec![]
        }
    }
}

// Sink that consumes events  
struct EventLogger;

impl Step for EventLogger {
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        println!("Received event: {:?}", event.payload);
        vec![] // Sinks consume events
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Create instances of our steps
    let processor = DataProcessor;
    let generator = EventGenerator {
        count: Arc::new(AtomicU64::new(0)),
        max_events: 10,
    };
    let logger = EventLogger;
    
    // Use the flow! macro with the new middleware syntax
    let handle = flow! {
        name: "middleware_example",
        ("generator" => generator, [USE::monitoring()])
        |> ("processor" => processor, [RED::monitoring()])  
        |> ("logger" => logger, [GoldenSignals::monitoring()])
    }?;
    
    // Run for a bit
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    
    // Shutdown gracefully
    handle.shutdown().await?;
    
    Ok(())
}
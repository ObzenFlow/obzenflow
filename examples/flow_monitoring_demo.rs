//! Example demonstrating FLOWIP-055: Flow as a Monitored Resource
//! 
//! This shows how flows can have their own middleware for monitoring,
//! separate from stage monitoring.

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::lifecycle::{EventHandler, ProcessingMode};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Source that generates events
struct EventGenerator {
    count: Arc<AtomicU64>,
    max_events: u64,
}

impl EventHandler for EventGenerator {
    fn transform(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.count.fetch_add(1, Ordering::Relaxed);
        if current < self.max_events {
            vec![ChainEvent::new("generated", json!({
                "id": current,
                "timestamp": chrono::Utc::now().to_string()
            }))]
        } else {
            vec![]
        }
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

/// Processing stage
struct DataProcessor;

impl EventHandler for DataProcessor {
    fn transform(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        event.payload["processed"] = json!(true);
        event.payload["process_time"] = json!(chrono::Utc::now().to_string());
        vec![event]
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

/// Sink that consumes events
struct EventLogger {
    received: Arc<AtomicU64>,
}

impl EventHandler for EventLogger {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let count = self.received.fetch_add(1, Ordering::Relaxed);
        println!("Event #{}: {:?}", count, event.payload);
        vec![] // Sinks consume events
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Enable debug logging for initialization
    std::env::set_var("RUST_LOG", "flowstate_rs=debug");
    tracing_subscriber::fmt::init();
    
    println!("🚀 FLOWIP-055 Demo: Flow-level Monitoring");
    println!("=========================================");
    println!();
    
    // Create step instances
    let generator = EventGenerator {
        count: Arc::new(AtomicU64::new(0)),
        max_events: 5,
    };
    
    let processor = DataProcessor;
    
    let logger = EventLogger {
        received: Arc::new(AtomicU64::new(0)),
    };
    
    // Create flow with flow-level monitoring middleware
    // This demonstrates the new FLOWIP-055 syntax
    let handle = flow! {
        name: "flow_monitoring_demo",
        middleware: [GoldenSignals::monitoring()],        // Flow-level monitoring
        ("source" => generator, [RED::monitoring()])      // Stage monitoring
        |> ("processor" => processor, [USE::monitoring()]) // Stage monitoring
        |> ("sink" => logger, [RED::monitoring()])        // Stage monitoring
    }?;
    
    println!("📊 Flow Monitoring Configuration:");
    println!("  - Flow: GoldenSignals (latency, traffic, errors, saturation)");
    println!("  - Source Stage: RED (rate, errors, duration)");
    println!("  - Processor Stage: USE (utilization, saturation, errors)");
    println!("  - Sink Stage: RED (rate, errors, duration)");
    println!();
    println!("This demonstrates how flow-level metrics are independent from stage metrics!");
    println!();
    
    println!("Pipeline created, waiting for processing...");
    
    // Let it run
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Shutdown
    handle.shutdown().await?;
    
    println!();
    println!("✅ Flow completed successfully!");
    println!();
    println!("In a production system, flow-level metrics would show:");
    println!("  - End-to-end latency (not sum of stage latencies)");
    println!("  - Flow success rate");
    println!("  - Business-level SLAs");
    
    Ok(())
}
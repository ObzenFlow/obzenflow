//! Example demonstrating FLOWIP-055: Flow as a Monitored Resource
//! 
//! This shows how flows can have their own middleware for monitoring,
//! separate from stage monitoring.

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::lifecycle::{EventHandler, ProcessingMode};
use flowstate_rs::topology::StageId;
use serde_json::json;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::sync::Arc;

/// Source that generates events
struct EventGenerator {
    count: Arc<AtomicU64>,
    max_events: u64,
    stage_id: StageId,
    completion_sent: Arc<AtomicBool>,
}

impl EventHandler for EventGenerator {
    fn transform(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.count.load(Ordering::Relaxed);
        
        if current < self.max_events {
            // Generate normal events
            self.count.fetch_add(1, Ordering::Relaxed);
            vec![ChainEvent::new("generated", json!({
                "id": current,
                "timestamp": chrono::Utc::now().to_string()
            }))]
        } else if !self.completion_sent.load(Ordering::Relaxed) {
            // Emit completion event exactly once
            self.completion_sent.store(true, Ordering::Relaxed);
            println!("Generator: Emitting source completion event");
            vec![ChainEvent::source_complete(self.stage_id, true)]
        } else {
            // Already sent completion, return empty
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
    let count = Arc::new(AtomicU64::new(0));
    let max_events = 5;
    
    // Note: We need a way to get the stage_id. For now, we'll use a placeholder.
    // In a real implementation, the stage would be injected with its ID during initialization.
    let generator = EventGenerator {
        count: count.clone(),
        max_events,
        stage_id: StageId::from_u32(0), // Source is typically stage 0
        completion_sent: Arc::new(AtomicBool::new(false)),
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
    
    // Let it run - but the flow might complete naturally first!
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Shutdown (this will detect if stages already completed naturally)
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
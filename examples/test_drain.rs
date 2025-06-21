//! Simple test to check if flow draining works properly

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Simple source that generates a fixed number of events
struct SimpleSource {
    count: Arc<AtomicU64>,
}

impl Step for SimpleSource {
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.count.fetch_add(1, Ordering::Relaxed);
        if current < 3 {
            println!("Source generating event {}", current);
            vec![ChainEvent::new("test_event", json!({ "id": current }))]
        } else {
            vec![]
        }
    }
}

/// Simple sink
struct SimpleSink {
    received: Arc<AtomicU64>,
}

impl Step for SimpleSink {
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let count = self.received.fetch_add(1, Ordering::Relaxed);
        println!("Sink received event #{}: {:?}", count, event.payload);
        vec![]
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter("flowstate_rs=debug")
        .init();
    
    println!("🚀 Testing basic flow shutdown...");
    
    let source = SimpleSource {
        count: Arc::new(AtomicU64::new(0)),
    };
    
    let sink = SimpleSink {
        received: Arc::new(AtomicU64::new(0)),
    };
    
    // Test with flow middleware
    let handle = flow! {
        name: "test_drain",
        middleware: [GoldenSignals::monitoring()],  // Add flow-level monitoring
        ("source" => source, [])
        |> ("sink" => sink, [])
    }?;
    
    println!("Flow started, waiting 1 second...");
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    
    println!("Shutting down flow...");
    handle.shutdown().await?;
    
    println!("✅ Flow shut down successfully!");
    
    Ok(())
}
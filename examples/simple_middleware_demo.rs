//! Simple middleware demo showing basic usage

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use serde_json::json;
use std::sync::atomic::{AtomicU32, Ordering};

// Simple source that generates a few events
struct SimpleSource {
    count: AtomicU32,
}

impl SimpleSource {
    fn new() -> Self {
        Self {
            count: AtomicU32::new(0),
        }
    }
}

impl Step for SimpleSource {
    fn step_type(&self) -> StepType { StepType::Source }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let count = self.count.fetch_add(1, Ordering::SeqCst);
        if count < 3 {
            println!("Source emitting event {}", count);
            vec![ChainEvent::new("data", json!({"count": count}))]
        } else {
            vec![]
        }
    }
}

// Simple sink
struct SimpleSink;

impl SimpleSink {
    fn new() -> Self { Self }
}

impl Step for SimpleSink {
    fn step_type(&self) -> StepType { StepType::Sink }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(count) = event.payload.get("count") {
            println!("Sink received: {}", count);
        }
        vec![]
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("=== Simple Middleware Demo ===\n");

    let handle = flow! {
        name: "simple_middleware_demo",
        flow_taxonomy: RED,
        
        // Just monitoring middleware for now
        ("source" => SimpleSource::new(), [RED::monitoring()])
        |> ("sink" => SimpleSink::new(), [USE::monitoring()])
    }?;
    
    println!("Flow created with monitoring middleware");
    
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    
    handle.shutdown().await?;
    
    println!("\nDone!");
    
    Ok(())
}
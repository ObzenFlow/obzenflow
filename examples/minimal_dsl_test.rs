use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::step::{Step, StepType, ChainEvent};
use serde_json::json;
use std::sync::atomic::{AtomicU32, Ordering};

// Minimal source
struct MinimalSource {
    count: AtomicU32,
}

impl MinimalSource {
    fn new() -> Self {
        Self {
            count: AtomicU32::new(0),
        }
    }
}

impl Step for MinimalSource {
    fn step_type(&self) -> StepType { StepType::Source }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let count = self.count.fetch_add(1, Ordering::SeqCst);
        if count < 3 {
            println!("Source emitting event {}", count);
            vec![ChainEvent::new("test", json!({"count": count}))]
        } else {
            vec![]
        }
    }
}

// Minimal sink
struct MinimalSink;

impl MinimalSink {
    fn new() -> Self {
        Self
    }
}

impl Step for MinimalSink {
    fn step_type(&self) -> StepType { StepType::Sink }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(count) = event.payload.get("count") {
            println!("Sink received event with count: {}", count);
        }
        vec![]
    }
}

#[tokio::main]
async fn main() -> flowstate_rs::step::Result<()> {
    // Enable debug logging
    tracing_subscriber::fmt()
        .with_env_filter("flowstate_rs=debug")
        .init();

    println!("=== Testing DSL macro ===");
    
    let store = EventStore::default().await;
    
    let handle = flow! {
        store: store,
        flow_taxonomy: RED,
        ("source" => MinimalSource::new(), [RED::monitoring()])
        |> ("sink" => MinimalSink::new(), [RED::monitoring()])
    }?;
    
    println!("Flow created, waiting...");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    
    println!("Shutting down...");
    handle.shutdown().await?;
    
    println!("Done!");
    
    // Cleanup
    std::fs::remove_dir_all("./event_store").ok();
    
    Ok(())
}
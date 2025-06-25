use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::lifecycle::{EventHandler, ProcessingMode};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

struct DebugSource {
    fired: Arc<AtomicBool>,
}

impl DebugSource {
    fn new() -> Self {
        println!("DebugSource::new() called");
        Self {
            fired: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl EventHandler for DebugSource {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        println!("DebugSource::transform called with event type: {}", event.event_type);
        
        if event.event_type == "_tick" && !self.fired.swap(true, Ordering::SeqCst) {
            println!("DebugSource emitting test event");
            vec![ChainEvent::new("test", json!({"msg": "hello"}))]
        } else {
            println!("DebugSource no events");
            vec![]
        }
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

struct DebugSink;

impl EventHandler for DebugSink {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        println!("DebugSink received event type: {}", event.event_type);
        vec![]
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

#[tokio::test]
async fn test_debug_race_condition() -> Result<()> {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("flowstate_rs=debug")
        .try_init();
    
    println!("\n=== Starting debug test ===");
    
    let source = DebugSource::new();
    let fired = source.fired.clone();
    
    println!("Creating flow...");
    let handle = flow! {
        name: "debug_test",
        ("source" => source, [])
        |> ("sink" => DebugSink, [])
    }?;
    println!("Flow created");
    
    // Wait a bit
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    println!("Source fired: {}", fired.load(Ordering::SeqCst));
    assert!(fired.load(Ordering::SeqCst), "Source should have fired");
    
    // Shutdown
    handle.shutdown().await?;
    
    Ok(())
}
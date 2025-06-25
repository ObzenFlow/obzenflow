use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::lifecycle::{EventHandler, ProcessingMode};
use serde_json::json;

struct DebugSource;

impl EventHandler for DebugSource {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        println!("DebugSource::transform called with event type: {}", event.event_type);
        if event.event_type == "_tick" {
            vec![ChainEvent::new("test", json!({"msg": "hello"}))]
        } else {
            vec![]
        }
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

#[tokio::test]
async fn test_debug_flow() -> Result<()> {
    // Enable TRACE logging to see everything
    let _ = tracing_subscriber::fmt()
        .with_env_filter("flowstate_rs=trace")
        .try_init();
    
    println!("\n=== Starting debug flow test ===");
    
    let handle = flow! {
        name: "debug_flow",
        ("source" => DebugSource, [])
    }?;
    
    println!("Flow created, sleeping...");
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    
    println!("Shutting down...");
    handle.shutdown().await?;
    
    Ok(())
}
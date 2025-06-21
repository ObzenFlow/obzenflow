use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::monitoring::RED;
use flowstate_rs::step::{Step, StepType, ChainEvent};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

// Source that emits a fixed number of events
struct TestSource {
    total_events: u64,
    emitted: Arc<AtomicU64>,
}

impl Step for TestSource {
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
            vec![ChainEvent::new("test_event", json!({ "index": current }))]
        } else {
            vec![]
        }
    }
}

// Sink that counts events
struct TestSink {
    expected_count: u64,
    received: Arc<AtomicU64>,
}

impl Step for TestSink {
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        self.received.fetch_add(1, Ordering::Relaxed);
        vec![]
    }
}

#[tokio::main]
async fn main() -> flowstate_rs::step::Result<()> {
    let event_count = 50u64;
    
    println!("=== Testing minimal perf pipeline ===");
    
    let store = EventStore::default().await;
    
    let emitted_count = Arc::new(AtomicU64::new(0));
    let source = TestSource {
        total_events: event_count,
        emitted: emitted_count.clone(),
    };
    
    let sink = TestSink {
        expected_count: event_count,
        received: Arc::new(AtomicU64::new(0)),
    };
    let sink_clone = sink.received.clone();
    
    let handle = flow! {
        store: store,
        flow_taxonomy: RED,
        ("source" => source, [RED::monitoring()])
        |> ("sink" => sink, [RED::monitoring()])
    }?;
    
    // Give pipeline time to initialize before monitoring
    println!("Pipeline created, waiting for initialization...");
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    // Now monitor progress
    let start = Instant::now();
    let timeout = std::time::Duration::from_secs(5);
    
    while sink_clone.load(Ordering::Relaxed) < event_count {
        let current_count = sink_clone.load(Ordering::Relaxed);
        
        if start.elapsed() > timeout {
            println!("Timeout! Received {} of {} events", current_count, event_count);
            break;
        }
        
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    
    let received = sink_clone.load(Ordering::Relaxed);
    println!("Final: Received {} of {} events in {:?}", received, event_count, start.elapsed());
    
    handle.shutdown().await?;
    println!("Shutdown complete");
    
    // Clean up
    std::fs::remove_dir_all("./event_store").ok();
    
    Ok(())
}
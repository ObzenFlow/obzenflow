//! Throughput Testing Example
//!
//! This example tests FlowState's throughput capabilities across different
//! pipeline lengths, showing how event processing scales with pipeline complexity.
//!
//! Run with:
//! cargo run --example perf_throughput

use flowstate_rs::prelude::*;
use flowstate_rs::monitoring::{RED, USE, Taxonomy};
use flowstate_rs::flow;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Source that emits a fixed number of events
struct TestSource {
    total_events: u64,
    emitted: Arc<AtomicU64>,
}

impl Clone for TestSource {
    fn clone(&self) -> Self {
        Self {
            total_events: self.total_events,
            emitted: self.emitted.clone(),
        }
    }
}

impl Step for TestSource {
    fn step_type(&self) -> StepType { StepType::Source }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.load(Ordering::Relaxed);
        if current < self.total_events {
            self.emitted.fetch_add(1, Ordering::Relaxed);
            if current % 10 == 0 || current == self.total_events - 1 {
                println!("Source: emitting event {} of {}", current, self.total_events);
            }
            vec![ChainEvent::new("test", json!({ "index": current }))]
        } else {
            if current == self.total_events {
                println!("Source: DONE - emitted all {} events", self.total_events);
            }
            vec![]
        }
    }
}

/// Passthrough stage
struct PassthroughStage {
    name: String,
    processed: Arc<AtomicU64>,
}

impl Clone for PassthroughStage {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            processed: self.processed.clone(),
        }
    }
}

impl PassthroughStage {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            processed: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Step for PassthroughStage {
    fn step_type(&self) -> StepType { StepType::Stage }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let count = self.processed.fetch_add(1, Ordering::Relaxed) + 1;
        if count % 10 == 0 || count >= 105 {
            println!("{}: processed event {} (index={})", self.name, count, 
                event.payload.get("index").and_then(|v| v.as_u64()).unwrap_or(999));
        }
        vec![event] // Just forward
    }
}

/// Sink that counts events
struct TestSink {
    expected_count: u64,
    received: Arc<AtomicU64>,
}

impl Clone for TestSink {
    fn clone(&self) -> Self {
        Self {
            expected_count: self.expected_count,
            received: self.received.clone(),
        }
    }
}

impl Step for TestSink {
    fn step_type(&self) -> StepType { StepType::Sink }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(index) = event.payload.get("index").and_then(|v| v.as_u64()) {
            let count = self.received.fetch_add(1, Ordering::Relaxed) + 1;
            if count % 10 == 0 || count == self.expected_count || count >= 105 {
                println!("Sink: received event {} of {} (index={})", count, self.expected_count, index);
            }
        }
        vec![]
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Enable error logging to see dropped events
    tracing_subscriber::fmt()
        .with_env_filter("flowstate_rs::event_store=error")
        .init();
    
    let event_count = 110u64;
    
    // Test different pipeline lengths
    for stages in [1, 3, 5] {
        println!("\n=== Testing {}-stage pipeline ===", stages);
        
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
        let sink_clone = sink.clone();
        
        let handle = match stages {
            1 => {
                flow! {
                    store: store,
                    flow_taxonomy: GoldenSignals,
                    ("source" => source, [RED::monitoring()])
                    |> ("sink" => sink, [RED::monitoring()])
                }?
            }
            3 => {
                flow! {
                    store: store,
                    flow_taxonomy: GoldenSignals,
                    ("source" => source, [RED::monitoring()])
                    |> ("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()])
                    |> ("stage2" => PassthroughStage::new("stage2"), [USE::monitoring()])
                    |> ("sink" => sink, [RED::monitoring()])
                }?
            }
            5 => {
                flow! {
                    store: store,
                    flow_taxonomy: GoldenSignals,
                    ("source" => source, [RED::monitoring()])
                    |> ("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()])
                    |> ("stage2" => PassthroughStage::new("stage2"), [USE::monitoring()])
                    |> ("stage3" => PassthroughStage::new("stage3"), [USE::monitoring()])
                    |> ("stage4" => PassthroughStage::new("stage4"), [USE::monitoring()])
                    |> ("sink" => sink, [RED::monitoring()])
                }?
            }
            _ => unreachable!(),
        };
        
        // Wait for events with timeout
        let start = Instant::now();
        let timeout = std::time::Duration::from_secs(10);
        
        let mut last_count = 0;
        let mut stall_time = None;
        while sink_clone.received.load(Ordering::Relaxed) < event_count {
            let current_count = sink_clone.received.load(Ordering::Relaxed);
            
            // Detect stalls
            if current_count == last_count {
                if stall_time.is_none() {
                    stall_time = Some(Instant::now());
                    println!("Stalled at {} events...", current_count);
                }
            } else {
                if stall_time.is_some() {
                    println!("Resumed after stall, now at {} events", current_count);
                }
                stall_time = None;
                last_count = current_count;
            }
            
            if start.elapsed() > timeout {
                println!("WARNING: Timeout! Received {} of {} events", 
                    current_count, event_count);
                if let Some(stall_start) = stall_time {
                    println!("  Stalled for {:?} at {} events", stall_start.elapsed(), current_count);
                }
                
                // Check what the source has emitted
                let source_count = emitted_count.load(Ordering::Relaxed);
                println!("  Source has emitted {} events total", source_count);
                
                // Check if source actually emitted all required events
                if source_count < event_count {
                    println!("  ERROR: Source only emitted {} events, expected {}", source_count, event_count);
                }
                
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        
        let received = sink_clone.received.load(Ordering::Relaxed);
        println!("Final: Received {} of {} events in {:?}", 
            received, event_count, start.elapsed());
        
        // Debug: print the source emitted count
        println!("Source emitted: {} events", emitted_count.load(Ordering::Relaxed));
        
        // Force a small delay to let any pending events propagate
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        
        // Check again after delay
        let final_received = sink_clone.received.load(Ordering::Relaxed);
        if final_received != received {
            println!("After 500ms delay: Received {} events (gained {})", 
                final_received, final_received - received);
        }
        
        println!("Initiating shutdown...");
        handle.shutdown().await?;
        println!("Shutdown complete");
        
        // Clean up
        std::fs::remove_dir_all("./event_store").ok();
    }
    
    Ok(())
}
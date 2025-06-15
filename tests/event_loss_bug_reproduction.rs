//! Demonstrates the event loss bug in push subscriptions
//! 
//! The bug: EventWriter notifies subscribers BEFORE the event is guaranteed
//! to be readable, causing event loss when the read fails.

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::monitoring::taxonomies::red::REDMetrics;
use flowstate_rs::monitoring::taxonomies::use_taxonomy::USEMetrics;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Source that emits events rapidly to trigger race conditions
#[derive(Clone)]
struct RapidSource {
    events_to_emit: u64,
    emitted: Arc<AtomicU64>,
    metrics: Arc<REDMetrics>,
}

impl Step for RapidSource {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Source }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::SeqCst);
        if current >= self.events_to_emit {
            return vec![];
        }
        
        // Emit events as fast as possible to stress the system
        vec![ChainEvent::new("test_event", serde_json::json!({
            "id": current,
            "data": "x".repeat(1000), // Make events larger to slow writes
        }))]
    }
}

/// Stage that counts events
#[derive(Clone)]
struct CountingStage {
    name: String,
    received: Arc<AtomicU64>,
    metrics: Arc<USEMetrics>,
}

impl Step for CountingStage {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy { &USE }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Stage }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        self.received.fetch_add(1, Ordering::SeqCst);
        vec![event]
    }
}

/// Sink that verifies all events arrived
#[derive(Clone)]
struct VerifyingSink {
    expected: u64,
    received: Arc<AtomicU64>,
    seen_ids: Arc<tokio::sync::Mutex<Vec<u64>>>,
    metrics: Arc<REDMetrics>,
}

impl Step for VerifyingSink {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Sink }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        self.received.fetch_add(1, Ordering::SeqCst);
        
        if let Some(id) = event.payload.get("id").and_then(|v| v.as_u64()) {
            let seen = self.seen_ids.clone();
            tokio::spawn(async move {
                seen.lock().await.push(id);
            });
        }
        
        vec![]
    }
}

#[tokio::test]
#[ignore] // This test demonstrates a bug - expected to fail until FLOWIP-008
async fn test_event_loss_bug_reproduction() -> Result<()> {
    
    let events_to_send = 1000u64;
    
    // Use small segment size to increase file operations
    let store = EventStore::new(EventStoreConfig {
        path: "./test_event_loss_bug".into(),
        max_segment_size: 1024 * 10, // 10KB segments = more file ops = more races
    }).await?;
    
    let source = RapidSource {
        events_to_emit: events_to_send,
        emitted: Arc::new(AtomicU64::new(0)),
        metrics: Arc::new(RED::create_metrics("source")),
    };
    
    let stage1 = CountingStage {
        name: "stage_1".to_string(),
        received: Arc::new(AtomicU64::new(0)),
        metrics: Arc::new(USE::create_metrics("stage_1")),
    };
    
    let stage2 = CountingStage {
        name: "stage_2".to_string(),
        received: Arc::new(AtomicU64::new(0)),
        metrics: Arc::new(USE::create_metrics("stage_2")),
    };
    
    let sink = VerifyingSink {
        expected: events_to_send,
        received: Arc::new(AtomicU64::new(0)),
        seen_ids: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        metrics: Arc::new(RED::create_metrics("sink")),
    };
    
    
    let handle = flow! {
        store: store,
        flow_taxonomy: GoldenSignals,
        ("source" => source.clone(), RED)
        |> ("stage_1" => stage1.clone(), USE)
        |> ("stage_2" => stage2.clone(), USE)
        |> ("sink" => sink.clone(), RED)
    }?;
    
    // Let pipeline initialize
    tokio::time::sleep(Duration::from_millis(200)).await;
    
    let start = std::time::Instant::now();
    
    // Wait for completion with timeout
    while sink.received.load(Ordering::SeqCst) < events_to_send {
        if start.elapsed() > Duration::from_secs(30) {
            println!("\n⚠️  Timeout after 30 seconds!");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        
    }
    
    let elapsed = start.elapsed();
    
    let source_count = source.emitted.load(Ordering::SeqCst);
    let sink_count = sink.received.load(Ordering::SeqCst);
    
    handle.shutdown().await?;
    let _ = std::fs::remove_dir_all("./test_event_loss_bug");
    
    // This assertion is expected to fail, demonstrating the bug
    assert_eq!(
        sink_count,
        source_count,
        "Event loss bug reproduced! Lost {} events out of {}",
        source_count - sink_count,
        source_count
    );
    
    Ok(())
}
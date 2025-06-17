//! Black box test for event enrichment using the DSL
//! 
//! This test verifies that events are properly enriched as they flow through
//! the pipeline, without directly inspecting EventStore internals.

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::event_store::{EventStore, EventStoreConfig};
use serde_json::json;
use tempfile::tempdir;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[tokio::test]
async fn test_event_flow_through_dsl() {
    // Create event store
    let temp_dir = tempdir().unwrap();
    let store_path = temp_dir.path().join("test_enrichment_store");
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path,
        max_segment_size: 1024 * 1024,
    }).await.unwrap();
    
    // Counter to verify events flow through
    let counter = Arc::new(AtomicU64::new(0));
    let counter_clone = counter.clone();
    
    // Create a simple flow
    let handle = flow! {
        store: event_store,
        flow_taxonomy: RED,
        ("source" => SourceStage::new(5), RED)
        |> ("transform" => TransformStage, USE)  
        |> ("sink" => CounterSink { counter: counter_clone }, RED)
    }.unwrap();
    
    // Let it run briefly to generate events
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // Shutdown gracefully
    handle.shutdown().await.unwrap();
    
    // Verify events flowed through
    let final_count = counter.load(Ordering::Relaxed);
    assert_eq!(final_count, 5, "All 5 events should reach the sink");
    
    println!("✅ Event flow verified: {} events processed", final_count);
}

// Simple source that emits a fixed number of events
struct SourceStage {
    total: u64,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl SourceStage {
    fn new(total: u64) -> Self {
        Self {
            total,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("source"),
        }
    }
}

impl Step for SourceStage {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Source }
    
    fn handle(&self, _: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::SeqCst);
        if current < self.total {
            vec![ChainEvent::new("Data", json!({"sequence": current}))]
        } else {
            vec![]
        }
    }
}

// Transform that modifies the data
struct TransformStage;

impl Step for TransformStage {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy { &USE }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        static METRICS: std::sync::OnceLock<<USE as Taxonomy>::Metrics> = std::sync::OnceLock::new();
        METRICS.get_or_init(|| USE::create_metrics("transform"))
    }
    
    fn step_type(&self) -> StepType { StepType::Stage }
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "Data" {
            event.payload["transformed"] = json!(true);
            vec![event]
        } else {
            vec![]
        }
    }
}

// Sink that counts received events
struct CounterSink {
    counter: Arc<AtomicU64>,
}

impl Step for CounterSink {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        static METRICS: std::sync::OnceLock<<RED as Taxonomy>::Metrics> = std::sync::OnceLock::new();
        METRICS.get_or_init(|| RED::create_metrics("sink"))
    }
    
    fn step_type(&self) -> StepType { StepType::Sink }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "Data" && event.payload["transformed"].as_bool() == Some(true) {
            self.counter.fetch_add(1, Ordering::Relaxed);
        }
        vec![] // Sinks don't emit
    }
}
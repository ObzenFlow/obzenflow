// tests/basic_streaming.rs
use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::path::PathBuf;
use tempfile::tempdir;

/// Simple test sink that counts events
struct EventCounterSink {
    count: Arc<AtomicU64>,
    metrics: <RED as Taxonomy>::Metrics,
}

impl EventCounterSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
        let metrics = RED::create_metrics("EventCounterSink");
        (Self { count: count.clone(), metrics }, count)
    }
}

impl Step for EventCounterSink {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        self.count.fetch_add(1, Ordering::Relaxed);
        // Record success in RED metrics
        self.metrics.record_success(std::time::Duration::from_micros(100));
        vec![] // Sinks typically don't emit events
    }
}

/// Source that generates a fixed number of events
struct TestEventSource {
    count: u64,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl TestEventSource {
    fn new(count: u64) -> Self {
        Self { 
            count,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("TestEventSource"),
        }
    }
}

impl Step for TestEventSource {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.count {
            self.metrics.record_success(std::time::Duration::from_micros(50));
            vec![ChainEvent::new(
                "TestEvent",
                json!({ "index": current }),
            )]
        } else {
            vec![]
        }
    }
}

#[tokio::test]
async fn test_basic_flow() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_event_store");
    
    let (counter_sink, counter) = EventCounterSink::new();
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // Create a simple flow
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => TestEventSource::new(10), RED)
        |> ("sink" => counter_sink, USE)
    }?;
    
    // Let the flow run for a bit
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Gracefully shut down the flow
    handle.shutdown().await?;
    
    // Check that events were processed
    let final_count = counter.load(Ordering::Relaxed);
    assert!(final_count > 0, "Expected some events to be processed, but got {}", final_count);
    assert!(final_count <= 10, "Expected at most 10 events, but got {}", final_count);
    
    // Cleanup handled by tempdir
    Ok(())
}

/// Stage that doubles each event
struct Doubler {
    metrics: <USE as Taxonomy>::Metrics,
}

impl Doubler {
    fn new() -> Self {
        Self {
            metrics: USE::create_metrics("Doubler"),
        }
    }
}

impl Step for Doubler {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &USE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // For USE taxonomy, we might track utilization
        // For now, just return doubled events
        vec![event.clone(), event]
    }
}

#[tokio::test]
async fn test_multi_stage_flow() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_event_store_multi");
    
    let (counter_sink, counter) = EventCounterSink::new();
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => TestEventSource::new(5), RED)
        |> ("doubler" => Doubler::new(), USE)
        |> ("sink" => counter_sink, RED)
    }?;
    
    // Let the flow run for a bit
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Gracefully shut down the flow
    handle.shutdown().await?;
    
    // Would expect 10 events (5 * 2) after processing
    let final_count = counter.load(Ordering::Relaxed);
    assert!(final_count > 0, "Expected some events to be processed, but got {}", final_count);
    assert!(final_count <= 10, "Expected at most 10 events (5 * 2), but got {}", final_count);
    
    // Cleanup handled by tempdir
    Ok(())
}

/// A source that generates numbered events
struct NumberSource {
    count: u64,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl NumberSource {
    fn new(count: u64) -> Self {
        Self { 
            count,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("NumberSource"),
        }
    }
}

impl Step for NumberSource {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Source
    }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.count {
            vec![ChainEvent::new(
                "Number",
                json!({ "value": current + 1 }),
            )]
        } else {
            vec![]
        }
    }
}

/// A transform that doubles numbers
struct NumberDoubler {
    metrics: <USE as Taxonomy>::Metrics,
}

impl NumberDoubler {
    fn new() -> Self {
        Self {
            metrics: USE::create_metrics("NumberDoubler"),
        }
    }
}

impl Step for NumberDoubler {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &USE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(value) = event.payload.get("value").and_then(|v| v.as_u64()) {
            vec![ChainEvent::new(
                "DoubledNumber",
                json!({ "value": value * 2 }),
            )]
        } else {
            vec![]
        }
    }
}

/// A sink that sums all numbers it receives
struct SumSink {
    sum: Arc<AtomicU64>,
    metrics: <RED as Taxonomy>::Metrics,
}

impl SumSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let sum = Arc::new(AtomicU64::new(0));
        let metrics = RED::create_metrics("SumSink");
        (Self { sum: sum.clone(), metrics }, sum)
    }
}

impl Step for SumSink {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(value) = event.payload.get("value").and_then(|v| v.as_u64()) {
            self.sum.fetch_add(value, Ordering::Relaxed);
        }
        vec![]
    }
}

#[tokio::test]
async fn test_pipeline_topology() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_pipeline_topology");
    
    let (sum_sink, sum) = SumSink::new();
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // Pipeline: Source(1,2,3) -> Doubler(2,4,6) -> Sum
    // If topology filtering works, Sum should be 12 (2+4+6)
    // If it doesn't work (broadcast), Sum would be 21 (1+2+3+2+4+6)
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => NumberSource::new(3), RED)
        |> ("doubler" => NumberDoubler::new(), USE)
        |> ("sink" => sum_sink, RED)
    }?;
    
    // Let the flow run for a bit
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    
    // Gracefully shut down the flow
    handle.shutdown().await?;
    
    // Check the sum
    let final_sum = sum.load(Ordering::Relaxed);
    println!("Final sum: {}", final_sum);
    
    // With proper topology: 2 + 4 + 6 = 12
    // Without topology (broadcast): 1 + 2 + 3 + 2 + 4 + 6 = 18
    assert_eq!(final_sum, 12, "Expected sum of doubled values (2+4+6=12), but got {}", final_sum);
    
    // Cleanup handled by tempdir
    Ok(())
}
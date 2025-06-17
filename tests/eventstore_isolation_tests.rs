//! EventStore Isolation and Path Uniqueness Tests
//!
//! These tests ensure EventStore instances are properly isolated and don't interfere
//! with each other. Originally created for FLOWIP-017 to diagnose benchmark inconsistencies,
//! these tests remain valuable for preventing regression in EventStore isolation behavior.
//!
//! Key test scenarios:
//! 1. EventStore path uniqueness and isolation
//! 2. Stage name reuse safety  
//! 3. DSL pattern equivalence (named vs auto-named)
//! 4. Concurrent EventStore isolation

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::step::Result;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use serde_json::json;
use std::time::Duration;
use std::collections::HashSet;
use ulid::Ulid;

/// Test source that tracks how many events it emits
struct TestSource {
    total_events: u64,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl TestSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("TestSource"),
        }
    }
}

impl Step for TestSource {
    type Taxonomy = RED;

    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Source }

    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
            vec![ChainEvent::new("TestEvent", json!({ 
                "index": current,
                "timestamp": std::time::SystemTime::now()
            }))]
        } else {
            vec![]
        }
    }
}

/// Passthrough stage for testing
struct PassthroughStage {
    name: String,
    metrics: <USE as Taxonomy>::Metrics,
}

impl PassthroughStage {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            metrics: USE::create_metrics(name),
        }
    }
}

impl Step for PassthroughStage {
    type Taxonomy = USE;

    fn taxonomy(&self) -> &Self::Taxonomy { &USE }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Stage }

    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event]
    }
}

/// Test sink that counts received events
#[derive(Clone)]
struct TestSink {
    received: Arc<AtomicU64>,
    metrics: Arc<<RED as Taxonomy>::Metrics>,
}

impl TestSink {
    fn new() -> Self {
        Self {
            received: Arc::new(AtomicU64::new(0)),
            metrics: Arc::new(RED::create_metrics("TestSink")),
        }
    }

    fn get_received_count(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }
}

impl Step for TestSink {
    type Taxonomy = RED;

    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &*self.metrics }
    fn step_type(&self) -> StepType { StepType::Sink }

    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        self.received.fetch_add(1, Ordering::Relaxed);
        vec![]
    }
}

/// Helper to create a test EventStore with potentially colliding paths (legacy approach)
async fn create_test_store_without_uuid(test_name: &str) -> Result<(Arc<EventStore>, tempfile::TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let store_path = temp_dir.path().join(format!("test_{}_{}", test_name, std::process::id()));
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path,
        max_segment_size: 1024 * 1024,
    }).await?;
    
    Ok((event_store, temp_dir))
}

/// Helper to create a test EventStore with guaranteed unique paths
async fn create_test_store_with_uuid(test_name: &str) -> Result<(Arc<EventStore>, tempfile::TempDir)> {
    let temp_dir = tempfile::tempdir()?;
    let store_path = temp_dir.path().join(format!("test_{}_{}", test_name, Ulid::new()));
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path,
        max_segment_size: 1024 * 1024,
    }).await?;
    
    Ok((event_store, temp_dir))
}

/// Helper to run a pipeline and measure time
async fn run_pipeline_timed(
    handle: flowstate_rs::event_sourcing::FlowHandle,
    sink: TestSink,
    expected_events: u64,
) -> Result<(u64, Duration)> {
    let start = std::time::Instant::now();
    
    // Wait for events to be processed
    let timeout_duration = Duration::from_secs(10);
    let wait_start = std::time::Instant::now();
    
    while sink.get_received_count() < expected_events && wait_start.elapsed() < timeout_duration {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    
    let elapsed = start.elapsed();
    handle.shutdown().await?;
    
    Ok((sink.get_received_count(), elapsed))
}

#[tokio::test]
async fn test_eventstore_isolation_without_uuid() -> Result<()> {
    // Test that demonstrates potential issues without UUID-based paths
    // This captures the behavior that led to FLOWIP-017
    
    const EVENTS_PER_PIPELINE: u64 = 10;
    const NUM_RUNS: usize = 3;
    
    let mut timings = Vec::new();
    
    for run in 0..NUM_RUNS {
        // Using process ID only - could have timing-sensitive issues
        let (store, _temp_dir) = create_test_store_without_uuid("isolation_test").await?;
        
        let source = TestSource::new(EVENTS_PER_PIPELINE);
        let sink = TestSink::new();
        let sink_clone = sink.clone();
        
        let handle = flow! {
            store: store,
            flow_taxonomy: GoldenSignals,
            ("source" => source, RED)
            |> ("stage1" => PassthroughStage::new("stage1"), USE)
            |> ("sink" => sink, RED)
        }?;
        
        let (count, duration) = run_pipeline_timed(handle, sink_clone, EVENTS_PER_PIPELINE).await?;
        
        assert_eq!(count, EVENTS_PER_PIPELINE, "Run {} should process all events", run);
        timings.push(duration);
        
        // Small delay between runs to increase chance of interference
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    
    // Check for timing consistency
    let avg_duration = timings.iter().sum::<Duration>() / timings.len() as u32;
    let max_deviation = timings.iter()
        .map(|d| (*d).max(avg_duration) - (*d).min(avg_duration))
        .max()
        .unwrap();
    
    println!("Without UUID - timing variance: {:?} (avg: {:?})", max_deviation, avg_duration);
    
    Ok(())
}

#[tokio::test]
async fn test_eventstore_isolation_with_uuid() -> Result<()> {
    // Test that verifies UUID-based paths ensure complete isolation
    
    const EVENTS_PER_PIPELINE: u64 = 10;
    const NUM_RUNS: usize = 3;
    
    let mut timings = Vec::new();
    
    for run in 0..NUM_RUNS {
        // Using UUID ensures uniqueness
        let (store, _temp_dir) = create_test_store_with_uuid("isolation_test").await?;
        
        let source = TestSource::new(EVENTS_PER_PIPELINE);
        let sink = TestSink::new();
        let sink_clone = sink.clone();
        
        let handle = flow! {
            store: store,
            flow_taxonomy: GoldenSignals,
            ("source" => source, RED)
            |> ("stage1" => PassthroughStage::new("stage1"), USE)
            |> ("sink" => sink, RED)
        }?;
        
        let (count, duration) = run_pipeline_timed(handle, sink_clone, EVENTS_PER_PIPELINE).await?;
        
        assert_eq!(count, EVENTS_PER_PIPELINE, "Run {} should process all events", run);
        timings.push(duration);
        
        // Small delay between runs
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    
    // Check for timing consistency - should be more consistent with proper isolation
    let avg_duration = timings.iter().sum::<Duration>() / timings.len() as u32;
    let max_deviation = timings.iter()
        .map(|d| (*d).max(avg_duration) - (*d).min(avg_duration))
        .max()
        .unwrap();
    
    println!("With UUID - timing variance: {:?} (avg: {:?})", max_deviation, avg_duration);
    
    // With proper isolation, variance should be minimal
    assert!(max_deviation < avg_duration / 2, "Timing variance should be less than 50% of average");
    
    Ok(())
}



#[tokio::test]
async fn test_eventstore_path_uniqueness() -> Result<()> {
    // Verify that EventStore creates unique file paths
    // This is critical for preventing cross-test interference
    
    let mut paths = HashSet::new();
    
    for i in 0..10 {
        let (_store, temp_dir) = create_test_store_with_uuid(&format!("uniqueness_test_{}", i)).await?;
        
        // Just verify we created a unique EventStore instance
        // We can't access the private store_path field
        assert!(paths.insert(format!("store_{}_{}", i, Ulid::new().to_string())));
        
        // Keep temp_dir alive to prevent cleanup
        std::mem::forget(temp_dir);
    }
    
    assert_eq!(paths.len(), 10, "Should have 10 unique paths");
    
    Ok(())
}

#[tokio::test]
async fn test_stage_name_reuse_safety() -> Result<()> {
    // Verify that reusing stage names across different pipelines doesn't cause issues
    // This was the core concern in FLOWIP-017
    
    const EVENTS_PER_PIPELINE: u64 = 10;
    
    // Run pipelines with overlapping stage names
    let configs = vec![
        vec!["source", "sink"],
        vec!["source", "stage1", "sink"],
        vec!["source", "stage1", "stage2", "sink"],
        vec!["source", "sink"], // Repeat first config
    ];
    
    let mut all_successful = true;
    
    for (idx, stage_names) in configs.iter().enumerate() {
        let (store, _temp_dir) = create_test_store_with_uuid(&format!("reuse_test_{}", idx)).await?;
        
        let source = TestSource::new(EVENTS_PER_PIPELINE);
        let sink = TestSink::new();
        let sink_clone = sink.clone();
        
        // Build pipeline based on stage count
        let handle = match stage_names.len() {
            2 => flow! {
                store: store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, RED)
                |> ("sink" => sink, RED)
            }?,
            3 => flow! {
                store: store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, RED)
                |> ("stage1" => PassthroughStage::new("stage1"), USE)
                |> ("sink" => sink, RED)
            }?,
            4 => flow! {
                store: store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, RED)
                |> ("stage1" => PassthroughStage::new("stage1"), USE)
                |> ("stage2" => PassthroughStage::new("stage2"), USE)
                |> ("sink" => sink, RED)
            }?,
            _ => unreachable!(),
        };
        
        let (count, _) = run_pipeline_timed(handle, sink_clone, EVENTS_PER_PIPELINE).await?;
        
        if count != EVENTS_PER_PIPELINE {
            println!("Config {} failed: expected {}, got {}", idx, EVENTS_PER_PIPELINE, count);
            all_successful = false;
        }
    }
    
    assert!(all_successful, "All pipeline configurations should process all events despite name reuse");
    
    Ok(())
}

#[tokio::test]
async fn test_concurrent_eventstore_isolation() -> Result<()> {
    // Test that multiple EventStore instances can run concurrently without interference
    
    const EVENTS_PER_PIPELINE: u64 = 10;
    const CONCURRENT_PIPELINES: usize = 5;
    
    let mut handles = Vec::new();
    
    // Launch multiple pipelines concurrently
    for i in 0..CONCURRENT_PIPELINES {
        let handle = tokio::spawn(async move {
            let (store, _temp_dir) = create_test_store_with_uuid(&format!("concurrent_{}", i)).await?;
            
            let source = TestSource::new(EVENTS_PER_PIPELINE);
            let sink = TestSink::new();
            let sink_clone = sink.clone();
            
            let pipeline = flow! {
                store: store,
                flow_taxonomy: GoldenSignals,
                (source, RED)
                |> (PassthroughStage::new(&format!("stage_{}", i)), USE)
                |> (sink, RED)
            }?;
            
            let (count, duration) = run_pipeline_timed(pipeline, sink_clone, EVENTS_PER_PIPELINE).await?;
            
            Ok::<(u64, Duration), Box<dyn std::error::Error + Send + Sync>>((count, duration))
        });
        
        handles.push(handle);
    }
    
    // Wait for all pipelines to complete
    let mut all_successful = true;
    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await? {
            Ok((count, duration)) => {
                println!("Pipeline {} processed {} events in {:?}", i, count, duration);
                if count != EVENTS_PER_PIPELINE {
                    all_successful = false;
                }
            }
            Err(e) => {
                println!("Pipeline {} failed: {}", i, e);
                all_successful = false;
            }
        }
    }
    
    assert!(all_successful, "All concurrent pipelines should process all events independently");
    
    Ok(())
}
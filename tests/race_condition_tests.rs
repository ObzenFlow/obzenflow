//! Tests for race conditions in event delivery
//!
//! These tests verify race conditions where events can be lost when sources
//! emit before sinks have fully initialized their subscriptions.

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::monitoring::GoldenSignals;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::tempdir;
use std::time::{Duration, Instant};
use std::path::PathBuf;
use tokio::time::sleep;

/// Test source that emits events immediately 
struct FastSource {
    total_events: u64,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl FastSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("FastSource"),
        }
    }
}

impl Step for FastSource {
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
        if current < self.total_events {
            vec![ChainEvent::new("TestEvent", json!({
                "index": current,
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }))]
        } else {
            vec![]
        }
    }
}

/// Counting sink
#[derive(Clone)]
struct CountingSink {
    expected_count: u64,
    received: Arc<AtomicU64>,
    metrics: Arc<<RED as Taxonomy>::Metrics>,
}

impl CountingSink {
    fn new(expected_count: u64) -> Self {
        Self {
            expected_count,
            received: Arc::new(AtomicU64::new(0)),
            metrics: Arc::new(RED::create_metrics("CountingSink")),
        }
    }
}

impl Step for CountingSink {
    type Taxonomy = RED;

    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }

    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &*self.metrics
    }

    fn step_type(&self) -> StepType {
        StepType::Sink
    }

    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(_index) = event.payload.get("index").and_then(|v| v.as_u64()) {
            self.received.fetch_add(1, Ordering::Relaxed);
        }
        vec![]
    }
}

#[tokio::test]
#[ignore] // This test demonstrates a race condition - expected to fail
async fn test_race_condition_without_delay() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_race_condition_1");

    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;

    let total_events = 10u64;
    let source = FastSource::new(total_events);
    let sink = CountingSink::new(total_events);
    let sink_clone = sink.clone();

    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => source, RED)
        |> ("sink" => sink.clone(), RED)
    }?;

    // Wait for completion
    sleep(Duration::from_secs(1)).await;
    
    let received = sink_clone.received.load(Ordering::Relaxed);
    
    handle.shutdown().await?;
    // Cleanup handled by tempdir
    
    // This assertion is expected to fail, demonstrating the race condition
    assert_eq!(
        received,
        total_events,
        "Race condition detected! Lost {} events out of {}",
        total_events - received,
        total_events
    );
    
    Ok(())
}

#[tokio::test]
async fn test_race_condition_with_delay() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_race_condition_2");

    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;

    let total_events = 10u64;
    let source = FastSource::new(total_events);
    let sink = CountingSink::new(total_events);
    let sink_clone = sink.clone();

    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => source, RED)
        |> ("sink" => sink.clone(), RED)
    }?;

    // CRITICAL: Add delay to allow subscription setup
    sleep(Duration::from_millis(100)).await;

    // Wait for completion
    sleep(Duration::from_secs(1)).await;
    
    let received = sink_clone.received.load(Ordering::Relaxed);
    
    handle.shutdown().await?;
    let _ = std::fs::remove_dir_all(&store_path);
    
    // With delay, all events should be received
    assert_eq!(
        received,
        total_events,
        "Events lost even with delay! Lost {} events out of {}",
        total_events - received,
        total_events
    );
    
    Ok(())
}

#[tokio::test]
#[ignore] // This test demonstrates race conditions at scale
async fn test_race_condition_multi_stage() -> Result<()> {
    // Test with 5 stages and no delay - expect significant event loss
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_race_multistage");

    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;

    let total_events = 100u64;
    let source = FastSource::new(total_events);
    let sink = CountingSink::new(total_events);
    let sink_clone = sink.clone();

    // Simple passthrough stage
    #[derive(Clone)]
    struct PassStage;
    impl Step for PassStage {
        type Taxonomy = USE;
        fn taxonomy(&self) -> &Self::Taxonomy { &USE }
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
            static METRICS: std::sync::OnceLock<<USE as Taxonomy>::Metrics> = std::sync::OnceLock::new();
            METRICS.get_or_init(|| USE::create_metrics("PassStage"))
        }
        fn step_type(&self) -> StepType { StepType::Stage }
        fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> { vec![event] }
    }

    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => source, RED)
        |> ("stage1" => PassStage, USE)
        |> ("stage2" => PassStage, USE)
        |> ("stage3" => PassStage, USE)
        |> ("stage4" => PassStage, USE)
        |> ("sink" => sink.clone(), RED)
    }?;

    // Wait for completion
    sleep(Duration::from_secs(2)).await;
    
    let received = sink_clone.received.load(Ordering::Relaxed);
    
    handle.shutdown().await?;
    let _ = std::fs::remove_dir_all(&store_path);

    // This assertion is expected to fail with more stages
    assert!(
        received < total_events,
        "Expected event loss with 5 stages, but received all {} events",
        total_events
    );

    Ok(())
}
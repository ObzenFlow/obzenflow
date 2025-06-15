// tests/chunker_rag_tests.rs
use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::tempdir;

// Copy the exact Chunker and Rag implementations from flowstate.rs
struct Chunker {
    metrics: <RED as Taxonomy>::Metrics,
}

impl Chunker {
    fn new() -> Self {
        Self {
            metrics: RED::create_metrics("Chunker"),
        }
    }
}

impl Step for Chunker {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    fn handle(&self, ev: ChainEvent) -> Vec<ChainEvent> {
        if ev.event_type == "NewsFetched" {
            let out = ChainEvent::new("ChunkIndexed", json!({ "info": "chunked" }));
            vec![out]
        } else {
            vec![]
        }
    }
}

struct Rag {
    metrics: <USE as Taxonomy>::Metrics,
}

impl Rag {
    fn new() -> Self {
        Self {
            metrics: USE::create_metrics("Rag"),
        }
    }
}

impl Step for Rag {
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
    fn handle(&self, ev: ChainEvent) -> Vec<ChainEvent> {
        if ev.event_type == "ChunkIndexed" {
            let out = ChainEvent::new("RetrievedContext", json!({ "info": "retrieved" }));
            vec![out]
        } else {
            vec![]
        }
    }
}

struct ScriptWriter {
    metrics: <SAAFE as Taxonomy>::Metrics,
}

impl ScriptWriter {
    fn new() -> Self {
        Self {
            metrics: SAAFE::create_metrics("ScriptWriter"),
        }
    }
}

impl Step for ScriptWriter {
    type Taxonomy = SAAFE;
    
    fn taxonomy(&self) -> &Self::Taxonomy {
        &SAAFE
    }
    
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }
    
    fn step_type(&self) -> StepType {
        StepType::Stage
    }
    fn handle(&self, ev: ChainEvent) -> Vec<ChainEvent> {
        if ev.event_type == "RetrievedContext" {
            let out = ChainEvent::new("ScriptGenerated", json!({ "script": "Hello World" }));
            vec![out]
        } else {
            vec![]
        }
    }
}

#[tokio::test]
async fn test_chunker_stage_isolated() {
    let chunker = Chunker::new();
    
    // Test with matching event type
    let news_event = ChainEvent::new("NewsFetched", json!({ "headline": "Test News" }));
    let results = chunker.handle(news_event);
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].event_type, "ChunkIndexed");
    assert_eq!(results[0].payload["info"], "chunked");
    
    // Test with non-matching event type
    let other_event = ChainEvent::new("OtherEvent", json!({}));
    let results = chunker.handle(other_event);
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn test_rag_stage_isolated() {
    let rag = Rag::new();
    
    // Test with matching event type
    let chunk_event = ChainEvent::new("ChunkIndexed", json!({ "info": "chunked" }));
    let results = rag.handle(chunk_event);
    
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].event_type, "RetrievedContext");
    assert_eq!(results[0].payload["info"], "retrieved");
    
    // Test with non-matching event type
    let other_event = ChainEvent::new("OtherEvent", json!({}));
    let results = rag.handle(other_event);
    assert_eq!(results.len(), 0);
}

// Source that generates news events
struct NewsSource {
    count: u64,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl NewsSource {
    fn new(count: u64) -> Self {
        Self {
            count,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("NewsSource"),
        }
    }
}

impl Step for NewsSource {
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
            vec![ChainEvent::new("NewsFetched", json!({ "headline": format!("News {}", current + 1) }))]
        } else {
            vec![]
        }
    }
}

// Sink that counts scripts
struct ScriptCounterSink {
    scripts: Arc<AtomicU64>,
    metrics: <RED as Taxonomy>::Metrics,
}

impl ScriptCounterSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let scripts = Arc::new(AtomicU64::new(0));
        (Self { 
            scripts: scripts.clone(),
            metrics: RED::create_metrics("ScriptCounterSink"),
        }, scripts)
    }
}

impl Step for ScriptCounterSink {
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
        if event.event_type == "ScriptGenerated" {
            self.scripts.fetch_add(1, Ordering::Relaxed);
        }
        vec![] // Sinks don't emit
    }
}

#[tokio::test]
async fn test_full_pipeline_with_manual_events() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_chunker_rag_store");
    
    let (script_sink, script_count) = ScriptCounterSink::new();
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // Run the pipeline
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => NewsSource::new(3), RED)
        |> ("chunker" => Chunker::new(), USE)
        |> ("rag" => Rag::new(), SAAFE)
        |> ("writer" => ScriptWriter::new(), GoldenSignals)
        |> ("sink" => script_sink, RED)
    }?;
    
    // Let the flow run
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Gracefully shut down
    handle.shutdown().await?;
    
    // Verify 3 scripts were generated
    let final_count = script_count.load(Ordering::Relaxed);
    assert_eq!(final_count, 3, "Expected 3 scripts to be generated");
    
    println!("Successfully generated {} scripts", final_count);
    
    // Clean up
    // Cleanup handled by tempdir
    Ok(())
}

#[tokio::test]
async fn test_pipeline_with_event_capture() -> Result<()> {
    // Since we're testing pipeline behavior, we can just use the standard stages
    // and verify the flow works correctly. The EventStore already captures all events.
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_capture_store");
    
    let (script_sink, script_count) = ScriptCounterSink::new();
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // Run pipeline with a single news event
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => NewsSource::new(1), RED)
        |> ("chunker" => Chunker::new(), USE)
        |> ("rag" => Rag::new(), SAAFE)
        |> ("writer" => ScriptWriter::new(), GoldenSignals)
        |> ("sink" => script_sink, RED)
    }?;
    
    // Let the flow run
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    handle.shutdown().await?;
    
    // Verify the script was generated
    let final_count = script_count.load(Ordering::Relaxed);
    assert_eq!(final_count, 1, "Expected 1 script to be generated");
    
    println!("Pipeline successfully processed 1 news event through all stages");
    
    // Clean up
    let _ = std::fs::remove_dir_all(&store_path);
    Ok(())
}

// Source that emits mixed event types
struct MixedEventSource {
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl MixedEventSource {
    fn new() -> Self {
        Self {
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("MixedEventSource"),
        }
    }
}

impl Step for MixedEventSource {
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
        match current {
            0 => vec![ChainEvent::new("NewsFetched", json!({ "headline": "Valid News" }))],
            1 => vec![ChainEvent::new("UserAction", json!({ "action": "click" }))],
            2 => vec![ChainEvent::new("SystemEvent", json!({ "type": "startup" }))],
            3 => vec![ChainEvent::new("NewsFetched", json!({ "headline": "Another News" }))],
            _ => vec![],
        }
    }
}

#[tokio::test]
async fn test_chunker_rag_with_different_inputs() -> Result<()> {
    // Test that Chunker and Rag properly filter event types
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_filtering_store");
    
    let (script_sink, script_count) = ScriptCounterSink::new();
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // Run pipeline - should only process NewsFetched events
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => MixedEventSource::new(), RED)
        |> ("chunker" => Chunker::new(), USE)
        |> ("rag" => Rag::new(), SAAFE)
        |> ("writer" => ScriptWriter::new(), GoldenSignals)
        |> ("sink" => script_sink, RED)
    }?;
    
    // Let the flow run
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    handle.shutdown().await?;
    
    // Should only have 2 ScriptGenerated events (from 2 NewsFetched events)
    let final_count = script_count.load(Ordering::Relaxed);
    assert_eq!(final_count, 2, "Expected 2 scripts from 2 NewsFetched events");
    
    println!("Pipeline correctly filtered events - processed only NewsFetched events");
    
    // Clean up
    let _ = std::fs::remove_dir_all(&store_path);
    Ok(())
}
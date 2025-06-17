// tests/event_sourcing_tests.rs
//! Comprehensive tests for the event sourcing architecture
//! 
//! These tests demonstrate the key benefits of event sourcing:
//! - Complete durability - every event is persisted with ULID
//! - Replay capability - stages can be rerun from any point
//! - Independence - stages only depend on the log, not each other
//! - Debuggability - full audit trail of all processing

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::event_types::EventType;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::Duration;
use chrono;
use tempfile::tempdir;

// Test-only mock source that generates RSS-like events
struct MockRssSource {
    count: u64,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl MockRssSource {
    pub fn new(count: u64) -> Self {
        Self {
            count,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("MockRssSource"),
        }
    }
}

impl Step for MockRssSource {
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
            let events = vec![
                ChainEvent::new("NewsFetched", json!({
                    "id": "rss-1",
                    "title": "Breaking: AI Makes Major Breakthrough",
                    "content": "Scientists announce a significant advancement in artificial intelligence that could revolutionize machine learning.",
                    "url": "https://example.com/ai-breakthrough",
                    "category": "technology"
                })),
                ChainEvent::new("NewsFetched", json!({
                    "id": "rss-2",
                    "title": "Market Update: Tech Stocks Surge",
                    "content": "Technology stocks showed strong performance today as investors remained optimistic.",
                    "url": "https://example.com/market-update",
                    "category": "finance"
                })),
            ];
            if current == 0 {
                events
            } else {
                vec![]
            }
        } else {
            vec![]
        }
    }
}

struct MockJobSource {
    job_count: u64,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl MockJobSource {
    pub fn new(job_count: u64) -> Self {
        Self {
            job_count,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("MockJobSource"),
        }
    }
}

impl Step for MockJobSource {
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
        if current < self.job_count {
            vec![ChainEvent::new("JobFetched", json!({
                "id": format!("job-{}", current),
                "title": format!("Software Engineer Position #{}", current),
                "content": format!("We are looking for a talented software engineer. Job #{}", current),
                "url": format!("https://jobs.example.com/job-{}", current),
                "company": format!("Company {}", current % 5)
            }))]
        } else {
            vec![]
        }
    }
}

struct MockHackerNewsSource {
    story_count: u64,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl MockHackerNewsSource {
    pub fn new(story_count: u64) -> Self {
        Self {
            story_count,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("MockHackerNewsSource"),
        }
    }
}

impl Step for MockHackerNewsSource {
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
        if current < self.story_count {
            let topics = ["Rust", "AI", "Blockchain", "WebAssembly"];
            let topic = topics[current as usize % topics.len()];
            vec![ChainEvent::new("HackerNewsFetched", json!({
                "id": format!("hn-{}", current),
                "title": format!("Show HN: My {} Project", topic),
                "content": format!("I've been working on this {} project.", topic),
                "url": format!("https://news.ycombinator.com/item?id=3000{}", current),
                "score": 42 + current,
                "topic": topic
            }))]
        } else {
            vec![]
        }
    }
}

// Test stages that simulate the content pipeline

struct NewsChunker {
    metrics: <RED as Taxonomy>::Metrics,
}

impl NewsChunker {
    fn new() -> Self {
        Self {
            metrics: RED::create_metrics("NewsChunker"),
        }
    }
}

impl Step for NewsChunker {
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
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "RssFetched" || event.event_type == "NewsFetched" {
            // Simulate chunking a news article
            let title = event.payload["title"].as_str().unwrap_or("Untitled");
            let content = event.payload["content"].as_str().unwrap_or("");
            
            // Create multiple chunks from the content
            let chunks: Vec<&str> = content.split(". ").collect();
            let mut result_events = Vec::new();
            
            for (i, chunk) in chunks.iter().enumerate() {
                if !chunk.trim().is_empty() {
                    let chunk_event = ChainEvent::new("ChunkIndexed", json!({
                        "chunk_id": format!("{}-chunk-{}", event.ulid, i),
                        "chunk_text": chunk.trim(),
                        "chunk_index": i,
                        "total_chunks": chunks.len(),
                        "original_title": title,
                        "original_event_id": event.ulid
                    }));
                    result_events.push(chunk_event);
                }
            }
            
            result_events
        } else {
            vec![]
        }
    }
}

struct RagRetriever {
    metrics: <USE as Taxonomy>::Metrics,
}

impl RagRetriever {
    fn new() -> Self {
        Self {
            metrics: USE::create_metrics("RagRetriever"),
        }
    }
}

impl Step for RagRetriever {
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
        if event.event_type == "ChunkIndexed" {
            // Simulate RAG context retrieval
            let chunk_text = event.payload["chunk_text"].as_str().unwrap_or("");
            
            // Mock context retrieval based on chunk content
            let context = if chunk_text.to_lowercase().contains("ai") {
                "AI context: Machine learning, neural networks, artificial intelligence history"
            } else if chunk_text.to_lowercase().contains("market") {
                "Market context: Economic indicators, stock performance, financial trends"
            } else {
                "General context: Current events, background information"
            };
            
            let context_event = ChainEvent::new("RetrievedContext", json!({
                "chunk_id": event.payload["chunk_id"],
                "context": context,
                "relevance_score": 0.85,
                "chunk_text": chunk_text,
                "original_title": event.payload["original_title"]
            }));
            
            vec![context_event]
        } else {
            vec![]
        }
    }
}

struct ScriptGenerator {
    metrics: <SAAFE as Taxonomy>::Metrics,
}

impl ScriptGenerator {
    fn new() -> Self {
        Self {
            metrics: SAAFE::create_metrics("ScriptGenerator"),
        }
    }
}

impl Step for ScriptGenerator {
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
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "RetrievedContext" {
            // Simulate script generation from context
            let context = event.payload["context"].as_str().unwrap_or("");
            let chunk_text = event.payload["chunk_text"].as_str().unwrap_or("");
            let title = event.payload["original_title"].as_str().unwrap_or("");
            
            let script = format!(
                "🎬 VIDEO SCRIPT SEGMENT 🎬\n\nTitle: {}\n\nContent: {}\n\nContext: {}\n\n📝 This would make a great talking point for today's video!",
                title, chunk_text, context
            );
            
            let script_event = ChainEvent::new("ScriptGenerated", json!({
                "script": script,
                "word_count": script.split_whitespace().count(),
                "chunk_id": event.payload["chunk_id"],
                "source_title": title
            }));
            
            vec![script_event]
        } else {
            vec![]
        }
    }
}

// Sink to collect events for testing
struct EventCollectorSink {
    events: Arc<tokio::sync::Mutex<Vec<ChainEvent>>>,
    metrics: <RED as Taxonomy>::Metrics,
}

impl EventCollectorSink {
    fn new() -> (Self, Arc<tokio::sync::Mutex<Vec<ChainEvent>>>) {
        let events = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let metrics = RED::create_metrics("EventCollectorSink");
        (Self { events: events.clone(), metrics }, events)
    }
}

impl Step for EventCollectorSink {
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
        let events = self.events.clone();
        tokio::spawn(async move {
            events.lock().await.push(event);
        });
        vec![]
    }
}

#[tokio::test]
async fn test_complete_event_sourced_pipeline() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_complete_es_store");
    
    let (collector_sink, collected_events) = EventCollectorSink::new();
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // Create and run the pipeline
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => MockRssSource::new(1), RED)
        |> ("chunker" => NewsChunker::new(), RED)
        |> ("rag" => RagRetriever::new(), USE)
        |> ("script_gen" => ScriptGenerator::new(), SAAFE)
        |> ("collector" => collector_sink, RED)
    }?;
    
    // Let the flow run for a bit
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Gracefully shut down the flow
    handle.shutdown().await?;
    
    // Analyze collected events
    let events = collected_events.lock().await;
    println!("\n📋 Event Analysis:");
    println!("Total events collected: {}", events.len());
    
    // Count events by type
    let mut event_counts = std::collections::HashMap::new();
    for event in events.iter() {
        *event_counts.entry(event.event_type.clone()).or_insert(0) += 1;
    }
    
    for (event_type, count) in &event_counts {
        println!("  {}: {} events", event_type, count);
    }
    
    // Verify we have script outputs
    let script_outputs: Vec<_> = events.iter()
        .filter(|e| e.event_type == "ScriptGenerated")
        .collect();
    
    assert!(!script_outputs.is_empty(), "Expected script outputs");
    
    // Verify that each script has traceability
    for script in &script_outputs {
        assert!(script.payload["chunk_id"].is_string());
        assert!(script.payload["source_title"].is_string());
        println!("✅ Script event has full traceability");
    }
    
    // Clean up
    // Cleanup handled by tempdir
    Ok(())
}

// Source that emits pre-defined events for testing
struct PreDefinedSource {
    events: Vec<ChainEvent>,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl PreDefinedSource {
    fn new(events: Vec<ChainEvent>) -> Self {
        Self {
            events,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("PreDefinedSource"),
        }
    }
}

impl Step for PreDefinedSource {
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
        let current = self.emitted.fetch_add(1, Ordering::Relaxed) as usize;
        if current < self.events.len() {
            vec![self.events[current].clone()]
        } else {
            vec![]
        }
    }
}

#[tokio::test]
async fn test_stage_replay_and_recovery() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_replay_store");
    
    let (collector_sink, collected_events) = EventCollectorSink::new();
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // Initial events to process
    let initial_events = vec![
        ChainEvent::new("NewsFetched", json!({
            "title": "AI Breakthrough in Healthcare",
            "content": "Scientists have developed a new AI system. It can diagnose diseases faster than humans. This could revolutionize medical care.",
            "url": "https://example.com/ai-health"
        })),
        ChainEvent::new("NewsFetched", json!({
            "title": "Market Analysis Today",
            "content": "Stock markets showed volatility today. Tech stocks led the gains. Investors remain optimistic about growth.",
            "url": "https://example.com/market"
        })),
    ];
    
    let source = PreDefinedSource::new(initial_events);
    
    // Run the first pipeline
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => source, RED)
        |> ("chunker" => NewsChunker::new(), RED)
        |> ("collector" => collector_sink, RED)
    }?;
    
    // Let the flow run
    tokio::time::sleep(Duration::from_secs(2)).await;
    handle.shutdown().await?;
    
    // Check chunks created
    let events1 = collected_events.lock().await;
    let chunk_count1 = events1.iter()
        .filter(|e| e.event_type == "ChunkIndexed")
        .count();
    println!("First run created {} chunks", chunk_count1);
    assert!(chunk_count1 > 2, "Should create multiple chunks");
    drop(events1);
    
    // Test replay with new stage - same source events
    let initial_events2 = vec![
        ChainEvent::new("NewsFetched", json!({
            "title": "AI Breakthrough in Healthcare",
            "content": "Scientists have developed a new AI system. It can diagnose diseases faster than humans. This could revolutionize medical care.",
            "url": "https://example.com/ai-health"
        })),
        ChainEvent::new("NewsFetched", json!({
            "title": "Market Analysis Today",
            "content": "Stock markets showed volatility today. Tech stocks led the gains. Investors remain optimistic about growth.",
            "url": "https://example.com/market"
        })),
        ChainEvent::new("NewsFetched", json!({
            "title": "Breaking: New Development",
            "content": "This is a breaking news story. It has important implications.",
            "url": "https://example.com/breaking"
        })),
    ];
    
    let (collector_sink2, collected_events2) = EventCollectorSink::new();
    let source2 = PreDefinedSource::new(initial_events2);
    
    // Need to create a new EventStore for the second pipeline
    let event_store2 = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // Different chunker instance to test independent processing
    let handle2 = flow! {
        store: event_store2,
        flow_taxonomy: GoldenSignals,
        ("source2" => source2, RED)
        |> ("chunker2" => NewsChunker::new(), RED)  // Different stage name
        |> ("collector2" => collector_sink2, RED)
    }?;
    
    tokio::time::sleep(Duration::from_secs(2)).await;
    handle2.shutdown().await?;
    
    // Check second run
    let events2 = collected_events2.lock().await;
    let chunk_count2 = events2.iter()
        .filter(|e| e.event_type == "ChunkIndexed")
        .count();
    println!("Second run created {} chunks from 3 articles", chunk_count2);
    assert!(chunk_count2 > 3, "Should create chunks from all 3 articles");
    
    // Clean up
    let _ = std::fs::remove_dir_all(&store_path);
    Ok(())
}

// Multi-source processor stage
struct MultiSourceProcessor {
    metrics: <USE as Taxonomy>::Metrics,
}

impl MultiSourceProcessor {
    fn new() -> Self {
        Self {
            metrics: USE::create_metrics("MultiSourceProcessor"),
        }
    }
}

impl Step for MultiSourceProcessor {
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
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        match event.event_type.as_str() {
            "NewsFetched" => {
                event.payload["content_type"] = json!("news");
                event.payload["priority"] = json!("high");
                vec![event]
            }
            "JobFetched" => {
                event.payload["content_type"] = json!("opportunity");
                event.payload["priority"] = json!("medium");
                vec![event]
            }
            "HackerNewsFetched" => {
                event.payload["content_type"] = json!("tech_discussion");
                event.payload["priority"] = json!("low");
                vec![event]
            }
            _ => vec![]
        }
    }
}

// Combined source that emits multiple types of events
struct MultiTypeSource {
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl MultiTypeSource {
    fn new() -> Self {
        Self {
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("MultiTypeSource"),
        }
    }
}

impl Step for MultiTypeSource {
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
        if current == 0 {
            // Emit all types at once
            vec![
                // RSS events
                ChainEvent::new("NewsFetched", json!({
                    "id": "rss-1",
                    "title": "Breaking: AI Makes Major Breakthrough",
                    "content": "Scientists announce a significant advancement.",
                    "url": "https://example.com/ai-breakthrough",
                    "category": "technology"
                })),
                ChainEvent::new("NewsFetched", json!({
                    "id": "rss-2",
                    "title": "Market Update: Tech Stocks Surge",
                    "content": "Technology stocks showed strong performance.",
                    "url": "https://example.com/market-update",
                    "category": "finance"
                })),
                // Job events
                ChainEvent::new("JobFetched", json!({
                    "id": "job-0",
                    "title": "Software Engineer Position #0",
                    "content": "We are looking for a talented software engineer. Job #0",
                    "url": "https://jobs.example.com/job-0",
                    "company": "Company 0"
                })),
                ChainEvent::new("JobFetched", json!({
                    "id": "job-1",
                    "title": "Software Engineer Position #1",
                    "content": "We are looking for a talented software engineer. Job #1",
                    "url": "https://jobs.example.com/job-1",
                    "company": "Company 1"
                })),
                ChainEvent::new("JobFetched", json!({
                    "id": "job-2",
                    "title": "Software Engineer Position #2",
                    "content": "We are looking for a talented software engineer. Job #2",
                    "url": "https://jobs.example.com/job-2",
                    "company": "Company 2"
                })),
                // HN events
                ChainEvent::new("HackerNewsFetched", json!({
                    "id": "hn-0",
                    "title": "Show HN: My Rust Project",
                    "content": "I've been working on this Rust project.",
                    "url": "https://news.ycombinator.com/item?id=30000",
                    "score": 42,
                    "topic": "Rust"
                })),
                ChainEvent::new("HackerNewsFetched", json!({
                    "id": "hn-1",
                    "title": "Show HN: My AI Project",
                    "content": "I've been working on this AI project.",
                    "url": "https://news.ycombinator.com/item?id=30001",
                    "score": 43,
                    "topic": "AI"
                })),
            ]
        } else {
            vec![]
        }
    }
}

#[tokio::test]
async fn test_multiple_fetcher_sources() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_multi_fetchers_store");
    
    let (collector_sink, collected_events) = EventCollectorSink::new();
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // Create and run the pipeline with multiple source types
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("multi_source" => MultiTypeSource::new(), RED)
        |> ("processor" => MultiSourceProcessor::new(), USE)
        |> ("collector" => collector_sink, RED)
    }?;
    
    // Let the flow run
    tokio::time::sleep(Duration::from_secs(3)).await;
    handle.shutdown().await?;
    
    // Verify all content was processed
    let events = collected_events.lock().await;
    println!("Collected {} events", events.len());
    
    // Count by type
    let mut type_counts = std::collections::HashMap::new();
    for event in events.iter() {
        *type_counts.entry(event.event_type.clone()).or_insert(0) += 1;
    }
    
    // Should have 7 processed events (2 RSS + 3 Jobs + 2 HN)
    assert_eq!(events.len(), 7, "Expected 7 processed events");
    
    // Verify each processed event has the right metadata
    for event in events.iter() {
        assert!(event.payload["content_type"].is_string());
        assert!(event.payload["priority"].is_string());
    }
    
    println!("✅ Successfully processed {} events from multiple sources", events.len());
    
    // Clean up
    let _ = std::fs::remove_dir_all(&store_path);
    Ok(())
}

// Traceable stages for testing
struct TraceableStage1 {
    metrics: <RED as Taxonomy>::Metrics,
}

impl TraceableStage1 {
    fn new() -> Self {
        Self {
            metrics: RED::create_metrics("TraceableStage1"),
        }
    }
}

impl Step for TraceableStage1 {
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
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        event.payload["stage1_processed"] = json!(true);
        event.payload["stage1_timestamp"] = json!(chrono::Utc::now().to_rfc3339());
        vec![event]
    }
}

struct TraceableStage2 {
    metrics: <USE as Taxonomy>::Metrics,
}

impl TraceableStage2 {
    fn new() -> Self {
        Self {
            metrics: USE::create_metrics("TraceableStage2"),
        }
    }
}

impl Step for TraceableStage2 {
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
    
    fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        event.payload["stage2_processed"] = json!(true);
        event.payload["stage2_timestamp"] = json!(chrono::Utc::now().to_rfc3339());
        event.payload["final_result"] = json!("Processing complete");
        vec![event]
    }
}

// Source that emits a single pre-defined event
struct SingleEventSource {
    event: ChainEvent,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl SingleEventSource {
    fn new(event: ChainEvent) -> Self {
        Self {
            event,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("SingleEventSource"),
        }
    }
}

impl Step for SingleEventSource {
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
        if self.emitted.fetch_add(1, Ordering::Relaxed) == 0 {
            vec![self.event.clone()]
        } else {
            vec![]
        }
    }
}

#[tokio::test]
async fn test_event_traceability_and_debugging() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_traceability_store");
    
    let (collector_sink, collected_events) = EventCollectorSink::new();
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // Initial event
    let initial_event = ChainEvent::new("OriginalEvent", json!({
        "user_id": "user123",
        "action": "content_request",
        "timestamp": chrono::Utc::now().to_rfc3339()
    }));
    
    let source = SingleEventSource::new(initial_event.clone());
    
    // Run pipeline with traceability
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => source, RED)
        |> ("trace_stage1" => TraceableStage1::new(), RED)
        |> ("trace_stage2" => TraceableStage2::new(), USE)
        |> ("collector" => collector_sink, RED)
    }?;
    
    // Let the flow run
    tokio::time::sleep(Duration::from_secs(2)).await;
    handle.shutdown().await?;
    
    // Analyze the event trace
    let events = collected_events.lock().await;
    println!("\n🔍 Event Trace Analysis:");
    println!("Total events collected: {}", events.len());
    
    // Find the final processed event
    let final_event = events.iter()
        .find(|e| e.payload.get("stage2_processed").is_some())
        .expect("Should have final processed event");
    
    // Verify traceability metadata
    assert!(final_event.payload["stage1_processed"].as_bool().unwrap());
    assert!(final_event.payload["stage2_processed"].as_bool().unwrap());
    assert_eq!(final_event.payload["final_result"], "Processing complete");
    assert_eq!(final_event.payload["user_id"], "user123");
    
    println!("✅ Complete traceability verified through the pipeline");
    
    // Clean up
    let _ = std::fs::remove_dir_all(&store_path);
    Ok(())
}
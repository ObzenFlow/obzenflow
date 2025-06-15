use flowstate_rs::prelude::*;
use flowstate_rs::monitoring::{RED, Taxonomy, GoldenSignals};
use flowstate_rs::flow;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::collections::HashSet;
use std::fs;
use std::io::{BufRead, BufReader};

/// Source that tracks what it emitted
struct DiagnosticSource {
    total: u64,
    emitted: Arc<AtomicU64>,
    emitted_ids: Arc<tokio::sync::Mutex<Vec<String>>>,
    metrics: <RED as Taxonomy>::Metrics,
}

impl Clone for DiagnosticSource {
    fn clone(&self) -> Self {
        Self {
            total: self.total,
            emitted: self.emitted.clone(),
            emitted_ids: self.emitted_ids.clone(),
            metrics: RED::create_metrics("DiagnosticSource"),
        }
    }
}

impl Step for DiagnosticSource {
    type Taxonomy = RED;
    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Source }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total {
            let event = ChainEvent::new("test", json!({ "index": current }));
            let id = event.ulid.to_string();
            
            // Track what we emitted
            let ids = self.emitted_ids.clone();
            tokio::spawn(async move {
                ids.lock().await.push(id);
            });
            
            vec![event]
        } else {
            vec![]
        }
    }
}

/// Sink that tracks what it received
struct DiagnosticSink {
    expected: u64,
    received: Arc<AtomicU64>,
    received_ids: Arc<tokio::sync::Mutex<Vec<(String, u64)>>>,
    metrics: Arc<<RED as Taxonomy>::Metrics>,
}

impl Clone for DiagnosticSink {
    fn clone(&self) -> Self {
        Self {
            expected: self.expected,
            received: self.received.clone(),
            received_ids: self.received_ids.clone(),
            metrics: Arc::new(RED::create_metrics("DiagnosticSink")),
        }
    }
}

impl Step for DiagnosticSink {
    type Taxonomy = RED;
    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &*self.metrics }
    fn step_type(&self) -> StepType { StepType::Sink }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(index) = event.payload.get("index").and_then(|v| v.as_u64()) {
            self.received.fetch_add(1, Ordering::Relaxed);
            
            // Track what we received
            let ids = self.received_ids.clone();
            let id = event.ulid.to_string();
            tokio::spawn(async move {
                ids.lock().await.push((id, index));
            });
        }
        vec![]
    }
}

#[tokio::test]
async fn test_event_loss_detection_in_pipeline() -> Result<()> {
    // Clean up any existing store
    let _ = fs::remove_dir_all("./test_event_loss_detection");
    
    let store = EventStore::new(EventStoreConfig {
        path: "./test_event_loss_detection".into(),
        max_segment_size: 1024 * 1024,
    }).await?;
    let total_events = 110u64;
    
    let source = DiagnosticSource {
        total: total_events,
        emitted: Arc::new(AtomicU64::new(0)),
        emitted_ids: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        metrics: RED::create_metrics("DiagnosticSource"),
    };
    let source_ids = source.emitted_ids.clone();
    
    let sink = DiagnosticSink {
        expected: total_events,
        received: Arc::new(AtomicU64::new(0)),
        received_ids: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        metrics: Arc::new(RED::create_metrics("DiagnosticSink")),
    };
    let sink_clone = sink.clone();
    let sink_ids = sink.received_ids.clone();
    
    
    // Run test
    let handle = flow! {
        store: store,
        flow_taxonomy: GoldenSignals,
        ("source" => source, RED)
        |> ("stage1" => PassthroughStage, RED)
        |> ("stage2" => PassthroughStage, RED)
        |> ("sink" => sink, RED)
    }?;
    
    // Wait for completion
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(30);
    
    while sink_clone.received.load(Ordering::Relaxed) < total_events {
        if start.elapsed() > timeout {
            println!("\nTIMEOUT after 30 seconds");
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    
    let _received_count = sink_clone.received.load(Ordering::Relaxed);
    
    // Give a moment for async tracking to complete
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    
    // Shutdown the flow
    handle.shutdown().await?;
    
    // Now analyze the log files
    let log_files: Vec<_> = fs::read_dir("./test_event_loss_detection")?
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.path().extension().map_or(false, |ext| ext == "log"))
        .collect();
    
    let mut _total_events_in_logs = 0;
    let mut events_by_stage: std::collections::HashMap<String, Vec<Value>> = std::collections::HashMap::new();
    let mut all_event_ids = HashSet::new();
    
    for entry in log_files {
        let path = entry.path();
        
        let file = fs::File::open(&path)?;
        let reader = BufReader::new(file);
        let mut _line_count = 0;
        
        for line in reader.lines() {
            _line_count += 1;
            match line {
                Ok(line) if !line.trim().is_empty() => {
                    match serde_json::from_str::<Value>(&line) {
                        Ok(record) => {
                            _total_events_in_logs += 1;
                            
                            if let Some(event_id) = record.get("event_id").and_then(|v| v.as_str()) {
                                all_event_ids.insert(event_id.to_string());
                            }
                            
                            if let Some(writer_id) = record.get("writer_id").and_then(|v| v.as_str()) {
                                events_by_stage.entry(writer_id.to_string())
                                    .or_insert_with(Vec::new)
                                    .push(record);
                            }
                        }
                        Err(_) => {
                            // Skip parsing errors
                        }
                    }
                }
                Ok(_) => {}, // Empty line
                Err(_) => {}, // Skip read errors
            }
        }
        
    }
    
    
    // Compare source emissions with log
    let source_emitted = source_ids.lock().await;
    let sink_received = sink_ids.lock().await;
    
    
    // Find missing events
    let source_set: HashSet<_> = source_emitted.iter().cloned().collect();
    let sink_set: HashSet<_> = sink_received.iter().map(|(id, _)| id.clone()).collect();
    
    let missing_from_sink = source_set.difference(&sink_set);
    let missing_count = missing_from_sink.clone().count();
    
    // Cleanup before assertions
    let _ = fs::remove_dir_all("./test_event_loss_detection");
    
    // Test assertions
    assert_eq!(
        missing_count, 
        0, 
        "Lost {} events! {} emitted by source but NOT received by sink. Missing IDs: {:?}",
        missing_count,
        missing_count,
        missing_from_sink.take(10).collect::<Vec<_>>()
    );
    
    // Check for duplicates
    assert_eq!(
        sink_received.len(),
        sink_set.len(),
        "DUPLICATES detected: {} received vs {} unique",
        sink_received.len(),
        sink_set.len()
    );
    
    Ok(())
}

// Simple passthrough stage
#[derive(Clone)]
struct PassthroughStage;

impl Step for PassthroughStage {
    type Taxonomy = RED;
    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        static METRICS: std::sync::OnceLock<<RED as Taxonomy>::Metrics> = std::sync::OnceLock::new();
        METRICS.get_or_init(|| RED::create_metrics("PassthroughStage"))
    }
    fn step_type(&self) -> StepType { StepType::Stage }
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> { vec![event] }
}
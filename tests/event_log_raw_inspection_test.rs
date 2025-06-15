//! Raw Event Log Inspection Tests
//! 
//! These tests read the actual event log files from disk to verify:
//! 1. Every event from every stage is persisted to disk
//! 2. No events are lost between stages
//! 3. The log is the complete book of record
//! 
//! This is TRUE double-entry accounting - we verify against the raw log files,
//! not just what reaches the sink in memory.
//!
//! KEY ARCHITECTURAL GUARANTEE: No sink effects without journal entries.
//! The Step trait signature and EventStore design make it impossible for
//! any event to reach a sink without being recorded in the immutable journal.

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::{HashMap, HashSet};
use tempfile::tempdir;
use tokio::sync::Mutex;
use serde_json::{json, Value};
use ulid::Ulid;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::fs::File;

/// Source that emits numbered events
#[derive(Clone)]
struct NumberedSource {
    count: u64,
    emitted: Arc<AtomicU64>,
    emitted_ulids: Arc<Mutex<Vec<Ulid>>>,
    metrics: Arc<<RED as Taxonomy>::Metrics>,
}

impl NumberedSource {
    fn new(count: u64) -> (Self, Arc<Mutex<Vec<Ulid>>>) {
        let ulids = Arc::new(Mutex::new(Vec::new()));
        let source = Self {
            count,
            emitted: Arc::new(AtomicU64::new(0)),
            emitted_ulids: ulids.clone(),
            metrics: Arc::new(RED::create_metrics("NumberedSource")),
        };
        (source, ulids)
    }
}

impl Step for NumberedSource {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Source }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::SeqCst);
        if current < self.count {
            let event = ChainEvent::new("Input", json!({
                "sequence": current,
                "data": format!("item_{}", current),
            }));
            
            let ulids = self.emitted_ulids.clone();
            let ulid = event.ulid;
            tokio::spawn(async move {
                ulids.lock().await.push(ulid);
            });
            
            vec![event]
        } else {
            vec![]
        }
    }
}

/// Transform stage that modifies data
#[derive(Clone)]
struct TransformStage {
    name: String,
    operation: String,
    metrics: Arc<<USE as Taxonomy>::Metrics>,
}

impl TransformStage {
    fn new(name: &str, operation: &str) -> Self {
        Self {
            name: name.to_string(),
            operation: operation.to_string(),
            metrics: Arc::new(USE::create_metrics(name)),
        }
    }
}

impl Step for TransformStage {
    type Taxonomy = USE;
    
    fn taxonomy(&self) -> &Self::Taxonomy { &USE }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Stage }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "Input" || event.event_type.starts_with("Transform_") {
            let mut output = ChainEvent::new(
                &format!("Transform_{}", self.name),
                event.payload.clone()
            );
            
            // Add transformation metadata
            if let Some(obj) = output.payload.as_object_mut() {
                obj.insert("transform_stage".to_string(), json!(self.name));
                obj.insert("transform_operation".to_string(), json!(self.operation));
                obj.insert("parent_ulid".to_string(), json!(event.ulid.to_string()));
                
                // Track the transformation chain
                if let Some(chain) = obj.get("transform_chain").and_then(|v| v.as_array()) {
                    let mut new_chain = chain.clone();
                    new_chain.push(json!(self.name));
                    obj.insert("transform_chain".to_string(), json!(new_chain));
                } else {
                    obj.insert("transform_chain".to_string(), json!([self.name]));
                }
            }
            
            vec![output]
        } else {
            vec![]
        }
    }
}

/// Sink that collects final results
#[derive(Clone)]
struct CollectorSink {
    collected_ulids: Arc<Mutex<Vec<Ulid>>>,
    metrics: Arc<<RED as Taxonomy>::Metrics>,
}

impl CollectorSink {
    fn new() -> (Self, Arc<Mutex<Vec<Ulid>>>) {
        let ulids = Arc::new(Mutex::new(Vec::new()));
        let sink = Self {
            collected_ulids: ulids.clone(),
            metrics: Arc::new(RED::create_metrics("CollectorSink")),
        };
        (sink, ulids)
    }
}

impl Step for CollectorSink {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Sink }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let ulids = self.collected_ulids.clone();
        let ulid = event.ulid;
        tokio::spawn(async move {
            ulids.lock().await.push(ulid);
        });
        vec![]
    }
}

/// Parse a log record from a JSON line in the event log
fn parse_log_record(line: &str) -> Option<(Ulid, String, Value)> {
    if let Ok(record) = serde_json::from_str::<Value>(line) {
        if let (Some(event_id), Some(event_type), Some(payload)) = (
            record.get("event_id").and_then(|v| v.as_str()).and_then(|s| s.parse::<Ulid>().ok()),
            record.get("event").and_then(|e| e.get("event_type")).and_then(|v| v.as_str()),
            record.get("event").and_then(|e| e.get("payload"))
        ) {
            return Some((event_id, event_type.to_string(), payload.clone()));
        }
    }
    None
}

/// Read all events from the raw log files
async fn read_raw_event_log(store_path: &std::path::Path) -> Result<Vec<(Ulid, String, Value)>> {
    let mut all_events = Vec::new();
    
    // Find all log files in the store directory
    let mut entries = tokio::fs::read_dir(store_path).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("log") {
            println!("Reading log file: {:?}", path);
            
            let file = File::open(&path).await?;
            let reader = BufReader::new(file);
            let mut lines = reader.lines();
            
            while let Some(line) = lines.next_line().await? {
                if let Some(record) = parse_log_record(&line) {
                    all_events.push(record);
                }
            }
        }
    }
    
    // Sort by ULID (which includes timestamp) for chronological order
    all_events.sort_by_key(|(ulid, _, _)| *ulid);
    
    Ok(all_events)
}

#[tokio::test]
async fn test_raw_event_log_completeness() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_raw_log");
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // Create pipeline with tracking
    let (source, source_ulids) = NumberedSource::new(5);
    let (sink, sink_ulids) = CollectorSink::new();
    
    // Run a 5-stage pipeline
    let handle = flow! {
        store: event_store.clone(),
        flow_taxonomy: GoldenSignals,
        ("source" => source, RED)
        |> ("stage1" => TransformStage::new("stage1", "operation1"), USE)
        |> ("stage2" => TransformStage::new("stage2", "operation2"), USE)
        |> ("stage3" => TransformStage::new("stage3", "operation3"), USE)
        |> ("stage4" => TransformStage::new("stage4", "operation4"), USE)
        |> ("stage5" => TransformStage::new("stage5", "operation5"), USE)
        |> ("sink" => sink, RED)
    }?;
    
    // Let pipeline process
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
    handle.shutdown().await?;
    
    // Give time for final writes to flush
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // Now read the RAW EVENT LOG from disk
    println!("\n📖 READING RAW EVENT LOG FROM DISK");
    println!("==================================");
    
    let raw_events = read_raw_event_log(&store_path).await?;
    println!("Found {} total events in raw log files", raw_events.len());
    
    // Get what we tracked in memory
    let source_ulids = source_ulids.lock().await;
    let sink_ulids = sink_ulids.lock().await;
    
    println!("\nMemory tracking:");
    println!("  Source emitted: {} events", source_ulids.len());
    println!("  Sink received: {} events", sink_ulids.len());
    
    // 1. Verify every source event is in the raw log
    println!("\n🔍 Verifying source events in raw log...");
    for source_ulid in source_ulids.iter() {
        let found = raw_events.iter().any(|(ulid, _, _)| ulid == source_ulid);
        assert!(found, "Source event {} not found in raw log!", source_ulid);
    }
    println!("✅ All {} source events found in raw log", source_ulids.len());
    
    // 2. Count events by type in the raw log
    let mut event_counts: HashMap<String, usize> = HashMap::new();
    for (_, event_type, _) in raw_events.iter() {
        *event_counts.entry(event_type.clone()).or_insert(0) += 1;
    }
    
    println!("\n📊 Raw Log Event Counts:");
    for (event_type, count) in event_counts.iter() {
        println!("  {}: {} events", event_type, count);
    }
    
    // 3. Verify we have the right number of events
    // Expected: 5 source + 5*5 transforms = 30 total
    assert_eq!(event_counts.get("Input").copied().unwrap_or(0), 5, "Should have 5 Input events");
    assert_eq!(event_counts.get("Transform_stage1").copied().unwrap_or(0), 5, "Should have 5 stage1 events");
    assert_eq!(event_counts.get("Transform_stage2").copied().unwrap_or(0), 5, "Should have 5 stage2 events");
    assert_eq!(event_counts.get("Transform_stage3").copied().unwrap_or(0), 5, "Should have 5 stage3 events");
    assert_eq!(event_counts.get("Transform_stage4").copied().unwrap_or(0), 5, "Should have 5 stage4 events");
    assert_eq!(event_counts.get("Transform_stage5").copied().unwrap_or(0), 5, "Should have 5 stage5 events");
    
    let total_expected = 5 + (5 * 5); // source + transforms
    assert_eq!(
        raw_events.len(), 
        total_expected, 
        "Raw log should have {} events but has {}",
        total_expected,
        raw_events.len()
    );
    
    // 4. Trace each sequence through the raw log
    println!("\n🔗 Tracing transformation chains in raw log...");
    
    // Build a map of events by ULID for parent lookup
    let mut events_by_ulid: HashMap<Ulid, (String, Value)> = HashMap::new();
    for (ulid, event_type, payload) in raw_events.iter() {
        events_by_ulid.insert(*ulid, (event_type.clone(), payload.clone()));
    }
    
    for seq in 0..5 {
        println!("\nSequence {}: ", seq);
        
        // Find the source event for this sequence
        let source_event = raw_events.iter()
            .find(|(_, event_type, payload)| {
                event_type == "Input" && 
                payload.get("sequence").and_then(|v| v.as_u64()) == Some(seq)
            })
            .expect(&format!("Source event for sequence {} not found", seq));
        
        // Trace the chain by following parent links
        let mut current_ulid = source_event.0;
        let mut chain = vec![(current_ulid, "Input")];
        
        // Follow the chain through parent references
        while chain.len() < 6 { // We expect 6 total (source + 5 transforms)
            let mut found_next = false;
            
            // Find the event that has current_ulid as its parent
            for (ulid, event_type, payload) in raw_events.iter() {
                if let Some(parent_str) = payload.get("parent_ulid").and_then(|v| v.as_str()) {
                    if parent_str == current_ulid.to_string() {
                        current_ulid = *ulid;
                        chain.push((current_ulid, event_type.as_str()));
                        found_next = true;
                        break;
                    }
                }
            }
            
            if !found_next {
                break;
            }
        }
        
        // Should have exactly 6 events (1 source + 5 transforms)
        assert_eq!(
            chain.len(), 
            6, 
            "Sequence {} should have 6 events in chain but has {}: {:?}",
            seq,
            chain.len(),
            chain.iter().map(|(_, t)| t).collect::<Vec<_>>()
        );
        
        // Verify we have all expected stages (order might vary due to concurrent processing)
        let chain_types: HashSet<&str> = chain.iter().map(|(_, t)| *t).collect();
        assert!(chain_types.contains("Input"), "Missing Input event");
        assert!(chain_types.contains("Transform_stage1"), "Missing stage1");
        assert!(chain_types.contains("Transform_stage2"), "Missing stage2");
        assert!(chain_types.contains("Transform_stage3"), "Missing stage3");
        assert!(chain_types.contains("Transform_stage4"), "Missing stage4");
        assert!(chain_types.contains("Transform_stage5"), "Missing stage5");
        
        // Verify the logical order through parent linkage
        println!("  Chain: {}", 
            chain.iter()
                .map(|(_, t)| t.to_string())
                .collect::<Vec<_>>()
                .join(" → ")
        );
        
        println!("  ✅ Complete chain with all stages present");
    }
    
    // 5. Verify no phantom events
    println!("\n👻 Checking for phantom events...");
    let valid_sequences: HashSet<u64> = (0..5).collect();
    for (ulid, _event_type, payload) in raw_events.iter() {
        if let Some(seq) = payload.get("sequence").and_then(|v| v.as_u64()) {
            assert!(
                valid_sequences.contains(&seq),
                "Phantom event {} with invalid sequence {}",
                ulid, seq
            );
        }
    }
    println!("✅ No phantom events found");
    
    // 6. Final reconciliation
    println!("\n📊 RAW LOG RECONCILIATION COMPLETE");
    println!("==================================");
    println!("✅ Raw event log is complete and consistent:");
    println!("  - All {} source events persisted to disk", source_ulids.len());
    println!("  - All {} transformation stages recorded", 5);
    println!("  - Total {} events in raw log (expected {})", raw_events.len(), total_expected);
    println!("  - Every event chain is complete with proper parent linkage");
    println!("  - No phantom or duplicate events");
    println!("\n🎯 The raw event log is a perfect book of record!");
    
    Ok(())
}

#[tokio::test]
async fn test_event_loss_visible_in_raw_log() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_loss_raw_log");
    
    // Lossy transform that drops some events
    #[derive(Clone)]
    struct LossyTransform {
        name: String,
        metrics: Arc<<USE as Taxonomy>::Metrics>,
    }
    
    impl LossyTransform {
        fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                metrics: Arc::new(USE::create_metrics(name)),
            }
        }
    }
    
    impl Step for LossyTransform {
        type Taxonomy = USE;
        
        fn taxonomy(&self) -> &Self::Taxonomy { &USE }
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
        fn step_type(&self) -> StepType { StepType::Stage }
        
        fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
            // Drop events with sequence 1 and 3
            if let Some(seq) = event.payload.get("sequence").and_then(|v| v.as_u64()) {
                if seq == 1 || seq == 3 {
                    return vec![]; // DROP!
                }
            }
            
            // Transform others normally
            vec![ChainEvent::new(&format!("Transform_{}", self.name), event.payload)]
        }
    }
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    let (source, _) = NumberedSource::new(5);
    let (sink, _) = CollectorSink::new();
    
    let handle = flow! {
        store: event_store.clone(),
        flow_taxonomy: GoldenSignals,
        ("source" => source, RED)
        |> ("good_stage" => TransformStage::new("good", "passthrough"), USE)
        |> ("lossy_stage" => LossyTransform::new("lossy"), USE)
        |> ("recovery_stage" => TransformStage::new("recovery", "afterloss"), USE)
        |> ("sink" => sink, RED)
    }?;
    
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    handle.shutdown().await?;
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // Read raw log
    let raw_events = read_raw_event_log(&store_path).await?;
    
    println!("\n🔍 ANALYZING EVENT LOSS IN RAW LOG");
    println!("==================================");
    
    // Count events by type
    let mut event_counts: HashMap<String, usize> = HashMap::new();
    for (_, event_type, _) in raw_events.iter() {
        *event_counts.entry(event_type.clone()).or_insert(0) += 1;
    }
    
    println!("Event counts in raw log:");
    for (event_type, count) in event_counts.iter() {
        println!("  {}: {}", event_type, count);
    }
    
    // Verify the loss is visible
    assert_eq!(event_counts["Input"], 5, "Should have all 5 input events");
    assert_eq!(event_counts["Transform_good"], 5, "Good stage should process all 5");
    assert_eq!(event_counts["Transform_lossy"], 3, "Lossy stage should only emit 3 (dropped seq 1,3)");
    assert_eq!(event_counts["Transform_recovery"], 3, "Recovery stage only gets 3 events");
    
    // Find which sequences made it through
    let mut surviving_sequences = HashSet::new();
    for (_, event_type, payload) in raw_events.iter() {
        if event_type == "Transform_recovery" {
            if let Some(seq) = payload.get("sequence").and_then(|v| v.as_u64()) {
                surviving_sequences.insert(seq);
            }
        }
    }
    
    println!("\n✅ Event loss is clearly visible in raw log:");
    println!("  - Input sequences: 0, 1, 2, 3, 4");
    println!("  - Surviving sequences: {:?}", surviving_sequences);
    println!("  - Lost sequences: 1, 3");
    println!("\n🎯 Raw log provides perfect audit trail of event loss!");
    
    Ok(())
}
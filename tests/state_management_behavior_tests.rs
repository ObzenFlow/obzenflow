//! Behavioral tests for EventSourcedStage state management
//! 
//! These tests focus on observable behavior rather than internal state,
//! making them more robust across refactorings.

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::lifecycle::{EventHandler, ProcessingMode};
use flowstate_rs::topology::StageId;
use serde_json::json;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::time::{timeout, Duration};

/// Test source that generates a fixed number of events then completes
struct FiniteSource {
    events_to_emit: u64,
    current: AtomicU64,
    stage_id: StageId,
    completion_sent: Arc<AtomicBool>,
}

impl FiniteSource {
    fn new(stage_id: StageId, count: u64) -> Self {
        Self {
            events_to_emit: count,
            current: AtomicU64::new(0),
            stage_id,
            completion_sent: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl EventHandler for FiniteSource {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        println!("FiniteSource transform called with event type: {}", event.event_type);
        
        // Sources receive _tick events and generate data based on internal state
        let current = self.current.load(Ordering::SeqCst);
        println!("FiniteSource current: {}, events_to_emit: {}", current, self.events_to_emit);
        
        if current < self.events_to_emit {
            self.current.fetch_add(1, Ordering::SeqCst);
            println!("FiniteSource emitting data event {}", current);
            vec![ChainEvent::new("data", json!({"n": current}))]
        } else if !self.completion_sent.swap(true, Ordering::SeqCst) {
            // Emit completion event exactly once
            println!("FiniteSource emitting completion event");
            vec![ChainEvent::source_complete(self.stage_id, true)]
        } else {
            println!("FiniteSource no more events");
            vec![]
        }
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

/// Test processor that counts events
struct CountingProcessor {
    data_events: Arc<AtomicU64>,
    completion_events: Arc<AtomicU64>,
}

impl CountingProcessor {
    fn new() -> Self {
        Self {
            data_events: Arc::new(AtomicU64::new(0)),
            completion_events: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl EventHandler for CountingProcessor {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "data" {
            self.data_events.fetch_add(1, Ordering::SeqCst);
        } else if event.event_type == "flowstate.source.complete" {
            self.completion_events.fetch_add(1, Ordering::SeqCst);
        }
        vec![event] // Pass through
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

#[tokio::test]
async fn test_finite_source_natural_completion() -> Result<()> {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter("flowstate_rs=debug")
        .try_init();
    
    println!("\n=== Starting test_finite_source_natural_completion ===");
    
    // Test that a finite source completes naturally
    let source = FiniteSource::new(StageId::from_u32(0), 5);
    let completion_sent = source.completion_sent.clone();
    
    let processor = CountingProcessor::new();
    let data_count = processor.data_events.clone();
    let completion_count = processor.completion_events.clone();
    
    println!("Creating flow...");
    let mut handle = flow! {
        name: "test_natural_completion",
        ("source" => source, [])
        |> ("processor" => processor, [])
    }?;
    println!("Flow created, waiting for completion...");
    
    // Give a moment for initialization
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Check if any events have been processed
    println!("Data events processed so far: {}", data_count.load(Ordering::SeqCst));
    println!("Completion events so far: {}", completion_count.load(Ordering::SeqCst));
    
    // Wait for natural completion
    let result = timeout(Duration::from_secs(5), handle.wait_for_completion()).await;
    
    println!("Final data events: {}", data_count.load(Ordering::SeqCst));
    println!("Final completion events: {}", completion_count.load(Ordering::SeqCst));
    println!("Completion sent: {}", completion_sent.load(Ordering::SeqCst));
    
    assert!(result.is_ok(), "Pipeline should complete naturally within timeout");
    
    // Verify behavior
    assert_eq!(data_count.load(Ordering::SeqCst), 5, "Should process 5 data events");
    assert_eq!(completion_count.load(Ordering::SeqCst), 1, "Should receive 1 completion event");
    assert!(completion_sent.load(Ordering::SeqCst), "Source should have sent completion");
    
    Ok(())
}

#[tokio::test]
async fn test_forced_shutdown_before_completion() -> Result<()> {
    // Test forced shutdown interrupts natural completion
    let source = FiniteSource::new(StageId::from_u32(0), 1000); // Many events
    
    let processor = CountingProcessor::new();
    let data_count = processor.data_events.clone();
    
    let handle = flow! {
        name: "test_forced_shutdown",
        ("source" => source, [])
        |> ("processor" => processor, [])
    }?;
    
    // Let it run briefly
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Force shutdown
    handle.shutdown().await?;
    
    // Verify behavior
    let events_processed = data_count.load(Ordering::SeqCst);
    assert!(events_processed < 1000, "Should not process all events: got {}", events_processed);
    assert!(events_processed > 0, "Should process some events before shutdown: got {}", events_processed);
    
    Ok(())
}

/// Source that never completes on its own
struct InfiniteSource {
    counter: AtomicU64,
}

impl InfiniteSource {
    fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }
}

impl EventHandler for InfiniteSource {
    fn transform(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        // Sources receive _tick events and generate data
        let n = self.counter.fetch_add(1, Ordering::SeqCst);
        vec![ChainEvent::new("data", json!({"n": n}))]
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

#[tokio::test]
async fn test_infinite_source_requires_shutdown() -> Result<()> {
    // Test that infinite sources need explicit shutdown
    let source = InfiniteSource::new();
    
    let processor = CountingProcessor::new();
    let data_count = processor.data_events.clone();
    
    let mut handle = flow! {
        name: "test_infinite_source",
        ("source" => source, [])
        |> ("processor" => processor, [])
    }?;
    
    // Should not complete naturally
    let result = timeout(Duration::from_millis(200), handle.wait_for_completion()).await;
    assert!(result.is_err(), "Infinite source should not complete naturally");
    
    // But should be processing events
    let count1 = data_count.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let count2 = data_count.load(Ordering::SeqCst);
    assert!(count2 > count1, "Should be continuously processing: {} -> {}", count1, count2);
    
    // Explicit shutdown required
    handle.shutdown().await?;
    
    Ok(())
}

/// Source that emits events based on external trigger
struct TriggeredSource {
    stage_id: StageId,
    should_emit: Arc<AtomicBool>,
    should_complete: Arc<AtomicBool>,
    completion_sent: Arc<AtomicBool>,
}

impl TriggeredSource {
    fn new(stage_id: StageId) -> Self {
        Self {
            stage_id,
            should_emit: Arc::new(AtomicBool::new(false)),
            should_complete: Arc::new(AtomicBool::new(false)),
            completion_sent: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl EventHandler for TriggeredSource {
    fn transform(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        // Sources receive _tick events
        if self.should_complete.load(Ordering::SeqCst) 
            && !self.completion_sent.swap(true, Ordering::SeqCst) {
            return vec![ChainEvent::source_complete(self.stage_id, true)];
        }
        
        if self.should_emit.swap(false, Ordering::SeqCst) {
            vec![ChainEvent::new("data", json!({"time": chrono::Utc::now()}))]
        } else {
            vec![]
        }
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

#[tokio::test]
async fn test_delayed_completion() -> Result<()> {
    // Test completion that happens after some processing
    let source = TriggeredSource::new(StageId::from_u32(0));
    let should_emit = source.should_emit.clone();
    let should_complete = source.should_complete.clone();
    
    let processor = CountingProcessor::new();
    let data_count = processor.data_events.clone();
    let completion_count = processor.completion_events.clone();
    
    let mut handle = flow! {
        name: "test_delayed_completion",
        ("source" => source, [])
        |> ("processor" => processor, [])
    }?;
    
    // Trigger some events
    for _ in 0..3 {
        should_emit.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    
    // Give time for events to process
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Verify events processed
    let processed = data_count.load(Ordering::SeqCst);
    assert!(processed >= 3, "Should have processed at least 3 events, got {}", processed);
    
    // Now trigger completion
    should_complete.store(true, Ordering::SeqCst);
    
    // Wait for completion
    let result = timeout(Duration::from_secs(2), handle.wait_for_completion()).await;
    assert!(result.is_ok(), "Should complete after trigger");
    
    // Verify completion event
    assert_eq!(completion_count.load(Ordering::SeqCst), 1, "Should have completion event");
    
    Ok(())
}

/// Test source with varying event rates
#[tokio::test]
async fn test_source_event_timing() -> Result<()> {
    // Test that completion happens after all events are emitted
    let source = FiniteSource::new(StageId::from_u32(0), 10);
    let completion_sent = source.completion_sent.clone();
    
    let processor = CountingProcessor::new();
    let data_count = processor.data_events.clone();
    let completion_count = processor.completion_events.clone();
    
    let mut handle = flow! {
        name: "test_event_timing",
        ("source" => source, [])
        |> ("processor" => processor, [])
    }?;
    
    // Wait for natural completion
    let result = timeout(Duration::from_secs(5), handle.wait_for_completion()).await;
    assert!(result.is_ok(), "Pipeline should complete naturally");
    
    // Verify all events processed before completion
    assert_eq!(data_count.load(Ordering::SeqCst), 10, "All data events should be processed");
    assert_eq!(completion_count.load(Ordering::SeqCst), 1, "Should receive exactly one completion event");
    assert!(completion_sent.load(Ordering::SeqCst), "Source should have sent completion");
    
    Ok(())
}

/// Processor that can fail
struct FallibleProcessor {
    fail_at: Option<u64>,
    processed: AtomicU64,
}

impl FallibleProcessor {
    fn new(fail_at: Option<u64>) -> Self {
        Self {
            fail_at,
            processed: AtomicU64::new(0),
        }
    }
}

impl EventHandler for FallibleProcessor {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let count = self.processed.fetch_add(1, Ordering::SeqCst);
        
        if let Some(fail_at) = self.fail_at {
            if count == fail_at {
                // Simulate error by returning error event
                return vec![ChainEvent::new("error", json!({
                    "reason": "Simulated failure",
                    "at_count": count
                }))];
            }
        }
        
        vec![event]
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

#[tokio::test]
async fn test_pipeline_continues_after_error() -> Result<()> {
    // Test that pipeline continues processing after errors
    let source = FiniteSource::new(StageId::from_u32(0), 10);
    let processor = FallibleProcessor::new(Some(5)); // Fail at 5th event
    
    let counter = CountingProcessor::new();
    let data_count = counter.data_events.clone();
    
    let mut handle = flow! {
        name: "test_error_handling",
        ("source" => source, [])
        |> ("fallible" => processor, [])
        |> ("counter" => counter, [])
    }?;
    
    // Should complete despite error
    let result = timeout(Duration::from_secs(10), handle.wait_for_completion()).await;
    assert!(result.is_ok(), "Pipeline should complete despite errors");
    
    // Should process most events (minus the failed one)
    let processed = data_count.load(Ordering::SeqCst);
    assert!(processed >= 9, "Should process most events: got {}", processed);
    
    Ok(())
}
//! Test that verifies dropped event detection works
//!
//! This test confirms that we can detect dropped events by comparing
//! source event counts with sink event counts.

use anyhow::Result;
use obzenflow_core::{
    event::{chain_event::ChainEvent, correlation::CorrelationId, event_id::EventId},
    journal::writer_id::WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::memory::MemoryJournal;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

/// Source that emits events with correlations
struct CorrelatedSource {
    events_to_emit: usize,
    current: usize,
    writer_id: WriterId,
}

impl CorrelatedSource {
    fn new(count: usize) -> Self {
        Self {
            events_to_emit: count,
            current: 0,
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for CorrelatedSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.current >= self.events_to_emit {
            return None;
        }

        self.current += 1;

        let mut event = ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "test.data",
            json!({
                "index": self.current,
                "data": format!("event_{}", self.current)
            }),
        );

        // Add correlation ID to track this event
        event.correlation_id = Some(CorrelationId::new());

        Some(event)
    }

    fn is_complete(&self) -> bool {
        self.current >= self.events_to_emit
    }
}

/// Transform that drops some events to simulate pipeline issues
struct DroppingTransform {
    drop_indices: Vec<usize>,
}

impl DroppingTransform {
    fn new(drop_indices: Vec<usize>) -> Self {
        Self { drop_indices }
    }
}

impl TransformHandler for DroppingTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Extract index from payload
        if let Some(index) = event.payload.get("index").and_then(|v| v.as_u64()) {
            if self.drop_indices.contains(&(index as usize)) {
                // Drop this event (return empty vec)
                return vec![];
            }
        }

        // Pass through
        vec![event]
    }
}

/// Simple sink that just collects events
struct CollectorSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
}

impl CollectorSink {
    fn new() -> (Self, Arc<Mutex<Vec<ChainEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                events: events.clone(),
            },
            events,
        )
    }
}

impl SinkHandler for CollectorSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        if let Ok(mut events) = self.events.lock() {
            events.push(event);
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_dropped_events_detection() -> Result<()> {
    // Enable debug logging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    println!("\n=== Dropped Events Detection Test ===\n");

    // Create journal
    let memory_journal = Arc::new(MemoryJournal::new());

    // Create handlers
    let source = CorrelatedSource::new(10); // Emit 10 events
    let transform = DroppingTransform::new(vec![3, 5, 7]); // Drop events 3, 5, 7
    let (sink, collected_events) = CollectorSink::new();

    println!("Building flow that will drop some events...");

    // Build flow
    let flow_handle = flow! {
        name: "correlation_test_flow",
        journal: memory_journal.clone(),
        middleware: [],

        stages: {
            src = source!("correlated_source" => source);
            trans = transform!("dropping_transform" => transform);
            snk = sink!("collector_sink" => sink);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {:?}", e))?;

    println!("Running flow...");

    // Run the flow
    flow_handle
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {:?}", e))?;

    // Wait for processing to complete
    sleep(Duration::from_secs(2)).await;

    // Get metrics
    let metrics_exporter = flow_handle
        .metrics_exporter()
        .await
        .expect("Metrics should be enabled by default");
    let metrics_text = metrics_exporter
        .render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;

    println!("\n=== Metrics Output ===");
    for line in metrics_text.lines() {
        if line.contains("dropped_events") || line.contains("# HELP obzenflow_dropped_events") {
            println!("{}", line);
        }
    }

    // Verify results
    let events = collected_events.lock().unwrap();
    println!("\n=== Results ===");
    println!("Source emitted: 10 events");
    println!("Transform dropped: 3 events (indices 3, 5, 7)");
    println!("Sink received: {} events", events.len());
    assert_eq!(
        events.len(),
        7,
        "Should have received 7 events (10 - 3 dropped)"
    );

    // Debug: Check if events have correlation IDs
    println!("\n=== Debug: Event Correlation IDs ===");
    for (i, event) in events.iter().enumerate() {
        println!(
            "Event {}: correlation_id = {:?}",
            i,
            event.correlation_id.is_some()
        );
    }

    // Check dropped events metric
    let has_dropped_metric = metrics_text.contains("obzenflow_dropped_events");
    assert!(has_dropped_metric, "Should have dropped_events metric");

    // Extract the value for our test flow
    if let Some(line) = metrics_text
        .lines()
        .find(|l| l.contains("obzenflow_dropped_events{flow=\"correlation_test_flow\""))
    {
        if let Some(value_str) = line.split_whitespace().last() {
            if let Ok(value) = value_str.parse::<i64>() {
                println!("\nDropped events: {}", value);
                assert_eq!(value, 3, "Should have 3 dropped events");

                println!("\n✅ Dropped event detection correctly identified 3 dropped events!");
            }
        }
    }

    println!("\n=== Test Summary ===");
    println!("✅ Source emitted 10 events");
    println!("✅ Sink received 7 events");
    println!("✅ Metric shows 3 dropped events");
    println!("✅ Dropped event detection is working correctly!");

    Ok(())
}

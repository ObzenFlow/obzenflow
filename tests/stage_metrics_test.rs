//! Test that verifies stage-level metrics work automatically
//!
//! This test confirms that FLOWIP-056-666's core requirement is met:
//! Every stage automatically emits RED metrics without configuration.

use anyhow::Result;
use obzenflow_core::{
    event::{chain_event::ChainEvent, event_id::EventId},
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

/// Simple source that emits a few test events
struct TestSource {
    events: Vec<String>,
    index: usize,
    writer_id: WriterId,
}

impl TestSource {
    fn new() -> Self {
        Self {
            events: vec![
                "event1".to_string(),
                "event2".to_string(),
                "event3".to_string(),
            ],
            index: 0,
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for TestSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.index >= self.events.len() {
            return None;
        }

        let event = ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "test.data",
            json!({
                "data": self.events[self.index].clone(),
                "index": self.index
            }),
        );

        self.index += 1;
        Some(event)
    }

    fn is_complete(&self) -> bool {
        self.index >= self.events.len()
    }
}

/// Transform that uppercases the data
struct UppercaseTransform;

impl TransformHandler for UppercaseTransform {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(data) = event.payload.get("data").and_then(|v| v.as_str()) {
            event.payload["transformed"] = json!(data.to_uppercase());
        }
        vec![event]
    }
}

/// Sink that collects events
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
async fn test_stage_level_metrics_automatic() -> Result<()> {
    println!("\n=== Stage-Level Metrics Test ===\n");

    // Create journal
    let memory_journal = Arc::new(MemoryJournal::new());

    // Create handlers
    let source = TestSource::new();
    let transform = UppercaseTransform;
    let (sink, collected_events) = CollectorSink::new();

    println!("Building flow with automatic stage metrics...");

    // Build flow - metrics are automatically enabled
    let flow_handle = flow! {
        name: "stage_metrics_test",
        journal: memory_journal.clone(),
        middleware: [],

        stages: {
            src = source!("test_source" => source);
            trans = transform!("uppercase_transform" => transform);
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

    println!("\n=== Stage Metrics Output ===");
    for line in metrics_text.lines() {
        if line.contains("stage_metrics_test") || line.contains("# HELP") || line.contains("# TYPE")
        {
            println!("{}", line);
        }
    }

    // Verify automatic RED metrics for ALL stages
    println!("\n=== Verifying Automatic RED Metrics ===");

    // Check events_total for each stage
    let has_source_events = metrics_text
        .contains(r#"obzenflow_events_total{flow="stage_metrics_test",stage="test_source"}"#);
    let has_transform_events = metrics_text.contains(
        r#"obzenflow_events_total{flow="stage_metrics_test",stage="uppercase_transform"}"#,
    );
    let has_sink_events = metrics_text
        .contains(r#"obzenflow_events_total{flow="stage_metrics_test",stage="collector_sink"}"#);

    println!(
        "Source events_total: {}",
        if has_source_events { "✓" } else { "✗" }
    );
    println!(
        "Transform events_total: {}",
        if has_transform_events { "✓" } else { "✗" }
    );
    println!(
        "Sink events_total: {}",
        if has_sink_events { "✓" } else { "✗" }
    );

    // Check duration_seconds histograms
    let has_source_duration = metrics_text.contains(
        r#"obzenflow_duration_seconds_count{flow="stage_metrics_test",stage="test_source"}"#,
    );
    let has_transform_duration = metrics_text.contains(r#"obzenflow_duration_seconds_count{flow="stage_metrics_test",stage="uppercase_transform"}"#);
    let has_sink_duration = metrics_text.contains(
        r#"obzenflow_duration_seconds_count{flow="stage_metrics_test",stage="collector_sink"}"#,
    );

    println!(
        "\nSource duration histogram: {}",
        if has_source_duration { "✓" } else { "✗" }
    );
    println!(
        "Transform duration histogram: {}",
        if has_transform_duration { "✓" } else { "✗" }
    );
    println!(
        "Sink duration histogram: {}",
        if has_sink_duration { "✓" } else { "✗" }
    );

    // Verify counts match
    let events = collected_events.lock().unwrap();
    println!("\nEvents processed: {}", events.len());
    assert_eq!(events.len(), 3, "Should have processed 3 events");

    // Core assertions - EVERY stage MUST have metrics!
    assert!(
        has_source_events,
        "Source stage MUST have events_total metric"
    );
    assert!(
        has_transform_events,
        "Transform stage MUST have events_total metric"
    );
    assert!(has_sink_events, "Sink stage MUST have events_total metric");

    assert!(
        has_source_duration,
        "Source stage MUST have duration_seconds metric"
    );
    assert!(
        has_transform_duration,
        "Transform stage MUST have duration_seconds metric"
    );
    assert!(
        has_sink_duration,
        "Sink stage MUST have duration_seconds metric"
    );

    // Check that flow context was populated correctly
    println!("\n=== Verifying Flow Context ===");
    for (i, event) in events.iter().enumerate() {
        println!(
            "Event {}: flow={}, stage={}",
            i + 1,
            event.flow_context.flow_name,
            event.flow_context.stage_name
        );
        assert_ne!(
            event.flow_context.flow_name, "unknown",
            "Flow name must be populated"
        );
        assert_ne!(
            event.flow_context.stage_name, "unknown",
            "Stage name must be populated"
        );
    }

    println!("\n✅ Stage-Level Metrics Test PASSED!");
    println!("   - All stages automatically emit RED metrics");
    println!("   - No configuration required");
    println!("   - Flow context properly populated");

    Ok(())
}

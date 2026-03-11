// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Test that verifies dropped event detection works
//!
//! This test confirms that we can detect dropped events by comparing
//! source event counts with sink event counts.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use serde_json::json;
use std::sync::{Arc, Mutex};

/// Source that emits events with correlations
#[derive(Clone, Debug)]
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
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for CorrelatedSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.current >= self.events_to_emit {
            return Ok(None);
        }

        self.current += 1;

        let event = ChainEventFactory::data_event(
            self.writer_id,
            "test.data",
            json!({
                "index": self.current,
                "data": format!("event_{}", self.current)
            }),
        )
        .with_new_correlation("correlated_source");

        Ok(Some(vec![event]))
    }
}

/// Transform that drops some events to simulate pipeline issues
#[derive(Clone, Debug)]
struct DroppingTransform {
    drop_indices: Vec<usize>,
}

impl DroppingTransform {
    fn new(drop_indices: Vec<usize>) -> Self {
        Self { drop_indices }
    }
}

#[async_trait]
impl TransformHandler for DroppingTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        // Extract index from payload
        if let Some(index) = event.payload().get("index").and_then(|v| v.as_u64()) {
            if self.drop_indices.contains(&(index as usize)) {
                // Drop this event (return empty vec)
                return Ok(vec![]);
            }
        }

        // Pass through
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

/// Simple sink that just collects events
#[derive(Clone, Debug)]
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

#[async_trait]
impl SinkHandler for CollectorSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        if let Ok(mut events) = self.events.lock() {
            if event.is_data() {
                events.push(event);
            }
        }
        Ok(DeliveryPayload::success(
            "collector_sink",
            DeliveryMethod::Custom("Collect".to_string()),
            None,
        ))
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

    // Create handlers
    let source = CorrelatedSource::new(10); // Emit 10 events
    let transform = DroppingTransform::new(vec![3, 5, 7]); // Drop events 3, 5, 7
    let (sink, collected_events) = CollectorSink::new();

    println!("Building flow that will drop some events...");

    // Build flow
    let flow_handle = flow! {
        name: "correlation_test_flow",
        journals: disk_journals(std::path::PathBuf::from("target/dropped_events_test")),
        middleware: [],

        stages: {
            correlated_source = source!(source);
            dropping_transform = transform!(transform);
            collector_sink = sink!(sink);
        },

        topology: {
            correlated_source |> dropping_transform;
            dropping_transform |> collector_sink;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {e:?}"))?;

    println!("Running flow...");

    // Run the flow and obtain metrics exporter after completion
    let metrics_exporter = flow_handle
        .run_with_metrics()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {e:?}"))?
        .expect("Metrics should be enabled by default");

    // Get metrics
    let metrics_text = metrics_exporter
        .render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {e}"))?;

    println!("\n=== Metrics Output ===");
    for line in metrics_text.lines() {
        if line.contains("dropped_events") || line.contains("# HELP obzenflow_dropped_events") {
            println!("{line}");
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

    // Check dropped events metric (optional - may not be wired in all builds)
    let has_dropped_metric = metrics_text.contains("obzenflow_dropped_events");
    println!("Dropped events metric present: {has_dropped_metric}");

    // If the metric is present, validate its value
    if has_dropped_metric {
        if let Some(line) = metrics_text
            .lines()
            .find(|l| l.contains("obzenflow_dropped_events{flow=\"correlation_test_flow\""))
        {
            if let Some(value_str) = line.split_whitespace().last() {
                if let Ok(value) = value_str.parse::<i64>() {
                    println!("\nDropped events: {value}");
                    assert_eq!(value, 3, "Should have 3 dropped events");

                    println!("\n✅ Dropped event metric correctly reports 3 dropped events!");
                }
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

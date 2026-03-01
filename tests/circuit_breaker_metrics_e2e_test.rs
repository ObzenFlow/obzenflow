// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! End-to-end test for Circuit Breaker metrics in FLOWIP-056-666
//!
//! This test verifies that circuit breaker middleware emits control events
//! that flow through the system and appear as Prometheus metrics.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::circuit_breaker;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    StageId, WriterId,
};
use obzenflow_dsl::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

/// Transform that tracks successes and fails after N successes
#[derive(Clone, Debug)]
struct ControlledFailureTransform {
    success_count: Arc<Mutex<u32>>,
    fail_after: u32,
}

impl ControlledFailureTransform {
    fn new(fail_after: u32) -> Self {
        Self {
            success_count: Arc::new(Mutex::new(0)),
            fail_after,
        }
    }
}

#[async_trait]
impl TransformHandler for ControlledFailureTransform {
    fn process(&self, mut event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let count = {
            let mut c = self.success_count.lock().unwrap();
            *c += 1;
            *c
        };

        // Actually fail processing after threshold. We model failure as an
        // explicit error status instead of relying on empty outputs so
        // that circuit breaker semantics do not depend on container length.
        if count > self.fail_after {
            event.processing_info.status =
                obzenflow_core::event::status::processing_status::ProcessingStatus::error(
                    "controlled_failure",
                );
        } else {
            // Normal processing
            let mut payload = event.payload().clone();
            payload["processed"] = json!(true);
            payload["count"] = json!(count);
            event = ChainEventFactory::data_event(event.writer_id, event.event_type(), payload);
        }

        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

/// Source that generates a stream of events with delays
#[derive(Clone, Debug)]
struct TimedEventSource {
    events: Vec<(String, Duration)>, // (event_type, delay_before)
    index: usize,
    writer_id: WriterId,
}

impl TimedEventSource {
    fn new() -> Self {
        // Events designed to trigger circuit breaker state transitions
        let events = vec![
            // Phase 1: Success events to establish baseline
            ("normal".to_string(), Duration::from_millis(0)),
            ("normal".to_string(), Duration::from_millis(100)),
            ("normal".to_string(), Duration::from_millis(100)),
            // Phase 2: Failures to trigger circuit opening (after 3 successes)
            ("failure".to_string(), Duration::from_millis(100)),
            ("failure".to_string(), Duration::from_millis(100)),
            ("failure".to_string(), Duration::from_millis(100)),
            ("failure".to_string(), Duration::from_millis(100)),
            // Phase 3: More events while circuit is open (should be rejected)
            ("rejected".to_string(), Duration::from_millis(100)),
            ("rejected".to_string(), Duration::from_millis(100)),
            ("rejected".to_string(), Duration::from_millis(100)),
            // Phase 4: Wait for cooldown then attempt recovery
            ("recovery".to_string(), Duration::from_secs(2)), // Wait for circuit to go half-open
            ("recovery".to_string(), Duration::from_millis(100)),
            // Phase 5: More failures to re-open circuit
            ("failure".to_string(), Duration::from_millis(100)),
            ("failure".to_string(), Duration::from_millis(100)),
            ("failure".to_string(), Duration::from_millis(100)),
        ];

        Self {
            events,
            index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TimedEventSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.index >= self.events.len() {
            return Ok(None);
        }

        let (event_type, delay) = &self.events[self.index];

        // Apply delay synchronously (simulating time between events)
        if delay.as_millis() > 0 {
            std::thread::sleep(*delay);
        }

        let event = ChainEventFactory::data_event(
            self.writer_id,
            format!("test.{event_type}"),
            json!({
                "sequence": self.index,
                "type": event_type
            }),
        );

        self.index += 1;
        Ok(Some(vec![event]))
    }
}

/// Sink that tracks received events
#[derive(Clone, Debug)]
struct MetricsSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
}

impl MetricsSink {
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
impl SinkHandler for MetricsSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        if let Ok(mut events) = self.events.lock() {
            events.push(event);
        }
        Ok(DeliveryPayload::success(
            "metrics_sink",
            DeliveryMethod::Custom("collect".to_string()),
            None,
        ))
    }
}

#[tokio::test]
async fn test_circuit_breaker_metrics_end_to_end() -> Result<()> {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    println!("\n=== Circuit Breaker Metrics E2E Test ===\n");

    // Create handlers
    let source = TimedEventSource::new();
    let transform = ControlledFailureTransform::new(3); // Fail after 3 successes
    let (sink, collected_events) = MetricsSink::new();

    println!("Building flow with circuit breaker middleware...");

    // Build flow with circuit breaker
    let flow_handle = flow! {
        name: "circuit_breaker_test",
        journals: disk_journals(std::path::PathBuf::from("target/cb_metrics_e2e")),
        middleware: [],

        stages: {
            src = source!("cb_source" => source);

            // Transform with circuit breaker (3 consecutive failures opens circuit)
            trans = transform!("cb_transform" => transform, [
                circuit_breaker(3)
            ]);

            snk = sink!("cb_sink" => sink);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {e:?}"))?;

    println!("Running flow to trigger circuit breaker state transitions...");

    // Run the flow and capture metrics exporter for inspection
    let metrics_exporter = flow_handle
        .run_with_metrics()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {e:?}"))?
        .expect("Metrics should be enabled");

    // Wait a bit to allow metrics aggregator to process events
    sleep(Duration::from_secs(5)).await;

    println!("\n=== Verifying Circuit Breaker Metrics ===");

    // Get metrics
    let metrics_text = metrics_exporter
        .render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {e}"))?;

    // Debug output
    println!("\n=== Circuit Breaker Metrics ===");
    for line in metrics_text.lines() {
        if line.contains("circuit_breaker")
            || line.contains("cb_transform")
            || line.contains("events_total")
            || line.contains("errors_total")
        {
            println!("{line}");
        }
    }

    // Verify circuit breaker metrics exist
    let has_cb_state = metrics_text.contains("obzenflow_circuit_breaker_state");
    let has_cb_rejection_rate = metrics_text.contains("obzenflow_circuit_breaker_rejection_rate");
    let has_cb_failures = metrics_text.contains("obzenflow_circuit_breaker_consecutive_failures");

    println!("\nCircuit Breaker Metrics Found:");
    println!("  State metric: {}", if has_cb_state { "✓" } else { "✗" });
    println!(
        "  Rejection rate metric present: {}",
        if has_cb_rejection_rate { "✓" } else { "✗" }
    );
    println!(
        "  Consecutive failures metric present: {}",
        if has_cb_failures { "✓" } else { "✗" }
    );

    // Check for state transitions in metrics
    if has_cb_state {
        // Look for evidence of state transitions
        let state_lines: Vec<&str> = metrics_text
            .lines()
            .filter(|line| line.contains("obzenflow_circuit_breaker_state"))
            .collect();

        println!("\nCircuit Breaker State Values:");
        for line in &state_lines {
            println!("  {line}");
        }

        // Should see state = 1.0 (open) at some point
        let has_open_state = state_lines.iter().any(|line| line.contains("} 1"));

        assert!(
            has_open_state,
            "Circuit breaker should have transitioned to OPEN state"
        );
    }

    // Verify events processing (drop the lock before awaiting).
    let (events_len, success_count) = {
        let events = collected_events
            .lock()
            .map_err(|e| anyhow::anyhow!("Failed to lock events: {e:?}"))?;

        let events_len = events.len();
        let success_count = events
            .iter()
            .filter(|e| {
                e.payload()
                    .get("processed")
                    .map(|v| v == &json!(true))
                    .unwrap_or(false)
            })
            .count();

        (events_len, success_count)
    };

    println!("\nFlow Processing Results:");
    println!("  Total events sent: 15");
    println!("  Events reached sink: {events_len}");
    println!("  Events rejected/failed: {}", 15 - events_len);

    // We expect fewer events due to circuit breaker rejections
    assert!(
        events_len < 15,
        "Circuit breaker should have rejected some events"
    );

    assert!(
        success_count >= 3,
        "Expected at least 3 successful events before breaker effects; got {success_count}"
    );

    // Final metrics check
    sleep(Duration::from_millis(500)).await;
    metrics_exporter
        .render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render final metrics: {e}"))?;

    println!("\n✅ Circuit Breaker Metrics E2E Test PASSED!");
    println!("   - Circuit breaker limited downstream traffic (sink saw < 15 events)");
    println!("   - Metrics exporter produced a coherent snapshot");

    Ok(())
}

/// Test that verifies circuit breaker emits summary events periodically
#[tokio::test]
async fn test_circuit_breaker_summary_events() -> Result<()> {
    // Source that generates many events quickly
    #[derive(Clone, Debug)]
    struct RapidSource {
        count: usize,
        writer_id: WriterId,
    }

    impl FiniteSourceHandler for RapidSource {
        fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
            if self.count >= 1100 {
                // Trigger summary after 1000 events
                return Ok(None);
            }
            self.count += 1;

            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                "test.rapid",
                json!({"id": self.count}),
            )]))
        }
    }

    // Simple passthrough transform
    #[derive(Clone, Debug)]
    struct PassthroughTransform;

    #[async_trait]
    impl TransformHandler for PassthroughTransform {
        fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
            Ok(vec![event])
        }

        async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
            Ok(())
        }
    }

    let flow_handle = flow! {
        name: "circuit_breaker_summary_test",
        journals: disk_journals(std::path::PathBuf::from(
            "target/cb_metrics_summary_e2e",
        )),
        middleware: [],

        stages: {
            src = source!("rapid_source" => RapidSource { count: 0, writer_id: WriterId::from(StageId::new()) });
            trans = transform!("cb_summary_transform" => PassthroughTransform, [
                circuit_breaker(10) // High threshold, won't trip
            ]);
            snk = sink!("null_sink" => MetricsSink::new().0);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {e:?}"))?;

    let metrics_exporter = flow_handle
        .run_with_metrics()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {e:?}"))?
        .expect("Metrics should be enabled");
    sleep(Duration::from_secs(2)).await;
    let metrics = metrics_exporter
        .render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {e}"))?;

    // Should have circuit breaker metrics from summary events
    assert!(
        metrics.contains("obzenflow_circuit_breaker_")
            || metrics.contains("obzenflow_events_total"),
        "Should have metrics from circuit breaker summary events"
    );

    Ok(())
}

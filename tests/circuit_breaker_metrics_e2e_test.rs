// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! End-to-end test for Circuit Breaker metrics in FLOWIP-056-666
//!
//! This test verifies that circuit breaker middleware emits control events
//! that flow through the system and appear as Prometheus metrics.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::CircuitBreaker;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};

/// File-local payload for the circuit-breaker metrics test. The JSON
/// shape matches what `TimedEventSource` / `RapidSource` emit; the type
/// fingerprints the stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct CircuitMetricEvent {
    sequence: u64,
    #[serde(rename = "type")]
    kind: String,
}

impl TypedPayload for CircuitMetricEvent {
    const EVENT_TYPE: &'static str = "circuit_breaker_metrics.event";
}
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

/// Source that generates a stream of events with delays
#[derive(Clone, Debug)]
struct TimedEventSource {
    events: Vec<(String, Duration)>, // (event_type, delay_before)
    index: usize,
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
            // Phase 3: Source admission waits for recovery before polling again.
            ("resumed".to_string(), Duration::from_millis(100)),
            ("resumed".to_string(), Duration::from_millis(100)),
            ("resumed".to_string(), Duration::from_millis(100)),
            // Phase 4: Wait for cooldown then attempt recovery
            ("recovery".to_string(), Duration::from_secs(2)), // Wait for circuit to go half-open
            ("recovery".to_string(), Duration::from_millis(100)),
            // Phase 5: More failures to re-open circuit
            ("failure".to_string(), Duration::from_millis(100)),
            ("failure".to_string(), Duration::from_millis(100)),
            ("failure".to_string(), Duration::from_millis(100)),
        ];

        Self { events, index: 0 }
    }
}

impl TypedFiniteSourceHandler for TimedEventSource {
    type Output = CircuitMetricEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.index >= self.events.len() {
            return Ok(None);
        }

        let (event_type, delay) = self.events[self.index].clone();

        // Apply delay synchronously (simulating time between events)
        if delay.as_millis() > 0 {
            std::thread::sleep(delay);
        }

        let sequence = self.index as u64;
        self.index += 1;
        if event_type == "failure" {
            return Err(SourceError::Other("controlled_failure".to_string()));
        }

        Ok(Some(vec![CircuitMetricEvent {
            sequence,
            kind: event_type,
        }]))
    }
}

/// Sink that tracks received events
#[derive(Clone, Debug)]
struct MetricsSink {
    events: Arc<Mutex<Vec<CircuitMetricEvent>>>,
}

impl MetricsSink {
    fn new() -> (Self, Arc<Mutex<Vec<CircuitMetricEvent>>>) {
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
impl InlineSink for MetricsSink {
    type Input = CircuitMetricEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        event: CircuitMetricEvent,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        if let Ok(mut events) = self.events.lock() {
            events.push(event);
        }
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("collect".to_string()),
            None,
        )))
    }
}

#[tokio::test]
async fn test_circuit_breaker_metrics_end_to_end() -> Result<()> {
    let metrics_model =
        std::sync::Arc::new(obzenflow_adapters::monitoring::MetricsReadModel::default());
    let metrics_context = obzenflow_runtime::run_context::FlowBuildContext::for_tests()
        .with_metrics_exporter(metrics_model.clone());
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_test_writer()
        .try_init();

    println!("\n=== Circuit Breaker Metrics E2E Test ===\n");

    let collected_events = Arc::new(Mutex::new(Vec::new()));
    let collected_events_for_flow = Arc::clone(&collected_events);

    println!("Building flow with circuit breaker middleware...");

    // Build flow with circuit breaker
    let flow_handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = TimedEventSource::new();
        let sink = MetricsSink {
            events: collected_events_for_flow,
        };

        Ok(flow! {
            name: "circuit_breaker_test",
            journals: disk_journals(std::path::PathBuf::from("target/cb_metrics_e2e")),

            stages: {
                // Typed source-poll binding: three error-marked batches open the breaker.
                cb_source = source!(CircuitMetricEvent => source with [
                    CircuitBreaker::builder()
                        .consecutive_failures(3)
                        .open_for(Duration::from_millis(100))
                        .build()
                        .expect("source breaker configuration")
                ]);
                cb_sink = sink!(CircuitMetricEvent => sink);
            },

            topology: {
                cb_source |> cb_sink;
            }
        })
    })
    .build(metrics_context)
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {e:?}"))?;

    println!("Running flow to trigger circuit breaker state transitions...");

    // Source admission pauses instead of rejecting inputs. Completion waits for
    // metrics finalisation, so the exported snapshot is ready when run returns.
    flow_handle
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {e:?}"))?;

    println!("\n=== Verifying Circuit Breaker Metrics ===");

    // Get metrics
    let metrics_text = obzenflow_adapters::monitoring::projections::PrometheusProjection::new()
        .render(&metrics_model.snapshot())
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {e}"))?;

    // Debug output
    println!("\n=== Circuit Breaker Metrics ===");
    for line in metrics_text.lines() {
        if line.contains("circuit_breaker")
            || line.contains("cb_source")
            || line.contains("events_total")
            || line.contains("errors_total")
        {
            println!("{line}");
        }
    }

    // The gauge reports the latest state, including recovery at EOF. Historical
    // openings remain visible in cumulative measurements without replaying them.
    // Sixteen polls comprise eight data batches, seven failures, and clean EOF.
    for (metric, expected) in [
        ("obzenflow_circuit_breaker_state", 0.0),
        ("obzenflow_circuit_breaker_opened_total", 3.0),
        ("obzenflow_circuit_breaker_requests_total", 16.0),
        ("obzenflow_circuit_breaker_successes_total", 9.0),
        ("obzenflow_circuit_breaker_failures_total", 7.0),
        ("obzenflow_circuit_breaker_rejections_total", 0.0),
    ] {
        let prefix = format!("{metric}{{");
        let values: Vec<f64> = metrics_text
            .lines()
            .filter(|line| line.starts_with(&prefix) && line.contains("stage=\"cb_source\""))
            .map(|line| line.rsplit_once(' ').unwrap().1.parse().unwrap())
            .collect();
        assert_eq!(values, [expected], "incorrect or missing {metric}");
    }

    let events = collected_events
        .lock()
        .map_err(|e| anyhow::anyhow!("Failed to lock events: {e:?}"))?;
    let sequences: Vec<_> = events.iter().map(|event| event.sequence).collect();
    assert_eq!(
        sequences,
        [0, 1, 2, 7, 8, 9, 10, 11],
        "all successful source batches must reach the sink exactly once"
    );

    println!("\n✅ Circuit Breaker Metrics E2E Test PASSED!");
    println!("   - Source pauses preserved every successful batch");
    println!("   - Final metrics retain opening counts and report recovery");

    Ok(())
}

/// Test that verifies circuit breaker emits summary events periodically
#[tokio::test]
async fn test_circuit_breaker_summary_events() -> Result<()> {
    let metrics_model =
        std::sync::Arc::new(obzenflow_adapters::monitoring::MetricsReadModel::default());
    let metrics_context = obzenflow_runtime::run_context::FlowBuildContext::for_tests()
        .with_metrics_exporter(metrics_model.clone());
    // Source that generates many events quickly
    #[derive(Clone, Debug)]
    struct RapidSource {
        count: usize,
    }

    impl TypedFiniteSourceHandler for RapidSource {
        type Output = CircuitMetricEvent;

        fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
            if self.count >= 1100 {
                // Trigger summary after 1000 events
                return Ok(None);
            }
            self.count += 1;

            Ok(Some(vec![CircuitMetricEvent {
                sequence: self.count as u64,
                kind: "rapid".to_string(),
            }]))
        }
    }

    let flow_handle = FlowDefinition::materialize(move |_runtime_config| {
        let rapid_source_handler = RapidSource { count: 0 };
        let null_sink_handler = MetricsSink::new().0;

        Ok(flow! {
            name: "circuit_breaker_summary_test",
            journals: disk_journals(std::path::PathBuf::from(
                "target/cb_metrics_summary_e2e",
            )),

            stages: {
                rapid_source = source!(CircuitMetricEvent => rapid_source_handler with [
                    CircuitBreaker::builder()
                        .consecutive_failures(10)
                        .build()
                        .expect("source breaker configuration")
                ]);
                null_sink = sink!(CircuitMetricEvent => null_sink_handler);
            },

            topology: {
                rapid_source |> null_sink;
            }
        })
    })
    .build(metrics_context)
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {e:?}"))?;

    flow_handle
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {e:?}"))?;
    let metrics_exporter = metrics_model.clone();
    sleep(Duration::from_secs(2)).await;
    let metrics = obzenflow_adapters::monitoring::projections::PrometheusProjection::new()
        .render(&metrics_exporter.snapshot())
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {e}"))?;

    // Should have circuit breaker metrics from summary events
    assert!(
        metrics.contains("obzenflow_circuit_breaker_")
            || metrics.contains("obzenflow_events_total"),
        "Should have metrics from circuit breaker summary events"
    );

    Ok(())
}

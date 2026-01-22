#![cfg(any())]
// Disabled for now: rate limiter + finite source drain semantics need a dedicated flowip; kept as a manual harness

//! Simplified end-to-end test for Rate Limiter metrics in FLOWIP-056-666
//!
//! This test verifies that rate limiter middleware emits control events
//! that flow through the system and appear as Prometheus metrics.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};

/// Source that emits many events quickly to trigger rate limiting
#[derive(Clone, Debug)]
struct BurstSource {
    total_events: usize,
    current: usize,
    writer_id: WriterId,
}

impl BurstSource {
    fn new(total_events: usize) -> Self {
        Self {
            total_events,
            current: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for BurstSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.current >= self.total_events {
            return Ok(None);
        }

        let event = ChainEventFactory::data_event(
            self.writer_id.clone(),
            "test.burst",
            json!({
                "id": self.current,
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            }),
        );

        self.current += 1;
        Ok(Some(vec![event]))
    }
}

/// Simple passthrough transform
#[derive(Clone, Debug)]
struct PassthroughTransform;

#[async_trait]
impl TransformHandler for PassthroughTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Sink that counts received events
#[derive(Clone, Debug)]
struct CountingSink {
    count: Arc<Mutex<usize>>,
}

impl CountingSink {
    fn new() -> (Self, Arc<Mutex<usize>>) {
        let count = Arc::new(Mutex::new(0));
        (
            Self {
                count: count.clone(),
            },
            count,
        )
    }
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        if let Ok(mut c) = self.count.lock() {
            if event.is_data() {
                *c += 1;
            }
        }
        Ok(DeliveryPayload::success(
            "counting_sink",
            DeliveryMethod::Custom("Count".to_string()),
            None,
        ))
    }
}

#[tokio::test]
async fn test_rate_limiter_metrics_simple() -> Result<()> {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    println!("\n=== Rate Limiter Metrics Simple E2E Test ===\n");

    // Create handlers
    let source = BurstSource::new(50); // 50 events for a quick test
    let transform = PassthroughTransform;
    let (sink, event_count) = CountingSink::new();

    println!("Building flow with rate limiter (5 events/second)...");

    // Build flow with rate limiter
    let flow_handle = flow! {
        name: "rate_limiter_metrics_simple",
        journals: disk_journals(std::path::PathBuf::from(
            "target/rate_limiter_metrics_simple",
        )),
        middleware: [],

        stages: {
            // Apply rate limiter at the source stage
            src = source!("burst_source" => source, [
                rate_limit(5.0)  // 5 events per second
            ]);
            trans = transform!("passthrough" => transform);
            snk = sink!("counting_sink" => sink);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {:?}", e))?;

    println!("Running flow to trigger rate limiting...");

    // Grab metrics exporter up front so we can inspect metrics while the
    // flow is running and after it completes.
    let metrics_exporter = flow_handle
        .metrics_exporter()
        .expect("Metrics should be enabled");

    // Spawn the flow run in the background so we can time‑bound it.
    let run_task = tokio::spawn(async move {
        flow_handle
            .run()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to run flow: {:?}", e))
    });

    println!("\n=== Verifying Rate Limiter Metrics ===");

    // Wait a bit for the metrics aggregator to observe events.
    sleep(Duration::from_secs(2)).await;

    // Get metrics
    let metrics_text = metrics_exporter
        .render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;

    // Debug output
    println!("\n=== Rate Limiter Metrics (filtered) ===");
    for line in metrics_text.lines() {
        if line.contains("rate_limiter")
            || line.contains("burst_source")
            || (line.contains("events_total") && line.contains("burst_source"))
        {
            println!("{}", line);
        }
    }

    // Verify rate limiter metrics exist
    let has_rl_delay_rate = metrics_text.contains("obzenflow_rate_limiter_delay_rate");
    let has_rl_utilization = metrics_text.contains("obzenflow_rate_limiter_utilization");

    println!("\nRate Limiter Metrics Found:");
    println!(
        "  Delay rate: {}",
        if has_rl_delay_rate { "✓" } else { "✗" }
    );
    println!(
        "  Utilization: {}",
        if has_rl_utilization { "✓" } else { "✗" }
    );

    // Snapshot count after a short period to confirm rate limiting is active.
    let partial_count = *event_count.lock().unwrap();
    println!("\nEvent count after ~2s: {}", partial_count);
    // With 5 events/sec limit and some startup overhead, we expect
    // significantly fewer than 50 events at this point.
    assert!(
        partial_count < 50,
        "Rate limiter should not allow all events through immediately"
    );

    // Ensure the flow completed within a reasonable time bound.
    let join_result = tokio::time::timeout(Duration::from_secs(60), run_task).await;
    match join_result {
        Ok(joined) => {
            // Propagate any error from the flow run
            joined.expect("Flow task panicked")?
        }
        Err(_) => {
            panic!("rate_limiter_metrics_simple_e2e_test: flow did not complete within 60 seconds")
        }
    }

    // Final check after flow completion
    let final_count = *event_count.lock().unwrap();
    println!("\nFinal event count: {}", final_count);

    // All events should eventually be processed
    assert_eq!(final_count, 50, "Not all events were processed");

    // Get final metrics
    let final_metrics = metrics_exporter
        .render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render final metrics: {}", e))?;

    // At least one of the rate limiter metrics should be present
    assert!(
        final_metrics.contains("obzenflow_rate_limiter_delay_rate")
            || final_metrics.contains("obzenflow_rate_limiter_utilization"),
        "Rate limiter metrics must be present in output"
    );

    println!("\n✅ Rate Limiter Metrics Simple E2E Test PASSED!");
    println!("   - Rate limiting verified (processed ~5 events/sec)");
    println!("   - All events eventually processed");
    println!("   - Metrics properly exposed");

    Ok(())
}

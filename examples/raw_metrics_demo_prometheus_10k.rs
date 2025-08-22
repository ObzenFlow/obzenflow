//! Simple demo showing raw Prometheus metrics output
//! Run with: cargo run -p obzenflow --example raw_metrics_demo

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use serde_json::json;

/// Simple source that generates 100 events
#[derive(Clone, Debug)]
struct TestSource {
    count: usize,
    writer_id: WriterId,
}

impl TestSource {
    fn new() -> Self {
        Self {
            count: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TestSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= 10_000 {
            return None;
        }

        self.count += 1;

        // Every 10th event will fail
        let should_fail = self.count % 10 == 0;

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "test.request",
            json!({
                "id": self.count,
                "should_fail": should_fail,
            }),
        ))
    }

    fn is_complete(&self) -> bool {
        self.count >= 10_000
    }
}

/// Transform that fails on certain events
#[derive(Clone, Debug)]
struct TestTransform;

#[async_trait]
impl TransformHandler for TestTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Add some processing time
        let start = std::time::Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(5));
        let elapsed = start.elapsed();

        let payload = event.payload();
        if payload["should_fail"].as_bool().unwrap_or(false) {
            // Return error event
            let mut error_payload = payload.clone();
            error_payload["_debug_sleep_ms"] = serde_json::json!(elapsed.as_millis());
            vec![ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                "error",
                error_payload,
            )]
        } else {
            let mut result_payload = payload.clone();
            result_payload["_debug_sleep_ms"] = serde_json::json!(elapsed.as_millis());
            vec![ChainEventFactory::derived_data_event(
                event.writer_id.clone(),
                &event,
                event.event_type(),
                result_payload,
            )]
        }
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Simple sink that counts events
#[derive(Clone, Debug)]
struct TestSink {
    count: usize,
}

impl TestSink {
    fn new() -> Self {
        Self { count: 0 }
    }
}

#[async_trait]
impl SinkHandler for TestSink {
    async fn consume(&mut self, _event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        self.count += 1;
        
        Ok(DeliveryPayload::success(
            "test_sink",
            DeliveryMethod::Custom("InMemory".to_string()),
            Some(1), // One event processed
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment to use prometheus exporter
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "prometheus");
    
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=info,raw_metrics_demo_prometheus_10k=info")
        .with_target(true)
        .with_thread_ids(true)
        .init();

    // Create journal path for disk journals
    let journal_path = std::path::PathBuf::from("target/raw_metrics_prometheus_demo_journal");

    println!("▶️  Running flow...\n");

    // Create flow with middleware
    let flow_handle = flow! {
        name: "raw_metrics_demo",
        journals: disk_journals(journal_path.clone()),
        middleware: [],  // No global middleware

        stages: {
            // Source
            src = source!("event_source" => TestSource::new());

            // Transform with middleware
            trans = transform!("processor" => TestTransform, []);

            // Sink
            snk = sink!("event_sink" => TestSink::new());
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))?;

    println!("▶️  Running flow...\n");

    // Run the flow to completion and get the metrics exporter
    let metrics_exporter = flow_handle.run_with_metrics().await?;

    // Get the Prometheus metrics output
    if let Some(exporter) = metrics_exporter {
        let prometheus_output = exporter
            .render_metrics()
            .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
        println!("{}", prometheus_output);
    } else {
        println!("No metrics exporter configured");
    }

    Ok(())
}

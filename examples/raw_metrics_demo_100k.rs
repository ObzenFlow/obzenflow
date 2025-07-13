//! Simple demo showing raw Prometheus metrics output
//! Run with: cargo run -p obzenflow --example raw_metrics_demo

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::{circuit_breaker, rate_limit};
use obzenflow_core::{
    event::{chain_event::ChainEvent, event_id::EventId},
    journal::writer_id::WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime_services::supervised_base::SupervisorHandle;
use serde_json::json;
use tokio::time::Duration;

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
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for TestSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= 100_000 {
            return None;
        }

        self.count += 1;

        // Every 10th event will fail
        let should_fail = self.count % 10 == 0;

        Some(ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "test.request",
            json!({
                "id": self.count,
                "should_fail": should_fail,
            }),
        ))
    }

    fn is_complete(&self) -> bool {
        self.count >= 100_000
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
        std::thread::sleep(Duration::from_millis(5));
        let elapsed = start.elapsed();

        if event.payload["should_fail"].as_bool().unwrap_or(false) {
            // Return error event
            let mut error = event.clone();
            error.event_type = "error".to_string();
            error.payload["_debug_sleep_ms"] = serde_json::json!(elapsed.as_millis());
            vec![error]
        } else {
            let mut result = event;
            result.payload["_debug_sleep_ms"] = serde_json::json!(elapsed.as_millis());
            vec![result]
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

impl SinkHandler for TestSink {
    fn consume(&mut self, _event: ChainEvent) -> obzenflow_core::Result<()> {
        self.count += 1;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,raw_metrics_demo=debug")
        .init();

    println!("🚀 Raw Metrics Demo - Showing Prometheus Output");
    println!("==============================================\n");

    // Create journal path for disk journals
    let journal_path = std::path::PathBuf::from("target/raw_metrics_demo_journal");
    println!("📁 Using DiskJournal at: {}", journal_path.display());

    println!("📊 Creating flow with automatic metrics...\n");

    // Create flow with middleware
    let flow_handle = flow! {
        name: "raw_metrics_demo",
        journals: disk_journals(journal_path.clone()),
        middleware: [],  // No global middleware

        stages: {
            // Source
            src = source!("event_source" => TestSource::new());

            // Transform with middleware
            trans = transform!("processor" => TestTransform, [
                rate_limit(1.0)  // 1 event per second
            ]);

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

    println!("\n✅ Flow completed!");

    // Get the metrics text after completion
    let metrics_text = if let Some(exporter) = metrics_exporter {
        exporter
            .render_metrics()
            .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?
    } else {
        "No metrics exporter configured".to_string()
    };

    println!("{}", "=".repeat(80));
    println!("RAW PROMETHEUS METRICS OUTPUT (This is what Grafana scrapes):");
    println!("{}", "=".repeat(80));
    println!();
    println!("{}", metrics_text);
    println!();
    println!("{}", "=".repeat(80));

    // Parse and display summary
    println!("\n📊 Summary:");

    // Count total events
    let source_events: u64 = metrics_text
        .lines()
        .filter(|l| l.starts_with("obzenflow_events_total") && l.contains("event_source"))
        .filter_map(|l| l.split_whitespace().last())
        .filter_map(|v| v.parse::<u64>().ok())
        .sum();

    let sink_events: u64 = metrics_text
        .lines()
        .filter(|l| l.starts_with("obzenflow_events_total") && l.contains("event_sink"))
        .filter_map(|l| l.split_whitespace().last())
        .filter_map(|v| v.parse::<u64>().ok())
        .sum();

    println!("  Source generated: {} events", source_events);
    println!("  Sink consumed: {} events", sink_events);

    // Check dropped events
    if let Some(dropped_line) = metrics_text
        .lines()
        .find(|l| l.starts_with("obzenflow_dropped_events"))
    {
        if let Some(count) = dropped_line.split_whitespace().last() {
            println!("  Dropped events: {}", count);
        }
    }

    // Check circuit breaker state
    if let Some(cb_line) = metrics_text
        .lines()
        .find(|l| l.contains("obzenflow_circuit_breaker_state"))
    {
        if cb_line.ends_with("1") {
            println!("  Circuit breaker: OPEN ⚠️");
        } else if cb_line.ends_with("0.5") {
            println!("  Circuit breaker: HALF-OPEN ⚡");
        } else {
            println!("  Circuit breaker: CLOSED ✅");
        }
    }

    println!("\n💡 Key Metrics Explained:");
    println!("  - events_total: Total events processed by each stage");
    println!("  - duration_seconds: Processing time histograms");
    println!("  - dropped_events: Events lost in the pipeline");
    println!("  - circuit_breaker_state: 0=closed, 1=open, 0.5=half-open");
    println!("  - rate_limiter_*: Rate limiting metrics");

    Ok(())
}

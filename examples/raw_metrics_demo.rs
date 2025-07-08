//! Simple demo showing raw Prometheus metrics output
//! Run with: cargo run -p obzenflow --example raw_metrics_demo

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_adapters::middleware::{circuit_breaker, rate_limit};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_core::{
    event::{chain_event::ChainEvent, event_id::EventId},
    journal::writer_id::WriterId,
};
use serde_json::json;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use anyhow::Result;

/// Simple source that generates 100 events
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
        if self.count >= 100 {
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
        self.count >= 100
    }
}

/// Transform that fails on certain events
struct TestTransform;

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
}

/// Simple sink that counts events
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

    // Create journal - using DiskJournal to inspect events
    let journal_path = std::path::PathBuf::from("target/raw_metrics_demo_journal");
    let flow_id = "raw_metrics_demo";
    println!("📁 Using DiskJournal at: {}/{}.log", journal_path.display(), flow_id);
    let journal = Arc::new(DiskJournal::new(journal_path, flow_id).await
        .map_err(|e| anyhow::anyhow!("Failed to create disk journal: {:?}", e))?);

    println!("📊 Creating flow with automatic metrics...\n");

    // Create flow with middleware
    let flow_handle = flow! {
        name: "raw_metrics_demo",
        journal: journal,
        middleware: [],  // No global middleware
        
        stages: {
            // Source
            src = source!("event_source" => TestSource::new());
            
            // Transform with middleware
            trans = transform!("processor" => TestTransform, [
                rate_limit(50.0),  // 50 events per second
                circuit_breaker(3) // Opens after 3 failures
            ]);
            
            // Sink
            snk = sink!("event_sink" => TestSink::new());
        },
        
        topology: {
            src |> trans;
            trans |> snk;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    println!("▶️  Running flow...\n");
    
    // Run the flow
    flow_handle.run().await?;
    
    // Wait for metrics to settle
    println!("⏳ Waiting for metrics collection cycle (20 seconds)...");
    sleep(Duration::from_secs(20)).await;
    
    println!("\n📈 Flow completed! Getting metrics...\n");
    
    // Get metrics
    match flow_handle.render_metrics().await {
        Ok(metrics_text) => {
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
                    let source_events: u64 = metrics_text.lines()
                        .filter(|l| l.starts_with("obzenflow_events_total") && l.contains("event_source"))
                        .filter_map(|l| l.split_whitespace().last())
                        .filter_map(|v| v.parse::<u64>().ok())
                        .sum();
                        
                    let sink_events: u64 = metrics_text.lines()
                        .filter(|l| l.starts_with("obzenflow_events_total") && l.contains("event_sink"))
                        .filter_map(|l| l.split_whitespace().last())
                        .filter_map(|v| v.parse::<u64>().ok())
                        .sum();
                        
                    println!("  Source generated: {} events", source_events);
                    println!("  Sink consumed: {} events", sink_events);
                    
                    // Check dropped events
                    if let Some(dropped_line) = metrics_text.lines()
                        .find(|l| l.starts_with("obzenflow_dropped_events")) {
                        if let Some(count) = dropped_line.split_whitespace().last() {
                            println!("  Dropped events: {}", count);
                        }
                    }
                    
                    // Check circuit breaker state
                    if let Some(cb_line) = metrics_text.lines()
                        .find(|l| l.contains("obzenflow_circuit_breaker_state")) {
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
        }
        Err(e) => {
            eprintln!("❌ Failed to render metrics: {}", e);
        }
    }
    
    Ok(())
}
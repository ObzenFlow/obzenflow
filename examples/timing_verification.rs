//! Verify that timing middleware correctly captures processing duration
//! Run with: cargo run -p obzenflow --example timing_verification

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_core::{
    event::{chain_event::ChainEvent, event_id::EventId},
    journal::writer_id::WriterId,
};
use serde_json::json;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use anyhow::Result;

/// Simple source that generates just 5 events
struct SimpleSource {
    count: usize,
    writer_id: WriterId,
}

impl SimpleSource {
    fn new() -> Self {
        Self {
            count: 0,
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for SimpleSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= 5 {
            return None;
        }
        
        self.count += 1;
        
        Some(ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "test.event",
            json!({
                "id": self.count,
            }),
        ))
    }
    
    fn is_complete(&self) -> bool {
        self.count >= 5
    }
}

/// Transform that always sleeps 10ms
struct SleepTransform;

impl TransformHandler for SleepTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Sleep for a consistent 10ms
        std::thread::sleep(Duration::from_millis(10));
        vec![event]
    }
}

/// Simple sink
struct SimpleSink {
    count: usize,
}

impl SimpleSink {
    fn new() -> Self {
        Self { count: 0 }
    }
}

impl SinkHandler for SimpleSink {
    fn consume(&mut self, _event: ChainEvent) -> obzenflow_core::Result<()> {
        self.count += 1;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging with debug for timing
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow_adapters::middleware::timing=debug,obzenflow_runtime_services::pipeline::supervisor=info,obzenflow_adapters::monitoring::aggregator=info,timing_verification=info")
        .init();

    println!("🔍 Timing Verification Test");
    println!("==========================\n");

    // Create journal
    let journal_path = std::path::PathBuf::from("target/timing_verification_journal");
    let flow_id = "timing_test";
    let journal = Arc::new(DiskJournal::new(journal_path, flow_id).await
        .map_err(|e| anyhow::anyhow!("Failed to create disk journal: {:?}", e))?);

    println!("Creating flow with NO middleware (just system middleware)...\n");

    // Create flow without any user middleware
    let flow_handle = flow! {
        name: "timing_test",
        journal: journal,
        middleware: [],
        
        stages: {
            src = source!("source" => SimpleSource::new());
            trans = transform!("processor" => SleepTransform);
            snk = sink!("sink" => SimpleSink::new());
        },
        
        topology: {
            src |> trans;
            trans |> snk;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    println!("Running flow (5 events, each sleeps 10ms)...\n");
    
    // Run the flow
    flow_handle.run().await?;
    
    // Wait for metrics - aggregator pushes snapshots every 10 seconds
    println!("Waiting for metrics collection cycle (20 seconds)...");
    sleep(Duration::from_secs(20)).await;
    
    // Get metrics
    match flow_handle.render_metrics().await {
        Ok(metrics_text) => {
            // Print ALL metrics to see what we have
            println!("\n=== ALL METRICS ===");
            println!("{}", metrics_text);
            println!("=== END METRICS ===\n");
            
            // Extract just the processing time metrics
            for line in metrics_text.lines() {
                if line.contains("processing_time_seconds_sum") || 
                   line.contains("processing_time_seconds_count") {
                    println!("{}", line);
                }
            }
            
            // Calculate average
            let sum_line = metrics_text.lines()
                .find(|l| l.contains("processing_time_seconds_sum") && l.contains("processor"))
                .unwrap_or("");
            let count_line = metrics_text.lines()
                .find(|l| l.contains("processing_time_seconds_count") && l.contains("processor"))
                .unwrap_or("");
            
            if let (Some(sum_str), Some(count_str)) = (
                sum_line.split_whitespace().last(),
                count_line.split_whitespace().last()
            ) {
                if let (Ok(sum), Ok(count)) = (
                    sum_str.parse::<f64>(),
                    count_str.parse::<f64>()
                ) {
                    let avg_ms = (sum / count) * 1000.0;
                    println!("\n📊 Results:");
                    println!("  Total processing time: {:.3}s", sum);
                    println!("  Events processed: {}", count);
                    println!("  Average per event: {:.1}ms", avg_ms);
                    println!("  Expected: ~10ms per event");
                    
                    if avg_ms >= 9.0 && avg_ms <= 11.0 {
                        println!("\n✅ SUCCESS: Timing is accurate!");
                    } else {
                        println!("\n❌ FAIL: Timing is off (expected ~10ms, got {:.1}ms)", avg_ms);
                    }
                }
            }
        }
        Err(e) => eprintln!("Failed to render metrics: {}", e),
    }
    
    Ok(())
}
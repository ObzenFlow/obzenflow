//! Test error metrics by creating a stage that fails
//! Run with: cargo run -p obzenflow --example error_metrics_test

use obzenflow_dsl_infra::flow;
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
use anyhow::Result;

/// Source that generates 10 events
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
        if self.count >= 10 {
            return None;
        }
        
        self.count += 1;
        
        Some(ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "test.event",
            json!({
                "id": self.count,
                "should_fail": self.count % 3 == 0, // Every 3rd event should fail
            }),
        ))
    }
    
    fn is_complete(&self) -> bool {
        self.count >= 10
    }
}

/// Transform that actually fails with real errors
struct FailingTransform;

impl TransformHandler for FailingTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Get the event ID
        let id = event.payload.get("id").and_then(|v| v.as_u64()).unwrap_or(0);
        
        // Fail every 3rd event with a divide by zero
        if id % 3 == 0 {
            let _boom = 1 / (id % 3); // This will panic with divide by zero
        }
        
        // Normal processing - double the ID
        let mut result = event.clone();
        result.payload["doubled"] = json!(id * 2);
        vec![result]
    }
}

/// Simple sink that counts events
struct CountingSink {
    success_count: usize,
    error_count: usize,
}

impl CountingSink {
    fn new() -> Self {
        Self { 
            success_count: 0,
            error_count: 0,
        }
    }
}

impl SinkHandler for CountingSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        match &event.processing_info.status {
            obzenflow_core::event::processing_outcome::ProcessingOutcome::Error(_) => {
                self.error_count += 1;
                println!("Sink received error event: {:?}", event.processing_info.status);
            }
            _ => {
                self.success_count += 1;
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow_adapters::monitoring::aggregator=info,error_metrics_test=info")
        .init();

    println!("🔍 Error Metrics Test");
    println!("=====================\n");

    // Create journal
    let journal_path = std::path::PathBuf::from("target/error_metrics_journal");
    let flow_id = "error_test";
    let journal = Arc::new(DiskJournal::new(journal_path, flow_id).await
        .map_err(|e| anyhow::anyhow!("Failed to create disk journal: {:?}", e))?);

    println!("Creating flow that fails every 3rd event...\n");

    // Import the macros
    use obzenflow_dsl_infra::{source, transform, sink};
    
    // Create flow (metrics infrastructure created internally by DSL)
    let flow_handle = flow! {
        name: "error_test",
        journal: journal,
        middleware: [],
        
        stages: {
            src = source!("source" => TestSource::new());
            trans = transform!("processor" => FailingTransform);
            snk = sink!("sink" => CountingSink::new());
        },
        
        topology: {
            src |> trans;
            trans |> snk;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    println!("Running flow (10 events, 3 should fail)...\n");
    
    // Run the flow
    flow_handle.run().await?;
    
    // Wait for metrics
    println!("Waiting for metrics collection...");
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    
    // Get metrics
    match flow_handle.render_metrics().await {
        Ok(metrics_text) => {
            println!("\n=== METRICS OUTPUT ===");
            
            // Print relevant metrics
            for line in metrics_text.lines() {
                if line.contains("events_total") || 
                   line.contains("errors_total") ||
                   line.contains("processing_time_seconds_count") {
                    println!("{}", line);
                }
            }
            
            println!("\n=== ANALYSIS ===");
            
            // Parse error counts
            let error_lines: Vec<_> = metrics_text.lines()
                .filter(|l| l.contains("errors_total"))
                .collect();
                
            if error_lines.is_empty() {
                println!("❌ FAIL: No error metrics found!");
            } else {
                println!("✅ SUCCESS: Error metrics found:");
                for line in &error_lines {
                    println!("  {}", line);
                }
                
                // Check if processor has errors
                if error_lines.iter().any(|l| l.contains("processor") && l.ends_with(" 3")) {
                    println!("✅ Processor correctly shows 3 errors");
                } else {
                    println!("❌ Processor error count is incorrect");
                }
            }
        }
        Err(e) => eprintln!("Failed to render metrics: {}", e),
    }
    
    Ok(())
}
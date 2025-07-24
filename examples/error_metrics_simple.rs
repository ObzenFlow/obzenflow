//! Simple test to see if error events are counted
//! Run with: cargo run -p obzenflow --example error_metrics_simple

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

/// Source that generates both normal and error events
struct MixedSource {
    count: usize,
    writer_id: WriterId,
}

impl MixedSource {
    fn new() -> Self {
        Self {
            count: 0,
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for MixedSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= 10 {
            return None;
        }
        
        self.count += 1;
        
        // Every 3rd event is an error event
        let event_type = if self.count % 3 == 0 {
            "error.source.bad_data"
        } else {
            "data.source.normal"
        };
        
        Some(ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            event_type,
            json!({
                "id": self.count,
                "error": if self.count % 3 == 0 { 
                    Some(format!("Bad data in event {}", self.count))
                } else { 
                    None
                },
            }),
        ))
    }
    
    fn is_complete(&self) -> bool {
        self.count >= 10
    }
}

/// Pass-through transform
struct PassThroughTransform;

impl TransformHandler for PassThroughTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Just pass through - let's see if error events are counted
        vec![event]
    }
}

/// Sink that logs what it receives
struct LoggingSink {
    total_count: usize,
}

impl LoggingSink {
    fn new() -> Self {
        Self { total_count: 0 }
    }
}

impl SinkHandler for LoggingSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        self.total_count += 1;
        println!("Sink received event #{}: type={}, outcome={:?}", 
            self.total_count, event.event_type, event.processing_info.status);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow_adapters::monitoring::aggregator=info,error_metrics_simple=info")
        .init();

    println!("🔍 Simple Error Metrics Test");
    println!("===========================\n");

    // Create journal
    let journal_path = std::path::PathBuf::from("target/error_metrics_simple_journal");
    let flow_id = "error_simple";
    let journal = Arc::new(DiskJournal::new(journal_path, flow_id).await
        .map_err(|e| anyhow::anyhow!("Failed to create disk journal: {:?}", e))?);

    println!("Creating flow with error events from source...\n");

    // Import the macros
    use obzenflow_dsl_infra::{source, transform, sink};
    
    // Create flow
    let flow_handle = flow! {
        name: "error_simple",
        journal: journal,
        middleware: [],
        
        stages: {
            src = source!("source" => MixedSource::new());
            trans = transform!("processor" => PassThroughTransform);
            snk = sink!("sink" => LoggingSink::new());
        },
        
        topology: {
            src |> trans;
            trans |> snk;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    println!("Running flow (10 events, 3 should be errors)...\n");
    
    // Run the flow
    flow_handle.run().await?;
    
    // Wait for metrics
    println!("\nWaiting for metrics collection...");
    tokio::time::sleep(tokio::time::Duration::from_secs(15)).await;
    
    // Get metrics
    match flow_handle.render_metrics().await {
        Ok(metrics_text) => {
            println!("\n=== FULL METRICS ===");
            println!("{}", metrics_text);
            println!("=== END METRICS ===\n");
            
            // Check for errors_total
            if metrics_text.contains("errors_total") {
                println!("✅ SUCCESS: Error metrics found!");
                
                // Count actual errors
                for line in metrics_text.lines() {
                    if line.contains("errors_total") && !line.starts_with("#") {
                        println!("  {}", line);
                    }
                }
            } else {
                println!("❌ FAIL: No error metrics found!");
                println!("\nDEBUG: OutcomeEnrichmentMiddleware should detect event types containing 'error'");
            }
        }
        Err(e) => eprintln!("Failed to render metrics: {}", e),
    }
    
    Ok(())
}
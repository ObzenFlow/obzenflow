//! Simplified end-to-end test for Rate Limiter metrics in FLOWIP-056-666
//! 
//! This test verifies that rate limiter middleware emits control events
//! that flow through the system and appear as Prometheus metrics.

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::{
    stages::common::handlers::{FiniteSourceHandler, TransformHandler, SinkHandler},
};
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_infra::journal::memory::MemoryJournal;
use obzenflow_core::{
    event::{chain_event::ChainEvent, event_id::EventId},
    journal::writer_id::WriterId,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use anyhow::Result;

/// Source that emits many events quickly to trigger rate limiting
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
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for BurstSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.current >= self.total_events {
            return None;
        }
        
        let event = ChainEvent::new(
            EventId::new(),
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
        Some(event)
    }
    
    fn is_complete(&self) -> bool {
        self.current >= self.total_events
    }
}

/// Simple passthrough transform
struct PassthroughTransform;

impl TransformHandler for PassthroughTransform {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        event.payload["transformed"] = json!(true);
        vec![event]
    }
}

/// Sink that counts received events
struct CountingSink {
    count: Arc<Mutex<usize>>,
}

impl CountingSink {
    fn new() -> (Self, Arc<Mutex<usize>>) {
        let count = Arc::new(Mutex::new(0));
        (Self { count: count.clone() }, count)
    }
}

impl SinkHandler for CountingSink {
    fn consume(&mut self, _event: ChainEvent) -> obzenflow_core::Result<()> {
        if let Ok(mut c) = self.count.lock() {
            *c += 1;
        }
        Ok(())
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

    // Create journal
    let memory_journal = Arc::new(MemoryJournal::new());
    
    // Create handlers
    let source = BurstSource::new(100); // 100 events
    let transform = PassthroughTransform;
    let (sink, event_count) = CountingSink::new();

    println!("Building flow with rate limiter (5 events/second)...");

    // Build flow with rate limiter
    let flow_handle = flow! {
        journal: memory_journal.clone(),
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
    }.await.map_err(|e| anyhow::anyhow!("Flow creation failed: {:?}", e))?;

    println!("Running flow to trigger rate limiting...");
    let start = std::time::Instant::now();

    // Run the flow
    flow_handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {:?}", e))?;

    // Wait for processing - with 100 events at 5/sec, should take ~20 seconds
    // But let's wait less to see rate limiting in action
    sleep(Duration::from_secs(5)).await;
    
    let elapsed = start.elapsed();
    println!("\nProcessed for {:?}", elapsed);
    
    // Check how many events were processed
    let processed = *event_count.lock().unwrap();
    println!("Events processed in 5 seconds: {}", processed);
    
    // With 5 events/sec limit, we expect ~25 events in 5 seconds
    assert!(processed <= 30, "Too many events processed - rate limiter not working");
    assert!(processed >= 20, "Too few events processed - rate limiter too restrictive");
    
    // Get metrics exporter
    let metrics_exporter = flow_handle.metrics_exporter().await
        .expect("Metrics should be enabled");
    
    println!("\n=== Verifying Rate Limiter Metrics ===");
    
    // Get metrics
    let metrics_text = metrics_exporter.render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
    
    // Debug output
    println!("\n=== Rate Limiter Metrics (filtered) ===");
    for line in metrics_text.lines() {
        if line.contains("rate_limiter") || 
           line.contains("burst_source") ||
           (line.contains("events_total") && line.contains("burst_source")) {
            println!("{}", line);
        }
    }
    
    // Verify rate limiter metrics exist
    let has_rl_delay_rate = metrics_text.contains("obzenflow_rate_limiter_delay_rate");
    let has_rl_utilization = metrics_text.contains("obzenflow_rate_limiter_utilization");
    
    println!("\nRate Limiter Metrics Found:");
    println!("  Delay rate: {}", if has_rl_delay_rate { "✓" } else { "✗" });
    println!("  Utilization: {}", if has_rl_utilization { "✓" } else { "✗" });
    
    // Wait for flow to complete
    sleep(Duration::from_secs(20)).await;
    
    // Stop the flow
    flow_handle.shutdown().await?;
    
    // Final check
    let final_count = *event_count.lock().unwrap();
    println!("\nFinal event count: {}", final_count);
    
    // All 100 events should eventually be processed
    assert_eq!(final_count, 100, "Not all events were processed");
    
    // Get final metrics
    let final_metrics = metrics_exporter.render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render final metrics: {}", e))?;
    
    // At least one of the rate limiter metrics should be present
    assert!(
        final_metrics.contains("obzenflow_rate_limiter_delay_rate") ||
        final_metrics.contains("obzenflow_rate_limiter_utilization"),
        "Rate limiter metrics must be present in output"
    );
    
    println!("\n✅ Rate Limiter Metrics Simple E2E Test PASSED!");
    println!("   - Rate limiting verified (processed ~5 events/sec)");
    println!("   - All events eventually processed");
    println!("   - Metrics properly exposed");
    
    Ok(())
}
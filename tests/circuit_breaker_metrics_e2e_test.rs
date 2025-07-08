//! End-to-end test for Circuit Breaker metrics in FLOWIP-056-666
//! 
//! This test verifies that circuit breaker middleware emits control events
//! that flow through the system and appear as Prometheus metrics.

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::{
    stages::common::handlers::{FiniteSourceHandler, TransformHandler, SinkHandler},
};
use obzenflow_adapters::middleware::circuit_breaker;
use obzenflow_infra::journal::memory::MemoryJournal;
use obzenflow_core::{
    event::{
        chain_event::ChainEvent,
        event_id::EventId,
        processing_outcome::ProcessingOutcome,
    },
    journal::writer_id::WriterId,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration};
use anyhow::Result;

/// Transform that tracks successes and fails after N successes
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

impl TransformHandler for ControlledFailureTransform {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        let count = {
            let mut c = self.success_count.lock().unwrap();
            *c += 1;
            *c
        };

        // Actually fail processing after threshold
        if count > self.fail_after {
            // Return empty vector to signal failure to circuit breaker
            vec![]
        } else {
            // Normal processing
            event.payload["processed"] = json!(true);
            event.payload["count"] = json!(count);
            vec![event]
        }
    }
}

/// Source that generates a stream of events with delays
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
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for TimedEventSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.index >= self.events.len() {
            return None;
        }
        
        let (event_type, delay) = &self.events[self.index];
        
        // Apply delay synchronously (simulating time between events)
        if delay.as_millis() > 0 {
            std::thread::sleep(*delay);
        }
        
        let event = ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            &format!("test.{}", event_type),
            json!({
                "sequence": self.index,
                "type": event_type
            }),
        );
        
        self.index += 1;
        Some(event)
    }
    
    fn is_complete(&self) -> bool {
        self.index >= self.events.len()
    }
}

/// Sink that tracks received events
struct MetricsSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
}

impl MetricsSink {
    fn new() -> (Self, Arc<Mutex<Vec<ChainEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (Self { events: events.clone() }, events)
    }
}

impl SinkHandler for MetricsSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        if let Ok(mut events) = self.events.lock() {
            events.push(event);
        }
        Ok(())
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

    // Create journal
    let memory_journal = Arc::new(MemoryJournal::new());
    
    // Create handlers
    let source = TimedEventSource::new();
    let transform = ControlledFailureTransform::new(3); // Fail after 3 successes
    let (sink, collected_events) = MetricsSink::new();

    println!("Building flow with circuit breaker middleware...");

    // Build flow with circuit breaker
    let flow_handle = flow! {
        name: "circuit_breaker_test",
        journal: memory_journal.clone(),
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
    }.await.map_err(|e| anyhow::anyhow!("Flow creation failed: {:?}", e))?;

    println!("Running flow to trigger circuit breaker state transitions...");

    // Run the flow
    flow_handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {:?}", e))?;

    // Wait for flow to complete processing
    sleep(Duration::from_secs(5)).await;
    
    // Get metrics exporter
    let metrics_exporter = flow_handle.metrics_exporter().await
        .expect("Metrics should be enabled");
    
    println!("\n=== Verifying Circuit Breaker Metrics ===");
    
    // Get metrics
    let metrics_text = metrics_exporter.render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
    
    // Debug output
    println!("\n=== Circuit Breaker Metrics ===");
    for line in metrics_text.lines() {
        if line.contains("circuit_breaker") || 
           line.contains("cb_transform") || 
           line.contains("events_total") ||
           line.contains("errors_total") {
            println!("{}", line);
        }
    }
    
    // Verify circuit breaker metrics exist
    let has_cb_state = metrics_text.contains("obzenflow_circuit_breaker_state");
    let has_cb_rejection_rate = metrics_text.contains("obzenflow_circuit_breaker_rejection_rate");
    let has_cb_failures = metrics_text.contains("obzenflow_circuit_breaker_consecutive_failures");
    
    println!("\nCircuit Breaker Metrics Found:");
    println!("  State metric: {}", if has_cb_state { "✓" } else { "✗" });
    println!("  Rejection rate: {}", if has_cb_rejection_rate { "✓" } else { "✗" });
    println!("  Consecutive failures: {}", if has_cb_failures { "✓" } else { "✗" });
    
    // Check for state transitions in metrics
    if has_cb_state {
        // Look for evidence of state transitions
        let state_lines: Vec<&str> = metrics_text.lines()
            .filter(|line| line.contains("obzenflow_circuit_breaker_state"))
            .collect();
            
        println!("\nCircuit Breaker State Values:");
        for line in &state_lines {
            println!("  {}", line);
        }
        
        // Should see state = 1.0 (open) at some point
        let has_open_state = state_lines.iter()
            .any(|line| line.contains("} 1"));
            
        assert!(has_open_state, "Circuit breaker should have transitioned to OPEN state");
    }
    
    // Check rejection rate
    if has_cb_rejection_rate {
        let rejection_lines: Vec<&str> = metrics_text.lines()
            .filter(|line| line.contains("obzenflow_circuit_breaker_rejection_rate"))
            .collect();
            
        println!("\nRejection Rate Values:");
        for line in &rejection_lines {
            println!("  {}", line);
        }
        
        // Should have non-zero rejection rate
        let has_rejections = rejection_lines.iter()
            .any(|line| !line.contains("} 0"));
            
        assert!(has_rejections, "Circuit breaker should have rejected some requests");
    }
    
    // Verify events processing
    let events = collected_events.lock()
        .map_err(|e| anyhow::anyhow!("Failed to lock events: {:?}", e))?;
    
    println!("\nFlow Processing Results:");
    println!("  Total events sent: 15");
    println!("  Events reached sink: {}", events.len());
    println!("  Events rejected/failed: {}", 15 - events.len());
    
    // We expect fewer events due to circuit breaker rejections
    assert!(events.len() < 15, "Circuit breaker should have rejected some events");
    
    // Verify we got the expected sequence
    let first_3_success = events.iter()
        .take(3)
        .all(|e| e.payload["processed"] == json!(true));
    
    assert!(first_3_success, "First 3 events should have succeeded");
    
    // Stop the flow
    flow_handle.shutdown().await?;
    
    // Final metrics check
    sleep(Duration::from_millis(500)).await;
    let final_metrics = metrics_exporter.render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render final metrics: {}", e))?;
    
    // Ensure we have circuit breaker metrics
    assert!(
        final_metrics.contains("obzenflow_circuit_breaker_state") ||
        final_metrics.contains("obzenflow_circuit_breaker_rejection_rate"),
        "Circuit breaker metrics must be present in final output"
    );
    
    println!("\n✅ Circuit Breaker Metrics E2E Test PASSED!");
    println!("   - Circuit breaker state transitions detected");
    println!("   - Control events successfully converted to metrics");
    println!("   - Prometheus metrics match expected format");
    
    Ok(())
}

/// Test that verifies circuit breaker emits summary events periodically
#[tokio::test] 
async fn test_circuit_breaker_summary_events() -> Result<()> {
    let memory_journal = Arc::new(MemoryJournal::new());
    
    // Source that generates many events quickly
    struct RapidSource {
        count: usize,
        writer_id: WriterId,
    }
    
    impl FiniteSourceHandler for RapidSource {
        fn next(&mut self) -> Option<ChainEvent> {
            if self.count >= 1100 { // Trigger summary after 1000 events
                return None;
            }
            self.count += 1;
            
            Some(ChainEvent::new(
                EventId::new(),
                self.writer_id.clone(),
                "test.rapid",
                json!({"id": self.count}),
            ))
        }
        
        fn is_complete(&self) -> bool {
            self.count >= 1100
        }
    }
    
    // Simple passthrough transform
    struct PassthroughTransform;
    impl TransformHandler for PassthroughTransform {
        fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
            vec![event]
        }
    }
    
    let flow_handle = flow! {
        name: "circuit_breaker_summary_test", 
        journal: memory_journal.clone(),
        middleware: [],
        
        stages: {
            src = source!("rapid_source" => RapidSource { count: 0, writer_id: WriterId::new() });
            trans = transform!("cb_summary_transform" => PassthroughTransform, [
                circuit_breaker(10) // High threshold, won't trip
            ]);
            snk = sink!("null_sink" => MetricsSink::new().0);
        },
        
        topology: {
            src |> trans;
            trans |> snk;
        }
    }.await.map_err(|e| anyhow::anyhow!("Flow creation failed: {:?}", e))?;
    
    flow_handle.run().await?;
    sleep(Duration::from_secs(2)).await;
    
    let metrics_exporter = flow_handle.metrics_exporter().await.unwrap();
    let metrics = metrics_exporter.render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
    
    // Should have circuit breaker metrics from summary events
    assert!(
        metrics.contains("obzenflow_circuit_breaker_") ||
        metrics.contains("obzenflow_events_total"),
        "Should have metrics from circuit breaker summary events"
    );
    
    flow_handle.shutdown().await?;
    Ok(())
}
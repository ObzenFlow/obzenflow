//! End-to-end test for Rate Limiter metrics in FLOWIP-056-666
//! 
//! This test verifies that rate limiter middleware emits control events
//! that flow through the system and appear as Prometheus metrics.

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::{
    stages::common::handlers::{SourceHandler, TransformHandler, SinkHandler},
};
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_infra::journal::memory::MemoryJournal;
use obzenflow_core::{
    event::{chain_event::ChainEvent, event_id::EventId},
    journal::writer_id::WriterId,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::time::{sleep, Duration, Instant};
use anyhow::Result;

/// Source that emits events in controlled bursts
struct BurstSource {
    burst_size: usize,
    burst_delay: Duration,
    events_per_burst: usize,
    current_burst: usize,
    current_event: usize,
    writer_id: WriterId,
}

impl BurstSource {
    fn new(burst_size: usize, events_per_burst: usize, burst_delay: Duration) -> Self {
        Self {
            burst_size,
            burst_delay,
            events_per_burst,
            current_burst: 0,
            current_event: 0,
            writer_id: WriterId::new(),
        }
    }
}

#[async_trait::async_trait]
impl SourceHandler for BurstSource {
    async fn run(&mut self, emitter: obzenflow_runtime_services::EventEmitter) -> obzenflow_core::Result<()> {
        println!("Starting burst source: {} bursts of {} events each", self.burst_size, self.events_per_burst);
        
        for burst in 0..self.burst_size {
            let burst_start = Instant::now();
            println!("\nBurst {}: Emitting {} events rapidly", burst + 1, self.events_per_burst);
            
            // Emit rapid events within burst
            for event_in_burst in 0..self.events_per_burst {
                let event = ChainEvent::new(
                    EventId::new(),
                    self.writer_id.clone(),
                    "test.burst",
                    json!({
                        "burst": burst,
                        "event": event_in_burst,
                        "timestamp": std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis()
                    }),
                );
                
                emitter.emit(event).await?;
            }
            
            let burst_duration = burst_start.elapsed();
            println!("  Burst {} completed in {:?}", burst + 1, burst_duration);
            
            // Wait between bursts (except after last burst)
            if burst < self.burst_size - 1 {
                println!("  Waiting {:?} before next burst...", self.burst_delay);
                sleep(self.burst_delay).await;
            }
        }
        
        println!("\nAll bursts completed");
        Ok(())
    }
}

/// Transform that adds processing timestamp
struct TimestampTransform;

impl TransformHandler for TimestampTransform {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        event.payload["processed_at"] = json!(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        vec![event]
    }
}

/// Sink that tracks timing of received events
struct TimingSink {
    events: Arc<Mutex<Vec<(ChainEvent, Instant)>>>,
}

impl TimingSink {
    fn new() -> (Self, Arc<Mutex<Vec<(ChainEvent, Instant)>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (Self { events: events.clone() }, events)
    }
}

impl SinkHandler for TimingSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        if let Ok(mut events) = self.events.lock() {
            events.push((event, Instant::now()));
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_rate_limiter_metrics_end_to_end() -> Result<()> {
    // Initialize tracing
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_test_writer()
        .try_init();

    println!("\n=== Rate Limiter Metrics E2E Test ===\n");

    // Create journal
    let memory_journal = Arc::new(MemoryJournal::new());
    
    // Create handlers
    let source = BurstSource::new(
        3,                          // 3 bursts
        10,                         // 10 events per burst
        Duration::from_millis(500), // 500ms between bursts
    );
    let transform = TimestampTransform;
    let (sink, collected_events) = TimingSink::new();

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
            trans = transform!("timestamp_transform" => transform);
            snk = sink!("timing_sink" => sink);
        },
        
        topology: {
            src |> trans;
            trans |> snk;
        }
    }.await.map_err(|e| anyhow::anyhow!("Flow creation failed: {:?}", e))?;

    println!("Running flow to trigger rate limiting...");
    let start_time = Instant::now();

    // Run the flow
    flow_handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run flow: {:?}", e))?;

    // Wait for flow to complete processing
    sleep(Duration::from_secs(8)).await;
    
    let total_duration = start_time.elapsed();
    println!("\nFlow completed in {:?}", total_duration);
    
    // Get metrics exporter
    let metrics_exporter = flow_handle.metrics_exporter().await
        .expect("Metrics should be enabled");
    
    println!("\n=== Verifying Rate Limiter Metrics ===");
    
    // Get metrics
    let metrics_text = metrics_exporter.render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
    
    // Debug output
    println!("\n=== Rate Limiter Metrics ===");
    for line in metrics_text.lines() {
        if line.contains("rate_limiter") || 
           line.contains("burst_source") ||
           line.contains("events_total") {
            println!("{}", line);
        }
    }
    
    // Verify rate limiter metrics exist
    let has_rl_delay_rate = metrics_text.contains("obzenflow_rate_limiter_delay_rate");
    let has_rl_utilization = metrics_text.contains("obzenflow_rate_limiter_utilization");
    
    println!("\nRate Limiter Metrics Found:");
    println!("  Delay rate: {}", if has_rl_delay_rate { "✓" } else { "✗" });
    println!("  Utilization: {}", if has_rl_utilization { "✓" } else { "✗" });
    
    // Check for actual rate limiting evidence
    if has_rl_delay_rate {
        let delay_lines: Vec<&str> = metrics_text.lines()
            .filter(|line| line.contains("obzenflow_rate_limiter_delay_rate"))
            .collect();
            
        println!("\nDelay Rate Values:");
        for line in &delay_lines {
            println!("  {}", line);
        }
        
        // Should have non-zero delay rate due to bursts
        let has_delays = delay_lines.iter()
            .any(|line| !line.ends_with("} 0"));
            
        assert!(has_delays, "Rate limiter should have delayed some events");
    }
    
    // Check utilization
    if has_rl_utilization {
        let util_lines: Vec<&str> = metrics_text.lines()
            .filter(|line| line.contains("obzenflow_rate_limiter_utilization"))
            .collect();
            
        println!("\nUtilization Values:");
        for line in &util_lines {
            println!("  {}", line);
        }
        
        // During bursts, utilization should approach or exceed 1.0
        let high_utilization = util_lines.iter()
            .any(|line| {
                // Parse the metric value
                if let Some(val_str) = line.split("} ").nth(1) {
                    val_str.parse::<f64>().unwrap_or(0.0) > 0.8
                } else {
                    false
                }
            });
            
        assert!(high_utilization, "Rate limiter should show high utilization during bursts");
    }
    
    // Analyze event timing
    let events = collected_events.lock()
        .map_err(|e| anyhow::anyhow!("Failed to lock events: {:?}", e))?;
    
    println!("\nFlow Processing Results:");
    println!("  Total events sent: 30 (3 bursts × 10 events)");
    println!("  Events reached sink: {}", events.len());
    
    // All events should eventually get through
    assert_eq!(events.len(), 30, "All events should eventually be processed");
    
    // Analyze timing to verify rate limiting
    println!("\nEvent Timing Analysis:");
    
    // Group events by burst
    let mut burst_timings: Vec<Vec<Duration>> = vec![vec![], vec![], vec![]];
    for (event, received_at) in events.iter() {
        if let Some(burst_num) = event.payload["burst"].as_u64() {
            if burst_num < 3 {
                burst_timings[burst_num as usize].push(received_at.duration_since(start_time));
            }
        }
    }
    
    // Calculate inter-arrival times within bursts
    for (burst_idx, timings) in burst_timings.iter().enumerate() {
        if timings.len() > 1 {
            let mut inter_arrival_times = Vec::new();
            for i in 1..timings.len() {
                inter_arrival_times.push(timings[i] - timings[i-1]);
            }
            
            let avg_inter_arrival = inter_arrival_times.iter()
                .sum::<Duration>() / inter_arrival_times.len() as u32;
            
            println!("  Burst {}: avg inter-arrival time = {:?}", burst_idx + 1, avg_inter_arrival);
            
            // With 5 events/sec limit, average should be ~200ms
            assert!(
                avg_inter_arrival >= Duration::from_millis(150),
                "Rate limiter should enforce ~200ms spacing (5/sec)"
            );
        }
    }
    
    // Stop the flow
    flow_handle.shutdown().await?;
    
    // Final metrics check
    sleep(Duration::from_millis(500)).await;
    let final_metrics = metrics_exporter.render_metrics()
        .map_err(|e| anyhow::anyhow!("Failed to render final metrics: {}", e))?;
    
    // Ensure we have rate limiter metrics
    assert!(
        final_metrics.contains("obzenflow_rate_limiter_delay_rate") ||
        final_metrics.contains("obzenflow_rate_limiter_utilization"),
        "Rate limiter metrics must be present in final output"
    );
    
    println!("\n✅ Rate Limiter Metrics E2E Test PASSED!");
    println!("   - Rate limiting behavior verified");
    println!("   - Control events successfully converted to metrics");
    println!("   - Burst handling shows proper utilization metrics");
    
    Ok(())
}

/// Test rate limiter with varying rates
#[tokio::test]
async fn test_rate_limiter_adaptive_behavior() -> Result<()> {
    let memory_journal = Arc::new(MemoryJournal::new());
    
    // Source with varying event rates
    struct VariableRateSource {
        phases: Vec<(usize, Duration)>, // (events_count, delay_between)
        current_phase: usize,
        current_event: usize,
        writer_id: WriterId,
    }
    
    impl VariableRateSource {
        fn new() -> Self {
            Self {
                phases: vec![
                    (5, Duration::from_millis(50)),    // Fast: 20/sec
                    (5, Duration::from_millis(500)),   // Slow: 2/sec  
                    (10, Duration::from_millis(20)),   // Burst: 50/sec
                    (5, Duration::from_millis(1000)),  // Very slow: 1/sec
                ],
                current_phase: 0,
                current_event: 0,
                writer_id: WriterId::new(),
            }
        }
    }
    
    #[async_trait::async_trait]
    impl SourceHandler for VariableRateSource {
        async fn run(&mut self, emitter: obzenflow_runtime_services::EventEmitter) -> obzenflow_core::Result<()> {
            for (phase_idx, (count, delay)) in self.phases.clone().into_iter().enumerate() {
                println!("\nPhase {}: {} events with {:?} spacing", phase_idx + 1, count, delay);
                
                for i in 0..count {
                    let event = ChainEvent::new(
                        EventId::new(),
                        self.writer_id.clone(),
                        "test.variable",
                        json!({
                            "phase": phase_idx,
                            "event": i
                        }),
                    );
                    
                    emitter.emit(event).await?;
                    
                    if i < count - 1 {
                        sleep(delay).await;
                    }
                }
            }
            
            Ok(())
        }
    }
    
    let flow_handle = flow! {
        journal: memory_journal.clone(),
        middleware: [],
        
        stages: {
            src = source!("variable_source" => VariableRateSource::new(), [
                rate_limit(10.0) // 10 events/sec
            ]);
            snk = sink!("counting_sink" => TimingSink::new().0);
        },
        
        topology: {
            src |> snk;
        }
    }.await?;
    
    flow_handle.run().await?;
    sleep(Duration::from_secs(5)).await;
    
    let metrics_exporter = flow_handle.metrics_exporter().await.unwrap();
    let metrics = metrics_exporter.render_metrics()?;
    
    // Should show varying utilization based on input rate
    assert!(
        metrics.contains("obzenflow_rate_limiter_utilization"),
        "Should have rate limiter utilization metrics"
    );
    
    flow_handle.shutdown().await?;
    Ok(())
}
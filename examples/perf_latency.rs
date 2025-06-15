//! Latency Measurement Example
//!
//! This example measures end-to-end latency between event emission and reception
//! in a simple pipeline over a time period, reporting proper percentiles.
//!
//! Run with:
//! cargo run --example perf_latency
//! cargo run --example perf_latency -- --duration 60  # Run for 60 seconds

use flowstate_rs::prelude::*;
use flowstate_rs::event_store::{EventStore, EventStoreConfig};
use flowstate_rs::monitoring::RED;
use flowstate_rs::monitoring::taxonomies::red::REDMetrics;
use flowstate_rs::step::{Step, StepType, ChainEvent};
use flowstate_rs::flow;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::{Instant, Duration};
use tokio::sync::Mutex;

const DEFAULT_DURATION_SECS: u64 = 30;

#[derive(Clone)]
struct TimestampedSource {
    event_id: Arc<AtomicU64>,
    metrics: Arc<REDMetrics>,
    running: Arc<AtomicBool>,
}

impl Step for TimestampedSource {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as flowstate_rs::monitoring::Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Source }
    
    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        if !self.running.load(Ordering::Relaxed) {
            return vec![];
        }
        
        let n = self.event_id.fetch_add(1, Ordering::Relaxed);
        
        // Emit multiple events per handle() to increase throughput
        let mut events = Vec::new();
        for i in 0..10 {
            let event_id = n * 10 + i;
            
            // Embed high-precision timestamp for accurate latency measurement
            // Using SystemTime for absolute timestamp that can be compared across stages
            let emit_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            
            events.push(ChainEvent::new("test_event", serde_json::json!({ 
                "id": event_id,
                "batch": n,
                "emit_time_nanos": emit_time,
            })));
        }
        events
    }
}

#[derive(Clone)]
struct TimestampedSink {
    received: Arc<AtomicU64>,
    latencies: Arc<Mutex<Vec<Duration>>>,
    metrics: Arc<REDMetrics>,
}

impl Step for TimestampedSink {
    type Taxonomy = RED;
    
    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as flowstate_rs::monitoring::Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Sink }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let Some(emit_time_nanos) = event.payload.get("emit_time_nanos").and_then(|v| v.as_u64()) {
            // Get current time with same method as source
            let receive_time_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            
            // Calculate latency
            if receive_time_nanos > emit_time_nanos {
                let latency_nanos = receive_time_nanos - emit_time_nanos;
                let latency = Duration::from_nanos(latency_nanos);
                
                let latencies = self.latencies.clone();
                tokio::spawn(async move {
                    latencies.lock().await.push(latency);
                });
            }
            
            self.received.fetch_add(1, Ordering::Relaxed);
        }
        vec![]
    }
}

fn calculate_percentiles(mut latencies: Vec<Duration>) -> (Duration, Duration, Duration, Duration, Duration) {
    latencies.sort();
    let len = latencies.len();
    if len == 0 {
        return (Duration::ZERO, Duration::ZERO, Duration::ZERO, Duration::ZERO, Duration::ZERO);
    }
    
    // Use safer indexing for percentiles
    let p50_idx = (len * 50 / 100).min(len - 1);
    let p90_idx = (len * 90 / 100).min(len - 1);
    let p95_idx = (len * 95 / 100).min(len - 1);
    let p99_idx = (len * 99 / 100).min(len - 1);
    let p999_idx = (len * 999 / 1000).min(len - 1);
    
    (latencies[p50_idx], latencies[p90_idx], latencies[p95_idx], latencies[p99_idx], latencies[p999_idx])
}

#[tokio::main]
async fn main() -> flowstate_rs::step::Result<()> {
    // Parse command line args
    let args: Vec<String> = std::env::args().collect();
    let duration_secs = if args.len() > 2 && args[1] == "--duration" {
        args[2].parse::<u64>().unwrap_or(DEFAULT_DURATION_SECS)
    } else {
        DEFAULT_DURATION_SECS
    };
    
    println!("\n📊 FlowState Latency Measurement");
    println!("================================");
    println!("Running latency test for {} seconds...", duration_secs);
    println!("(Press Ctrl+C to stop early)");
    
    let store = EventStore::new(EventStoreConfig {
        path: "./latency_test".into(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    let running = Arc::new(AtomicBool::new(true));
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(100_000)));
    
    let source = TimestampedSource { 
        event_id: Arc::new(AtomicU64::new(0)),
        metrics: Arc::new(RED::create_metrics("source")),
        running: running.clone(),
    };
    
    let sink = TimestampedSink { 
        received: Arc::new(AtomicU64::new(0)),
        latencies: latencies.clone(),
        metrics: Arc::new(RED::create_metrics("sink")),
    };
    
    let sink_clone = sink.clone();
    
    let handle = flow! {
        store: store,
        flow_taxonomy: GoldenSignals,
        ("source" => source, RED)
        |> ("sink" => sink, RED)
    }?;
    
    // Allow pipeline to initialize
    println!("⏳ Initializing pipeline...");
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Run for specified duration with progress updates
    let test_start = Instant::now();
    let test_duration = Duration::from_secs(duration_secs);
    let mut last_count = 0;
    let mut last_print = Instant::now();
    
    while test_start.elapsed() < test_duration {
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        let current_count = sink_clone.received.load(Ordering::Relaxed);
        let elapsed = test_start.elapsed();
        
        // Update progress every second
        if last_print.elapsed() >= Duration::from_secs(1) {
            let rate = (current_count - last_count) as f64 / last_print.elapsed().as_secs_f64();
            print!("\r📈 Progress: {} events | {:.1}s elapsed | {:.0} events/sec    ", 
                current_count, elapsed.as_secs_f64(), rate);
            use std::io::{self, Write};
            io::stdout().flush().unwrap();
            last_count = current_count;
            last_print = Instant::now();
        }
    }
    
    // Stop source from generating more events
    running.store(false, Ordering::Relaxed);
    
    // Wait a bit for in-flight events to complete
    println!("\n⏳ Waiting for in-flight events...");
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    let total_received = sink_clone.received.load(Ordering::Relaxed);
    let total_duration = test_start.elapsed();
    println!("\r✅ Test completed: {} events in {:.2}s ({:.0} events/sec average)",
        total_received, total_duration.as_secs_f64(), 
        total_received as f64 / total_duration.as_secs_f64());
    
    handle.shutdown().await?;
    
    // Get latencies
    println!("\n📊 Calculating latency distribution...");
    let collected_latencies = latencies.lock().await;
    let latencies = collected_latencies.clone();
    
    if latencies.is_empty() {
        println!("❌ No latency data collected!");
        return Ok(());
    }
    
    let (p50, p90, p95, p99, p999) = calculate_percentiles(latencies.clone());
    
    println!("\n🎯 Latency Percentiles:");
    println!("═══════════════════════");
    println!("  p50  (median): {:>8.3}ms", p50.as_secs_f64() * 1000.0);
    println!("  p90:           {:>8.3}ms", p90.as_secs_f64() * 1000.0);
    println!("  p95:           {:>8.3}ms", p95.as_secs_f64() * 1000.0);
    println!("  p99:           {:>8.3}ms", p99.as_secs_f64() * 1000.0);
    println!("  p99.9:         {:>8.3}ms", p999.as_secs_f64() * 1000.0);
    
    // Show min/max for context
    let min = latencies.iter().min().unwrap();
    let max = latencies.iter().max().unwrap();
    println!("\n📈 Range:");
    println!("  Min:           {:>8.3}ms", min.as_secs_f64() * 1000.0);
    println!("  Max:           {:>8.3}ms", max.as_secs_f64() * 1000.0);
    
    // Validate latency measurements
    println!("\n📊 Measurement Validation:");
    println!("  Total events emitted: {}", total_received);
    println!("  Latencies collected: {}", latencies.len());
    println!("  Collection rate: {:.1}%", latencies.len() as f64 / total_received as f64 * 100.0);
    
    // Show histogram
    println!("\n📊 Latency Distribution:");
    let buckets = [
        (0.0, 0.1, "< 0.1ms"),
        (0.1, 0.5, "0.1-0.5ms"),
        (0.5, 1.0, "0.5-1ms"),
        (1.0, 5.0, "1-5ms"),
        (5.0, 10.0, "5-10ms"),
        (10.0, 50.0, "10-50ms"),
        (50.0, 100.0, "50-100ms"),
        (100.0, 200.0, "100-200ms"),
        (200.0, 500.0, "200-500ms"),
        (500.0, f64::INFINITY, "> 500ms"),
    ];
    
    for (min_ms, max_ms, label) in buckets {
        let count = latencies.iter()
            .filter(|&d| {
                let ms = d.as_secs_f64() * 1000.0;
                ms >= min_ms && ms < max_ms
            })
            .count();
        
        if count > 0 {
            let percentage = count as f64 / latencies.len() as f64 * 100.0;
            let bar_length = (percentage / 2.0).min(50.0) as usize;
            let bar = "█".repeat(bar_length);
            println!("  {:>10} [{:>6.2}%] {}", label, percentage, bar);
        }
    }
    
    
    println!("\n🎯 FLOWIP-011 Target: < 1ms per stage (p99)");
    if p99.as_secs_f64() * 1000.0 < 1.0 {
        println!("✅ PASSED! Current p99 latency meets target.");
    } else {
        println!("❌ FAILED! Current p99 latency exceeds target.");
    }
    
    // Cleanup
    let _ = std::fs::remove_dir_all("./latency_test");
    
    Ok(())
}
//! Performance Benchmark Example
//!
//! This example measures event delivery performance metrics including latency,
//! throughput, and CPU usage across different pipeline configurations.
//!
//! Run with:
//! cargo run --example perf_benchmark --release

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::path::PathBuf;
use serde_json::json;

/// Configuration for performance tests
const TEST_EVENT_COUNT: u64 = 100;
const WARMUP_EVENT_COUNT: u64 = 10;
const STAGE_COUNTS: &[usize] = &[1, 3, 5, 10, 20, 100];

/// Test source that emits events with timestamps
struct TimestampedSource {
    total_events: u64,
    emitted: AtomicU64,
    
}

impl TimestampedSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: AtomicU64::new(0),
            
        }
    }
}

impl Step for TimestampedSource {



    fn step_type(&self) -> StepType {
        StepType::Source
    }

    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
            // Embed high-precision timestamp for accurate latency measurement
            let emit_time_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            vec![ChainEvent::new("TimestampedEvent", json!({
                "index": current,
                "emit_time_nanos": emit_time_nanos,
            }))]
        } else {
            vec![]
        }
    }
}

/// Passthrough stage that just forwards events
struct PassthroughStage {
    name: String,
    
}

impl PassthroughStage {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            
        }
    }
}

impl Step for PassthroughStage {



    fn step_type(&self) -> StepType {
        StepType::Stage
    }

    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event]
    }
}

/// Sink that records latencies
#[derive(Clone)]
struct TimestampedSink {
    expected_count: u64,
    received: Arc<AtomicU64>,
    latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
    metrics: Arc<<RED as Taxonomy>::Metrics>,
}

impl TimestampedSink {
    fn new(expected_count: u64) -> (Self, Arc<tokio::sync::Mutex<Vec<Duration>>>) {
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(expected_count as usize)));
        let received = Arc::new(AtomicU64::new(0));
        (Self {
            expected_count,
            received: received.clone(),
            latencies: latencies.clone(),
            metrics: Arc::new(RED::create_metrics("TimestampedSink")),
        }, latencies)
    }
}

impl Step for TimestampedSink {



    fn step_type(&self) -> StepType {
        StepType::Sink
    }

    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let (Some(emit_time_nanos), Some(index)) = (
            event.payload.get("emit_time_nanos").and_then(|v| v.as_u64()),
            event.payload.get("index").and_then(|v| v.as_u64())
        ) {
            self.received.fetch_add(1, Ordering::Relaxed);
            
            // Skip warmup events for latency calculation
            if index >= WARMUP_EVENT_COUNT {
                // Calculate latency from embedded timestamp
                let receive_time_nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64;
                
                if receive_time_nanos > emit_time_nanos {
                    let latency = Duration::from_nanos(receive_time_nanos - emit_time_nanos);
                    let latencies = self.latencies.clone();
                    tokio::spawn(async move {
                        latencies.lock().await.push(latency);
                    });
                }
            }
        }
        vec![]
    }
}

/// Calculate latency statistics
#[derive(Debug)]
struct LatencyStats {
    p50: Duration,
    p95: Duration,
    p99: Duration,
    mean: Duration,
    min: Duration,
    max: Duration,
}


fn calculate_stats(mut latencies: Vec<Duration>) -> LatencyStats {
    if latencies.is_empty() {
        return LatencyStats {
            p50: Duration::ZERO,
            p95: Duration::ZERO,
            p99: Duration::ZERO,
            mean: Duration::ZERO,
            min: Duration::ZERO,
            max: Duration::ZERO,
        };
    }

    latencies.sort();
    let len = latencies.len();

    LatencyStats {
        p50: latencies[len / 2],
        p95: latencies[len * 95 / 100],
        p99: latencies[len * 99 / 100],
        mean: Duration::from_nanos(
            latencies.iter().map(|d| d.as_nanos() as u64).sum::<u64>() / len as u64
        ),
        min: latencies[0],
        max: latencies[len - 1],
    }
}

/// Test latency for a pipeline with N stages
async fn test_pipeline_latency(stage_count: usize) -> Result<LatencyStats> {
    let store_path = PathBuf::from(format!("./flowip_011_test_{}_stages", stage_count));
    let _ = std::fs::remove_dir_all(&store_path);

    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;

    // Create source and sink with timing
    let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let (sink, latencies) = TimestampedSink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let sink_clone = sink.clone();

    // Build pipeline with passthrough stages
    let handle = match stage_count {
        1 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, [RED::monitoring()])
                |> ("sink" => sink.clone(), [RED::monitoring()])
            }?
        }
        3 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, [RED::monitoring()])
                |> ("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()])
                |> ("stage2" => PassthroughStage::new("stage2"), [USE::monitoring()])
                |> ("sink" => sink.clone(), [RED::monitoring()])
            }?
        }
        5 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, [RED::monitoring()])
                |> ("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()])
                |> ("stage2" => PassthroughStage::new("stage2"), [USE::monitoring()])
                |> ("stage3" => PassthroughStage::new("stage3"), [USE::monitoring()])
                |> ("stage4" => PassthroughStage::new("stage4"), [USE::monitoring()])
                |> ("sink" => sink.clone(), [RED::monitoring()])
            }?
        }
        10 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, [RED::monitoring()])
                |> ("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()])
                |> ("stage2" => PassthroughStage::new("stage2"), [USE::monitoring()])
                |> ("stage3" => PassthroughStage::new("stage3"), [USE::monitoring()])
                |> ("stage4" => PassthroughStage::new("stage4"), [USE::monitoring()])
                |> ("stage5" => PassthroughStage::new("stage5"), [USE::monitoring()])
                |> ("stage6" => PassthroughStage::new("stage6"), [USE::monitoring()])
                |> ("stage7" => PassthroughStage::new("stage7"), [USE::monitoring()])
                |> ("stage8" => PassthroughStage::new("stage8"), [USE::monitoring()])
                |> ("stage9" => PassthroughStage::new("stage9"), [USE::monitoring()])
                |> ("sink" => sink.clone(), [RED::monitoring()])
            }?
        }
        20 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, [RED::monitoring()])
                |> ("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()])
                |> ("stage2" => PassthroughStage::new("stage2"), [USE::monitoring()])
                |> ("stage3" => PassthroughStage::new("stage3"), [USE::monitoring()])
                |> ("stage4" => PassthroughStage::new("stage4"), [USE::monitoring()])
                |> ("stage5" => PassthroughStage::new("stage5"), [USE::monitoring()])
                |> ("stage6" => PassthroughStage::new("stage6"), [USE::monitoring()])
                |> ("stage7" => PassthroughStage::new("stage7"), [USE::monitoring()])
                |> ("stage8" => PassthroughStage::new("stage8"), [USE::monitoring()])
                |> ("stage9" => PassthroughStage::new("stage9"), [USE::monitoring()])
                |> ("stage10" => PassthroughStage::new("stage10"), [USE::monitoring()])
                |> ("stage11" => PassthroughStage::new("stage11"), [USE::monitoring()])
                |> ("stage12" => PassthroughStage::new("stage12"), [USE::monitoring()])
                |> ("stage13" => PassthroughStage::new("stage13"), [USE::monitoring()])
                |> ("stage14" => PassthroughStage::new("stage14"), [USE::monitoring()])
                |> ("stage15" => PassthroughStage::new("stage15"), [USE::monitoring()])
                |> ("stage16" => PassthroughStage::new("stage16"), [USE::monitoring()])
                |> ("stage17" => PassthroughStage::new("stage17"), [USE::monitoring()])
                |> ("stage18" => PassthroughStage::new("stage18"), [USE::monitoring()])
                |> ("stage19" => PassthroughStage::new("stage19"), [USE::monitoring()])
                |> ("sink" => sink.clone(), [RED::monitoring()])
            }?
        }
        100 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, [RED::monitoring()])
                |> ("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()])
                |> ("stage2" => PassthroughStage::new("stage2"), [USE::monitoring()])
                |> ("stage3" => PassthroughStage::new("stage3"), [USE::monitoring()])
                |> ("stage4" => PassthroughStage::new("stage4"), [USE::monitoring()])
                |> ("stage5" => PassthroughStage::new("stage5"), [USE::monitoring()])
                |> ("stage6" => PassthroughStage::new("stage6"), [USE::monitoring()])
                |> ("stage7" => PassthroughStage::new("stage7"), [USE::monitoring()])
                |> ("stage8" => PassthroughStage::new("stage8"), [USE::monitoring()])
                |> ("stage9" => PassthroughStage::new("stage9"), [USE::monitoring()])
                |> ("stage10" => PassthroughStage::new("stage10"), [USE::monitoring()])
                |> ("stage11" => PassthroughStage::new("stage11"), [USE::monitoring()])
                |> ("stage12" => PassthroughStage::new("stage12"), [USE::monitoring()])
                |> ("stage13" => PassthroughStage::new("stage13"), [USE::monitoring()])
                |> ("stage14" => PassthroughStage::new("stage14"), [USE::monitoring()])
                |> ("stage15" => PassthroughStage::new("stage15"), [USE::monitoring()])
                |> ("stage16" => PassthroughStage::new("stage16"), [USE::monitoring()])
                |> ("stage17" => PassthroughStage::new("stage17"), [USE::monitoring()])
                |> ("stage18" => PassthroughStage::new("stage18"), [USE::monitoring()])
                |> ("stage19" => PassthroughStage::new("stage19"), [USE::monitoring()])
                |> ("stage20" => PassthroughStage::new("stage20"), [USE::monitoring()])
                |> ("stage21" => PassthroughStage::new("stage21"), [USE::monitoring()])
                |> ("stage22" => PassthroughStage::new("stage22"), [USE::monitoring()])
                |> ("stage23" => PassthroughStage::new("stage23"), [USE::monitoring()])
                |> ("stage24" => PassthroughStage::new("stage24"), [USE::monitoring()])
                |> ("stage25" => PassthroughStage::new("stage25"), [USE::monitoring()])
                |> ("stage26" => PassthroughStage::new("stage26"), [USE::monitoring()])
                |> ("stage27" => PassthroughStage::new("stage27"), [USE::monitoring()])
                |> ("stage28" => PassthroughStage::new("stage28"), [USE::monitoring()])
                |> ("stage29" => PassthroughStage::new("stage29"), [USE::monitoring()])
                |> ("stage30" => PassthroughStage::new("stage30"), [USE::monitoring()])
                |> ("stage31" => PassthroughStage::new("stage31"), [USE::monitoring()])
                |> ("stage32" => PassthroughStage::new("stage32"), [USE::monitoring()])
                |> ("stage33" => PassthroughStage::new("stage33"), [USE::monitoring()])
                |> ("stage34" => PassthroughStage::new("stage34"), [USE::monitoring()])
                |> ("stage35" => PassthroughStage::new("stage35"), [USE::monitoring()])
                |> ("stage36" => PassthroughStage::new("stage36"), [USE::monitoring()])
                |> ("stage37" => PassthroughStage::new("stage37"), [USE::monitoring()])
                |> ("stage38" => PassthroughStage::new("stage38"), [USE::monitoring()])
                |> ("stage39" => PassthroughStage::new("stage39"), [USE::monitoring()])
                |> ("stage40" => PassthroughStage::new("stage40"), [USE::monitoring()])
                |> ("stage41" => PassthroughStage::new("stage41"), [USE::monitoring()])
                |> ("stage42" => PassthroughStage::new("stage42"), [USE::monitoring()])
                |> ("stage43" => PassthroughStage::new("stage43"), [USE::monitoring()])
                |> ("stage44" => PassthroughStage::new("stage44"), [USE::monitoring()])
                |> ("stage45" => PassthroughStage::new("stage45"), [USE::monitoring()])
                |> ("stage46" => PassthroughStage::new("stage46"), [USE::monitoring()])
                |> ("stage47" => PassthroughStage::new("stage47"), [USE::monitoring()])
                |> ("stage48" => PassthroughStage::new("stage48"), [USE::monitoring()])
                |> ("stage49" => PassthroughStage::new("stage49"), [USE::monitoring()])
                |> ("stage50" => PassthroughStage::new("stage50"), [USE::monitoring()])
                |> ("stage51" => PassthroughStage::new("stage51"), [USE::monitoring()])
                |> ("stage52" => PassthroughStage::new("stage52"), [USE::monitoring()])
                |> ("stage53" => PassthroughStage::new("stage53"), [USE::monitoring()])
                |> ("stage54" => PassthroughStage::new("stage54"), [USE::monitoring()])
                |> ("stage55" => PassthroughStage::new("stage55"), [USE::monitoring()])
                |> ("stage56" => PassthroughStage::new("stage56"), [USE::monitoring()])
                |> ("stage57" => PassthroughStage::new("stage57"), [USE::monitoring()])
                |> ("stage58" => PassthroughStage::new("stage58"), [USE::monitoring()])
                |> ("stage59" => PassthroughStage::new("stage59"), [USE::monitoring()])
                |> ("stage60" => PassthroughStage::new("stage60"), [USE::monitoring()])
                |> ("stage61" => PassthroughStage::new("stage61"), [USE::monitoring()])
                |> ("stage62" => PassthroughStage::new("stage62"), [USE::monitoring()])
                |> ("stage63" => PassthroughStage::new("stage63"), [USE::monitoring()])
                |> ("stage64" => PassthroughStage::new("stage64"), [USE::monitoring()])
                |> ("stage65" => PassthroughStage::new("stage65"), [USE::monitoring()])
                |> ("stage66" => PassthroughStage::new("stage66"), [USE::monitoring()])
                |> ("stage67" => PassthroughStage::new("stage67"), [USE::monitoring()])
                |> ("stage68" => PassthroughStage::new("stage68"), [USE::monitoring()])
                |> ("stage69" => PassthroughStage::new("stage69"), [USE::monitoring()])
                |> ("stage70" => PassthroughStage::new("stage70"), [USE::monitoring()])
                |> ("stage71" => PassthroughStage::new("stage71"), [USE::monitoring()])
                |> ("stage72" => PassthroughStage::new("stage72"), [USE::monitoring()])
                |> ("stage73" => PassthroughStage::new("stage73"), [USE::monitoring()])
                |> ("stage74" => PassthroughStage::new("stage74"), [USE::monitoring()])
                |> ("stage75" => PassthroughStage::new("stage75"), [USE::monitoring()])
                |> ("stage76" => PassthroughStage::new("stage76"), [USE::monitoring()])
                |> ("stage77" => PassthroughStage::new("stage77"), [USE::monitoring()])
                |> ("stage78" => PassthroughStage::new("stage78"), [USE::monitoring()])
                |> ("stage79" => PassthroughStage::new("stage79"), [USE::monitoring()])
                |> ("stage80" => PassthroughStage::new("stage80"), [USE::monitoring()])
                |> ("stage81" => PassthroughStage::new("stage81"), [USE::monitoring()])
                |> ("stage82" => PassthroughStage::new("stage82"), [USE::monitoring()])
                |> ("stage83" => PassthroughStage::new("stage83"), [USE::monitoring()])
                |> ("stage84" => PassthroughStage::new("stage84"), [USE::monitoring()])
                |> ("stage85" => PassthroughStage::new("stage85"), [USE::monitoring()])
                |> ("stage86" => PassthroughStage::new("stage86"), [USE::monitoring()])
                |> ("stage87" => PassthroughStage::new("stage87"), [USE::monitoring()])
                |> ("stage88" => PassthroughStage::new("stage88"), [USE::monitoring()])
                |> ("stage89" => PassthroughStage::new("stage89"), [USE::monitoring()])
                |> ("stage90" => PassthroughStage::new("stage90"), [USE::monitoring()])
                |> ("stage91" => PassthroughStage::new("stage91"), [USE::monitoring()])
                |> ("stage92" => PassthroughStage::new("stage92"), [USE::monitoring()])
                |> ("stage93" => PassthroughStage::new("stage93"), [USE::monitoring()])
                |> ("stage94" => PassthroughStage::new("stage94"), [USE::monitoring()])
                |> ("stage95" => PassthroughStage::new("stage95"), [USE::monitoring()])
                |> ("stage96" => PassthroughStage::new("stage96"), [USE::monitoring()])
                |> ("stage97" => PassthroughStage::new("stage97"), [USE::monitoring()])
                |> ("stage98" => PassthroughStage::new("stage98"), [USE::monitoring()])
                |> ("stage99" => PassthroughStage::new("stage99"), [USE::monitoring()])
                |> ("sink" => sink.clone(), [RED::monitoring()])
            }?
        }
        _ => panic!("Unsupported stage count"),
    };

    // Wait for all events to be processed
    let timeout = Duration::from_secs(30);
    let start = Instant::now();

    while sink_clone.received.load(Ordering::Relaxed) < WARMUP_EVENT_COUNT + TEST_EVENT_COUNT {
        if start.elapsed() > timeout {
            println!("WARNING: Timeout waiting for events. Received {} of {}",
                sink_clone.received.load(Ordering::Relaxed),
                WARMUP_EVENT_COUNT + TEST_EVENT_COUNT
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Shut down
    handle.shutdown().await?;

    // Get latencies (already excludes warmup since we only collect after WARMUP_EVENT_COUNT)
    let collected_latencies = latencies.lock().await;
    let mut test_latencies: Vec<Duration> = collected_latencies.clone();
    
    // Sort for statistics
    test_latencies.sort();
    
    let stats = calculate_stats(test_latencies);

    // Cleanup
    let _ = std::fs::remove_dir_all(&store_path);

    Ok(stats)
}

/// Test throughput
async fn test_throughput() -> Result<f64> {
    let store_path = PathBuf::from("./flowip_011_throughput_test");
    let _ = std::fs::remove_dir_all(&store_path);

    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;

    let event_count = 1000;
    let source = TimestampedSource::new(event_count);
    let (sink, _) = TimestampedSink::new(event_count);
    let sink_clone = sink.clone();

    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => source, [RED::monitoring()])
        |> ("transform" => PassthroughStage::new("transform"), [USE::monitoring()])
        |> ("sink" => sink, [RED::monitoring()])
    }?;

    let start = Instant::now();

    // Wait for completion
    let timeout = Duration::from_secs(30);
    while sink_clone.received.load(Ordering::Relaxed) < event_count {
        if start.elapsed() > timeout {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let elapsed = start.elapsed();
    let received = sink_clone.received.load(Ordering::Relaxed);
    let throughput = received as f64 / elapsed.as_secs_f64();

    handle.shutdown().await?;
    let _ = std::fs::remove_dir_all(&store_path);

    Ok(throughput)
}

/// Test CPU usage when idle
async fn test_idle_cpu() -> Result<f64> {
    use sysinfo::{System, SystemExt, ProcessExt, Pid};

    let store_path = PathBuf::from("./flowip_011_idle_test");
    let _ = std::fs::remove_dir_all(&store_path);

    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;

    // Source that never emits
    struct IdleSource;

    impl Step for IdleSource {
        fn step_type(&self) -> StepType { StepType::Source }
        fn handle(&self, _: ChainEvent) -> Vec<ChainEvent> { vec![] }
    }

    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => IdleSource, [RED::monitoring()])
        |> ("sink" => TimestampedSink::new(0).0.clone(), [RED::monitoring()])
    }?;

    // Let it stabilize
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Measure CPU over 5 seconds
    let mut system = System::new_all();
    let pid = Pid::from(std::process::id() as usize);

    system.refresh_process(pid);
    let mut cpu_samples = Vec::new();

    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        system.refresh_process(pid);

        if let Some(process) = system.process(pid) {
            cpu_samples.push(process.cpu_usage());
        }
    }

    let avg_cpu = cpu_samples.iter().sum::<f32>() / cpu_samples.len() as f32;

    handle.shutdown().await?;
    let _ = std::fs::remove_dir_all(&store_path);

    Ok(avg_cpu as f64)
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║         FLOWIP-011 Performance Measurement Results           ║");
    println!("╚══════════════════════════════════════════════════════════════╝");

    println!("\nTest Configuration:");
    println!("  Events per test: {}", TEST_EVENT_COUNT);
    println!("  Warmup events: {}", WARMUP_EVENT_COUNT);
    println!("  Pipeline stages tested: {:?}", STAGE_COUNTS);

    // Test latency for different pipeline lengths
    println!("\n┌─────────────────────────────────────────────────────────────┐");
    println!("│ End-to-End Latency Measurements                            │");
    println!("├─────────────────────────────────────────────────────────────┤");

    let mut all_stats = Vec::new();

    for &stage_count in STAGE_COUNTS {
        match test_pipeline_latency(stage_count).await {
            Ok(stats) => {
                println!("│ {}-stage pipeline:                                          │", stage_count);
                println!("│   Percentiles: P50={:>7.1}ms  P95={:>7.1}ms  P99={:>7.1}ms │",
                    stats.p50.as_secs_f64() * 1000.0,
                    stats.p95.as_secs_f64() * 1000.0,
                    stats.p99.as_secs_f64() * 1000.0);
                println!("│   Statistics:  Min={:>7.1}ms  Max={:>7.1}ms  Mean={:>6.1}ms │",
                    stats.min.as_secs_f64() * 1000.0,
                    stats.max.as_secs_f64() * 1000.0,
                    stats.mean.as_secs_f64() * 1000.0);

                let latency_per_stage = stats.mean.as_secs_f64() * 1000.0 / stage_count as f64;
                println!("│   Per-stage latency: {:.1}ms                                │", latency_per_stage);

                all_stats.push((stage_count, stats));
            }
            Err(e) => {
                println!("│ {}-stage pipeline: ERROR - {}                    │", stage_count, e);
            }
        }
        if stage_count != *STAGE_COUNTS.last().unwrap() {
            println!("├─────────────────────────────────────────────────────────────┤");
        }
    }
    println!("└─────────────────────────────────────────────────────────────┘");

    // Show latency scaling
    if all_stats.len() >= 2 {
        println!("\n┌─────────────────────────────────────────────────────────────┐");
        println!("│ Latency Scaling Analysis                                    │");
        println!("├─────────────────────────────────────────────────────────────┤");

        for window in all_stats.windows(2) {
            let (stages1, stats1) = &window[0];
            let (stages2, stats2) = &window[1];
            let added_stages = stages2 - stages1;
            let added_latency = stats2.mean.saturating_sub(stats1.mean);
            let latency_per_added_stage = added_latency.as_secs_f64() * 1000.0 / added_stages as f64;

            println!("│ {} → {} stages: +{:.1}ms total (+{:.1}ms per stage)        │",
                stages1, stages2,
                added_latency.as_secs_f64() * 1000.0,
                latency_per_added_stage);
        }
        println!("└─────────────────────────────────────────────────────────────┘");
    }

    // Test throughput
    println!("\n┌─────────────────────────────────────────────────────────────┐");
    println!("│ Throughput Measurement                                      │");
    println!("├─────────────────────────────────────────────────────────────┤");

    match test_throughput().await {
        Ok(throughput) => {
            println!("│ Sustained throughput: {:.2} events/second                 │", throughput);
            println!("│ Time per event: {:.2}ms                                    │", 1000.0 / throughput);
        }
        Err(e) => {
            println!("│ ERROR: {}                                         │", e);
        }
    }
    println!("└─────────────────────────────────────────────────────────────┘");

    // Test idle CPU
    println!("\n┌─────────────────────────────────────────────────────────────┐");
    println!("│ Idle CPU Usage                                              │");
    println!("├─────────────────────────────────────────────────────────────┤");

    match test_idle_cpu().await {
        Ok(cpu) => {
            println!("│ Average CPU usage when idle: {:.3}%                        │", cpu);
        }
        Err(e) => {
            println!("│ ERROR: {}                                         │", e);
        }
    }
    println!("└─────────────────────────────────────────────────────────────┘");

    println!("\n═══════════════════════════════════════════════════════════════");
    println!("Save these results to compare after implementing FLOWIP-011!");
    println!("═══════════════════════════════════════════════════════════════\n");

    Ok(())
}

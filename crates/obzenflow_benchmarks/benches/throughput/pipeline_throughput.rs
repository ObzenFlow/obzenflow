//! Pipeline Throughput Benchmarks
//!
//! Measures sustained event processing rate (events per second) across
//! different pipeline depths. This is critical for understanding capacity
//! limits and how pipeline complexity affects streaming performance.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use obzenflow_benchmarks::prelude::*;
use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::control_plane::stages::handler_traits::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_adapters::monitoring::taxonomies::{
    golden_signals::GoldenSignals,
    red::RED,
    use_taxonomy::USE,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use serde_json::json;
use tokio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use async_trait::async_trait;

const STAGE_COUNTS: &[usize] = &[1, 3, 5, 10]; // Simplified for maintainability

/// Configuration for throughput testing
const THROUGHPUT_EVENT_COUNT: u64 = 1000; // More events for accurate throughput measurement
const THROUGHPUT_WARMUP: u64 = 100;

/// Test source that emits events with timestamps
struct TimestampedSource {
    total_events: u64,
    emitted: AtomicU64,
    writer_id: WriterId,
}

impl TimestampedSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: AtomicU64::new(0),
            writer_id: WriterId::new(),
        }
    }
}

impl FiniteSourceHandler for TimestampedSource {
    fn next(&mut self) -> Option<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
            let emit_time_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            Some(ChainEvent::new(
                EventId::new(),
                self.writer_id.clone(),
                "TimestampedEvent",
                json!({
                    "index": current,
                    "emit_time_nanos": emit_time_nanos,
                })
            ))
        } else {
            None
        }
    }

    fn is_complete(&self) -> bool {
        self.emitted.load(Ordering::Relaxed) >= self.total_events
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

impl TransformHandler for PassthroughStage {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event]
    }
}

/// Sink that records latencies
#[derive(Clone)]
struct TimestampedSink {
    expected_count: u64,
    received: Arc<AtomicU64>,
    latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
}

impl TimestampedSink {
    fn new(expected_count: u64) -> (Self, Arc<tokio::sync::Mutex<Vec<Duration>>>) {
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(expected_count as usize)));
        let received = Arc::new(AtomicU64::new(0));
        (Self {
            expected_count,
            received: received.clone(),
            latencies: latencies.clone(),
        }, latencies)
    }
}

#[async_trait]
impl SinkHandler for TimestampedSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        if let (Some(emit_time_nanos), Some(index)) = (
            event.payload.get("emit_time_nanos").and_then(|v| v.as_u64()),
            event.payload.get("index").and_then(|v| v.as_u64())
        ) {
            self.received.fetch_add(1, Ordering::Relaxed);
            
            // Skip warmup events for latency calculation
            if index >= THROUGHPUT_WARMUP {
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
        Ok(())
    }
}

/// Create a temporary journal for benchmarking
async fn create_temp_journal(test_name: &str) -> anyhow::Result<(Arc<DiskJournal>, TempDir)> {
    let temp_dir = tempdir()?;
    let journal_path = temp_dir.path().join(format!("bench_{}_{}", test_name, std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()));
    std::fs::create_dir_all(&journal_path)?;

    let journal = Arc::new(DiskJournal::new(journal_path, test_name).await?);

    Ok((journal, temp_dir))
}

/// Build pipeline with specified stage count
async fn build_pipeline(
    stage_count: usize, 
    source: TimestampedSource, 
    sink: TimestampedSink,
    journal: Arc<DiskJournal>
) -> anyhow::Result<FlowHandle> {
    let handle = match stage_count {
        1 => flow! {
            journal: journal,
            middleware: [GoldenSignals::monitoring()],
            
            stages: {
                src = source!("source" => source, [RED::monitoring()]);
                snk = sink!("sink" => sink, [RED::monitoring()]);
            },
            
            topology: {
                src |> snk;
            }
        }.await.map_err(|e| anyhow::anyhow!("Failed to create 1-stage flow: {:?}", e))?,
        3 => flow! {
            journal: journal,
            middleware: [GoldenSignals::monitoring()],
            
            stages: {
                src = source!("source" => source, [RED::monitoring()]);
                s1 = transform!("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()]);
                s2 = transform!("stage2" => PassthroughStage::new("stage2"), [USE::monitoring()]);
                snk = sink!("sink" => sink, [RED::monitoring()]);
            },
            
            topology: {
                src |> s1;
                s1 |> s2;
                s2 |> snk;
            }
        }.await.map_err(|e| anyhow::anyhow!("Failed to create 3-stage flow: {:?}", e))?,
        5 => flow! {
            journal: journal,
            middleware: [GoldenSignals::monitoring()],
            
            stages: {
                src = source!("source" => source, [RED::monitoring()]);
                s1 = transform!("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()]);
                s2 = transform!("stage2" => PassthroughStage::new("stage2"), [USE::monitoring()]);
                s3 = transform!("stage3" => PassthroughStage::new("stage3"), [USE::monitoring()]);
                s4 = transform!("stage4" => PassthroughStage::new("stage4"), [USE::monitoring()]);
                snk = sink!("sink" => sink, [RED::monitoring()]);
            },
            
            topology: {
                src |> s1;
                s1 |> s2;
                s2 |> s3;
                s3 |> s4;
                s4 |> snk;
            }
        }.await.map_err(|e| anyhow::anyhow!("Failed to create 5-stage flow: {:?}", e))?,
        10 => {
            // For larger pipelines, build stages programmatically
            let handle = flow! {
                journal: journal,
                middleware: [GoldenSignals::monitoring()],
                
                stages: {
                    src = source!("source" => source, [RED::monitoring()]);
                    s1 = transform!("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()]);
                    s2 = transform!("stage2" => PassthroughStage::new("stage2"), [USE::monitoring()]);
                    s3 = transform!("stage3" => PassthroughStage::new("stage3"), [USE::monitoring()]);
                    s4 = transform!("stage4" => PassthroughStage::new("stage4"), [USE::monitoring()]);
                    s5 = transform!("stage5" => PassthroughStage::new("stage5"), [USE::monitoring()]);
                    s6 = transform!("stage6" => PassthroughStage::new("stage6"), [USE::monitoring()]);
                    s7 = transform!("stage7" => PassthroughStage::new("stage7"), [USE::monitoring()]);
                    s8 = transform!("stage8" => PassthroughStage::new("stage8"), [USE::monitoring()]);
                    s9 = transform!("stage9" => PassthroughStage::new("stage9"), [USE::monitoring()]);
                    snk = sink!("sink" => sink, [RED::monitoring()]);
                },
                
                topology: {
                    src |> s1;
                    s1 |> s2;
                    s2 |> s3;
                    s3 |> s4;
                    s4 |> s5;
                    s5 |> s6;
                    s6 |> s7;
                    s7 |> s8;
                    s8 |> s9;
                    s9 |> snk;
                }
            }.await.map_err(|e| anyhow::anyhow!("Failed to create 10-stage flow: {:?}", e))?;
            handle
        },
        _ => return Err(anyhow::anyhow!("Unsupported stage count: {}", stage_count)),
    };
    
    // Start the pipeline
    handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;
    
    Ok(handle)
}

/// Run throughput test for a specific pipeline depth
async fn run_throughput_test(stage_count: usize) -> anyhow::Result<f64> {
    let (journal, _temp_dir) = create_temp_journal(&format!("throughput_{}_stages", stage_count)).await?;

    let total_events = THROUGHPUT_WARMUP + THROUGHPUT_EVENT_COUNT;
    let source = TimestampedSource::new(total_events);
    let (sink, _latencies) = TimestampedSink::new(total_events);
    let sink_clone = sink.clone();

    // Build pipeline
    let handle = build_pipeline(stage_count, source, sink, journal).await?;

    // Start timing after warmup events
    let timeout = Duration::from_secs(60);
    let start_time = Instant::now();
    
    // Wait for warmup
    while sink_clone.received.load(Ordering::Relaxed) < THROUGHPUT_WARMUP {
        if start_time.elapsed() > timeout {
            eprintln!("WARNING: Timeout during warmup");
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // Now measure throughput timing
    let measurement_start = Instant::now();
    let measurement_start_count = sink_clone.received.load(Ordering::Relaxed);

    // Wait for all events
    while sink_clone.received.load(Ordering::Relaxed) < total_events {
        if start_time.elapsed() > timeout {
            eprintln!("WARNING: Timeout waiting for events. Received {} of {}",
                sink_clone.received.load(Ordering::Relaxed),
                total_events
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let measurement_elapsed = measurement_start.elapsed();
    let events_processed = sink_clone.received.load(Ordering::Relaxed) - measurement_start_count;
    
    // Note: handle.run() was already called in build_pipeline

    // Calculate events per second
    let throughput = events_processed as f64 / measurement_elapsed.as_secs_f64();
    
    Ok(throughput)
}

/// Benchmark throughput across different pipeline depths
fn bench_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("throughput");
    
    // Configure for throughput measurement
    group.throughput(Throughput::Elements(THROUGHPUT_EVENT_COUNT));
    group.sample_size(20);  // Consistent sample size across benchmarks
    group.measurement_time(Duration::from_secs(30));
    
    for &stage_count in STAGE_COUNTS {
        group.bench_with_input(
            BenchmarkId::new("events_per_second", format!("{}_stages", stage_count)),
            &stage_count,
            |b, &stage_count| {
                b.to_async(&rt).iter(|| async {
                    run_throughput_test(stage_count).await.unwrap()
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark time per event (inverse of throughput) for different perspectives
fn bench_time_per_event(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("time_per_event");
    
    group.sample_size(20);  // Consistent sample size across benchmarks
    
    for &stage_count in STAGE_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_stages", stage_count)),
            &stage_count,
            |b, &stage_count| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let mut total_time_per_event = Duration::ZERO;
                    
                    for _ in 0..iters {
                        let throughput = run_throughput_test(stage_count).await.unwrap();
                        // Convert throughput to time per event
                        let time_per_event = Duration::from_secs_f64(1.0 / throughput);
                        total_time_per_event += time_per_event;
                    }
                    
                    total_time_per_event
                });
            },
        );
    }
    
    group.finish();
}

/// Benchmark throughput degradation relative to single-stage pipeline
fn bench_relative_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("relative_throughput");
    
    group.sample_size(20);  // Consistent sample size across benchmarks
    
    // Get baseline throughput for single stage
    let baseline_throughput = rt.block_on(async {
        run_throughput_test(1).await.unwrap()
    });
    
    for &stage_count in STAGE_COUNTS {
        group.bench_with_input(
            BenchmarkId::new("percentage_of_baseline", format!("{}_stages", stage_count)),
            &stage_count,
            |b, &stage_count| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let mut total_percentage = 0f64;
                    
                    for _ in 0..iters {
                        let throughput = run_throughput_test(stage_count).await.unwrap();
                        let percentage = (throughput / baseline_throughput) * 100.0;
                        total_percentage += percentage;
                    }
                    
                    // Return as duration for Criterion (hack to show percentage)
                    Duration::from_secs_f64(total_percentage / iters as f64 / 1000.0)
                });
            },
        );
    }
    
    group.finish();
}

criterion_group!(
    benches,
    bench_throughput,
    bench_time_per_event,
    bench_relative_throughput
);
criterion_main!(benches);
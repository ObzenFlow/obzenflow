//! Pipeline Execution Time Benchmarks
//!
//! Measures total execution time - how long it takes to process all events
//! through the entire pipeline. This is different from per-event latency as
//! it measures overall system performance for batch processing scenarios.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
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

const TEST_EVENT_COUNT: u64 = 100;
const WARMUP_EVENT_COUNT: u64 = 10;
const STAGE_COUNTS: &[usize] = &[1, 3, 5, 10]; // Simplified for maintainability

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
        1 => {
            flow! {
                journal: journal,
                middleware: [GoldenSignals::monitoring()],
                
                stages: {
                    src = source!("source" => source, [RED::monitoring()]);
                    snk = sink!("sink" => sink, [RED::monitoring()]);
                },
                
                topology: {
                    src |> snk;
                }
            }.await.map_err(|e| anyhow::anyhow!("Failed to create 1-stage flow: {:?}", e))?
        }
        3 => {
            flow! {
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
            }.await.map_err(|e| anyhow::anyhow!("Failed to create 3-stage flow: {:?}", e))?
        }
        5 => {
            flow! {
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
            }.await.map_err(|e| anyhow::anyhow!("Failed to create 5-stage flow: {:?}", e))?
        }
        10 => {
            flow! {
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
            }.await.map_err(|e| anyhow::anyhow!("Failed to create 10-stage flow: {:?}", e))?
        }
        _ => return Err(anyhow::anyhow!("Unsupported stage count: {}", stage_count)),
    };

    // Start the pipeline
    handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;
    
    Ok(handle)
}

/// Run a complete pipeline execution and measure total time
async fn run_execution_test(stage_count: usize) -> anyhow::Result<Duration> {
    let (journal, _temp_dir) = create_temp_journal(&format!("execution_{}_stages", stage_count)).await?;

    let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let (sink, _latencies) = TimestampedSink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let sink_clone = sink.clone();

    // Start timing BEFORE building the pipeline
    let start = Instant::now();

    // Build and run pipeline
    let handle = build_pipeline(stage_count, source, sink, journal).await?;

    // Wait for all events to be processed
    let timeout = Duration::from_secs(60);
    let wait_start = Instant::now();

    while sink_clone.received.load(Ordering::Relaxed) < WARMUP_EVENT_COUNT + TEST_EVENT_COUNT {
        if wait_start.elapsed() > timeout {
            eprintln!("WARNING: Timeout waiting for events. Received {} of {}",
                sink_clone.received.load(Ordering::Relaxed),
                WARMUP_EVENT_COUNT + TEST_EVENT_COUNT
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Stop timing after completion
    let elapsed = start.elapsed();

    Ok(elapsed)
}

/// Benchmark total execution time for different pipeline depths
/// Uses iter() to let Criterion measure the full execution timing
fn bench_total_execution_time(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("total_execution_time");

    // Configure for longer benchmarks since we're measuring actual pipeline execution
    group.sample_size(20);  // Consistent sample size across benchmarks
    group.measurement_time(Duration::from_secs(20));

    for &stage_count in STAGE_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_stages", stage_count)),
            &stage_count,
            |b, &stage_count| {
                // Use iter() to let Criterion handle the timing
                b.to_async(&rt).iter(|| async {
                    run_execution_test(stage_count).await.unwrap()
                });
            },
        );
    }

    group.finish();
}

/// Additional benchmark that shows execution time per event
/// This helps understand the amortized cost
fn bench_execution_time_per_event(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("execution_time_per_event");

    group.sample_size(20);  // Consistent sample size across benchmarks

    for &stage_count in STAGE_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{}_stages", stage_count)),
            &stage_count,
            |b, &stage_count| {
                b.to_async(&rt).iter_custom(|iters| async move {
                    let mut total_per_event = Duration::ZERO;

                    for _ in 0..iters {
                        let total_time = run_execution_test(stage_count).await.unwrap();
                        // Calculate time per event
                        let per_event = total_time / (TEST_EVENT_COUNT as u32);
                        total_per_event += per_event;
                    }

                    total_per_event
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_total_execution_time,
    bench_execution_time_per_event
);
criterion_main!(benches);

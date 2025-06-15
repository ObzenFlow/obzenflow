//! Pipeline Execution Time Benchmarks
//!
//! Measures total execution time - how long it takes to process all events
//! through the entire pipeline. This is different from per-event latency as
//! it measures overall system performance for batch processing scenarios.

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::event_sourcing::FlowHandle;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use serde_json::json;
use tokio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use ulid::Ulid;

const TEST_EVENT_COUNT: u64 = 100;
const WARMUP_EVENT_COUNT: u64 = 10;
const STAGE_COUNTS: &[usize] = &[1, 3, 5, 10, 20, 100];

/// Test source that emits events with timestamps
struct TimestampedSource {
    total_events: u64,
    emitted: AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
}

impl TimestampedSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("TimestampedSource"),
        }
    }
}

impl Step for TimestampedSource {
    type Taxonomy = RED;

    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }

    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }

    fn step_type(&self) -> StepType {
        StepType::Source
    }

    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
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
    metrics: <USE as Taxonomy>::Metrics,
}

impl PassthroughStage {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            metrics: USE::create_metrics(name),
        }
    }
}

impl Step for PassthroughStage {
    type Taxonomy = USE;

    fn taxonomy(&self) -> &Self::Taxonomy {
        &USE
    }

    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &self.metrics
    }

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
    type Taxonomy = RED;

    fn taxonomy(&self) -> &Self::Taxonomy {
        &RED
    }

    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
        &*self.metrics
    }

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

/// Create a temporary event store for benchmarking
async fn create_temp_event_store(test_name: &str) -> Result<(Arc<EventStore>, TempDir)> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join(format!("bench_{}_{}", test_name, Ulid::new()));

    let event_store = EventStore::new(EventStoreConfig {
        path: store_path,
        max_segment_size: 1024 * 1024,
    }).await?;

    Ok((event_store, temp_dir))
}

/// Build pipeline with specified stage count
async fn build_pipeline(
    stage_count: usize,
    source: TimestampedSource,
    sink: TimestampedSink,
    event_store: Arc<EventStore>
) -> Result<FlowHandle> {
    let handle = match stage_count {
        1 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, RED)
                |> ("sink" => sink, RED)
            }?
        }
        3 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, RED)
                |> ("stage1" => PassthroughStage::new("stage1"), USE)
                |> ("stage2" => PassthroughStage::new("stage2"), USE)
                |> ("sink" => sink, RED)
            }?
        }
        5 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, RED)
                |> ("stage1" => PassthroughStage::new("stage1"), USE)
                |> ("stage2" => PassthroughStage::new("stage2"), USE)
                |> ("stage3" => PassthroughStage::new("stage3"), USE)
                |> ("stage4" => PassthroughStage::new("stage4"), USE)
                |> ("sink" => sink, RED)
            }?
        }
        10 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, RED)
                |> ("stage1" => PassthroughStage::new("stage1"), USE)
                |> ("stage2" => PassthroughStage::new("stage2"), USE)
                |> ("stage3" => PassthroughStage::new("stage3"), USE)
                |> ("stage4" => PassthroughStage::new("stage4"), USE)
                |> ("stage5" => PassthroughStage::new("stage5"), USE)
                |> ("stage6" => PassthroughStage::new("stage6"), USE)
                |> ("stage7" => PassthroughStage::new("stage7"), USE)
                |> ("stage8" => PassthroughStage::new("stage8"), USE)
                |> ("stage9" => PassthroughStage::new("stage9"), USE)
                |> ("sink" => sink, RED)
            }?
        }
        20 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, RED)
                |> ("stage1" => PassthroughStage::new("stage1"), USE)
                |> ("stage2" => PassthroughStage::new("stage2"), USE)
                |> ("stage3" => PassthroughStage::new("stage3"), USE)
                |> ("stage4" => PassthroughStage::new("stage4"), USE)
                |> ("stage5" => PassthroughStage::new("stage5"), USE)
                |> ("stage6" => PassthroughStage::new("stage6"), USE)
                |> ("stage7" => PassthroughStage::new("stage7"), USE)
                |> ("stage8" => PassthroughStage::new("stage8"), USE)
                |> ("stage9" => PassthroughStage::new("stage9"), USE)
                |> ("stage10" => PassthroughStage::new("stage10"), USE)
                |> ("stage11" => PassthroughStage::new("stage11"), USE)
                |> ("stage12" => PassthroughStage::new("stage12"), USE)
                |> ("stage13" => PassthroughStage::new("stage13"), USE)
                |> ("stage14" => PassthroughStage::new("stage14"), USE)
                |> ("stage15" => PassthroughStage::new("stage15"), USE)
                |> ("stage16" => PassthroughStage::new("stage16"), USE)
                |> ("stage17" => PassthroughStage::new("stage17"), USE)
                |> ("stage18" => PassthroughStage::new("stage18"), USE)
                |> ("stage19" => PassthroughStage::new("stage19"), USE)
                |> ("sink" => sink, RED)
            }?
        }
        100 => {
            flow! {
                store: event_store,
                flow_taxonomy: GoldenSignals,
                ("source" => source, RED)
                |> ("stage1" => PassthroughStage::new("stage1"), USE)
                |> ("stage2" => PassthroughStage::new("stage2"), USE)
                |> ("stage3" => PassthroughStage::new("stage3"), USE)
                |> ("stage4" => PassthroughStage::new("stage4"), USE)
                |> ("stage5" => PassthroughStage::new("stage5"), USE)
                |> ("stage6" => PassthroughStage::new("stage6"), USE)
                |> ("stage7" => PassthroughStage::new("stage7"), USE)
                |> ("stage8" => PassthroughStage::new("stage8"), USE)
                |> ("stage9" => PassthroughStage::new("stage9"), USE)
                |> ("stage10" => PassthroughStage::new("stage10"), USE)
                |> ("stage11" => PassthroughStage::new("stage11"), USE)
                |> ("stage12" => PassthroughStage::new("stage12"), USE)
                |> ("stage13" => PassthroughStage::new("stage13"), USE)
                |> ("stage14" => PassthroughStage::new("stage14"), USE)
                |> ("stage15" => PassthroughStage::new("stage15"), USE)
                |> ("stage16" => PassthroughStage::new("stage16"), USE)
                |> ("stage17" => PassthroughStage::new("stage17"), USE)
                |> ("stage18" => PassthroughStage::new("stage18"), USE)
                |> ("stage19" => PassthroughStage::new("stage19"), USE)
                |> ("stage20" => PassthroughStage::new("stage20"), USE)
                |> ("stage21" => PassthroughStage::new("stage21"), USE)
                |> ("stage22" => PassthroughStage::new("stage22"), USE)
                |> ("stage23" => PassthroughStage::new("stage23"), USE)
                |> ("stage24" => PassthroughStage::new("stage24"), USE)
                |> ("stage25" => PassthroughStage::new("stage25"), USE)
                |> ("stage26" => PassthroughStage::new("stage26"), USE)
                |> ("stage27" => PassthroughStage::new("stage27"), USE)
                |> ("stage28" => PassthroughStage::new("stage28"), USE)
                |> ("stage29" => PassthroughStage::new("stage29"), USE)
                |> ("stage30" => PassthroughStage::new("stage30"), USE)
                |> ("stage31" => PassthroughStage::new("stage31"), USE)
                |> ("stage32" => PassthroughStage::new("stage32"), USE)
                |> ("stage33" => PassthroughStage::new("stage33"), USE)
                |> ("stage34" => PassthroughStage::new("stage34"), USE)
                |> ("stage35" => PassthroughStage::new("stage35"), USE)
                |> ("stage36" => PassthroughStage::new("stage36"), USE)
                |> ("stage37" => PassthroughStage::new("stage37"), USE)
                |> ("stage38" => PassthroughStage::new("stage38"), USE)
                |> ("stage39" => PassthroughStage::new("stage39"), USE)
                |> ("stage40" => PassthroughStage::new("stage40"), USE)
                |> ("stage41" => PassthroughStage::new("stage41"), USE)
                |> ("stage42" => PassthroughStage::new("stage42"), USE)
                |> ("stage43" => PassthroughStage::new("stage43"), USE)
                |> ("stage44" => PassthroughStage::new("stage44"), USE)
                |> ("stage45" => PassthroughStage::new("stage45"), USE)
                |> ("stage46" => PassthroughStage::new("stage46"), USE)
                |> ("stage47" => PassthroughStage::new("stage47"), USE)
                |> ("stage48" => PassthroughStage::new("stage48"), USE)
                |> ("stage49" => PassthroughStage::new("stage49"), USE)
                |> ("stage50" => PassthroughStage::new("stage50"), USE)
                |> ("stage51" => PassthroughStage::new("stage51"), USE)
                |> ("stage52" => PassthroughStage::new("stage52"), USE)
                |> ("stage53" => PassthroughStage::new("stage53"), USE)
                |> ("stage54" => PassthroughStage::new("stage54"), USE)
                |> ("stage55" => PassthroughStage::new("stage55"), USE)
                |> ("stage56" => PassthroughStage::new("stage56"), USE)
                |> ("stage57" => PassthroughStage::new("stage57"), USE)
                |> ("stage58" => PassthroughStage::new("stage58"), USE)
                |> ("stage59" => PassthroughStage::new("stage59"), USE)
                |> ("stage60" => PassthroughStage::new("stage60"), USE)
                |> ("stage61" => PassthroughStage::new("stage61"), USE)
                |> ("stage62" => PassthroughStage::new("stage62"), USE)
                |> ("stage63" => PassthroughStage::new("stage63"), USE)
                |> ("stage64" => PassthroughStage::new("stage64"), USE)
                |> ("stage65" => PassthroughStage::new("stage65"), USE)
                |> ("stage66" => PassthroughStage::new("stage66"), USE)
                |> ("stage67" => PassthroughStage::new("stage67"), USE)
                |> ("stage68" => PassthroughStage::new("stage68"), USE)
                |> ("stage69" => PassthroughStage::new("stage69"), USE)
                |> ("stage70" => PassthroughStage::new("stage70"), USE)
                |> ("stage71" => PassthroughStage::new("stage71"), USE)
                |> ("stage72" => PassthroughStage::new("stage72"), USE)
                |> ("stage73" => PassthroughStage::new("stage73"), USE)
                |> ("stage74" => PassthroughStage::new("stage74"), USE)
                |> ("stage75" => PassthroughStage::new("stage75"), USE)
                |> ("stage76" => PassthroughStage::new("stage76"), USE)
                |> ("stage77" => PassthroughStage::new("stage77"), USE)
                |> ("stage78" => PassthroughStage::new("stage78"), USE)
                |> ("stage79" => PassthroughStage::new("stage79"), USE)
                |> ("stage80" => PassthroughStage::new("stage80"), USE)
                |> ("stage81" => PassthroughStage::new("stage81"), USE)
                |> ("stage82" => PassthroughStage::new("stage82"), USE)
                |> ("stage83" => PassthroughStage::new("stage83"), USE)
                |> ("stage84" => PassthroughStage::new("stage84"), USE)
                |> ("stage85" => PassthroughStage::new("stage85"), USE)
                |> ("stage86" => PassthroughStage::new("stage86"), USE)
                |> ("stage87" => PassthroughStage::new("stage87"), USE)
                |> ("stage88" => PassthroughStage::new("stage88"), USE)
                |> ("stage89" => PassthroughStage::new("stage89"), USE)
                |> ("stage90" => PassthroughStage::new("stage90"), USE)
                |> ("stage91" => PassthroughStage::new("stage91"), USE)
                |> ("stage92" => PassthroughStage::new("stage92"), USE)
                |> ("stage93" => PassthroughStage::new("stage93"), USE)
                |> ("stage94" => PassthroughStage::new("stage94"), USE)
                |> ("stage95" => PassthroughStage::new("stage95"), USE)
                |> ("stage96" => PassthroughStage::new("stage96"), USE)
                |> ("stage97" => PassthroughStage::new("stage97"), USE)
                |> ("stage98" => PassthroughStage::new("stage98"), USE)
                |> ("stage99" => PassthroughStage::new("stage99"), USE)
                |> ("sink" => sink, RED)
            }?
        }
        _ => return Err(format!("Unsupported stage count: {}", stage_count).into()),
    };

    Ok(handle)
}

/// Run a complete pipeline execution and measure total time
async fn run_execution_test(stage_count: usize) -> Result<Duration> {
    let (event_store, _temp_dir) = create_temp_event_store(&format!("execution_{}_stages", stage_count)).await?;

    let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let (sink, _latencies) = TimestampedSink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let sink_clone = sink.clone();

    // Start timing BEFORE building the pipeline
    let start = Instant::now();

    // Build and run pipeline
    let handle = build_pipeline(stage_count, source, sink, event_store).await?;

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

    // Stop timing after shutdown
    handle.shutdown().await?;
    let elapsed = start.elapsed();

    Ok(elapsed)
}

/// Benchmark total execution time for different pipeline depths
/// Uses iter() to let Criterion measure the full execution timing
fn bench_total_execution_time(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("total_execution_time");

    // Configure for longer benchmarks since we're measuring actual pipeline execution
    group.sample_size(10);
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

    group.sample_size(10);

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

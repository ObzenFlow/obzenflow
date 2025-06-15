//! Tokio Worker Thread Experiment for 3-Stage Pipelines
//!
//! This benchmark investigates tokio scheduler behavior, NOT FlowState performance.
//! It tests the hypothesis that tokio's default 4 worker threads create performance
//! anomalies for pipelines with 3-4 concurrent tasks.
//! 
//! A 3-stage pipeline has 4 tasks: source + stage1 + stage2 + sink = 4 tasks
//! With 4 worker threads, this creates a 1:1 task-to-worker ratio.
//!
//! This benchmark tests different worker thread configurations to understand
//! the tokio scheduler's behavior with small task counts.

use criterion::{criterion_group, criterion_main, Criterion};
use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use serde_json::json;
use tokio::runtime::{Runtime, Builder};
use tempfile::tempdir;
use ulid::Ulid;

const WARMUP_EVENT_COUNT: u64 = 10;
const TEST_EVENT_COUNT: u64 = 100;

/// Test source that emits timestamped events
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

    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Source }

    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
            vec![ChainEvent::new("TimestampedEvent", json!({
                "event_id": current,
                "emit_time_nanos": SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos() as u64,
            }))]
        } else {
            vec![]
        }
    }
}

/// Passthrough stage
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

    fn taxonomy(&self) -> &Self::Taxonomy { &USE }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Stage }

    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event]
    }
}

/// Sink that collects latencies
#[derive(Clone)]
struct LatencySink {
    expected_count: u64,
    received: Arc<AtomicU64>,
    latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
    metrics: Arc<<RED as Taxonomy>::Metrics>,
}

impl LatencySink {
    fn new(expected_count: u64) -> (Self, Arc<tokio::sync::Mutex<Vec<Duration>>>) {
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(expected_count as usize)));
        (Self {
            expected_count,
            received: Arc::new(AtomicU64::new(0)),
            latencies: latencies.clone(),
            metrics: Arc::new(RED::create_metrics("LatencySink")),
        }, latencies)
    }
}

impl Step for LatencySink {
    type Taxonomy = RED;

    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &*self.metrics }
    fn step_type(&self) -> StepType { StepType::Sink }

    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if let (Some(emit_time_nanos), Some(event_id)) = (
            event.payload.get("emit_time_nanos").and_then(|v| v.as_u64()),
            event.payload.get("event_id").and_then(|v| v.as_u64())
        ) {
            self.received.fetch_add(1, Ordering::Relaxed);
            
            // Skip warmup events
            if event_id >= WARMUP_EVENT_COUNT {
                let receive_time_nanos = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
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

/// Run a single 3-stage pipeline test with custom runtime
async fn run_3_stage_pipeline_with_runtime(runtime_name: &str, rt: Arc<Runtime>) -> Result<Duration> {
    let handle = rt.handle();
    let runtime_name = runtime_name.to_string();
    
    Ok(handle.spawn(async move {
        let temp_dir = tempdir().unwrap();
        let store_path = temp_dir.path().join(format!("three_stage_{}_{}", runtime_name, Ulid::new()));
        
        let event_store = EventStore::new(EventStoreConfig {
            path: store_path,
            max_segment_size: 1024 * 1024,
        }).await.unwrap();

        let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
        let (sink, latencies) = LatencySink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
        let sink_clone = sink.clone();

        let handle = flow! {
            store: event_store,
            flow_taxonomy: GoldenSignals,
            ("source" => source, RED)
            |> ("stage1" => PassthroughStage::new("stage1"), USE)
            |> ("stage2" => PassthroughStage::new("stage2"), USE)
            |> ("sink" => sink, RED)
        }.unwrap();

        // Wait for completion
        let timeout = Duration::from_secs(30);
        let start = Instant::now();

        while sink_clone.received.load(Ordering::Relaxed) < WARMUP_EVENT_COUNT + TEST_EVENT_COUNT {
            if start.elapsed() > timeout {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        handle.shutdown().await.unwrap();

        // Calculate median latency
        let mut collected = latencies.lock().await.clone();
        if collected.is_empty() {
            return Duration::ZERO;
        }
        
        collected.sort();
        collected[collected.len() / 2]
    }).await.unwrap())
}

fn bench_3_stage_with_different_workers(c: &mut Criterion) {
    let mut group = c.benchmark_group("3_stage_worker_experiments");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(30));

    // Test 1: Default 4 workers (expecting worst performance - 1:1 ratio)
    let rt_4_workers = Arc::new(
        Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("flowstate-4w")
            .enable_all()
            .build()
            .unwrap()
    );

    group.bench_function("4_workers_1to1_ratio", |b| {
        let rt = rt_4_workers.clone();
        b.to_async(rt.as_ref()).iter_custom(|iters| {
            let rt = rt.clone();
            async move {
                let mut total_latency = Duration::ZERO;
                
                for _ in 0..iters {
                    let median = run_3_stage_pipeline_with_runtime("4w", rt.clone()).await.unwrap();
                    total_latency = total_latency.saturating_add(median);
                }
                
                total_latency
            }
        });
    });

    // Test 2: 3 workers (avoiding 1:1 ratio)
    let rt_3_workers = Arc::new(
        Builder::new_multi_thread()
            .worker_threads(3)
            .thread_name("flowstate-3w")
            .enable_all()
            .build()
            .unwrap()
    );

    group.bench_function("3_workers_avoid_ratio", |b| {
        let rt = rt_3_workers.clone();
        b.to_async(rt.as_ref()).iter_custom(|iters| {
            let rt = rt.clone();
            async move {
                let mut total_latency = Duration::ZERO;
                
                for _ in 0..iters {
                    let median = run_3_stage_pipeline_with_runtime("3w", rt.clone()).await.unwrap();
                    total_latency = total_latency.saturating_add(median);
                }
                
                total_latency
            }
        });
    });

    // Test 3: 6 workers (more than tasks)
    let rt_6_workers = Arc::new(
        Builder::new_multi_thread()
            .worker_threads(6)
            .thread_name("flowstate-6w")
            .enable_all()
            .build()
            .unwrap()
    );

    group.bench_function("6_workers_excess", |b| {
        let rt = rt_6_workers.clone();
        b.to_async(rt.as_ref()).iter_custom(|iters| {
            let rt = rt.clone();
            async move {
                let mut total_latency = Duration::ZERO;
                
                for _ in 0..iters {
                    let median = run_3_stage_pipeline_with_runtime("6w", rt.clone()).await.unwrap();
                    total_latency = total_latency.saturating_add(median);
                }
                
                total_latency
            }
        });
    });

    // Test 4: Single-threaded runtime (no work-stealing)
    let rt_single = Arc::new(
        Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    );

    group.bench_function("single_threaded", |b| {
        let rt = rt_single.clone();
        b.to_async(rt.as_ref()).iter_custom(|iters| {
            let rt = rt.clone();
            async move {
                let mut total_latency = Duration::ZERO;
                
                for _ in 0..iters {
                    let median = run_3_stage_pipeline_with_runtime("1t", rt.clone()).await.unwrap();
                    total_latency = total_latency.saturating_add(median);
                }
                
                total_latency
            }
        });
    });

    group.finish();
}

fn bench_5_stage_control(c: &mut Criterion) {
    // Control: Run 5-stage pipeline with default runtime to compare
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("5_stage_control");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(30));

    group.bench_function("default_runtime", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total_latency = Duration::ZERO;
            
            for _ in 0..iters {
                let temp_dir = tempdir().unwrap();
                let store_path = temp_dir.path().join(format!("five_stage_control_{}", Ulid::new()));
                
                let event_store = EventStore::new(EventStoreConfig {
                    path: store_path,
                    max_segment_size: 1024 * 1024,
                }).await.unwrap();

                let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
                let (sink, latencies) = LatencySink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
                let sink_clone = sink.clone();

                let handle = flow! {
                    store: event_store,
                    flow_taxonomy: GoldenSignals,
                    ("source" => source, RED)
                    |> ("stage1" => PassthroughStage::new("stage1"), USE)
                    |> ("stage2" => PassthroughStage::new("stage2"), USE)
                    |> ("stage3" => PassthroughStage::new("stage3"), USE)
                    |> ("stage4" => PassthroughStage::new("stage4"), USE)
                    |> ("sink" => sink, RED)
                }.unwrap();

                // Wait for completion
                let timeout = Duration::from_secs(30);
                let start = Instant::now();

                while sink_clone.received.load(Ordering::Relaxed) < WARMUP_EVENT_COUNT + TEST_EVENT_COUNT {
                    if start.elapsed() > timeout {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                handle.shutdown().await.unwrap();

                // Calculate median latency
                let mut collected = latencies.lock().await.clone();
                if !collected.is_empty() {
                    collected.sort();
                    let median = collected[collected.len() / 2];
                    total_latency = total_latency.saturating_add(median);
                }
            }
            
            total_latency
        });
    });

    group.finish();
}

criterion_group!(benches, bench_3_stage_with_different_workers, bench_5_stage_control);
criterion_main!(benches);
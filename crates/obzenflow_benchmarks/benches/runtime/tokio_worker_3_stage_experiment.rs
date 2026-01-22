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
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::WriterId;
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime_services::stages::SourceError;
// Monitoring removed per FLOWIP-056-666
use async_trait::async_trait;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use tokio::runtime::{Builder, Runtime};

const WARMUP_EVENT_COUNT: u64 = 10;
const TEST_EVENT_COUNT: u64 = 100;

/// Test source that emits timestamped events
#[derive(Clone, Debug)]
struct TimestampedSource {
    total_events: u64,
    emitted: Arc<AtomicU64>,
    writer_id: WriterId,
}

impl TimestampedSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: Arc::new(AtomicU64::new(0)),
            writer_id: WriterId::from(obzenflow_core::StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TimestampedSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                "TimestampedEvent",
                json!({
                    "event_id": current,
                    "emit_time_nanos": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64,
                }),
            )]))
        } else {
            Ok(None)
        }
    }
}

/// Passthrough stage
#[derive(Clone, Debug)]
struct PassthroughStage;

impl PassthroughStage {
    fn new(_name: &str) -> Self {
        Self
    }
}

#[async_trait]
impl TransformHandler for PassthroughStage {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Sink that collects latencies
#[derive(Clone, Debug)]
struct LatencySink {
    received: Arc<AtomicU64>,
    latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
}

impl LatencySink {
    fn new(expected_count: u64) -> (Self, Arc<tokio::sync::Mutex<Vec<Duration>>>) {
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
            expected_count as usize,
        )));
        (
            Self {
                received: Arc::new(AtomicU64::new(0)),
                latencies: latencies.clone(),
            },
            latencies,
        )
    }
}

#[async_trait]
impl SinkHandler for LatencySink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if let ChainEventContent::Data { payload, .. } = &event.content {
            if let (Some(emit_time_nanos), Some(event_id)) = (
                payload.get("emit_time_nanos").and_then(|v| v.as_u64()),
                payload.get("event_id").and_then(|v| v.as_u64()),
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
                        self.latencies.lock().await.push(latency);
                    }
                }
            }
        }

        Ok(DeliveryPayload::success("noop", DeliveryMethod::Noop, None))
    }
}

/// Run a single 3-stage pipeline test with custom runtime
async fn run_3_stage_pipeline_with_runtime(
    runtime_name: &str,
    rt: Arc<Runtime>,
) -> anyhow::Result<Duration> {
    let handle = rt.handle();
    let runtime_name = runtime_name.to_string();

    Ok(handle
        .spawn(async move {
            let temp_dir = tempdir().unwrap();
            let journals_base_path = temp_dir.path().join(format!("three_stage_{runtime_name}"));
            std::fs::create_dir_all(&journals_base_path).unwrap();

            let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
            let (sink, latencies) = LatencySink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
            let sink_clone = sink.clone();

            let handle = flow! {
                journals: disk_journals(journals_base_path),
                middleware: [],

                stages: {
                    src = source!("source" => source);
                    s1 = transform!("stage1" => PassthroughStage::new("stage1"));
                    s2 = transform!("stage2" => PassthroughStage::new("stage2"));
                    snk = sink!("sink" => sink);
                },

                topology: {
                    src |> s1;
                    s1 |> s2;
                    s2 |> snk;
                }
            }
            .await
            .unwrap();

            // Start the pipeline
            handle.run().await.unwrap();

            // Wait for completion
            let timeout = Duration::from_secs(60); // Increased timeout for runtime experiments
            let start = Instant::now();

            while sink_clone.received.load(Ordering::Relaxed)
                < WARMUP_EVENT_COUNT + TEST_EVENT_COUNT
            {
                if start.elapsed() > timeout {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            // Pipeline runs to completion

            // Calculate median latency
            let mut collected = latencies.lock().await.clone();
            if collected.is_empty() {
                return Duration::ZERO;
            }

            collected.sort();
            collected[collected.len() / 2]
        })
        .await
        .unwrap())
}

fn bench_3_stage_with_different_workers(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    let mut group = c.benchmark_group("3_stage_worker_experiments");
    group.sample_size(10); // Reduced to minimum for faster benchmarking
    group.measurement_time(Duration::from_secs(45)); // Increased for runtime stability

    // Test 1: Default 4 workers (expecting worst performance - 1:1 ratio)
    let rt_4_workers = Arc::new(
        Builder::new_multi_thread()
            .worker_threads(4)
            .thread_name("flowstate-4w")
            .enable_all()
            .build()
            .unwrap(),
    );

    group.bench_function("4_workers_1to1_ratio", |b| {
        let rt = rt_4_workers.clone();
        b.to_async(rt.as_ref()).iter_custom(|iters| {
            let rt = rt.clone();
            async move {
                let mut total_latency = Duration::ZERO;

                for _ in 0..iters {
                    let median = run_3_stage_pipeline_with_runtime("4w", rt.clone())
                        .await
                        .unwrap();
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
            .unwrap(),
    );

    group.bench_function("3_workers_avoid_ratio", |b| {
        let rt = rt_3_workers.clone();
        b.to_async(rt.as_ref()).iter_custom(|iters| {
            let rt = rt.clone();
            async move {
                let mut total_latency = Duration::ZERO;

                for _ in 0..iters {
                    let median = run_3_stage_pipeline_with_runtime("3w", rt.clone())
                        .await
                        .unwrap();
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
            .unwrap(),
    );

    group.bench_function("6_workers_excess", |b| {
        let rt = rt_6_workers.clone();
        b.to_async(rt.as_ref()).iter_custom(|iters| {
            let rt = rt.clone();
            async move {
                let mut total_latency = Duration::ZERO;

                for _ in 0..iters {
                    let median = run_3_stage_pipeline_with_runtime("6w", rt.clone())
                        .await
                        .unwrap();
                    total_latency = total_latency.saturating_add(median);
                }

                total_latency
            }
        });
    });

    // Test 4: Single-threaded runtime (no work-stealing)
    let rt_single = Arc::new(Builder::new_current_thread().enable_all().build().unwrap());

    group.bench_function("single_threaded", |b| {
        let rt = rt_single.clone();
        b.to_async(rt.as_ref()).iter_custom(|iters| {
            let rt = rt.clone();
            async move {
                let mut total_latency = Duration::ZERO;

                for _ in 0..iters {
                    let median = run_3_stage_pipeline_with_runtime("1t", rt.clone())
                        .await
                        .unwrap();
                    total_latency = total_latency.saturating_add(median);
                }

                total_latency
            }
        });
    });

    group.finish();
}

fn bench_5_stage_control(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    // Control: Run 5-stage pipeline with default runtime to compare
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("5_stage_control");
    group.sample_size(10); // Minimum required by Criterion
    group.measurement_time(Duration::from_secs(45)); // Increased for 5-stage stability

    group.bench_function("default_runtime", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total_latency = Duration::ZERO;

            for _ in 0..iters {
                let temp_dir = tempdir().unwrap();
                let journals_base_path = temp_dir.path().join("five_stage_control");
                std::fs::create_dir_all(&journals_base_path).unwrap();

                let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
                let (sink, latencies) = LatencySink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
                let sink_clone = sink.clone();

                let handle = flow! {
                    journals: disk_journals(journals_base_path),
                    middleware: [],

                    stages: {
                        src = source!("source" => source);
                        s1 = transform!("stage1" => PassthroughStage::new("stage1"));
                        s2 = transform!("stage2" => PassthroughStage::new("stage2"));
                        s3 = transform!("stage3" => PassthroughStage::new("stage3"));
                        s4 = transform!("stage4" => PassthroughStage::new("stage4"));
                        snk = sink!("sink" => sink);
                    },

                    topology: {
                        src |> s1;
                        s1 |> s2;
                        s2 |> s3;
                        s3 |> s4;
                        s4 |> snk;
                    }
                }
                .await
                .unwrap();

                // Start the pipeline
                handle.run().await.unwrap();

                // Wait for completion
                let timeout = Duration::from_secs(60); // Increased timeout for 5-stage control
                let start = Instant::now();

                while sink_clone.received.load(Ordering::Relaxed)
                    < WARMUP_EVENT_COUNT + TEST_EVENT_COUNT
                {
                    if start.elapsed() > timeout {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }

                // Pipeline runs to completion

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

criterion_group!(
    benches,
    bench_3_stage_with_different_workers,
    bench_5_stage_control
);
criterion_main!(benches);

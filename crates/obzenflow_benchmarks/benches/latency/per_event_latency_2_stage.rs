//! Isolated 2-Stage Pipeline Latency Benchmark
//!
//! This is a completely standalone benchmark for ONLY 2-stage pipelines.
//! No other configurations, no shared code with other stage counts.
//! This isolation helps determine if the performance anomaly is due to
//! benchmark ordering, warmup effects, or genuine framework issues.

use criterion::{criterion_group, criterion_main, Criterion};
use obzenflow_benchmarks::prelude::*;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::DiskJournal;
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
use tokio::runtime::Runtime;

const WARMUP_EVENT_COUNT: u64 = 10;
const TEST_EVENT_COUNT: u64 = 100;

/// Test source that emits timestamped events
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
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
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

/// Sink that collects latencies
#[derive(Clone)]
struct LatencySink {
    expected_count: u64,
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
                expected_count,
                received: Arc::new(AtomicU64::new(0)),
                latencies: latencies.clone(),
            },
            latencies,
        )
    }
}

#[async_trait]
impl SinkHandler for LatencySink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        if let (Some(emit_time_nanos), Some(event_id)) = (
            event
                .payload
                .get("emit_time_nanos")
                .and_then(|v| v.as_u64()),
            event.payload.get("event_id").and_then(|v| v.as_u64()),
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
        Ok(())
    }
}

/// Run a single 2-stage pipeline test
async fn run_2_stage_pipeline() -> anyhow::Result<Duration> {
    let temp_dir = tempdir()?;
    let journal_path = temp_dir.path().join(format!(
        "two_stage_{}",
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&journal_path)?;

    let journal = Arc::new(DiskJournal::new(journal_path, "benchmark_2_stage").await?);

    let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let (sink, latencies) = LatencySink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let sink_clone = sink.clone();

    let handle = flow! {
        journal: journal,
        middleware: [],

        stages: {
            src = source!("source" => source);
            s1 = transform!("stage1" => PassthroughStage::new("stage1"));
            snk = sink!("sink" => sink);
        },

        topology: {
            src |> s1;
            s1 |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    // Start the pipeline
    handle
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;

    // Wait for completion
    let timeout = Duration::from_secs(30);
    let start = Instant::now();

    while sink_clone.received.load(Ordering::Relaxed) < WARMUP_EVENT_COUNT + TEST_EVENT_COUNT {
        if start.elapsed() > timeout {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Pipeline runs to completion

    // Calculate median latency
    let mut collected = latencies.lock().await.clone();
    if collected.is_empty() {
        return Ok(Duration::ZERO);
    }

    collected.sort();
    Ok(collected[collected.len() / 2])
}

fn bench_2_stage_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("2_stage_latency");

    group.sample_size(20); // Reduced for faster benchmarking
    group.measurement_time(Duration::from_secs(30)); // Longer measurement

    group.bench_function("median_latency", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total_latency = Duration::ZERO;

            for _ in 0..iters {
                let median = run_2_stage_pipeline().await.unwrap();
                total_latency = total_latency.saturating_add(median);
            }

            total_latency
        });
    });

    group.finish();
}

criterion_group!(benches, bench_2_stage_latency);
criterion_main!(benches);

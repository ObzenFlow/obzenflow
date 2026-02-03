// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline Throughput Benchmarks
//!
//! Measures sustained event processing rate (events per second) across
//! different pipeline depths. This is critical for understanding capacity
//! limits and how pipeline complexity affects streaming performance.

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use obzenflow_benchmarks::prelude::*;
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
use obzenflow_runtime_services::supervised_base::SupervisorHandle;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};
use tokio::runtime::Runtime;

const STAGE_COUNTS: &[usize] = &[1, 3, 5, 10]; // Simplified for maintainability

/// Configuration for throughput testing
const THROUGHPUT_EVENT_COUNT: u64 = 1000; // More events for accurate throughput measurement
const THROUGHPUT_WARMUP: u64 = 100;

/// Test source that emits events with timestamps
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
            let emit_time_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                "TimestampedEvent",
                json!({
                    "index": current,
                    "emit_time_nanos": emit_time_nanos,
                }),
            )]))
        } else {
            Ok(None)
        }
    }
}

/// Passthrough stage that just forwards events
#[derive(Clone, Debug)]
struct PassthroughStage {}

impl PassthroughStage {
    fn new(_name: &str) -> Self {
        Self {}
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

/// Sink that records latencies
#[derive(Clone, Debug)]
struct TimestampedSink {
    received: Arc<AtomicU64>,
    latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
}

impl TimestampedSink {
    fn new(expected_count: u64) -> (Self, Arc<tokio::sync::Mutex<Vec<Duration>>>) {
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
            expected_count as usize,
        )));
        let received = Arc::new(AtomicU64::new(0));
        (
            Self {
                received: received.clone(),
                latencies: latencies.clone(),
            },
            latencies,
        )
    }
}

#[async_trait]
impl SinkHandler for TimestampedSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if let ChainEventContent::Data { payload, .. } = &event.content {
            if let (Some(emit_time_nanos), Some(index)) = (
                payload.get("emit_time_nanos").and_then(|v| v.as_u64()),
                payload.get("index").and_then(|v| v.as_u64()),
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
                        self.latencies.lock().await.push(latency);
                    }
                }
            }
        }

        Ok(DeliveryPayload::success("noop", DeliveryMethod::Noop, None))
    }
}

/// Create a temporary journal for benchmarking
fn create_temp_journals_base(test_name: &str) -> anyhow::Result<(std::path::PathBuf, TempDir)> {
    let temp_dir = tempdir()?;
    let journal_path = temp_dir.path().join(format!(
        "bench_{}_{}",
        test_name,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&journal_path)?;
    Ok((journal_path, temp_dir))
}

/// Build pipeline with specified stage count
async fn build_pipeline(
    stage_count: usize,
    source: TimestampedSource,
    sink: TimestampedSink,
    journals_base_path: std::path::PathBuf,
) -> anyhow::Result<FlowHandle> {
    let handle = match stage_count {
        1 => flow! {
            journals: disk_journals(journals_base_path.clone()),
            middleware: [],

            stages: {
                src = source!("source" => source);
                snk = sink!("sink" => sink);
            },

            topology: {
                src |> snk;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create 1-stage flow: {e:?}"))?,
        3 => flow! {
            journals: disk_journals(journals_base_path.clone()),
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
        .map_err(|e| anyhow::anyhow!("Failed to create 3-stage flow: {e:?}"))?,
        5 => flow! {
            journals: disk_journals(journals_base_path.clone()),
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
        .map_err(|e| anyhow::anyhow!("Failed to create 5-stage flow: {e:?}"))?,
        10 => {
            // For larger pipelines, build stages programmatically
            let handle = flow! {
                journals: disk_journals(journals_base_path.clone()),
                middleware: [],

                stages: {
                    src = source!("source" => source);
                    s1 = transform!("stage1" => PassthroughStage::new("stage1"));
                    s2 = transform!("stage2" => PassthroughStage::new("stage2"));
                    s3 = transform!("stage3" => PassthroughStage::new("stage3"));
                    s4 = transform!("stage4" => PassthroughStage::new("stage4"));
                    s5 = transform!("stage5" => PassthroughStage::new("stage5"));
                    s6 = transform!("stage6" => PassthroughStage::new("stage6"));
                    s7 = transform!("stage7" => PassthroughStage::new("stage7"));
                    s8 = transform!("stage8" => PassthroughStage::new("stage8"));
                    s9 = transform!("stage9" => PassthroughStage::new("stage9"));
                    snk = sink!("sink" => sink);
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
            }
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create 10-stage flow: {e:?}"))?;
            handle
        }
        _ => return Err(anyhow::anyhow!("Unsupported stage count: {stage_count}")),
    };

    handle
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start pipeline: {e:?}"))?;

    Ok(handle)
}

/// Run throughput test for a specific pipeline depth
async fn run_throughput_test(stage_count: usize) -> anyhow::Result<f64> {
    let test_name = format!("throughput_{stage_count}_stages");
    let (journals_base_path, _temp_dir) = create_temp_journals_base(&test_name)?;

    let total_events = THROUGHPUT_WARMUP + THROUGHPUT_EVENT_COUNT;
    let source = TimestampedSource::new(total_events);
    let (sink, _latencies) = TimestampedSink::new(total_events);
    let sink_clone = sink.clone();

    // Build pipeline
    let handle = build_pipeline(stage_count, source, sink, journals_base_path).await?;

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
            eprintln!(
                "WARNING: Timeout waiting for events. Received {} of {}",
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

    // Ensure pipeline has terminated before returning (avoids interference between iterations).
    handle.wait_for_completion().await?;

    Ok(throughput)
}

/// Benchmark throughput across different pipeline depths
fn bench_throughput(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("throughput");

    // Configure for throughput measurement
    group.throughput(Throughput::Elements(THROUGHPUT_EVENT_COUNT));
    group.sample_size(20); // Consistent sample size across benchmarks
    group.measurement_time(Duration::from_secs(30));

    for &stage_count in STAGE_COUNTS {
        group.bench_with_input(
            BenchmarkId::new("events_per_second", format!("{stage_count}_stages")),
            &stage_count,
            |b, &stage_count| {
                b.to_async(&rt)
                    .iter(|| async { run_throughput_test(stage_count).await.unwrap() });
            },
        );
    }

    group.finish();
}

/// Benchmark time per event (inverse of throughput) for different perspectives
fn bench_time_per_event(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("time_per_event");

    group.sample_size(20); // Consistent sample size across benchmarks

    for &stage_count in STAGE_COUNTS {
        group.bench_with_input(
            BenchmarkId::from_parameter(format!("{stage_count}_stages")),
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
    obzenflow_benchmarks::init_tracing();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("relative_throughput");

    group.sample_size(20); // Consistent sample size across benchmarks

    // Get baseline throughput for single stage
    let baseline_throughput = rt.block_on(async { run_throughput_test(1).await.unwrap() });

    for &stage_count in STAGE_COUNTS {
        group.bench_with_input(
            BenchmarkId::new("percentage_of_baseline", format!("{stage_count}_stages")),
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

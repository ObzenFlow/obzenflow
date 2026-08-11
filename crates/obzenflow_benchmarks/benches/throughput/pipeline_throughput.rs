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
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkDeliveryDeclaration, SinkInputContext, SinkTerminalOutcome, TypedFiniteSourceHandler,
    TypedSinkConsumeReport, TypedSinkHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::{tempdir, TempDir};
use tokio::runtime::Runtime;

const STAGE_COUNTS: &[usize] = &[1, 3, 5, 10]; // Simplified for maintainability

/// Configuration for throughput testing
const THROUGHPUT_EVENT_COUNT: u64 = 1000; // More events for accurate throughput measurement
const THROUGHPUT_WARMUP: u64 = 100;

/// File-local payload type for the throughput bench. The JSON shape matches
/// what `TimestampedSource` emits; the type itself is a FLOWIP-114c
/// topology fingerprint, not enforced at runtime.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BenchEvent {
    index: u64,
    emit_time_nanos: u64,
}

impl TypedPayload for BenchEvent {
    const EVENT_TYPE: &'static str = "bench.throughput_event";
}

/// Test source that emits events with timestamps
#[derive(Clone, Debug)]
struct TimestampedSource {
    total_events: u64,
    emitted: Arc<AtomicU64>,
}

impl TimestampedSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl TypedFiniteSourceHandler for TimestampedSource {
    type Output = BenchEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
            let emit_time_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            Ok(Some(vec![BenchEvent {
                index: current,
                emit_time_nanos,
            }]))
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

impl TypedTransformHandler for PassthroughStage {
    type Input = BenchEvent;
    type Output = BenchEvent;

    fn process(&self, event: BenchEvent) -> Result<BenchEvent, HandlerError> {
        Ok(event)
    }
}

/// Sink that records latencies
#[derive(Clone, Debug)]
struct TimestampedSink {
    received: Arc<AtomicU64>,
    latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
}

#[async_trait]
impl TypedSinkHandler for TimestampedSink {
    type Input = BenchEvent;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        SinkDeliveryDeclaration::undeclared()
    }

    async fn consume(
        &mut self,
        event: BenchEvent,
        _context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError> {
        self.received.fetch_add(1, Ordering::Relaxed);
        if event.index >= THROUGHPUT_WARMUP {
            let receive_time_nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;
            if receive_time_nanos > event.emit_time_nanos {
                let latency = Duration::from_nanos(receive_time_nanos - event.emit_time_nanos);
                self.latencies.lock().await.push(latency);
            }
        }

        Ok(TypedSinkConsumeReport::terminal(
            SinkTerminalOutcome::success(DeliveryMethod::Noop, None),
        ))
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
    total_events: u64,
    received: Arc<AtomicU64>,
    latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
    journals_base_path: std::path::PathBuf,
) -> anyhow::Result<FlowHandle> {
    let handle = match stage_count {
        1 => FlowDefinition::materialize(move |_runtime_config| {
            let source = TimestampedSource::new(total_events);
            let sink = TimestampedSink {
                received,
                latencies,
            };

            Ok(flow! {
                journals: disk_journals(journals_base_path.clone()),

                stages: {
                    src = source!(BenchEvent => source);
                    snk = sink!(BenchEvent => sink);
                },

                topology: {
                    src |> snk;
                }
            })
        })
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create 1-stage flow: {e:?}"))?,
        3 => FlowDefinition::materialize(move |_runtime_config| {
            let source = TimestampedSource::new(total_events);
            let sink = TimestampedSink {
                received,
                latencies,
            };
            let stage1_handler = PassthroughStage::new("stage1");
            let stage2_handler = PassthroughStage::new("stage2");

            Ok(flow! {
                journals: disk_journals(journals_base_path.clone()),

                stages: {
                    src = source!(BenchEvent => source);
                    s1 = transform!(BenchEvent -> BenchEvent => stage1_handler);
                    s2 = transform!(BenchEvent -> BenchEvent => stage2_handler);
                    snk = sink!(BenchEvent => sink);
                },

                topology: {
                    src |> s1;
                    s1 |> s2;
                    s2 |> snk;
                }
            })
        })
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create 3-stage flow: {e:?}"))?,
        5 => FlowDefinition::materialize(move |_runtime_config| {
            let source = TimestampedSource::new(total_events);
            let sink = TimestampedSink {
                received,
                latencies,
            };
            let stage1_handler = PassthroughStage::new("stage1");
            let stage2_handler = PassthroughStage::new("stage2");
            let stage3_handler = PassthroughStage::new("stage3");
            let stage4_handler = PassthroughStage::new("stage4");

            Ok(flow! {
                journals: disk_journals(journals_base_path.clone()),

                stages: {
                    src = source!(BenchEvent => source);
                    s1 = transform!(BenchEvent -> BenchEvent => stage1_handler);
                    s2 = transform!(BenchEvent -> BenchEvent => stage2_handler);
                    s3 = transform!(BenchEvent -> BenchEvent => stage3_handler);
                    s4 = transform!(BenchEvent -> BenchEvent => stage4_handler);
                    snk = sink!(BenchEvent => sink);
                },

                topology: {
                    src |> s1;
                    s1 |> s2;
                    s2 |> s3;
                    s3 |> s4;
                    s4 |> snk;
                }
            })
        })
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create 5-stage flow: {e:?}"))?,
        10 => {
            // For larger pipelines, build stages programmatically
            let handle = FlowDefinition::materialize(move |_runtime_config| {
                let source = TimestampedSource::new(total_events);
                let sink = TimestampedSink {
                    received,
                    latencies,
                };
                let stage1_handler = PassthroughStage::new("stage1");
                let stage2_handler = PassthroughStage::new("stage2");
                let stage3_handler = PassthroughStage::new("stage3");
                let stage4_handler = PassthroughStage::new("stage4");
                let stage5_handler = PassthroughStage::new("stage5");
                let stage6_handler = PassthroughStage::new("stage6");
                let stage7_handler = PassthroughStage::new("stage7");
                let stage8_handler = PassthroughStage::new("stage8");
                let stage9_handler = PassthroughStage::new("stage9");

                Ok(flow! {
                    journals: disk_journals(journals_base_path.clone()),

                    stages: {
                        src = source!(BenchEvent => source);
                        s1 = transform!(BenchEvent -> BenchEvent => stage1_handler);
                        s2 = transform!(BenchEvent -> BenchEvent => stage2_handler);
                        s3 = transform!(BenchEvent -> BenchEvent => stage3_handler);
                        s4 = transform!(BenchEvent -> BenchEvent => stage4_handler);
                        s5 = transform!(BenchEvent -> BenchEvent => stage5_handler);
                        s6 = transform!(BenchEvent -> BenchEvent => stage6_handler);
                        s7 = transform!(BenchEvent -> BenchEvent => stage7_handler);
                        s8 = transform!(BenchEvent -> BenchEvent => stage8_handler);
                        s9 = transform!(BenchEvent -> BenchEvent => stage9_handler);
                        snk = sink!(BenchEvent => sink);
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
                })
            })
            .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
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
    let received = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
        total_events as usize,
    )));

    // Build pipeline
    let handle = build_pipeline(
        stage_count,
        total_events,
        Arc::clone(&received),
        latencies,
        journals_base_path,
    )
    .await?;

    // Start timing after warmup events
    let timeout = Duration::from_secs(60);
    let start_time = Instant::now();

    // Wait for warmup
    while received.load(Ordering::Relaxed) < THROUGHPUT_WARMUP {
        if start_time.elapsed() > timeout {
            eprintln!("WARNING: Timeout during warmup");
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    // Now measure throughput timing
    let measurement_start = Instant::now();
    let measurement_start_count = received.load(Ordering::Relaxed);

    // Wait for all events
    while received.load(Ordering::Relaxed) < total_events {
        if start_time.elapsed() > timeout {
            eprintln!(
                "WARNING: Timeout waiting for events. Received {} of {}",
                received.load(Ordering::Relaxed),
                total_events
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let measurement_elapsed = measurement_start.elapsed();
    let events_processed = received.load(Ordering::Relaxed) - measurement_start_count;

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

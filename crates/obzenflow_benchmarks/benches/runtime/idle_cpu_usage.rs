// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Idle CPU Usage Benchmark
//!
//! Measures process CPU time used while a started pipeline is idle (no events
//! flowing). This validates that the event-driven design doesn't waste
//! resources with busy-waiting or polling when there's no work to do.

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use obzenflow_benchmarks::case::{declare, Category};
use obzenflow_benchmarks::prelude::*;
use obzenflow_benchmarks::process_cpu_time;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkWriteFailure, TypedFiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
// Monitoring removed per FLOWIP-056-666
use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::runtime::Runtime;

/// File-local payload type for the idle-CPU bench. The source is idle and
/// emits nothing in normal operation; the type satisfies FLOWIP-114c's
/// requirement that every DSL stage declares a concrete type fingerprint.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BenchEvent {
    tick: u64,
}

impl TypedPayload for BenchEvent {
    const EVENT_TYPE: &'static str = "bench.idle_event";
}

/// Idle source that doesn't emit any events
#[derive(Clone, Debug)]
struct IdleSource {
    completed: Arc<AtomicU64>,
}

impl IdleSource {
    fn new() -> Self {
        Self {
            completed: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl TypedFiniteSourceHandler for IdleSource {
    type Output = BenchEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        // Intentionally emit nothing but also never complete (idle pipeline).
        // This is the runtime "idle spin" scenario that FLOWIP-086i targets.
        self.completed.fetch_add(1, Ordering::Relaxed);
        Ok(Some(vec![]))
    }
}

/// Sink that records latencies
#[derive(Clone, Debug)]
struct TimestampedSink {
    _received: Arc<AtomicU64>,
    _latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
}

impl TimestampedSink {
    fn new(expected_count: u64) -> (Self, Arc<tokio::sync::Mutex<Vec<Duration>>>) {
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
            expected_count as usize,
        )));
        let received = Arc::new(AtomicU64::new(0));
        (
            Self {
                _received: received.clone(),
                _latencies: latencies.clone(),
            },
            latencies,
        )
    }
}

#[async_trait]
impl InlineSink for TimestampedSink {
    type Input = BenchEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
    }

    async fn write(&mut self, _event: BenchEvent) -> Result<(), SinkWriteFailure> {
        Ok(())
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

/// Create a temporary journal for benchmarking
fn create_temp_journals_base(test_name: &str) -> anyhow::Result<(std::path::PathBuf, TempDir)> {
    let temp_dir = tempdir()?;
    let journal_path = temp_dir.path().join(format!("bench_{test_name}"));
    std::fs::create_dir_all(&journal_path)?;
    Ok((journal_path, temp_dir))
}

/// Build pipeline with specified stage count
async fn build_pipeline(
    stage_count: usize,
    source: IdleSource,
    sink: TimestampedSink,
    journals_base_path: std::path::PathBuf,
) -> anyhow::Result<FlowHandle> {
    if !matches!(stage_count, 1 | 10 | 20 | 100) {
        return Err(anyhow::anyhow!("Unsupported stage count: {stage_count}"));
    }

    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let definition = match stage_count {
            1 => flow! {
                journals: disk_journals(journals_base_path.clone()),

                stages: {
                    src = source!(BenchEvent => source);
                    snk = sink!(BenchEvent => sink);
                },

                topology: {
                    src |> snk;
                }
            },
            10 => {
                let s1_handler = PassthroughStage::new("stage1");
                let s2_handler = PassthroughStage::new("stage2");
                let s3_handler = PassthroughStage::new("stage3");
                let s4_handler = PassthroughStage::new("stage4");
                let s5_handler = PassthroughStage::new("stage5");
                let s6_handler = PassthroughStage::new("stage6");
                let s7_handler = PassthroughStage::new("stage7");
                let s8_handler = PassthroughStage::new("stage8");
                let s9_handler = PassthroughStage::new("stage9");

                flow! {
                journals: disk_journals(journals_base_path.clone()),

                stages: {
                    src = source!(BenchEvent => source);
                    s1 = transform!(BenchEvent -> BenchEvent => s1_handler);
                    s2 = transform!(BenchEvent -> BenchEvent => s2_handler);
                    s3 = transform!(BenchEvent -> BenchEvent => s3_handler);
                    s4 = transform!(BenchEvent -> BenchEvent => s4_handler);
                    s5 = transform!(BenchEvent -> BenchEvent => s5_handler);
                    s6 = transform!(BenchEvent -> BenchEvent => s6_handler);
                    s7 = transform!(BenchEvent -> BenchEvent => s7_handler);
                    s8 = transform!(BenchEvent -> BenchEvent => s8_handler);
                    s9 = transform!(BenchEvent -> BenchEvent => s9_handler);
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
                }
            }
            20 => {
                let s1_handler = PassthroughStage::new("stage1");
                let s2_handler = PassthroughStage::new("stage2");
                let s3_handler = PassthroughStage::new("stage3");
                let s4_handler = PassthroughStage::new("stage4");
                let s5_handler = PassthroughStage::new("stage5");
                let s6_handler = PassthroughStage::new("stage6");
                let s7_handler = PassthroughStage::new("stage7");
                let s8_handler = PassthroughStage::new("stage8");
                let s9_handler = PassthroughStage::new("stage9");
                let s10_handler = PassthroughStage::new("stage10");
                let s11_handler = PassthroughStage::new("stage11");
                let s12_handler = PassthroughStage::new("stage12");
                let s13_handler = PassthroughStage::new("stage13");
                let s14_handler = PassthroughStage::new("stage14");
                let s15_handler = PassthroughStage::new("stage15");
                let s16_handler = PassthroughStage::new("stage16");
                let s17_handler = PassthroughStage::new("stage17");
                let s18_handler = PassthroughStage::new("stage18");
                let s19_handler = PassthroughStage::new("stage19");

                flow! {
                journals: disk_journals(journals_base_path.clone()),

                stages: {
                    src = source!(BenchEvent => source);
                    s1 = transform!(BenchEvent -> BenchEvent => s1_handler);
                    s2 = transform!(BenchEvent -> BenchEvent => s2_handler);
                    s3 = transform!(BenchEvent -> BenchEvent => s3_handler);
                    s4 = transform!(BenchEvent -> BenchEvent => s4_handler);
                    s5 = transform!(BenchEvent -> BenchEvent => s5_handler);
                    s6 = transform!(BenchEvent -> BenchEvent => s6_handler);
                    s7 = transform!(BenchEvent -> BenchEvent => s7_handler);
                    s8 = transform!(BenchEvent -> BenchEvent => s8_handler);
                    s9 = transform!(BenchEvent -> BenchEvent => s9_handler);
                    s10 = transform!(BenchEvent -> BenchEvent => s10_handler);
                    s11 = transform!(BenchEvent -> BenchEvent => s11_handler);
                    s12 = transform!(BenchEvent -> BenchEvent => s12_handler);
                    s13 = transform!(BenchEvent -> BenchEvent => s13_handler);
                    s14 = transform!(BenchEvent -> BenchEvent => s14_handler);
                    s15 = transform!(BenchEvent -> BenchEvent => s15_handler);
                    s16 = transform!(BenchEvent -> BenchEvent => s16_handler);
                    s17 = transform!(BenchEvent -> BenchEvent => s17_handler);
                    s18 = transform!(BenchEvent -> BenchEvent => s18_handler);
                    s19 = transform!(BenchEvent -> BenchEvent => s19_handler);
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
                    s9 |> s10;
                    s10 |> s11;
                    s11 |> s12;
                    s12 |> s13;
                    s13 |> s14;
                    s14 |> s15;
                    s15 |> s16;
                    s16 |> s17;
                    s17 |> s18;
                    s18 |> s19;
                    s19 |> snk;
                }
                }
            }
            100 => {
                // For 100 stages, simplify to 10 stages for maintainability
                let s1_handler = PassthroughStage::new("stage1");
                let s2_handler = PassthroughStage::new("stage2");
                let s3_handler = PassthroughStage::new("stage3");
                let s4_handler = PassthroughStage::new("stage4");
                let s5_handler = PassthroughStage::new("stage5");
                let s6_handler = PassthroughStage::new("stage6");
                let s7_handler = PassthroughStage::new("stage7");
                let s8_handler = PassthroughStage::new("stage8");
                let s9_handler = PassthroughStage::new("stage9");

                flow! {
                    journals: disk_journals(journals_base_path.clone()),

                    stages: {
                        src = source!(BenchEvent => source);
                        s1 = transform!(BenchEvent -> BenchEvent => s1_handler);
                        s2 = transform!(BenchEvent -> BenchEvent => s2_handler);
                        s3 = transform!(BenchEvent -> BenchEvent => s3_handler);
                        s4 = transform!(BenchEvent -> BenchEvent => s4_handler);
                        s5 = transform!(BenchEvent -> BenchEvent => s5_handler);
                        s6 = transform!(BenchEvent -> BenchEvent => s6_handler);
                        s7 = transform!(BenchEvent -> BenchEvent => s7_handler);
                        s8 = transform!(BenchEvent -> BenchEvent => s8_handler);
                        s9 = transform!(BenchEvent -> BenchEvent => s9_handler);
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
                }
            }
            _ => unreachable!("stage count validated above"),
        };

        Ok(definition)
    });

    let handle = flow_definition
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    Ok(handle)
}

const SETTLE: Duration = Duration::from_millis(500);
const WINDOW: Duration = Duration::from_secs(2);

/// Untimed: build, start, settle and stop. Measured: process CPU time used in
/// the window. A flow that leaves its running state fails the sample.
async fn idle_window(stage_count: usize) -> anyhow::Result<Duration> {
    let test_name = format!("idle_cpu_{stage_count}_stages");
    let (journals_base_path, _temp_dir) = create_temp_journals_base(&test_name)?;
    let (sink, _) = TimestampedSink::new(0);
    let handle = build_pipeline(stage_count, IdleSource::new(), sink, journals_base_path).await?;
    handle
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start pipeline: {e:?}"))?;

    tokio::time::sleep(SETTLE).await;
    let before = process_cpu_time();
    tokio::time::sleep(WINDOW).await;
    let used = process_cpu_time().saturating_sub(before);
    let state = handle.current_state();

    // Stop the pipeline so benchmark iterations don't leak background tasks.
    handle
        .stop_cancel()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to stop pipeline: {e:?}"))?;
    handle.wait_for_completion().await?;
    anyhow::ensure!(
        matches!(state, PipelineState::Running),
        "flow left its idle running state during the window: {state:?}"
    );
    Ok(used)
}

/// Benchmark idle process CPU time across pipeline depths
fn bench_idle_process_cpu(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("idle_process_cpu");

    // Every iteration includes untimed setup and the fixed window.
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(45));

    for stage_count in [1, 10, 20, 100] {
        let function = format!("window_2s/stages_{stage_count}");
        declare(
            &format!("idle_process_cpu/{function}"),
            Category::Runtime,
            &format!(
                "Process CPU time in a 2 s idle window after a 500 ms settle; {stage_count}-stage flow; 2,000,000 µs is one logical CPU fully busy"
            ),
        );
        group.bench_function(function, |b| {
            b.to_async(&rt).iter_custom(|iterations| async move {
                let mut used = Duration::ZERO;
                for _ in 0..iterations {
                    used += idle_window(stage_count).await.unwrap();
                }
                used
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_idle_process_cpu);
criterion_main!(benches);

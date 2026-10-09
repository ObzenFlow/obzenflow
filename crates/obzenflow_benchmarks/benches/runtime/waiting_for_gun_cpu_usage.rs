// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! WaitingForGun CPU Usage Benchmark
//!
//! Measures process CPU time used while the pipeline is materialized but not
//! started. This specifically targets “pure wait” busy-spin scenarios
//! (notably sources in `WaitingForGun`).

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion, SamplingMode};
use obzenflow_benchmarks::case::{declare, Category};
use obzenflow_benchmarks::process_cpu_time;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::bootstrap::{install_bootstrap_config, BootstrapConfig, StartupMode};
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkWriteFailure, TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::{tempdir, TempDir};
use tokio::runtime::Runtime;

/// File-local payload type for the waiting-for-gun bench. The idle source
/// never emits; the type satisfies FLOWIP-114c's requirement that every
/// DSL stage declares a concrete type fingerprint.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BenchEvent {
    tick: u64,
}

impl TypedPayload for BenchEvent {
    const EVENT_TYPE: &'static str = "bench.waiting_for_gun_event";
}

/// Source that never emits and never completes.
#[derive(Clone, Debug)]
struct IdleSource {
    polls: Arc<AtomicU64>,
}

impl IdleSource {
    fn new() -> Self {
        Self {
            polls: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl TypedFiniteSourceHandler for IdleSource {
    type Output = BenchEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Ok(Some(vec![]))
    }
}

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl InlineSink for NoopSink {
    type Input = BenchEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
    }

    async fn write(&mut self, _event: BenchEvent) -> Result<(), SinkWriteFailure> {
        Ok(())
    }
}

fn create_temp_journals_base(test_name: &str) -> anyhow::Result<(std::path::PathBuf, TempDir)> {
    let temp_dir = tempdir()?;
    let journal_path = temp_dir.path().join(format!("bench_{test_name}"));
    std::fs::create_dir_all(&journal_path)?;
    Ok((journal_path, temp_dir))
}

const SETTLE: Duration = Duration::from_millis(500);
const WINDOW: Duration = Duration::from_secs(2);

/// Untimed: build, settle and stop. Measured: process CPU time used in the
/// window. A flow that leaves `ReadyForRun` fails the sample.
async fn waiting_window() -> anyhow::Result<Duration> {
    let _bootstrap_guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..BootstrapConfig::default()
    });

    let (journals_base_path, _temp_dir) = create_temp_journals_base("waiting_for_gun_cpu")?;

    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let idle_source = IdleSource::new();
        let noop_sink = NoopSink;

        Ok(flow! {
            journals: disk_journals(journals_base_path),

            stages: {
                src = source!(BenchEvent => idle_source);
                snk = sink!(BenchEvent => noop_sink);
            },

            topology: {
                src |> snk;
            }
        })
    });

    let handle = flow_definition
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    // Wait until the pipeline is materialized (but not started).
    let mut state_rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if matches!(&*state_rx.borrow(), PipelineState::ReadyForRun) {
                break;
            }
            if state_rx.changed().await.is_err() {
                break;
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("Timed out waiting for pipeline to reach ReadyForRun"))?;

    tokio::time::sleep(SETTLE).await;
    let before = process_cpu_time();
    tokio::time::sleep(WINDOW).await;
    let used = process_cpu_time().saturating_sub(before);
    let state = handle.current_state();

    // Stop the pipeline so benchmark iterations don't leak background tasks.
    handle
        .stop_cancel()
        .await
        .map_err(|e| anyhow::anyhow!("Pipeline stop failed: {e}"))?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow::anyhow!("Timed out waiting for pipeline to stop"))?
        .map_err(|e| anyhow::anyhow!("Pipeline stop failed: {e}"))?;
    anyhow::ensure!(
        matches!(state, PipelineState::ReadyForRun),
        "flow left ReadyForRun during the window: {state:?}"
    );
    Ok(used)
}

fn bench_waiting_for_gun_process_cpu(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("waiting_for_gun_process_cpu");

    // Every iteration includes untimed setup and the fixed window.
    group.sampling_mode(SamplingMode::Flat);
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(45));

    declare(
        "waiting_for_gun_process_cpu/window_2s",
        Category::Runtime,
        "Process CPU time in a 2 s window while a built flow waits for its start command, after a 500 ms settle; 2,000,000 µs is one logical CPU fully busy",
    );
    group.bench_function("window_2s", |b| {
        b.to_async(&rt).iter_custom(|iterations| async move {
            let mut used = Duration::ZERO;
            for _ in 0..iterations {
                used += waiting_window().await.unwrap();
            }
            used
        });
    });

    group.finish();
}

criterion_group!(benches, bench_waiting_for_gun_process_cpu);
criterion_main!(benches);

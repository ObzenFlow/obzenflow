// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! WaitingForGun CPU Usage Benchmark
//!
//! Measures CPU usage while the pipeline is materialized but not started.
//! This specifically targets “pure wait” busy-spin scenarios
//! (notably sources in `WaitingForGun`).

use async_trait::async_trait;
use criterion::{criterion_group, criterion_main, Criterion};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_dsl::{flow, sink, source};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{Pid, ProcessExt, System, SystemExt};
use tempfile::{tempdir, TempDir};
use tokio::runtime::Runtime;

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

impl FiniteSourceHandler for IdleSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Ok(Some(vec![]))
    }
}

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl SinkHandler for NoopSink {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success("noop", DeliveryMethod::Noop, None))
    }
}

fn create_temp_journals_base(test_name: &str) -> anyhow::Result<(std::path::PathBuf, TempDir)> {
    let temp_dir = tempdir()?;
    let journal_path = temp_dir.path().join(format!("bench_{test_name}"));
    std::fs::create_dir_all(&journal_path)?;
    Ok((journal_path, temp_dir))
}

async fn measure_waiting_for_gun_cpu() -> anyhow::Result<f64> {
    // Must be set before the pipeline first checks startup mode (OnceLock cached).
    std::env::set_var("OBZENFLOW_STARTUP_MODE", "manual");

    let (journals_base_path, _temp_dir) = create_temp_journals_base("waiting_for_gun_cpu")?;

    let handle = flow! {
        journals: disk_journals(journals_base_path),
        middleware: [],

        stages: {
            src = source!(IdleSource::new());
            snk = sink!(NoopSink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    // Wait until the pipeline is materialized (but not started).
    let mut state_rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if matches!(&*state_rx.borrow(), PipelineState::Materialized) {
                break;
            }
            if state_rx.changed().await.is_err() {
                break;
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("Timed out waiting for pipeline to reach Materialized"))?;

    // Let it stabilize in the waiting state.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Measure CPU over 2 seconds (20 samples).
    let mut system = System::new_all();
    let pid = Pid::from(std::process::id() as usize);

    system.refresh_process(pid);
    let mut cpu_samples = Vec::new();

    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        system.refresh_process(pid);

        if let Some(process) = system.process(pid) {
            cpu_samples.push(process.cpu_usage());
        }
    }

    let avg_cpu = if !cpu_samples.is_empty() {
        cpu_samples.iter().sum::<f32>() / cpu_samples.len() as f32
    } else {
        0.0
    };

    // Stop the pipeline so benchmark iterations don't leak background tasks.
    let _ = handle.stop_cancel().await;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow::anyhow!("Timed out waiting for pipeline to stop"))?
        .map_err(|e| anyhow::anyhow!("Pipeline stop failed: {e}"))?;

    Ok(avg_cpu as f64)
}

fn bench_waiting_for_gun_cpu_usage(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("waiting_for_gun_cpu_usage");

    group.sample_size(10);
    group.measurement_time(Duration::from_secs(45));

    group.bench_function("cpu_percentage", |b| {
        b.to_async(&rt)
            .iter(|| async { measure_waiting_for_gun_cpu().await.unwrap() });
    });

    group.finish();
}

criterion_group!(benches, bench_waiting_for_gun_cpu_usage);
criterion_main!(benches);

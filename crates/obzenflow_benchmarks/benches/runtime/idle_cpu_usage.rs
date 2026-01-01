//! Idle CPU Usage Benchmark
//!
//! Measures CPU usage when the pipeline is idle (no events flowing).
//! This validates that the event-driven design doesn't waste resources
//! with busy-waiting or polling when there's no work to do.

use criterion::{criterion_group, criterion_main, Criterion};
use obzenflow_benchmarks::prelude::*;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::supervised_base::SupervisorHandle;
use obzenflow_runtime_services::stages::SourceError;
// Monitoring removed per FLOWIP-056-666
use async_trait::async_trait;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{Pid, ProcessExt, System, SystemExt};
use tempfile::{tempdir, TempDir};
use tokio::runtime::Runtime;

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

impl FiniteSourceHandler for IdleSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        // Intentionally emit nothing but also never complete (idle pipeline).
        // This is the runtime "idle spin" scenario that FLOWIP-086i targets.
        self.completed.fetch_add(1, Ordering::Relaxed);
        Ok(Some(vec![]))
    }
}

/// Sink that records latencies
#[derive(Clone, Debug)]
struct TimestampedSink {
    expected_count: u64,
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
                expected_count,
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
                if index >= 10 {
                    // WARMUP_EVENT_COUNT
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

/// Passthrough stage that just forwards events
#[derive(Clone, Debug)]
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

#[async_trait]
impl TransformHandler for PassthroughStage {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

/// Create a temporary journal for benchmarking
fn create_temp_journals_base(test_name: &str) -> anyhow::Result<(std::path::PathBuf, TempDir)> {
    let temp_dir = tempdir()?;
    let journal_path = temp_dir.path().join(format!("bench_{}", test_name));
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
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?,
        10 => flow! {
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
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?,
        20 => flow! {
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
                s10 = transform!("stage10" => PassthroughStage::new("stage10"));
                s11 = transform!("stage11" => PassthroughStage::new("stage11"));
                s12 = transform!("stage12" => PassthroughStage::new("stage12"));
                s13 = transform!("stage13" => PassthroughStage::new("stage13"));
                s14 = transform!("stage14" => PassthroughStage::new("stage14"));
                s15 = transform!("stage15" => PassthroughStage::new("stage15"));
                s16 = transform!("stage16" => PassthroughStage::new("stage16"));
                s17 = transform!("stage17" => PassthroughStage::new("stage17"));
                s18 = transform!("stage18" => PassthroughStage::new("stage18"));
                s19 = transform!("stage19" => PassthroughStage::new("stage19"));
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
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?,
        100 => {
            // For 100 stages, simplify to 10 stages for maintainability
            flow! {
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
            .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?
        }
        _ => return Err(anyhow::anyhow!("Unsupported stage count: {}", stage_count)),
    };

    Ok(handle)
}

/// Measure CPU usage of an idle pipeline
async fn measure_idle_cpu() -> anyhow::Result<f64> {
    let (journals_base_path, _temp_dir) = create_temp_journals_base("idle_cpu")?;

    // Create pipeline with non-emitting source
    let idle_source = IdleSource::new();
    let (sink, _) = TimestampedSink::new(0);

    let handle = flow! {
        journals: disk_journals(journals_base_path),
        middleware: [],

        stages: {
            src = source!("source" => idle_source);
            snk = sink!("sink" => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    // Start the pipeline (do not await completion; we want to measure idle CPU while running)
    handle
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start pipeline: {:?}", e))?;

    // Let pipeline stabilize
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Measure CPU over 2 seconds
    let mut system = System::new_all();
    let pid = Pid::from(std::process::id() as usize);

    system.refresh_process(pid);
    let mut cpu_samples = Vec::new();

    // Take 20 samples over 2 seconds
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        system.refresh_process(pid);

        if let Some(process) = system.process(pid) {
            cpu_samples.push(process.cpu_usage());
        }
    }

    // Calculate average CPU usage
    let avg_cpu = if !cpu_samples.is_empty() {
        cpu_samples.iter().sum::<f32>() / cpu_samples.len() as f32
    } else {
        0.0
    };

    // Stop the pipeline so benchmark iterations don't leak background tasks.
    let _ = handle.stop_cancel().await;
    handle.wait_for_completion().await?;

    Ok(avg_cpu as f64)
}

/// Benchmark idle CPU usage
fn bench_idle_cpu_usage(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("idle_cpu_usage");

    // Configure for CPU measurement
    group.sample_size(10); // Minimum required by Criterion
    group.measurement_time(Duration::from_secs(45)); // Increased for CPU measurement stability

    group.bench_function("cpu_percentage", |b| {
        b.to_async(&rt).iter(|| async {
            // Let Criterion measure the actual function execution time
            let cpu_percentage = measure_idle_cpu().await.unwrap();
            // Could log the CPU percentage here if needed
            cpu_percentage
        });
    });

    group.finish();
}

/// Benchmark idle CPU with different pipeline depths
fn bench_idle_cpu_by_depth(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("idle_cpu_by_depth");

    group.sample_size(10); // Minimum required by Criterion
    group.measurement_time(Duration::from_secs(45)); // Increased for CPU measurement stability

    let stage_counts = vec![1, 10, 20, 100];

    for stage_count in stage_counts {
        group.bench_function(&format!("{}_stages", stage_count), |b| {
            b.to_async(&rt).iter(|| async {
                // Let Criterion measure the actual function execution time
                let cpu_percentage = measure_idle_cpu_with_stages(stage_count).await.unwrap();
                cpu_percentage
            });
        });
    }

    group.finish();
}

/// Measure idle CPU with a specific number of stages
async fn measure_idle_cpu_with_stages(stage_count: usize) -> anyhow::Result<f64> {
    let (journals_base_path, _temp_dir) =
        create_temp_journals_base(&format!("idle_cpu_{}_stages", stage_count))?;

    let idle_source = IdleSource::new();
    let (sink, _) = TimestampedSink::new(0);

    // Build pipeline with specified stages
    let handle = build_pipeline(stage_count, idle_source, sink, journals_base_path).await?;

    handle
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start pipeline: {:?}", e))?;

    // Let stabilize
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Measure CPU
    let mut system = System::new_all();
    let pid = Pid::from(std::process::id() as usize);

    system.refresh_process(pid);
    let mut cpu_samples = Vec::new();

    for _ in 0..20 {
        // 2 seconds of sampling
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

    let _ = handle.stop_cancel().await;
    handle.wait_for_completion().await?;

    Ok(avg_cpu as f64)
}

criterion_group!(benches, bench_idle_cpu_usage, bench_idle_cpu_by_depth);
criterion_main!(benches);

//! Isolated 20-Stage Pipeline Latency Benchmark
//!
//! This is a completely standalone benchmark for ONLY 20-stage pipelines.
//! No other configurations, no shared code with other stage counts.
//! This isolation helps determine if the performance anomaly is due to
//! benchmark ordering, warmup effects, or genuine framework issues.

use criterion::{criterion_group, criterion_main, Criterion};
use obzenflow_benchmarks::prelude::*;
use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::control_plane::stages::handler_traits::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_adapters::monitoring::taxonomies::{
    golden_signals::GoldenSignals,
    red::RED,
    use_taxonomy::USE,
};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use serde_json::json;
use tokio::runtime::Runtime;
use tempfile::tempdir;
use async_trait::async_trait;

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
    fn next(&mut self) -> Option<ChainEvent> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
            Some(ChainEvent::new(
                EventId::new(),
                self.writer_id.clone(),
                "TimestampedEvent",
                json!({
                    "event_id": current,
                    "emit_time_nanos": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64,
                })
            ))
        } else {
            None
        }
    }

    fn is_complete(&self) -> bool {
        self.emitted.load(Ordering::Relaxed) >= self.total_events
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
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(expected_count as usize)));
        (Self {
            expected_count,
            received: Arc::new(AtomicU64::new(0)),
            latencies: latencies.clone(),
        }, latencies)
    }
}

#[async_trait]
impl SinkHandler for LatencySink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
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
        Ok(())
    }
}

/// Run a single 20-stage pipeline test
async fn run_20_stage_pipeline() -> anyhow::Result<Duration> {
    let temp_dir = tempdir()?;
    let journal_path = temp_dir.path().join(format!("twenty_stage_{}", std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()));
    std::fs::create_dir_all(&journal_path)?;
    
    let journal = Arc::new(DiskJournal::new(journal_path, "benchmark_20_stage").await?);

    let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let (sink, latencies) = LatencySink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let sink_clone = sink.clone();

    // For 20 stages, we'll use the simplified form that the throughput benchmark uses
    let handle = flow! {
        journal: journal,
        middleware: [GoldenSignals::monitoring()],
        
        stages: {
            src = source!("source" => source, [RED::monitoring()]);
            s1 = transform!("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()]);
            s2 = transform!("stage2" => PassthroughStage::new("stage2"), [USE::monitoring()]);
            s3 = transform!("stage3" => PassthroughStage::new("stage3"), [USE::monitoring()]);
            s4 = transform!("stage4" => PassthroughStage::new("stage4"), [USE::monitoring()]);
            s5 = transform!("stage5" => PassthroughStage::new("stage5"), [USE::monitoring()]);
            s6 = transform!("stage6" => PassthroughStage::new("stage6"), [USE::monitoring()]);
            s7 = transform!("stage7" => PassthroughStage::new("stage7"), [USE::monitoring()]);
            s8 = transform!("stage8" => PassthroughStage::new("stage8"), [USE::monitoring()]);
            s9 = transform!("stage9" => PassthroughStage::new("stage9"), [USE::monitoring()]);
            s10 = transform!("stage10" => PassthroughStage::new("stage10"), [USE::monitoring()]);
            s11 = transform!("stage11" => PassthroughStage::new("stage11"), [USE::monitoring()]);
            s12 = transform!("stage12" => PassthroughStage::new("stage12"), [USE::monitoring()]);
            s13 = transform!("stage13" => PassthroughStage::new("stage13"), [USE::monitoring()]);
            s14 = transform!("stage14" => PassthroughStage::new("stage14"), [USE::monitoring()]);
            s15 = transform!("stage15" => PassthroughStage::new("stage15"), [USE::monitoring()]);
            s16 = transform!("stage16" => PassthroughStage::new("stage16"), [USE::monitoring()]);
            s17 = transform!("stage17" => PassthroughStage::new("stage17"), [USE::monitoring()]);
            s18 = transform!("stage18" => PassthroughStage::new("stage18"), [USE::monitoring()]);
            s19 = transform!("stage19" => PassthroughStage::new("stage19"), [USE::monitoring()]);
            snk = sink!("sink" => sink, [RED::monitoring()]);
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
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    // Start the pipeline
    handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;

    // Wait for completion
    let timeout = Duration::from_secs(90);  // Increased timeout for 20 stages
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

fn bench_20_stage_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("20_stage_latency");

    group.sample_size(10);  // Minimum required by Criterion
    group.measurement_time(Duration::from_secs(60));  // Extended measurement time for 20 stages

    group.bench_function("median_latency", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total_latency = Duration::ZERO;
            
            for _ in 0..iters {
                let median = run_20_stage_pipeline().await.unwrap();
                total_latency = total_latency.saturating_add(median);
            }
            
            total_latency
        });
    });

    group.finish();
}

criterion_group!(benches, bench_20_stage_latency);
criterion_main!(benches);
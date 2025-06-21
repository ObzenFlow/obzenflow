//! Isolated 2-Stage Pipeline Latency Benchmark
//!
//! This is a completely standalone benchmark for ONLY 2-stage pipelines.
//! No other configurations, no shared code with other stage counts.
//! This isolation helps determine if the performance anomaly is due to
//! benchmark ordering, warmup effects, or genuine framework issues.

use criterion::{criterion_group, criterion_main, Criterion};
use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use serde_json::json;
use tokio::runtime::Runtime;
use tempfile::tempdir;
use ulid::Ulid;

const WARMUP_EVENT_COUNT: u64 = 10;
const TEST_EVENT_COUNT: u64 = 100;

/// Test source that emits timestamped events
struct TimestampedSource {
    total_events: u64,
    emitted: AtomicU64,
}

impl TimestampedSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: AtomicU64::new(0),
        }
    }
}

impl Step for TimestampedSource {
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
}

impl PassthroughStage {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }
}

impl Step for PassthroughStage {
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

impl Step for LatencySink {
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

/// Run a single 2-stage pipeline test
async fn run_2_stage_pipeline() -> Result<Duration> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join(format!("two_stage_{}", Ulid::new()));
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path,
        max_segment_size: 1024 * 1024,
    }).await?;

    let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let (sink, latencies) = LatencySink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let sink_clone = sink.clone();

    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("source" => source, [RED::monitoring()])
        |> ("stage1" => PassthroughStage::new("stage1"), [USE::monitoring()])
        |> ("sink" => sink, [RED::monitoring()])
    }?;

    // Wait for completion
    let timeout = Duration::from_secs(30);
    let start = Instant::now();

    while sink_clone.received.load(Ordering::Relaxed) < WARMUP_EVENT_COUNT + TEST_EVENT_COUNT {
        if start.elapsed() > timeout {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    handle.shutdown().await?;

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

    group.sample_size(50);  // More samples for statistical significance
    group.measurement_time(Duration::from_secs(30));  // Longer measurement

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
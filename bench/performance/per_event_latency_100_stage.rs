//! Isolated 100-Stage Pipeline Latency Benchmark
//!
//! This is a completely standalone benchmark for ONLY 100-stage pipelines.
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
    metrics: <RED as Taxonomy>::Metrics,
}

impl TimestampedSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: AtomicU64::new(0),
            metrics: RED::create_metrics("TimestampedSource"),
        }
    }
}

impl Step for TimestampedSource {
    type Taxonomy = RED;

    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
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
    metrics: <USE as Taxonomy>::Metrics,
}

impl PassthroughStage {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            metrics: USE::create_metrics(name),
        }
    }
}

impl Step for PassthroughStage {
    type Taxonomy = USE;

    fn taxonomy(&self) -> &Self::Taxonomy { &USE }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
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
    metrics: Arc<<RED as Taxonomy>::Metrics>,
}

impl LatencySink {
    fn new(expected_count: u64) -> (Self, Arc<tokio::sync::Mutex<Vec<Duration>>>) {
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(expected_count as usize)));
        (Self {
            expected_count,
            received: Arc::new(AtomicU64::new(0)),
            latencies: latencies.clone(),
            metrics: Arc::new(RED::create_metrics("LatencySink")),
        }, latencies)
    }
}

impl Step for LatencySink {
    type Taxonomy = RED;

    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &*self.metrics }
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

/// Run a single 100-stage pipeline test
async fn run_100_stage_pipeline() -> Result<Duration> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join(format!("hundred_stage_{}", Ulid::new()));
    
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
        ("source" => source, RED)
        |> ("stage1" => PassthroughStage::new("stage1"), USE)
        |> ("stage2" => PassthroughStage::new("stage2"), USE)
        |> ("stage3" => PassthroughStage::new("stage3"), USE)
        |> ("stage4" => PassthroughStage::new("stage4"), USE)
        |> ("stage5" => PassthroughStage::new("stage5"), USE)
        |> ("stage6" => PassthroughStage::new("stage6"), USE)
        |> ("stage7" => PassthroughStage::new("stage7"), USE)
        |> ("stage8" => PassthroughStage::new("stage8"), USE)
        |> ("stage9" => PassthroughStage::new("stage9"), USE)
        |> ("stage10" => PassthroughStage::new("stage10"), USE)
        |> ("stage11" => PassthroughStage::new("stage11"), USE)
        |> ("stage12" => PassthroughStage::new("stage12"), USE)
        |> ("stage13" => PassthroughStage::new("stage13"), USE)
        |> ("stage14" => PassthroughStage::new("stage14"), USE)
        |> ("stage15" => PassthroughStage::new("stage15"), USE)
        |> ("stage16" => PassthroughStage::new("stage16"), USE)
        |> ("stage17" => PassthroughStage::new("stage17"), USE)
        |> ("stage18" => PassthroughStage::new("stage18"), USE)
        |> ("stage19" => PassthroughStage::new("stage19"), USE)
        |> ("stage20" => PassthroughStage::new("stage20"), USE)
        |> ("stage21" => PassthroughStage::new("stage21"), USE)
        |> ("stage22" => PassthroughStage::new("stage22"), USE)
        |> ("stage23" => PassthroughStage::new("stage23"), USE)
        |> ("stage24" => PassthroughStage::new("stage24"), USE)
        |> ("stage25" => PassthroughStage::new("stage25"), USE)
        |> ("stage26" => PassthroughStage::new("stage26"), USE)
        |> ("stage27" => PassthroughStage::new("stage27"), USE)
        |> ("stage28" => PassthroughStage::new("stage28"), USE)
        |> ("stage29" => PassthroughStage::new("stage29"), USE)
        |> ("stage30" => PassthroughStage::new("stage30"), USE)
        |> ("stage31" => PassthroughStage::new("stage31"), USE)
        |> ("stage32" => PassthroughStage::new("stage32"), USE)
        |> ("stage33" => PassthroughStage::new("stage33"), USE)
        |> ("stage34" => PassthroughStage::new("stage34"), USE)
        |> ("stage35" => PassthroughStage::new("stage35"), USE)
        |> ("stage36" => PassthroughStage::new("stage36"), USE)
        |> ("stage37" => PassthroughStage::new("stage37"), USE)
        |> ("stage38" => PassthroughStage::new("stage38"), USE)
        |> ("stage39" => PassthroughStage::new("stage39"), USE)
        |> ("stage40" => PassthroughStage::new("stage40"), USE)
        |> ("stage41" => PassthroughStage::new("stage41"), USE)
        |> ("stage42" => PassthroughStage::new("stage42"), USE)
        |> ("stage43" => PassthroughStage::new("stage43"), USE)
        |> ("stage44" => PassthroughStage::new("stage44"), USE)
        |> ("stage45" => PassthroughStage::new("stage45"), USE)
        |> ("stage46" => PassthroughStage::new("stage46"), USE)
        |> ("stage47" => PassthroughStage::new("stage47"), USE)
        |> ("stage48" => PassthroughStage::new("stage48"), USE)
        |> ("stage49" => PassthroughStage::new("stage49"), USE)
        |> ("stage50" => PassthroughStage::new("stage50"), USE)
        |> ("stage51" => PassthroughStage::new("stage51"), USE)
        |> ("stage52" => PassthroughStage::new("stage52"), USE)
        |> ("stage53" => PassthroughStage::new("stage53"), USE)
        |> ("stage54" => PassthroughStage::new("stage54"), USE)
        |> ("stage55" => PassthroughStage::new("stage55"), USE)
        |> ("stage56" => PassthroughStage::new("stage56"), USE)
        |> ("stage57" => PassthroughStage::new("stage57"), USE)
        |> ("stage58" => PassthroughStage::new("stage58"), USE)
        |> ("stage59" => PassthroughStage::new("stage59"), USE)
        |> ("stage60" => PassthroughStage::new("stage60"), USE)
        |> ("stage61" => PassthroughStage::new("stage61"), USE)
        |> ("stage62" => PassthroughStage::new("stage62"), USE)
        |> ("stage63" => PassthroughStage::new("stage63"), USE)
        |> ("stage64" => PassthroughStage::new("stage64"), USE)
        |> ("stage65" => PassthroughStage::new("stage65"), USE)
        |> ("stage66" => PassthroughStage::new("stage66"), USE)
        |> ("stage67" => PassthroughStage::new("stage67"), USE)
        |> ("stage68" => PassthroughStage::new("stage68"), USE)
        |> ("stage69" => PassthroughStage::new("stage69"), USE)
        |> ("stage70" => PassthroughStage::new("stage70"), USE)
        |> ("stage71" => PassthroughStage::new("stage71"), USE)
        |> ("stage72" => PassthroughStage::new("stage72"), USE)
        |> ("stage73" => PassthroughStage::new("stage73"), USE)
        |> ("stage74" => PassthroughStage::new("stage74"), USE)
        |> ("stage75" => PassthroughStage::new("stage75"), USE)
        |> ("stage76" => PassthroughStage::new("stage76"), USE)
        |> ("stage77" => PassthroughStage::new("stage77"), USE)
        |> ("stage78" => PassthroughStage::new("stage78"), USE)
        |> ("stage79" => PassthroughStage::new("stage79"), USE)
        |> ("stage80" => PassthroughStage::new("stage80"), USE)
        |> ("stage81" => PassthroughStage::new("stage81"), USE)
        |> ("stage82" => PassthroughStage::new("stage82"), USE)
        |> ("stage83" => PassthroughStage::new("stage83"), USE)
        |> ("stage84" => PassthroughStage::new("stage84"), USE)
        |> ("stage85" => PassthroughStage::new("stage85"), USE)
        |> ("stage86" => PassthroughStage::new("stage86"), USE)
        |> ("stage87" => PassthroughStage::new("stage87"), USE)
        |> ("stage88" => PassthroughStage::new("stage88"), USE)
        |> ("stage89" => PassthroughStage::new("stage89"), USE)
        |> ("stage90" => PassthroughStage::new("stage90"), USE)
        |> ("stage91" => PassthroughStage::new("stage91"), USE)
        |> ("stage92" => PassthroughStage::new("stage92"), USE)
        |> ("stage93" => PassthroughStage::new("stage93"), USE)
        |> ("stage94" => PassthroughStage::new("stage94"), USE)
        |> ("stage95" => PassthroughStage::new("stage95"), USE)
        |> ("stage96" => PassthroughStage::new("stage96"), USE)
        |> ("stage97" => PassthroughStage::new("stage97"), USE)
        |> ("stage98" => PassthroughStage::new("stage98"), USE)
        |> ("stage99" => PassthroughStage::new("stage99"), USE)
        |> ("sink" => sink, RED)
    }?;

    // Wait for completion
    let timeout = Duration::from_secs(60);  // Longer timeout for 100 stages
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

fn bench_100_stage_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("100_stage_latency");

    group.sample_size(10);  // Fewer samples due to longer runtime
    group.measurement_time(Duration::from_secs(60));  // Longer measurement

    group.bench_function("median_latency", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total_latency = Duration::ZERO;
            
            for _ in 0..iters {
                let median = run_100_stage_pipeline().await.unwrap();
                total_latency = total_latency.saturating_add(median);
            }
            
            total_latency
        });
    });

    group.finish();
}

criterion_group!(benches, bench_100_stage_latency);
criterion_main!(benches);
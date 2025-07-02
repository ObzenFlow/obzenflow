//! 100-Stage Pipeline Latency Benchmark with MemoryJournal
//!
//! This benchmark tests the same 100-stage pipeline as per_event_latency_100_stage.rs
//! but uses MemoryJournal instead of DiskJournal to isolate I/O overhead.
//! By comparing the two benchmarks, we can determine how much of the latency
//! is due to disk I/O versus other factors (middleware, task scheduling, etc).

use criterion::{criterion_group, criterion_main, Criterion};
use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::MemoryJournal;
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

/// Run a single 100-stage pipeline test with MemoryJournal
async fn run_100_stage_pipeline_memory() -> anyhow::Result<Duration> {
    // Use MemoryJournal instead of DiskJournal - no disk I/O!
    let journal = Arc::new(MemoryJournal::new());

    let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let (sink, latencies) = LatencySink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let sink_clone = sink.clone();

    // Create 100 stages for true performance testing
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
            s20 = transform!("stage20" => PassthroughStage::new("stage20"), [USE::monitoring()]);
            s21 = transform!("stage21" => PassthroughStage::new("stage21"), [USE::monitoring()]);
            s22 = transform!("stage22" => PassthroughStage::new("stage22"), [USE::monitoring()]);
            s23 = transform!("stage23" => PassthroughStage::new("stage23"), [USE::monitoring()]);
            s24 = transform!("stage24" => PassthroughStage::new("stage24"), [USE::monitoring()]);
            s25 = transform!("stage25" => PassthroughStage::new("stage25"), [USE::monitoring()]);
            s26 = transform!("stage26" => PassthroughStage::new("stage26"), [USE::monitoring()]);
            s27 = transform!("stage27" => PassthroughStage::new("stage27"), [USE::monitoring()]);
            s28 = transform!("stage28" => PassthroughStage::new("stage28"), [USE::monitoring()]);
            s29 = transform!("stage29" => PassthroughStage::new("stage29"), [USE::monitoring()]);
            s30 = transform!("stage30" => PassthroughStage::new("stage30"), [USE::monitoring()]);
            s31 = transform!("stage31" => PassthroughStage::new("stage31"), [USE::monitoring()]);
            s32 = transform!("stage32" => PassthroughStage::new("stage32"), [USE::monitoring()]);
            s33 = transform!("stage33" => PassthroughStage::new("stage33"), [USE::monitoring()]);
            s34 = transform!("stage34" => PassthroughStage::new("stage34"), [USE::monitoring()]);
            s35 = transform!("stage35" => PassthroughStage::new("stage35"), [USE::monitoring()]);
            s36 = transform!("stage36" => PassthroughStage::new("stage36"), [USE::monitoring()]);
            s37 = transform!("stage37" => PassthroughStage::new("stage37"), [USE::monitoring()]);
            s38 = transform!("stage38" => PassthroughStage::new("stage38"), [USE::monitoring()]);
            s39 = transform!("stage39" => PassthroughStage::new("stage39"), [USE::monitoring()]);
            s40 = transform!("stage40" => PassthroughStage::new("stage40"), [USE::monitoring()]);
            s41 = transform!("stage41" => PassthroughStage::new("stage41"), [USE::monitoring()]);
            s42 = transform!("stage42" => PassthroughStage::new("stage42"), [USE::monitoring()]);
            s43 = transform!("stage43" => PassthroughStage::new("stage43"), [USE::monitoring()]);
            s44 = transform!("stage44" => PassthroughStage::new("stage44"), [USE::monitoring()]);
            s45 = transform!("stage45" => PassthroughStage::new("stage45"), [USE::monitoring()]);
            s46 = transform!("stage46" => PassthroughStage::new("stage46"), [USE::monitoring()]);
            s47 = transform!("stage47" => PassthroughStage::new("stage47"), [USE::monitoring()]);
            s48 = transform!("stage48" => PassthroughStage::new("stage48"), [USE::monitoring()]);
            s49 = transform!("stage49" => PassthroughStage::new("stage49"), [USE::monitoring()]);
            s50 = transform!("stage50" => PassthroughStage::new("stage50"), [USE::monitoring()]);
            s51 = transform!("stage51" => PassthroughStage::new("stage51"), [USE::monitoring()]);
            s52 = transform!("stage52" => PassthroughStage::new("stage52"), [USE::monitoring()]);
            s53 = transform!("stage53" => PassthroughStage::new("stage53"), [USE::monitoring()]);
            s54 = transform!("stage54" => PassthroughStage::new("stage54"), [USE::monitoring()]);
            s55 = transform!("stage55" => PassthroughStage::new("stage55"), [USE::monitoring()]);
            s56 = transform!("stage56" => PassthroughStage::new("stage56"), [USE::monitoring()]);
            s57 = transform!("stage57" => PassthroughStage::new("stage57"), [USE::monitoring()]);
            s58 = transform!("stage58" => PassthroughStage::new("stage58"), [USE::monitoring()]);
            s59 = transform!("stage59" => PassthroughStage::new("stage59"), [USE::monitoring()]);
            s60 = transform!("stage60" => PassthroughStage::new("stage60"), [USE::monitoring()]);
            s61 = transform!("stage61" => PassthroughStage::new("stage61"), [USE::monitoring()]);
            s62 = transform!("stage62" => PassthroughStage::new("stage62"), [USE::monitoring()]);
            s63 = transform!("stage63" => PassthroughStage::new("stage63"), [USE::monitoring()]);
            s64 = transform!("stage64" => PassthroughStage::new("stage64"), [USE::monitoring()]);
            s65 = transform!("stage65" => PassthroughStage::new("stage65"), [USE::monitoring()]);
            s66 = transform!("stage66" => PassthroughStage::new("stage66"), [USE::monitoring()]);
            s67 = transform!("stage67" => PassthroughStage::new("stage67"), [USE::monitoring()]);
            s68 = transform!("stage68" => PassthroughStage::new("stage68"), [USE::monitoring()]);
            s69 = transform!("stage69" => PassthroughStage::new("stage69"), [USE::monitoring()]);
            s70 = transform!("stage70" => PassthroughStage::new("stage70"), [USE::monitoring()]);
            s71 = transform!("stage71" => PassthroughStage::new("stage71"), [USE::monitoring()]);
            s72 = transform!("stage72" => PassthroughStage::new("stage72"), [USE::monitoring()]);
            s73 = transform!("stage73" => PassthroughStage::new("stage73"), [USE::monitoring()]);
            s74 = transform!("stage74" => PassthroughStage::new("stage74"), [USE::monitoring()]);
            s75 = transform!("stage75" => PassthroughStage::new("stage75"), [USE::monitoring()]);
            s76 = transform!("stage76" => PassthroughStage::new("stage76"), [USE::monitoring()]);
            s77 = transform!("stage77" => PassthroughStage::new("stage77"), [USE::monitoring()]);
            s78 = transform!("stage78" => PassthroughStage::new("stage78"), [USE::monitoring()]);
            s79 = transform!("stage79" => PassthroughStage::new("stage79"), [USE::monitoring()]);
            s80 = transform!("stage80" => PassthroughStage::new("stage80"), [USE::monitoring()]);
            s81 = transform!("stage81" => PassthroughStage::new("stage81"), [USE::monitoring()]);
            s82 = transform!("stage82" => PassthroughStage::new("stage82"), [USE::monitoring()]);
            s83 = transform!("stage83" => PassthroughStage::new("stage83"), [USE::monitoring()]);
            s84 = transform!("stage84" => PassthroughStage::new("stage84"), [USE::monitoring()]);
            s85 = transform!("stage85" => PassthroughStage::new("stage85"), [USE::monitoring()]);
            s86 = transform!("stage86" => PassthroughStage::new("stage86"), [USE::monitoring()]);
            s87 = transform!("stage87" => PassthroughStage::new("stage87"), [USE::monitoring()]);
            s88 = transform!("stage88" => PassthroughStage::new("stage88"), [USE::monitoring()]);
            s89 = transform!("stage89" => PassthroughStage::new("stage89"), [USE::monitoring()]);
            s90 = transform!("stage90" => PassthroughStage::new("stage90"), [USE::monitoring()]);
            s91 = transform!("stage91" => PassthroughStage::new("stage91"), [USE::monitoring()]);
            s92 = transform!("stage92" => PassthroughStage::new("stage92"), [USE::monitoring()]);
            s93 = transform!("stage93" => PassthroughStage::new("stage93"), [USE::monitoring()]);
            s94 = transform!("stage94" => PassthroughStage::new("stage94"), [USE::monitoring()]);
            s95 = transform!("stage95" => PassthroughStage::new("stage95"), [USE::monitoring()]);
            s96 = transform!("stage96" => PassthroughStage::new("stage96"), [USE::monitoring()]);
            s97 = transform!("stage97" => PassthroughStage::new("stage97"), [USE::monitoring()]);
            s98 = transform!("stage98" => PassthroughStage::new("stage98"), [USE::monitoring()]);
            s99 = transform!("stage99" => PassthroughStage::new("stage99"), [USE::monitoring()]);
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
            s19 |> s20;
            s20 |> s21;
            s21 |> s22;
            s22 |> s23;
            s23 |> s24;
            s24 |> s25;
            s25 |> s26;
            s26 |> s27;
            s27 |> s28;
            s28 |> s29;
            s29 |> s30;
            s30 |> s31;
            s31 |> s32;
            s32 |> s33;
            s33 |> s34;
            s34 |> s35;
            s35 |> s36;
            s36 |> s37;
            s37 |> s38;
            s38 |> s39;
            s39 |> s40;
            s40 |> s41;
            s41 |> s42;
            s42 |> s43;
            s43 |> s44;
            s44 |> s45;
            s45 |> s46;
            s46 |> s47;
            s47 |> s48;
            s48 |> s49;
            s49 |> s50;
            s50 |> s51;
            s51 |> s52;
            s52 |> s53;
            s53 |> s54;
            s54 |> s55;
            s55 |> s56;
            s56 |> s57;
            s57 |> s58;
            s58 |> s59;
            s59 |> s60;
            s60 |> s61;
            s61 |> s62;
            s62 |> s63;
            s63 |> s64;
            s64 |> s65;
            s65 |> s66;
            s66 |> s67;
            s67 |> s68;
            s68 |> s69;
            s69 |> s70;
            s70 |> s71;
            s71 |> s72;
            s72 |> s73;
            s73 |> s74;
            s74 |> s75;
            s75 |> s76;
            s76 |> s77;
            s77 |> s78;
            s78 |> s79;
            s79 |> s80;
            s80 |> s81;
            s81 |> s82;
            s82 |> s83;
            s83 |> s84;
            s84 |> s85;
            s85 |> s86;
            s86 |> s87;
            s87 |> s88;
            s88 |> s89;
            s89 |> s90;
            s90 |> s91;
            s91 |> s92;
            s92 |> s93;
            s93 |> s94;
            s94 |> s95;
            s95 |> s96;
            s96 |> s97;
            s97 |> s98;
            s98 |> s99;
            s99 |> snk;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    // Start the pipeline
    handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;

    // Wait for completion
    let timeout = Duration::from_secs(300);  // Extended timeout for true 100 stages
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

fn bench_100_stage_latency_memory(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("100_stage_latency_memory");
    
    group.sample_size(10);  // Minimum required by Criterion
    group.measurement_time(Duration::from_secs(180));  // Extended measurement time for true 100 stages

    group.bench_function("median_latency", |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total_latency = Duration::ZERO;
            
            for _ in 0..iters {
                let median = run_100_stage_pipeline_memory().await.unwrap();
                total_latency = total_latency.saturating_add(median);
            }
            
            total_latency
        });
    });

    group.finish();
}

criterion_group!(benches, bench_100_stage_latency_memory);
criterion_main!(benches);
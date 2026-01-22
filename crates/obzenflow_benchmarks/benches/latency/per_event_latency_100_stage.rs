//! Isolated 100-Stage Pipeline Latency Benchmark
//!
//! This is a completely standalone benchmark for ONLY 100-stage pipelines.
//! No other configurations, no shared code with other stage counts.
//! This isolation helps determine if the performance anomaly is due to
//! benchmark ordering, warmup effects, or genuine framework issues.

use criterion::{criterion_group, criterion_main, Criterion};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::WriterId;
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::pipeline::fsm::PipelineState;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime_services::stages::SourceError;
// Monitoring removed per FLOWIP-056-666
use async_trait::async_trait;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use tokio::runtime::Runtime;

const DEFAULT_WARMUP_EVENT_COUNT: u64 = 2;
const DEFAULT_TEST_EVENT_COUNT: u64 = 20;
const DEFAULT_PIPELINE_TIMEOUT_SECS: u64 = 180;

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

/// Test source that emits timestamped events
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
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                "TimestampedEvent",
                json!({
                    "event_id": current,
                    "emit_time_nanos": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64,
                }),
            )]))
        } else {
            Ok(None)
        }
    }
}

/// Passthrough stage
#[derive(Clone, Debug)]
struct PassthroughStage;

impl PassthroughStage {
    fn new(name: &str) -> Self {
        let _ = name;
        Self
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

/// Sink that collects latencies
#[derive(Clone, Debug)]
struct LatencySink {
    warmup_events: u64,
    received: Arc<AtomicU64>,
    latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
}

impl LatencySink {
    fn new(
        warmup_events: u64,
        expected_count: u64,
    ) -> (Self, Arc<tokio::sync::Mutex<Vec<Duration>>>) {
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
            expected_count as usize,
        )));
        (
            Self {
                warmup_events,
                received: Arc::new(AtomicU64::new(0)),
                latencies: latencies.clone(),
            },
            latencies,
        )
    }
}

#[async_trait]
impl SinkHandler for LatencySink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if let ChainEventContent::Data { payload, .. } = &event.content {
            if let (Some(emit_time_nanos), Some(event_id)) = (
                payload.get("emit_time_nanos").and_then(|v| v.as_u64()),
                payload.get("event_id").and_then(|v| v.as_u64()),
            ) {
                self.received.fetch_add(1, Ordering::Relaxed);

                // Skip warmup events
                if event_id >= self.warmup_events {
                    let receive_time_nanos = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
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

/// Run a single 100-stage pipeline test
async fn run_100_stage_pipeline() -> anyhow::Result<Duration> {
    let warmup_events = env_u64(
        "OBZENFLOW_BENCH_100_STAGE_WARMUP_EVENTS",
        DEFAULT_WARMUP_EVENT_COUNT,
    );
    let test_events = env_u64(
        "OBZENFLOW_BENCH_100_STAGE_TEST_EVENTS",
        DEFAULT_TEST_EVENT_COUNT,
    );
    let expected_events = warmup_events + test_events;
    let pipeline_timeout = Duration::from_secs(env_u64(
        "OBZENFLOW_BENCH_100_STAGE_TIMEOUT_SECS",
        DEFAULT_PIPELINE_TIMEOUT_SECS,
    ));

    let temp_dir = tempdir()?;
    let journals_base_path = temp_dir.path().join(format!(
        "hundred_stage_{}",
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&journals_base_path)?;

    let source = TimestampedSource::new(expected_events);
    let (sink, latencies) = LatencySink::new(warmup_events, expected_events);
    let sink_clone = sink.clone();

    // Create 100 stages for true performance testing
    let handle = flow! {
        journals: disk_journals(journals_base_path),
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
            s20 = transform!("stage20" => PassthroughStage::new("stage20"));
            s21 = transform!("stage21" => PassthroughStage::new("stage21"));
            s22 = transform!("stage22" => PassthroughStage::new("stage22"));
            s23 = transform!("stage23" => PassthroughStage::new("stage23"));
            s24 = transform!("stage24" => PassthroughStage::new("stage24"));
            s25 = transform!("stage25" => PassthroughStage::new("stage25"));
            s26 = transform!("stage26" => PassthroughStage::new("stage26"));
            s27 = transform!("stage27" => PassthroughStage::new("stage27"));
            s28 = transform!("stage28" => PassthroughStage::new("stage28"));
            s29 = transform!("stage29" => PassthroughStage::new("stage29"));
            s30 = transform!("stage30" => PassthroughStage::new("stage30"));
            s31 = transform!("stage31" => PassthroughStage::new("stage31"));
            s32 = transform!("stage32" => PassthroughStage::new("stage32"));
            s33 = transform!("stage33" => PassthroughStage::new("stage33"));
            s34 = transform!("stage34" => PassthroughStage::new("stage34"));
            s35 = transform!("stage35" => PassthroughStage::new("stage35"));
            s36 = transform!("stage36" => PassthroughStage::new("stage36"));
            s37 = transform!("stage37" => PassthroughStage::new("stage37"));
            s38 = transform!("stage38" => PassthroughStage::new("stage38"));
            s39 = transform!("stage39" => PassthroughStage::new("stage39"));
            s40 = transform!("stage40" => PassthroughStage::new("stage40"));
            s41 = transform!("stage41" => PassthroughStage::new("stage41"));
            s42 = transform!("stage42" => PassthroughStage::new("stage42"));
            s43 = transform!("stage43" => PassthroughStage::new("stage43"));
            s44 = transform!("stage44" => PassthroughStage::new("stage44"));
            s45 = transform!("stage45" => PassthroughStage::new("stage45"));
            s46 = transform!("stage46" => PassthroughStage::new("stage46"));
            s47 = transform!("stage47" => PassthroughStage::new("stage47"));
            s48 = transform!("stage48" => PassthroughStage::new("stage48"));
            s49 = transform!("stage49" => PassthroughStage::new("stage49"));
            s50 = transform!("stage50" => PassthroughStage::new("stage50"));
            s51 = transform!("stage51" => PassthroughStage::new("stage51"));
            s52 = transform!("stage52" => PassthroughStage::new("stage52"));
            s53 = transform!("stage53" => PassthroughStage::new("stage53"));
            s54 = transform!("stage54" => PassthroughStage::new("stage54"));
            s55 = transform!("stage55" => PassthroughStage::new("stage55"));
            s56 = transform!("stage56" => PassthroughStage::new("stage56"));
            s57 = transform!("stage57" => PassthroughStage::new("stage57"));
            s58 = transform!("stage58" => PassthroughStage::new("stage58"));
            s59 = transform!("stage59" => PassthroughStage::new("stage59"));
            s60 = transform!("stage60" => PassthroughStage::new("stage60"));
            s61 = transform!("stage61" => PassthroughStage::new("stage61"));
            s62 = transform!("stage62" => PassthroughStage::new("stage62"));
            s63 = transform!("stage63" => PassthroughStage::new("stage63"));
            s64 = transform!("stage64" => PassthroughStage::new("stage64"));
            s65 = transform!("stage65" => PassthroughStage::new("stage65"));
            s66 = transform!("stage66" => PassthroughStage::new("stage66"));
            s67 = transform!("stage67" => PassthroughStage::new("stage67"));
            s68 = transform!("stage68" => PassthroughStage::new("stage68"));
            s69 = transform!("stage69" => PassthroughStage::new("stage69"));
            s70 = transform!("stage70" => PassthroughStage::new("stage70"));
            s71 = transform!("stage71" => PassthroughStage::new("stage71"));
            s72 = transform!("stage72" => PassthroughStage::new("stage72"));
            s73 = transform!("stage73" => PassthroughStage::new("stage73"));
            s74 = transform!("stage74" => PassthroughStage::new("stage74"));
            s75 = transform!("stage75" => PassthroughStage::new("stage75"));
            s76 = transform!("stage76" => PassthroughStage::new("stage76"));
            s77 = transform!("stage77" => PassthroughStage::new("stage77"));
            s78 = transform!("stage78" => PassthroughStage::new("stage78"));
            s79 = transform!("stage79" => PassthroughStage::new("stage79"));
            s80 = transform!("stage80" => PassthroughStage::new("stage80"));
            s81 = transform!("stage81" => PassthroughStage::new("stage81"));
            s82 = transform!("stage82" => PassthroughStage::new("stage82"));
            s83 = transform!("stage83" => PassthroughStage::new("stage83"));
            s84 = transform!("stage84" => PassthroughStage::new("stage84"));
            s85 = transform!("stage85" => PassthroughStage::new("stage85"));
            s86 = transform!("stage86" => PassthroughStage::new("stage86"));
            s87 = transform!("stage87" => PassthroughStage::new("stage87"));
            s88 = transform!("stage88" => PassthroughStage::new("stage88"));
            s89 = transform!("stage89" => PassthroughStage::new("stage89"));
            s90 = transform!("stage90" => PassthroughStage::new("stage90"));
            s91 = transform!("stage91" => PassthroughStage::new("stage91"));
            s92 = transform!("stage92" => PassthroughStage::new("stage92"));
            s93 = transform!("stage93" => PassthroughStage::new("stage93"));
            s94 = transform!("stage94" => PassthroughStage::new("stage94"));
            s95 = transform!("stage95" => PassthroughStage::new("stage95"));
            s96 = transform!("stage96" => PassthroughStage::new("stage96"));
            s97 = transform!("stage97" => PassthroughStage::new("stage97"));
            s98 = transform!("stage98" => PassthroughStage::new("stage98"));
            s99 = transform!("stage99" => PassthroughStage::new("stage99"));
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
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    // Start the pipeline (bounded wait so Criterion warmup doesn't hang forever).
    handle
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start pipeline: {e:?}"))?;

    let mut state_rx = handle.state_receiver();
    let deadline = Instant::now() + pipeline_timeout;
    while handle.is_running() {
        let now = Instant::now();
        if now >= deadline {
            break;
        }

        let remaining = deadline.saturating_duration_since(now);
        let tick = std::cmp::min(remaining, Duration::from_millis(250));
        match tokio::time::timeout(tick, state_rx.changed()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => break, // sender dropped
            Err(_) => {}         // periodic tick to re-check deadline/is_running
        }
    }

    if handle.is_running() {
        let _ = handle.stop_cancel().await;

        let stop_deadline = Instant::now() + Duration::from_secs(10);
        while handle.is_running() && Instant::now() < stop_deadline {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        return Err(anyhow::anyhow!(
            "100-stage pipeline did not complete within {:?} (final_state={:?}). Set `OBZENFLOW_BENCH_100_STAGE_TIMEOUT_SECS` to override.",
            pipeline_timeout,
            state_rx.borrow(),
        ));
    }

    let final_state = state_rx.borrow().clone();
    match final_state {
        PipelineState::Drained => {}
        PipelineState::Failed { reason, .. } => {
            return Err(anyhow::anyhow!("100-stage pipeline failed: {reason}"));
        }
        other => {
            return Err(anyhow::anyhow!(
                "100-stage pipeline terminated unexpectedly (final_state={other:?})"
            ));
        }
    }

    // Verify expected delivery count (best-effort: allow a short settle window).
    let settle_deadline = Instant::now() + Duration::from_secs(2);
    while sink_clone.received.load(Ordering::Relaxed) < expected_events
        && Instant::now() < settle_deadline
    {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let received = sink_clone.received.load(Ordering::Relaxed);
    if received < expected_events {
        return Err(anyhow::anyhow!(
            "100-stage pipeline completed but sink received {received}/{expected_events} events"
        ));
    }

    // Calculate median latency
    let mut collected = latencies.lock().await.clone();
    if collected.is_empty() {
        return Ok(Duration::ZERO);
    }

    collected.sort();
    Ok(collected[collected.len() / 2])
}

fn bench_100_stage_latency(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("100_stage_latency");

    group.sample_size(10); // Minimum required by Criterion
    group.measurement_time(Duration::from_secs(30)); // Keep bounded so CI/dev runs complete

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

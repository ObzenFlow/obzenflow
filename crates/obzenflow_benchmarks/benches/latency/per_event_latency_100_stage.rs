// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

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
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
// Monitoring removed per FLOWIP-056-666
use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use tokio::runtime::Runtime;

const DEFAULT_WARMUP_EVENT_COUNT: u64 = 2;
const DEFAULT_TEST_EVENT_COUNT: u64 = 20;

/// File-local payload type for the latency bench. The JSON shape matches
/// what `TimestampedSource` emits; the type itself is a FLOWIP-114c
/// topology fingerprint, not enforced at runtime.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct BenchEvent {
    event_id: u64,
    emit_time_nanos: u128,
}

impl TypedPayload for BenchEvent {
    const EVENT_TYPE: &'static str = "bench.timestamped_event";
}

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
                BenchEvent::versioned_event_type(),
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

impl TypedTransformHandler for PassthroughStage {
    type Input = BenchEvent;
    type Output = BenchEvent;

    fn process(&self, event: BenchEvent) -> Result<BenchEvent, HandlerError> {
        Ok(event)
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
        received: Arc<AtomicU64>,
        latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
    ) -> Self {
        Self {
            warmup_events,
            received,
            latencies,
        }
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

        Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
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

    let received = Arc::new(AtomicU64::new(0));
    let received_for_flow = received.clone();
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
        expected_events as usize,
    )));
    let latencies_for_flow = latencies.clone();

    // Create 100 stages for true performance testing
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = TimestampedSource::new(expected_events);
        let sink = LatencySink::new(warmup_events, received_for_flow, latencies_for_flow);
        let [
            stage1_handler, stage2_handler, stage3_handler, stage4_handler, stage5_handler, stage6_handler,
            stage7_handler, stage8_handler, stage9_handler, stage10_handler, stage11_handler, stage12_handler,
            stage13_handler, stage14_handler, stage15_handler, stage16_handler, stage17_handler, stage18_handler,
            stage19_handler, stage20_handler, stage21_handler, stage22_handler, stage23_handler, stage24_handler,
            stage25_handler, stage26_handler, stage27_handler, stage28_handler, stage29_handler, stage30_handler,
            stage31_handler, stage32_handler, stage33_handler, stage34_handler, stage35_handler, stage36_handler,
            stage37_handler, stage38_handler, stage39_handler, stage40_handler, stage41_handler, stage42_handler,
            stage43_handler, stage44_handler, stage45_handler, stage46_handler, stage47_handler, stage48_handler,
            stage49_handler, stage50_handler, stage51_handler, stage52_handler, stage53_handler, stage54_handler,
            stage55_handler, stage56_handler, stage57_handler, stage58_handler, stage59_handler, stage60_handler,
            stage61_handler, stage62_handler, stage63_handler, stage64_handler, stage65_handler, stage66_handler,
            stage67_handler, stage68_handler, stage69_handler, stage70_handler, stage71_handler, stage72_handler,
            stage73_handler, stage74_handler, stage75_handler, stage76_handler, stage77_handler, stage78_handler,
            stage79_handler, stage80_handler, stage81_handler, stage82_handler, stage83_handler, stage84_handler,
            stage85_handler, stage86_handler, stage87_handler, stage88_handler, stage89_handler, stage90_handler,
            stage91_handler, stage92_handler, stage93_handler, stage94_handler, stage95_handler, stage96_handler,
            stage97_handler, stage98_handler, stage99_handler,
        ] = std::array::from_fn(|_| PassthroughStage);

        Ok(flow! {
        journals: disk_journals(journals_base_path),

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
            s10 = transform!(BenchEvent -> BenchEvent => stage10_handler);
            s11 = transform!(BenchEvent -> BenchEvent => stage11_handler);
            s12 = transform!(BenchEvent -> BenchEvent => stage12_handler);
            s13 = transform!(BenchEvent -> BenchEvent => stage13_handler);
            s14 = transform!(BenchEvent -> BenchEvent => stage14_handler);
            s15 = transform!(BenchEvent -> BenchEvent => stage15_handler);
            s16 = transform!(BenchEvent -> BenchEvent => stage16_handler);
            s17 = transform!(BenchEvent -> BenchEvent => stage17_handler);
            s18 = transform!(BenchEvent -> BenchEvent => stage18_handler);
            s19 = transform!(BenchEvent -> BenchEvent => stage19_handler);
            s20 = transform!(BenchEvent -> BenchEvent => stage20_handler);
            s21 = transform!(BenchEvent -> BenchEvent => stage21_handler);
            s22 = transform!(BenchEvent -> BenchEvent => stage22_handler);
            s23 = transform!(BenchEvent -> BenchEvent => stage23_handler);
            s24 = transform!(BenchEvent -> BenchEvent => stage24_handler);
            s25 = transform!(BenchEvent -> BenchEvent => stage25_handler);
            s26 = transform!(BenchEvent -> BenchEvent => stage26_handler);
            s27 = transform!(BenchEvent -> BenchEvent => stage27_handler);
            s28 = transform!(BenchEvent -> BenchEvent => stage28_handler);
            s29 = transform!(BenchEvent -> BenchEvent => stage29_handler);
            s30 = transform!(BenchEvent -> BenchEvent => stage30_handler);
            s31 = transform!(BenchEvent -> BenchEvent => stage31_handler);
            s32 = transform!(BenchEvent -> BenchEvent => stage32_handler);
            s33 = transform!(BenchEvent -> BenchEvent => stage33_handler);
            s34 = transform!(BenchEvent -> BenchEvent => stage34_handler);
            s35 = transform!(BenchEvent -> BenchEvent => stage35_handler);
            s36 = transform!(BenchEvent -> BenchEvent => stage36_handler);
            s37 = transform!(BenchEvent -> BenchEvent => stage37_handler);
            s38 = transform!(BenchEvent -> BenchEvent => stage38_handler);
            s39 = transform!(BenchEvent -> BenchEvent => stage39_handler);
            s40 = transform!(BenchEvent -> BenchEvent => stage40_handler);
            s41 = transform!(BenchEvent -> BenchEvent => stage41_handler);
            s42 = transform!(BenchEvent -> BenchEvent => stage42_handler);
            s43 = transform!(BenchEvent -> BenchEvent => stage43_handler);
            s44 = transform!(BenchEvent -> BenchEvent => stage44_handler);
            s45 = transform!(BenchEvent -> BenchEvent => stage45_handler);
            s46 = transform!(BenchEvent -> BenchEvent => stage46_handler);
            s47 = transform!(BenchEvent -> BenchEvent => stage47_handler);
            s48 = transform!(BenchEvent -> BenchEvent => stage48_handler);
            s49 = transform!(BenchEvent -> BenchEvent => stage49_handler);
            s50 = transform!(BenchEvent -> BenchEvent => stage50_handler);
            s51 = transform!(BenchEvent -> BenchEvent => stage51_handler);
            s52 = transform!(BenchEvent -> BenchEvent => stage52_handler);
            s53 = transform!(BenchEvent -> BenchEvent => stage53_handler);
            s54 = transform!(BenchEvent -> BenchEvent => stage54_handler);
            s55 = transform!(BenchEvent -> BenchEvent => stage55_handler);
            s56 = transform!(BenchEvent -> BenchEvent => stage56_handler);
            s57 = transform!(BenchEvent -> BenchEvent => stage57_handler);
            s58 = transform!(BenchEvent -> BenchEvent => stage58_handler);
            s59 = transform!(BenchEvent -> BenchEvent => stage59_handler);
            s60 = transform!(BenchEvent -> BenchEvent => stage60_handler);
            s61 = transform!(BenchEvent -> BenchEvent => stage61_handler);
            s62 = transform!(BenchEvent -> BenchEvent => stage62_handler);
            s63 = transform!(BenchEvent -> BenchEvent => stage63_handler);
            s64 = transform!(BenchEvent -> BenchEvent => stage64_handler);
            s65 = transform!(BenchEvent -> BenchEvent => stage65_handler);
            s66 = transform!(BenchEvent -> BenchEvent => stage66_handler);
            s67 = transform!(BenchEvent -> BenchEvent => stage67_handler);
            s68 = transform!(BenchEvent -> BenchEvent => stage68_handler);
            s69 = transform!(BenchEvent -> BenchEvent => stage69_handler);
            s70 = transform!(BenchEvent -> BenchEvent => stage70_handler);
            s71 = transform!(BenchEvent -> BenchEvent => stage71_handler);
            s72 = transform!(BenchEvent -> BenchEvent => stage72_handler);
            s73 = transform!(BenchEvent -> BenchEvent => stage73_handler);
            s74 = transform!(BenchEvent -> BenchEvent => stage74_handler);
            s75 = transform!(BenchEvent -> BenchEvent => stage75_handler);
            s76 = transform!(BenchEvent -> BenchEvent => stage76_handler);
            s77 = transform!(BenchEvent -> BenchEvent => stage77_handler);
            s78 = transform!(BenchEvent -> BenchEvent => stage78_handler);
            s79 = transform!(BenchEvent -> BenchEvent => stage79_handler);
            s80 = transform!(BenchEvent -> BenchEvent => stage80_handler);
            s81 = transform!(BenchEvent -> BenchEvent => stage81_handler);
            s82 = transform!(BenchEvent -> BenchEvent => stage82_handler);
            s83 = transform!(BenchEvent -> BenchEvent => stage83_handler);
            s84 = transform!(BenchEvent -> BenchEvent => stage84_handler);
            s85 = transform!(BenchEvent -> BenchEvent => stage85_handler);
            s86 = transform!(BenchEvent -> BenchEvent => stage86_handler);
            s87 = transform!(BenchEvent -> BenchEvent => stage87_handler);
            s88 = transform!(BenchEvent -> BenchEvent => stage88_handler);
            s89 = transform!(BenchEvent -> BenchEvent => stage89_handler);
            s90 = transform!(BenchEvent -> BenchEvent => stage90_handler);
            s91 = transform!(BenchEvent -> BenchEvent => stage91_handler);
            s92 = transform!(BenchEvent -> BenchEvent => stage92_handler);
            s93 = transform!(BenchEvent -> BenchEvent => stage93_handler);
            s94 = transform!(BenchEvent -> BenchEvent => stage94_handler);
            s95 = transform!(BenchEvent -> BenchEvent => stage95_handler);
            s96 = transform!(BenchEvent -> BenchEvent => stage96_handler);
            s97 = transform!(BenchEvent -> BenchEvent => stage97_handler);
            s98 = transform!(BenchEvent -> BenchEvent => stage98_handler);
            s99 = transform!(BenchEvent -> BenchEvent => stage99_handler);
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
    })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
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
    while received.load(Ordering::Relaxed) < expected_events && Instant::now() < settle_deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let received_count = received.load(Ordering::Relaxed);
    if received_count < expected_events {
        return Err(anyhow::anyhow!(
            "100-stage pipeline completed but sink received {received_count}/{expected_events} events"
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

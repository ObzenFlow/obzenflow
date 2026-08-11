// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! 100-Stage Pipeline Latency Benchmark with MemoryJournal
//!
//! This benchmark tests the same 100-stage pipeline as per_event_latency_100_stage.rs
//! but uses MemoryJournal instead of DiskJournal to isolate I/O overhead.
//! By comparing the two benchmarks, we can determine how much of the latency
//! is due to disk I/O versus other factors (middleware, task scheduling, etc).

use criterion::{criterion_group, criterion_main, Criterion};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkHandler, TypedFiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
// Metrics are automatically collected by MetricsAggregator from the event journal.
use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::runtime::Runtime;

const WARMUP_EVENT_COUNT: u64 = 10;
const TEST_EVENT_COUNT: u64 = 100;

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

/// Test source that emits timestamped events
#[derive(Clone, Debug)]
struct TimestampedSource {
    total_events: u64,
    emitted: Arc<AtomicU64>,
}

impl TimestampedSource {
    fn new(total_events: u64) -> Self {
        Self {
            total_events,
            emitted: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl TypedFiniteSourceHandler for TimestampedSource {
    type Output = BenchEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        let current = self.emitted.fetch_add(1, Ordering::Relaxed);
        if current < self.total_events {
            Ok(Some(vec![BenchEvent {
                event_id: current,
                emit_time_nanos: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
            }]))
        } else {
            Ok(None)
        }
    }
}

/// Passthrough stage
#[derive(Clone, Copy, Debug)]
struct PassthroughStage;

impl PassthroughStage {
    fn new(_name: &str) -> Self {
        Self
    }
}

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
    received: Arc<AtomicU64>,
    latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
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
                if event_id >= WARMUP_EVENT_COUNT {
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

/// Run a single 100-stage pipeline test with MemoryJournal
async fn run_100_stage_pipeline_memory() -> anyhow::Result<Duration> {
    let received = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
        (WARMUP_EVENT_COUNT + TEST_EVENT_COUNT) as usize,
    )));
    let received_for_flow = received.clone();
    let latencies_for_flow = latencies.clone();

    // Create 100 stages for true performance testing
    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let timestamped_source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
        let passthrough = PassthroughStage::new("all stages");
        let latency_sink = LatencySink {
            received: received_for_flow,
            latencies: latencies_for_flow,
        };

        Ok(flow! {
        journals: memory_journals(),

        stages: {
            src = source!(BenchEvent => timestamped_source);
            s1 = transform!(BenchEvent -> BenchEvent => passthrough);
            s2 = transform!(BenchEvent -> BenchEvent => passthrough);
            s3 = transform!(BenchEvent -> BenchEvent => passthrough);
            s4 = transform!(BenchEvent -> BenchEvent => passthrough);
            s5 = transform!(BenchEvent -> BenchEvent => passthrough);
            s6 = transform!(BenchEvent -> BenchEvent => passthrough);
            s7 = transform!(BenchEvent -> BenchEvent => passthrough);
            s8 = transform!(BenchEvent -> BenchEvent => passthrough);
            s9 = transform!(BenchEvent -> BenchEvent => passthrough);
            s10 = transform!(BenchEvent -> BenchEvent => passthrough);
            s11 = transform!(BenchEvent -> BenchEvent => passthrough);
            s12 = transform!(BenchEvent -> BenchEvent => passthrough);
            s13 = transform!(BenchEvent -> BenchEvent => passthrough);
            s14 = transform!(BenchEvent -> BenchEvent => passthrough);
            s15 = transform!(BenchEvent -> BenchEvent => passthrough);
            s16 = transform!(BenchEvent -> BenchEvent => passthrough);
            s17 = transform!(BenchEvent -> BenchEvent => passthrough);
            s18 = transform!(BenchEvent -> BenchEvent => passthrough);
            s19 = transform!(BenchEvent -> BenchEvent => passthrough);
            s20 = transform!(BenchEvent -> BenchEvent => passthrough);
            s21 = transform!(BenchEvent -> BenchEvent => passthrough);
            s22 = transform!(BenchEvent -> BenchEvent => passthrough);
            s23 = transform!(BenchEvent -> BenchEvent => passthrough);
            s24 = transform!(BenchEvent -> BenchEvent => passthrough);
            s25 = transform!(BenchEvent -> BenchEvent => passthrough);
            s26 = transform!(BenchEvent -> BenchEvent => passthrough);
            s27 = transform!(BenchEvent -> BenchEvent => passthrough);
            s28 = transform!(BenchEvent -> BenchEvent => passthrough);
            s29 = transform!(BenchEvent -> BenchEvent => passthrough);
            s30 = transform!(BenchEvent -> BenchEvent => passthrough);
            s31 = transform!(BenchEvent -> BenchEvent => passthrough);
            s32 = transform!(BenchEvent -> BenchEvent => passthrough);
            s33 = transform!(BenchEvent -> BenchEvent => passthrough);
            s34 = transform!(BenchEvent -> BenchEvent => passthrough);
            s35 = transform!(BenchEvent -> BenchEvent => passthrough);
            s36 = transform!(BenchEvent -> BenchEvent => passthrough);
            s37 = transform!(BenchEvent -> BenchEvent => passthrough);
            s38 = transform!(BenchEvent -> BenchEvent => passthrough);
            s39 = transform!(BenchEvent -> BenchEvent => passthrough);
            s40 = transform!(BenchEvent -> BenchEvent => passthrough);
            s41 = transform!(BenchEvent -> BenchEvent => passthrough);
            s42 = transform!(BenchEvent -> BenchEvent => passthrough);
            s43 = transform!(BenchEvent -> BenchEvent => passthrough);
            s44 = transform!(BenchEvent -> BenchEvent => passthrough);
            s45 = transform!(BenchEvent -> BenchEvent => passthrough);
            s46 = transform!(BenchEvent -> BenchEvent => passthrough);
            s47 = transform!(BenchEvent -> BenchEvent => passthrough);
            s48 = transform!(BenchEvent -> BenchEvent => passthrough);
            s49 = transform!(BenchEvent -> BenchEvent => passthrough);
            s50 = transform!(BenchEvent -> BenchEvent => passthrough);
            s51 = transform!(BenchEvent -> BenchEvent => passthrough);
            s52 = transform!(BenchEvent -> BenchEvent => passthrough);
            s53 = transform!(BenchEvent -> BenchEvent => passthrough);
            s54 = transform!(BenchEvent -> BenchEvent => passthrough);
            s55 = transform!(BenchEvent -> BenchEvent => passthrough);
            s56 = transform!(BenchEvent -> BenchEvent => passthrough);
            s57 = transform!(BenchEvent -> BenchEvent => passthrough);
            s58 = transform!(BenchEvent -> BenchEvent => passthrough);
            s59 = transform!(BenchEvent -> BenchEvent => passthrough);
            s60 = transform!(BenchEvent -> BenchEvent => passthrough);
            s61 = transform!(BenchEvent -> BenchEvent => passthrough);
            s62 = transform!(BenchEvent -> BenchEvent => passthrough);
            s63 = transform!(BenchEvent -> BenchEvent => passthrough);
            s64 = transform!(BenchEvent -> BenchEvent => passthrough);
            s65 = transform!(BenchEvent -> BenchEvent => passthrough);
            s66 = transform!(BenchEvent -> BenchEvent => passthrough);
            s67 = transform!(BenchEvent -> BenchEvent => passthrough);
            s68 = transform!(BenchEvent -> BenchEvent => passthrough);
            s69 = transform!(BenchEvent -> BenchEvent => passthrough);
            s70 = transform!(BenchEvent -> BenchEvent => passthrough);
            s71 = transform!(BenchEvent -> BenchEvent => passthrough);
            s72 = transform!(BenchEvent -> BenchEvent => passthrough);
            s73 = transform!(BenchEvent -> BenchEvent => passthrough);
            s74 = transform!(BenchEvent -> BenchEvent => passthrough);
            s75 = transform!(BenchEvent -> BenchEvent => passthrough);
            s76 = transform!(BenchEvent -> BenchEvent => passthrough);
            s77 = transform!(BenchEvent -> BenchEvent => passthrough);
            s78 = transform!(BenchEvent -> BenchEvent => passthrough);
            s79 = transform!(BenchEvent -> BenchEvent => passthrough);
            s80 = transform!(BenchEvent -> BenchEvent => passthrough);
            s81 = transform!(BenchEvent -> BenchEvent => passthrough);
            s82 = transform!(BenchEvent -> BenchEvent => passthrough);
            s83 = transform!(BenchEvent -> BenchEvent => passthrough);
            s84 = transform!(BenchEvent -> BenchEvent => passthrough);
            s85 = transform!(BenchEvent -> BenchEvent => passthrough);
            s86 = transform!(BenchEvent -> BenchEvent => passthrough);
            s87 = transform!(BenchEvent -> BenchEvent => passthrough);
            s88 = transform!(BenchEvent -> BenchEvent => passthrough);
            s89 = transform!(BenchEvent -> BenchEvent => passthrough);
            s90 = transform!(BenchEvent -> BenchEvent => passthrough);
            s91 = transform!(BenchEvent -> BenchEvent => passthrough);
            s92 = transform!(BenchEvent -> BenchEvent => passthrough);
            s93 = transform!(BenchEvent -> BenchEvent => passthrough);
            s94 = transform!(BenchEvent -> BenchEvent => passthrough);
            s95 = transform!(BenchEvent -> BenchEvent => passthrough);
            s96 = transform!(BenchEvent -> BenchEvent => passthrough);
            s97 = transform!(BenchEvent -> BenchEvent => passthrough);
            s98 = transform!(BenchEvent -> BenchEvent => passthrough);
            s99 = transform!(BenchEvent -> BenchEvent => passthrough);
            snk = sink!(BenchEvent => latency_sink);
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
    });

    let handle = flow_definition
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    // Start the pipeline
    handle
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {e:?}"))?;

    // Wait for completion
    let timeout = Duration::from_secs(300); // Extended timeout for true 100 stages
    let start = Instant::now();

    while received.load(Ordering::Relaxed) < WARMUP_EVENT_COUNT + TEST_EVENT_COUNT {
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
    obzenflow_benchmarks::init_tracing();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("100_stage_latency_memory");

    group.sample_size(10); // Minimum required by Criterion
    group.measurement_time(Duration::from_secs(180)); // Extended measurement time for true 100 stages

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

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Isolated 2-Stage Pipeline Latency Benchmark
//!
//! This is a completely standalone benchmark for ONLY 2-stage pipelines.
//! No other configurations, no shared code with other stage counts.
//! This isolation helps determine if the performance anomaly is due to
//! benchmark ordering, warmup effects, or genuine framework issues.

use criterion::{criterion_group, criterion_main, Criterion};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::WriterId;
use obzenflow_dsl::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
// Monitoring removed per FLOWIP-056-666
use async_trait::async_trait;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tempfile::tempdir;
use tokio::runtime::Runtime;

const WARMUP_EVENT_COUNT: u64 = 10;
const TEST_EVENT_COUNT: u64 = 100;

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
    fn new(_name: &str) -> Self {
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
    received: Arc<AtomicU64>,
    latencies: Arc<tokio::sync::Mutex<Vec<Duration>>>,
}

impl LatencySink {
    fn new(expected_count: u64) -> (Self, Arc<tokio::sync::Mutex<Vec<Duration>>>) {
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::with_capacity(
            expected_count as usize,
        )));
        (
            Self {
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

        Ok(DeliveryPayload::success("noop", DeliveryMethod::Noop, None))
    }
}

/// Run a single 2-stage pipeline test
async fn run_2_stage_pipeline() -> anyhow::Result<Duration> {
    let temp_dir = tempdir()?;
    let journals_base_path = temp_dir.path().join(format!(
        "two_stage_{}",
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&journals_base_path)?;

    let source = TimestampedSource::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let (sink, latencies) = LatencySink::new(WARMUP_EVENT_COUNT + TEST_EVENT_COUNT);
    let sink_clone = sink.clone();

    let handle = flow! {
        journals: disk_journals(journals_base_path),
        middleware: [],

        stages: {
            src = source!("source" => source);
            s1 = transform!("stage1" => PassthroughStage::new("stage1"));
            snk = sink!("sink" => sink);
        },

        topology: {
            src |> s1;
            s1 |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    // Start the pipeline
    handle
        .run()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {e:?}"))?;

    // Wait for completion
    let timeout = Duration::from_secs(30);
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

fn bench_2_stage_latency(c: &mut Criterion) {
    obzenflow_benchmarks::init_tracing();
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("2_stage_latency");

    group.sample_size(20); // Reduced for faster benchmarking
    group.measurement_time(Duration::from_secs(30)); // Longer measurement

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

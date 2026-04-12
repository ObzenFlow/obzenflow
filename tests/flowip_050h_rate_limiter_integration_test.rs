// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::{rate_limit, RateLimiterBuilder};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde_json::json;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

#[derive(Clone, Debug)]
struct SequenceSource {
    total: usize,
    emitted: usize,
    writer_id: WriterId,
}

impl SequenceSource {
    fn new(total: usize) -> Self {
        Self {
            total,
            emitted: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for SequenceSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted >= self.total {
            return Ok(None);
        }

        let event = ChainEventFactory::data_event(
            self.writer_id,
            "test.sequence",
            json!({ "index": self.emitted }),
        );
        self.emitted += 1;
        Ok(Some(vec![event]))
    }
}

#[derive(Clone, Debug)]
struct BatchedSource {
    batches: Vec<usize>,
    batch_index: usize,
    next_event_id: usize,
    writer_id: WriterId,
}

impl BatchedSource {
    fn new(batches: Vec<usize>) -> Self {
        Self {
            batches,
            batch_index: 0,
            next_event_id: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for BatchedSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let Some(batch_size) = self.batches.get(self.batch_index).copied() else {
            return Ok(None);
        };

        self.batch_index += 1;
        let start = self.next_event_id;
        self.next_event_id += batch_size;

        let events = (start..start + batch_size)
            .map(|index| {
                ChainEventFactory::data_event(
                    self.writer_id,
                    "test.batch",
                    json!({ "index": index }),
                )
            })
            .collect();

        Ok(Some(events))
    }
}

#[derive(Clone, Debug)]
struct PassthroughTransform;

#[async_trait]
impl TransformHandler for PassthroughTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    count: Arc<AtomicUsize>,
}

impl CountingSink {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                count: count.clone(),
            },
            count,
        )
    }
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        if event.is_data() {
            self.count.fetch_add(1, Ordering::Relaxed);
        }

        Ok(DeliveryPayload::success(
            "counting_sink",
            DeliveryMethod::Custom("Count".to_string()),
            None,
        ))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_low_rate_half_eps_processes_all_events() -> Result<()> {
    let (sink, count) = CountingSink::new();
    let handle = flow! {
        name: "flowip_050h_low_rate_half_eps",
        journals: disk_journals(unique_journal_dir("flowip_050h_low_rate_half_eps")),
        middleware: [],

        stages: {
            src = source!(SequenceSource::new(2));
            throttled = transform!(PassthroughTransform, [
                rate_limit(0.5)
            ]);
            snk = sink!(sink);
        },

        topology: {
            src |> throttled;
            throttled |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create low-rate flow: {e}"))?;

    let start = Instant::now();
    tokio::time::timeout(Duration::from_secs(8), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("low-rate flow run timed out"))?
        .map_err(|e| anyhow::anyhow!("low-rate flow run failed: {e}"))?;
    let elapsed = start.elapsed();

    assert_eq!(count.load(Ordering::Relaxed), 2);
    assert!(
        elapsed >= Duration::from_millis(1500),
        "expected low-rate limiter to delay the second event, elapsed={elapsed:?}"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_weighted_default_burst_makes_progress() -> Result<()> {
    let (sink, count) = CountingSink::new();
    let handle = flow! {
        name: "flowip_050h_weighted_default_burst",
        journals: disk_journals(unique_journal_dir("flowip_050h_weighted_default_burst")),
        middleware: [],

        stages: {
            src = source!(SequenceSource::new(1));
            throttled = transform!(PassthroughTransform, [
                RateLimiterBuilder::new(2.0)
                    .with_cost_per_event(5.0)
                    .build()
            ]);
            snk = sink!(sink);
        },

        topology: {
            src |> throttled;
            throttled |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create weighted flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(3), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("weighted flow run timed out"))?
        .map_err(|e| anyhow::anyhow!("weighted flow run failed: {e}"))?;

    assert_eq!(count.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_invalid_explicit_burst_fails_at_materialisation() {
    let result = flow! {
        name: "flowip_050h_invalid_rate_limiter",
        journals: disk_journals(unique_journal_dir("flowip_050h_invalid_rate_limiter")),
        middleware: [],

        stages: {
            src = source!(SequenceSource::new(1));
            throttled = transform!(PassthroughTransform, [
                RateLimiterBuilder::new(10.0)
                    .with_burst(2.0)
                    .with_cost_per_event(5.0)
                    .build()
            ]);
            snk = sink!(CountingSink::new().0);
        },

        topology: {
            src |> throttled;
            throttled |> snk;
        }
    }
    .await;

    let err = match result {
        Ok(_) => panic!("expected invalid rate limiter configuration to fail"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("Invalid rate_limiter configuration"),
        "error: {err}"
    );
    assert!(err.contains("burst_capacity"), "error: {err}");
    assert!(err.contains("cost_per_event"), "error: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_source_stage_limits_per_poll_and_documents_batching() -> Result<()> {
    let (sink, count) = CountingSink::new();
    let handle = flow! {
        name: "flowip_050h_source_poll_gating",
        journals: disk_journals(unique_journal_dir("flowip_050h_source_poll_gating")),
        middleware: [],

        stages: {
            src = source!(BatchedSource::new(vec![2, 2]), [
                rate_limit(1.0)
            ]);
            passthrough = transform!(PassthroughTransform);
            snk = sink!(sink);
        },

        topology: {
            src |> passthrough;
            passthrough |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create source poll-gating flow: {e}"))?;

    let start = Instant::now();
    tokio::time::timeout(Duration::from_secs(5), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("source poll-gating flow run timed out"))?
        .map_err(|e| anyhow::anyhow!("source poll-gating flow run failed: {e}"))?;
    let elapsed = start.elapsed();

    assert_eq!(count.load(Ordering::Relaxed), 4);
    assert!(
        elapsed >= Duration::from_millis(900),
        "expected at least one poll to be delayed, elapsed={elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(2500),
        "source-stage rate limiting should gate polls, not each emitted event, elapsed={elapsed:?}"
    );

    Ok(())
}

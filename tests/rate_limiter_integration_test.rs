// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::middleware::{rate_limit_with_burst, RateLimiterBuilder};
use obzenflow_core::event::chain_event::ChainEventContent;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload, RateLimiterEvent,
};
use obzenflow_core::journal::Journal;
use obzenflow_core::TypedPayload;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{sink, source, test_flow, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// File-local payload for the rate-limiter integration test. The JSON
/// shape matches what `SequenceSource` / `BatchedSource` emit; the type
/// fingerprints the stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RateLimiterTestEvent {
    index: u64,
}

impl TypedPayload for RateLimiterTestEvent {
    const EVENT_TYPE: &'static str = "rate_limiter.event";
}
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

async fn rate_limiter_delayed_events(
    stage_journal: &Arc<dyn Journal<ChainEvent>>,
) -> Result<usize> {
    let mut reader = stage_journal
        .reader()
        .await
        .map_err(|e| anyhow!("failed to create stage journal reader: {e}"))?;

    let mut delayed: usize = 0;
    loop {
        match reader.next().await {
            Ok(Some(envelope)) => {
                if let ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::RateLimiter(RateLimiterEvent::Delayed { .. }),
                )) = &envelope.event.content
                {
                    delayed += 1;
                }
            }
            Ok(None) => return Ok(delayed),
            Err(e) => return Err(anyhow!("failed to read stage journal: {e}")),
        }
    }
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
    let test_handle = test_flow! {
        name: "rate_limiter_low_rate_half_eps",
        journals: disk_journals(unique_journal_dir("rate_limiter_low_rate_half_eps")),
        middleware: [],

        stages: {
            src = source!(RateLimiterTestEvent => SequenceSource::new(2));
            throttled = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => PassthroughTransform, [
                rate_limit_with_burst(50.0, 1.0)
            ]);
            snk = sink!(RateLimiterTestEvent => sink);
        },

        topology: {
            src |> throttled;
            throttled |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to create low-rate flow: {e}"))?;

    let (_, throttled_journal) = test_handle
        .stage_journal_for_test("throttled")
        .map_err(|e| anyhow!("failed to look up throttled stage journal: {e}"))?;

    tokio::time::timeout(Duration::from_secs(8), test_handle.into_inner().run())
        .await
        .map_err(|_| anyhow!("low-rate flow run timed out"))?
        .map_err(|e| anyhow!("low-rate flow run failed: {e}"))?;

    assert_eq!(count.load(Ordering::Relaxed), 2);
    let delayed = rate_limiter_delayed_events(&throttled_journal).await?;
    assert!(
        delayed >= 1,
        "expected at least one rate limiter delayed event on the throttled stage journal"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_weighted_default_burst_makes_progress() -> Result<()> {
    let (sink, count) = CountingSink::new();
    let test_handle = test_flow! {
        name: "rate_limiter_weighted_default_burst",
        journals: disk_journals(unique_journal_dir("rate_limiter_weighted_default_burst")),
        middleware: [],

        stages: {
            src = source!(RateLimiterTestEvent => SequenceSource::new(1));
            throttled = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => PassthroughTransform, [
                RateLimiterBuilder::new(2.0)
                    .with_cost_per_event(5.0)
                    .build()
            ]);
            snk = sink!(RateLimiterTestEvent => sink);
        },

        topology: {
            src |> throttled;
            throttled |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to create weighted flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(2), test_handle.into_inner().run())
        .await
        .map_err(|_| anyhow!("weighted flow run timed out"))?
        .map_err(|e| anyhow!("weighted flow run failed: {e}"))?;

    assert_eq!(count.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_invalid_explicit_burst_fails_at_materialisation() {
    let result = test_flow! {
        name: "rate_limiter_invalid",
        journals: disk_journals(unique_journal_dir("rate_limiter_invalid")),
        middleware: [],

        stages: {
            src = source!(RateLimiterTestEvent => SequenceSource::new(1));
            throttled = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => PassthroughTransform, [
                RateLimiterBuilder::new(10.0)
                    .with_burst(2.0)
                    .with_cost_per_event(5.0)
                    .build()
            ]);
            snk = sink!(RateLimiterTestEvent => CountingSink::new().0);
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
        err.contains("Invalid configuration for middleware 'rate_limiter'"),
        "error: {err}"
    );
    assert!(err.contains("burst_capacity"), "error: {err}");
    assert!(err.contains("cost_per_event"), "error: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_source_stage_limits_per_poll_and_documents_batching() -> Result<()> {
    let (sink, count) = CountingSink::new();
    let test_handle = test_flow! {
        name: "rate_limiter_source_poll_gating",
        journals: disk_journals(unique_journal_dir("rate_limiter_source_poll_gating")),
        middleware: [],

        stages: {
            src = source!(RateLimiterTestEvent => BatchedSource::new(vec![2, 2]), [
                rate_limit_with_burst(50.0, 1.0)
            ]);
            passthrough = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => PassthroughTransform);
            snk = sink!(RateLimiterTestEvent => sink);
        },

        topology: {
            src |> passthrough;
            passthrough |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to create source poll-gating flow: {e}"))?;

    let (_, src_journal) = test_handle
        .stage_journal_for_test("src")
        .map_err(|e| anyhow!("failed to look up src stage journal: {e}"))?;

    tokio::time::timeout(Duration::from_secs(8), test_handle.into_inner().run())
        .await
        .map_err(|_| anyhow!("source poll-gating flow run timed out"))?
        .map_err(|e| anyhow!("source poll-gating flow run failed: {e}"))?;

    assert_eq!(count.load(Ordering::Relaxed), 4);
    let delayed = rate_limiter_delayed_events(&src_journal).await?;
    assert!(
        delayed >= 1,
        "expected at least one rate limiter delayed event on the src stage journal"
    );

    Ok(())
}

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
use obzenflow_dsl::{async_source, join, sink, source, test_flow, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, FiniteSourceHandler, JoinHandler, SinkHandler, TransformHandler,
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
            RateLimiterTestEvent::versioned_event_type(),
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
                    RateLimiterTestEvent::versioned_event_type(),
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
            src = source!(RateLimiterTestEvent => SequenceSource::new(2), [
                rate_limit_with_burst(50.0, 1.0)
            ]);
            throttled = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => PassthroughTransform);
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
        .stage_journal_for_test("src")
        .map_err(|e| anyhow!("failed to look up source stage journal: {e}"))?;

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
            src = source!(RateLimiterTestEvent => SequenceSource::new(1), [
                RateLimiterBuilder::new(2.0)
                    .with_cost_per_event(5.0)
                    .build()
            ]);
            throttled = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => PassthroughTransform);
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
            src = source!(RateLimiterTestEvent => SequenceSource::new(1), [
                RateLimiterBuilder::new(10.0)
                    .with_burst(2.0)
                    .with_cost_per_event(5.0)
                    .build()
            ]);
            throttled = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => PassthroughTransform);
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

/// Source step in the FLOWIP-114m no-charge tests. `Done` ends the source.
#[derive(Clone, Copy, Debug)]
enum SourceStep {
    Data,
    Empty,
    Err,
    Done,
}

/// Sync finite source that scripts a sequence of polls so tests can exercise
/// empty-batch and error outcomes alongside data batches.
#[derive(Clone, Debug)]
struct ScriptedSyncSource {
    steps: Vec<SourceStep>,
    index: usize,
    next_event_id: usize,
    writer_id: WriterId,
}

impl ScriptedSyncSource {
    fn new(steps: Vec<SourceStep>) -> Self {
        Self {
            steps,
            index: 0,
            next_event_id: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for ScriptedSyncSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let step = self
            .steps
            .get(self.index)
            .copied()
            .unwrap_or(SourceStep::Done);
        self.index += 1;
        match step {
            SourceStep::Data => {
                let id = self.next_event_id;
                self.next_event_id += 1;
                Ok(Some(vec![ChainEventFactory::data_event(
                    self.writer_id,
                    RateLimiterTestEvent::versioned_event_type(),
                    json!({ "index": id }),
                )]))
            }
            SourceStep::Empty => Ok(Some(Vec::new())),
            SourceStep::Err => Err(SourceError::Other("scripted error".to_string())),
            SourceStep::Done => Ok(None),
        }
    }
}

/// Async counterpart of `ScriptedSyncSource`.
#[derive(Clone, Debug)]
struct ScriptedAsyncSource {
    steps: Vec<SourceStep>,
    index: usize,
    next_event_id: usize,
    writer_id: WriterId,
}

impl ScriptedAsyncSource {
    fn new(steps: Vec<SourceStep>) -> Self {
        Self {
            steps,
            index: 0,
            next_event_id: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for ScriptedAsyncSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let step = self
            .steps
            .get(self.index)
            .copied()
            .unwrap_or(SourceStep::Done);
        self.index += 1;
        tokio::task::yield_now().await;
        match step {
            SourceStep::Data => {
                let id = self.next_event_id;
                self.next_event_id += 1;
                Ok(Some(vec![ChainEventFactory::data_event(
                    self.writer_id,
                    RateLimiterTestEvent::versioned_event_type(),
                    json!({ "index": id }),
                )]))
            }
            SourceStep::Empty => Ok(Some(Vec::new())),
            SourceStep::Err => Err(SourceError::Other("scripted error".to_string())),
            SourceStep::Done => Ok(None),
        }
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
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

// ----- FLOWIP-114m no-charge regression tests -----
//
// These tests pin the new finite-source rule: the rate limiter charges only
// successful non-empty batches. EOF (`Ok(None)`), empty batches
// (`Ok(Some(vec![]))`) and source errors (`Err(...)`) consume no token,
// increment no admission counter, and emit no `Delayed` event.
//
// Each test uses `rate_limit_with_burst(refill_rate, capacity)` where
// `capacity == expected_admission_count`. If the rule were violated (i.e. the
// rate limiter charged a no-charge poll) the bucket would empty an admission
// early, the next genuine admission would block for at least `1 / refill_rate`
// seconds, and a `Delayed` event would be journaled. The assertions
// `count == expected_admission_count && delayed == 0` therefore fail under
// the buggy path.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_async_finite_does_not_charge_eof_poll() -> Result<()> {
    let (sink, count) = CountingSink::new();
    let test_handle = test_flow! {
        name: "rate_limiter_async_finite_eof_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_async_finite_eof_no_charge")),
        middleware: [],

        stages: {
            src = async_source!(RateLimiterTestEvent => ScriptedAsyncSource::new(vec![
                SourceStep::Data,
                SourceStep::Data,
                SourceStep::Done,
            ]), [
                rate_limit_with_burst(1.0, 2.0)
            ]);
            snk = sink!(RateLimiterTestEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to create async EOF no-charge flow: {e}"))?;

    let (_, src_journal) = test_handle
        .stage_journal_for_test("src")
        .map_err(|e| anyhow!("failed to look up src stage journal: {e}"))?;

    tokio::time::timeout(Duration::from_secs(4), test_handle.into_inner().run())
        .await
        .map_err(|_| anyhow!("async EOF no-charge flow run timed out"))?
        .map_err(|e| anyhow!("async EOF no-charge flow run failed: {e}"))?;

    assert_eq!(count.load(Ordering::Relaxed), 2);
    let delayed = rate_limiter_delayed_events(&src_journal).await?;
    assert_eq!(
        delayed, 0,
        "FLOWIP-114m: async finite EOF poll must not consume a rate-limiter token"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_sync_finite_does_not_charge_eof_poll() -> Result<()> {
    let (sink, count) = CountingSink::new();
    let test_handle = test_flow! {
        name: "rate_limiter_sync_finite_eof_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_sync_finite_eof_no_charge")),
        middleware: [],

        stages: {
            src = source!(RateLimiterTestEvent => ScriptedSyncSource::new(vec![
                SourceStep::Data,
                SourceStep::Data,
                SourceStep::Done,
            ]), [
                rate_limit_with_burst(1.0, 2.0)
            ]);
            snk = sink!(RateLimiterTestEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to create sync EOF no-charge flow: {e}"))?;

    let (_, src_journal) = test_handle
        .stage_journal_for_test("src")
        .map_err(|e| anyhow!("failed to look up src stage journal: {e}"))?;

    tokio::time::timeout(Duration::from_secs(4), test_handle.into_inner().run())
        .await
        .map_err(|_| anyhow!("sync EOF no-charge flow run timed out"))?
        .map_err(|e| anyhow!("sync EOF no-charge flow run failed: {e}"))?;

    assert_eq!(count.load(Ordering::Relaxed), 2);
    let delayed = rate_limiter_delayed_events(&src_journal).await?;
    assert_eq!(
        delayed, 0,
        "FLOWIP-114m: sync finite EOF poll must not consume a rate-limiter token"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_async_finite_does_not_charge_empty_batch() -> Result<()> {
    let (sink, count) = CountingSink::new();
    let test_handle = test_flow! {
        name: "rate_limiter_async_empty_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_async_empty_no_charge")),
        middleware: [],

        stages: {
            src = async_source!(RateLimiterTestEvent => ScriptedAsyncSource::new(vec![
                SourceStep::Data,
                SourceStep::Empty,
                SourceStep::Data,
                SourceStep::Done,
            ]), [
                rate_limit_with_burst(1.0, 2.0)
            ]);
            snk = sink!(RateLimiterTestEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to create async empty-batch no-charge flow: {e}"))?;

    let (_, src_journal) = test_handle
        .stage_journal_for_test("src")
        .map_err(|e| anyhow!("failed to look up src stage journal: {e}"))?;

    tokio::time::timeout(Duration::from_secs(4), test_handle.into_inner().run())
        .await
        .map_err(|_| anyhow!("async empty-batch no-charge flow run timed out"))?
        .map_err(|e| anyhow!("async empty-batch no-charge flow run failed: {e}"))?;

    assert_eq!(count.load(Ordering::Relaxed), 2);
    let delayed = rate_limiter_delayed_events(&src_journal).await?;
    assert_eq!(
        delayed, 0,
        "FLOWIP-114m: empty async batch must not consume a rate-limiter token"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_sync_finite_does_not_charge_empty_batch() -> Result<()> {
    let (sink, count) = CountingSink::new();
    let test_handle = test_flow! {
        name: "rate_limiter_sync_empty_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_sync_empty_no_charge")),
        middleware: [],

        stages: {
            src = source!(RateLimiterTestEvent => ScriptedSyncSource::new(vec![
                SourceStep::Data,
                SourceStep::Empty,
                SourceStep::Data,
                SourceStep::Done,
            ]), [
                rate_limit_with_burst(1.0, 2.0)
            ]);
            snk = sink!(RateLimiterTestEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to create sync empty-batch no-charge flow: {e}"))?;

    let (_, src_journal) = test_handle
        .stage_journal_for_test("src")
        .map_err(|e| anyhow!("failed to look up src stage journal: {e}"))?;

    tokio::time::timeout(Duration::from_secs(4), test_handle.into_inner().run())
        .await
        .map_err(|_| anyhow!("sync empty-batch no-charge flow run timed out"))?
        .map_err(|e| anyhow!("sync empty-batch no-charge flow run failed: {e}"))?;

    assert_eq!(count.load(Ordering::Relaxed), 2);
    let delayed = rate_limiter_delayed_events(&src_journal).await?;
    assert_eq!(
        delayed, 0,
        "FLOWIP-114m: empty sync batch must not consume a rate-limiter token"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_async_finite_does_not_charge_source_error() -> Result<()> {
    let (sink, count) = CountingSink::new();
    let test_handle = test_flow! {
        name: "rate_limiter_async_error_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_async_error_no_charge")),
        middleware: [],

        stages: {
            src = async_source!(RateLimiterTestEvent => ScriptedAsyncSource::new(vec![
                SourceStep::Data,
                SourceStep::Err,
                SourceStep::Data,
                SourceStep::Done,
            ]), [
                rate_limit_with_burst(1.0, 2.0)
            ]);
            snk = sink!(RateLimiterTestEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to create async error no-charge flow: {e}"))?;

    let (_, src_journal) = test_handle
        .stage_journal_for_test("src")
        .map_err(|e| anyhow!("failed to look up src stage journal: {e}"))?;

    tokio::time::timeout(Duration::from_secs(4), test_handle.into_inner().run())
        .await
        .map_err(|_| anyhow!("async error no-charge flow run timed out"))?
        .map_err(|e| anyhow!("async error no-charge flow run failed: {e}"))?;

    assert_eq!(count.load(Ordering::Relaxed), 2);
    let delayed = rate_limiter_delayed_events(&src_journal).await?;
    assert_eq!(
        delayed, 0,
        "FLOWIP-114m: async source error must not consume a rate-limiter token"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_sync_finite_does_not_charge_source_error() -> Result<()> {
    let (sink, count) = CountingSink::new();
    let test_handle = test_flow! {
        name: "rate_limiter_sync_error_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_sync_error_no_charge")),
        middleware: [],

        stages: {
            src = source!(RateLimiterTestEvent => ScriptedSyncSource::new(vec![
                SourceStep::Data,
                SourceStep::Err,
                SourceStep::Data,
                SourceStep::Done,
            ]), [
                rate_limit_with_burst(1.0, 2.0)
            ]);
            snk = sink!(RateLimiterTestEvent => sink);
        },

        topology: {
            src |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to create sync error no-charge flow: {e}"))?;

    let (_, src_journal) = test_handle
        .stage_journal_for_test("src")
        .map_err(|e| anyhow!("failed to look up src stage journal: {e}"))?;

    tokio::time::timeout(Duration::from_secs(4), test_handle.into_inner().run())
        .await
        .map_err(|_| anyhow!("sync error no-charge flow run timed out"))?
        .map_err(|e| anyhow!("sync error no-charge flow run failed: {e}"))?;

    assert_eq!(count.load(Ordering::Relaxed), 2);
    let delayed = rate_limiter_delayed_events(&src_journal).await?;
    assert_eq!(
        delayed, 0,
        "FLOWIP-114m: sync source error must not consume a rate-limiter token"
    );

    Ok(())
}

// ----- FLOWIP-114m join+rate_limit regression test -----

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RefPayload {
    id: u64,
}

impl TypedPayload for RefPayload {
    const EVENT_TYPE: &'static str = "flowip114m.ref_payload";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamPayload {
    id: u64,
}

impl TypedPayload for StreamPayload {
    const EVENT_TYPE: &'static str = "flowip114m.stream_payload";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct EnrichedPayload {
    source: String,
    id: u64,
}

impl TypedPayload for EnrichedPayload {
    const EVENT_TYPE: &'static str = "flowip114m.enriched_payload";
}

#[derive(Clone, Debug)]
struct SingleRefSource {
    emitted: bool,
    writer_id: WriterId,
}

impl SingleRefSource {
    fn new() -> Self {
        Self {
            emitted: false,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for SingleRefSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            RefPayload::EVENT_TYPE,
            json!({ "id": 1_u64 }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct TwoStreamEventsSource {
    emitted: usize,
    writer_id: Option<WriterId>,
}

impl TwoStreamEventsSource {
    fn new() -> Self {
        Self {
            emitted: 0,
            writer_id: None,
        }
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for TwoStreamEventsSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted >= 2 {
            return Ok(None);
        }
        let id = self.emitted as u64;
        self.emitted += 1;
        tokio::task::yield_now().await;
        let writer_id = self
            .writer_id
            .expect("stream writer_id should be bound by runtime");
        Ok(Some(vec![ChainEventFactory::data_event(
            writer_id,
            StreamPayload::EVENT_TYPE,
            json!({ "id": id }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct PassthroughJoin;

#[async_trait]
impl JoinHandler for PassthroughJoin {
    type State = ();

    fn initial_state(&self) -> Self::State {}

    fn process_event(
        &self,
        _state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        let source = if RefPayload::event_type_matches(&event.event_type()) {
            "ref"
        } else {
            "stream"
        };
        let id = event
            .payload()
            .get("id")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok(vec![ChainEventFactory::data_event(
            writer_id,
            EnrichedPayload::EVENT_TYPE,
            json!({ "source": source, "id": id }),
        )])
    }

    fn on_source_eof(
        &self,
        _state: &mut Self::State,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_join_stage_rejects_rate_limit_middleware() -> Result<()> {
    let (sink, _count) = CountingSink::new();
    let result = test_flow! {
        name: "rate_limiter_join_support",
        journals: disk_journals(unique_journal_dir("rate_limiter_join_support")),
        middleware: [],

        stages: {
            ref_src = source!(RefPayload => SingleRefSource::new());
            stream_src = async_source!(StreamPayload => TwoStreamEventsSource::new());
            joiner = join!(catalog ref_src: RefPayload, StreamPayload -> EnrichedPayload => PassthroughJoin, [
                // Joins are deterministic coordination surfaces under FLOWIP-120c H1.
                rate_limit_with_burst(1.0, 3.0)
            ]);
            snk = sink!(EnrichedPayload => sink);
        },

        topology: {
            stream_src |> joiner;
            joiner |> snk;
        }
    }
    .await;

    let err = match result {
        Ok(_) => return Err(anyhow!("rate limiter on a join must fail the build")),
        Err(err) => format!("{err:?}"),
    };
    assert!(
        err.contains("PolicyMiddlewareOnPureStage") || err.contains("pure sync surface"),
        "expected FLOWIP-120c H1 join rejection, got: {err}"
    );

    Ok(())
}

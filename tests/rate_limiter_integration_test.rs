// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::middleware::{rate_limit_with_burst, RateLimiterBuilder};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_dsl::{async_source, join, sink, source, stateful, test_flow, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, JoinReferenceView, SinkDescription, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, StatefulEmission, TypedAsyncFiniteSourceHandler, TypedFiniteSourceHandler,
    TypedJoinHandler, TypedStatefulHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};

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

async fn rate_limiter_delayed_total_from_runtime_context(
    stage_journal: &Arc<dyn Journal<ChainEvent>>,
) -> Result<u64> {
    let mut reader = stage_journal
        .reader()
        .await
        .map_err(|e| anyhow!("failed to create stage journal reader: {e}"))?;

    let mut delayed_total: u64 = 0;
    loop {
        match reader.next().await {
            Ok(Some(envelope)) => {
                if let Some(runtime_context) = &envelope.event.runtime_context {
                    delayed_total = delayed_total.max(runtime_context.rl_delayed_total);
                }
            }
            Ok(None) => return Ok(delayed_total),
            Err(e) => return Err(anyhow!("failed to read stage journal: {e}")),
        }
    }
}

#[derive(Clone, Debug)]
struct SequenceSource {
    total: usize,
    emitted: usize,
}

impl SequenceSource {
    fn new(total: usize) -> Self {
        Self { total, emitted: 0 }
    }
}

impl TypedFiniteSourceHandler for SequenceSource {
    type Output = RateLimiterTestEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted >= self.total {
            return Ok(None);
        }

        let event = RateLimiterTestEvent {
            index: self.emitted as u64,
        };
        self.emitted += 1;
        Ok(Some(vec![event]))
    }
}

#[derive(Clone, Debug)]
struct BatchedSource {
    batches: Vec<usize>,
    batch_index: usize,
    next_event_id: usize,
}

impl BatchedSource {
    fn new(batches: Vec<usize>) -> Self {
        Self {
            batches,
            batch_index: 0,
            next_event_id: 0,
        }
    }
}

impl TypedFiniteSourceHandler for BatchedSource {
    type Output = RateLimiterTestEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        let Some(batch_size) = self.batches.get(self.batch_index).copied() else {
            return Ok(None);
        };

        self.batch_index += 1;
        let start = self.next_event_id;
        self.next_event_id += batch_size;

        let events = (start..start + batch_size)
            .map(|index| RateLimiterTestEvent {
                index: index as u64,
            })
            .collect();

        Ok(Some(events))
    }
}

#[derive(Clone, Debug)]
struct PassthroughTransform;

impl TypedTransformHandler for PassthroughTransform {
    type Input = RateLimiterTestEvent;
    type Output = RateLimiterTestEvent;

    fn process(
        &self,
        event: RateLimiterTestEvent,
    ) -> std::result::Result<RateLimiterTestEvent, HandlerError> {
        Ok(event)
    }
}

/// Minimal stateful handler used only to prove that a rate limiter on a pure-sync
/// stateful shell fails the build (FLOWIP-115d / FLOWIP-120c H1). The build
/// rejects before any handler logic runs, so the body is empty.
#[derive(Clone, Debug)]
struct PassthroughStateful;

impl TypedStatefulHandler for PassthroughStateful {
    type State = ();
    type Input = RateLimiterTestEvent;
    type Output = RateLimiterTestEvent;

    fn accumulate(&self, _state: &mut Self::State, _event: RateLimiterTestEvent) {}
    fn initial_state(&self) -> Self::State {}
    fn emit(
        &self,
        _state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: (),
            outputs: vec![],
        })
    }
}

#[derive(Debug)]
struct CountingSink<T> {
    count: Arc<AtomicUsize>,
    _input: std::marker::PhantomData<fn() -> T>,
}

impl<T> Clone for CountingSink<T> {
    fn clone(&self) -> Self {
        Self {
            count: Arc::clone(&self.count),
            _input: std::marker::PhantomData,
        }
    }
}

impl<T> CountingSink<T> {
    fn new() -> (Self, Arc<AtomicUsize>) {
        let count = Arc::new(AtomicUsize::new(0));
        (
            Self {
                count: count.clone(),
                _input: std::marker::PhantomData,
            },
            count,
        )
    }
}

#[async_trait]
impl<T> InlineSink for CountingSink<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Input = T;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _event: T,
        _context: SinkWriteContext,
    ) -> std::result::Result<SinkWriteReport, HandlerError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Count".to_string()),
            None,
        )))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_low_rate_half_eps_processes_all_events() -> Result<()> {
    let (sink, count) = CountingSink::<RateLimiterTestEvent>::new();
    let source = SequenceSource::new(2);
    let passthrough = PassthroughTransform;
    let test_handle = test_flow! {
        name: "rate_limiter_low_rate_half_eps",
        journals: disk_journals(unique_journal_dir("rate_limiter_low_rate_half_eps")),

        stages: {
            src = source!(RateLimiterTestEvent => source with [
                rate_limit_with_burst(50.0, 1.0)
            ]);
            throttled = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => passthrough);
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
    let delayed = rate_limiter_delayed_total_from_runtime_context(&throttled_journal).await?;
    assert!(
        delayed >= 1,
        "expected at least one source rate limiter delayed admission in runtime context"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_weighted_default_burst_makes_progress() -> Result<()> {
    let (sink, count) = CountingSink::<RateLimiterTestEvent>::new();
    let source = SequenceSource::new(1);
    let passthrough = PassthroughTransform;
    let test_handle = test_flow! {
        name: "rate_limiter_weighted_default_burst",
        journals: disk_journals(unique_journal_dir("rate_limiter_weighted_default_burst")),

        stages: {
            src = source!(RateLimiterTestEvent => source with [
                RateLimiterBuilder::new(2.0)
                    .with_cost_per_event(5.0)
                    .build()
            ]);
            throttled = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => passthrough);
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
    let source = SequenceSource::new(1);
    let passthrough = PassthroughTransform;
    let (sink, _count) = CountingSink::<RateLimiterTestEvent>::new();
    let result = test_flow! {
        name: "rate_limiter_invalid",
        journals: disk_journals(unique_journal_dir("rate_limiter_invalid")),

        stages: {
            src = source!(RateLimiterTestEvent => source with [
                RateLimiterBuilder::new(10.0)
                    .with_burst(2.0)
                    .with_cost_per_event(5.0)
                    .build()
            ]);
            throttled = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => passthrough);
            snk = sink!(RateLimiterTestEvent => sink);
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
}

impl ScriptedSyncSource {
    fn new(steps: Vec<SourceStep>) -> Self {
        Self {
            steps,
            index: 0,
            next_event_id: 0,
        }
    }
}

impl TypedFiniteSourceHandler for ScriptedSyncSource {
    type Output = RateLimiterTestEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
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
                Ok(Some(vec![RateLimiterTestEvent { index: id as u64 }]))
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
}

impl ScriptedAsyncSource {
    fn new(steps: Vec<SourceStep>) -> Self {
        Self {
            steps,
            index: 0,
            next_event_id: 0,
        }
    }
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for ScriptedAsyncSource {
    type Output = RateLimiterTestEvent;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
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
                Ok(Some(vec![RateLimiterTestEvent { index: id as u64 }]))
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
    let (sink, count) = CountingSink::<RateLimiterTestEvent>::new();
    let source = BatchedSource::new(vec![2, 2]);
    let passthrough = PassthroughTransform;
    let test_handle = test_flow! {
        name: "rate_limiter_source_poll_gating",
        journals: disk_journals(unique_journal_dir("rate_limiter_source_poll_gating")),

        stages: {
            src = source!(RateLimiterTestEvent => source with [
                rate_limit_with_burst(50.0, 1.0)
            ]);
            passthrough = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => passthrough);
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
    let delayed = rate_limiter_delayed_total_from_runtime_context(&src_journal).await?;
    assert!(
        delayed >= 1,
        "expected at least one source rate limiter delayed admission in runtime context"
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
    let (sink, count) = CountingSink::<RateLimiterTestEvent>::new();
    let source =
        ScriptedAsyncSource::new(vec![SourceStep::Data, SourceStep::Data, SourceStep::Done]);
    let test_handle = test_flow! {
        name: "rate_limiter_async_finite_eof_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_async_finite_eof_no_charge")),

        stages: {
            src = async_source!(RateLimiterTestEvent => source with [
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
    let delayed = rate_limiter_delayed_total_from_runtime_context(&src_journal).await?;
    assert_eq!(
        delayed, 0,
        "FLOWIP-114m: async finite EOF poll must not consume a rate-limiter token"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_sync_finite_does_not_charge_eof_poll() -> Result<()> {
    let (sink, count) = CountingSink::<RateLimiterTestEvent>::new();
    let source =
        ScriptedSyncSource::new(vec![SourceStep::Data, SourceStep::Data, SourceStep::Done]);
    let test_handle = test_flow! {
        name: "rate_limiter_sync_finite_eof_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_sync_finite_eof_no_charge")),

        stages: {
            src = source!(RateLimiterTestEvent => source with [
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
    let delayed = rate_limiter_delayed_total_from_runtime_context(&src_journal).await?;
    assert_eq!(
        delayed, 0,
        "FLOWIP-114m: sync finite EOF poll must not consume a rate-limiter token"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_async_finite_does_not_charge_empty_batch() -> Result<()> {
    let (sink, count) = CountingSink::<RateLimiterTestEvent>::new();
    let source = ScriptedAsyncSource::new(vec![
        SourceStep::Data,
        SourceStep::Empty,
        SourceStep::Data,
        SourceStep::Done,
    ]);
    let test_handle = test_flow! {
        name: "rate_limiter_async_empty_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_async_empty_no_charge")),

        stages: {
            src = async_source!(RateLimiterTestEvent => source with [
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
    let delayed = rate_limiter_delayed_total_from_runtime_context(&src_journal).await?;
    assert_eq!(
        delayed, 0,
        "FLOWIP-114m: empty async batch must not consume a rate-limiter token"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_sync_finite_does_not_charge_empty_batch() -> Result<()> {
    let (sink, count) = CountingSink::<RateLimiterTestEvent>::new();
    let source = ScriptedSyncSource::new(vec![
        SourceStep::Data,
        SourceStep::Empty,
        SourceStep::Data,
        SourceStep::Done,
    ]);
    let test_handle = test_flow! {
        name: "rate_limiter_sync_empty_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_sync_empty_no_charge")),

        stages: {
            src = source!(RateLimiterTestEvent => source with [
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
    let delayed = rate_limiter_delayed_total_from_runtime_context(&src_journal).await?;
    assert_eq!(
        delayed, 0,
        "FLOWIP-114m: empty sync batch must not consume a rate-limiter token"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_async_finite_does_not_charge_source_error() -> Result<()> {
    let (sink, count) = CountingSink::<RateLimiterTestEvent>::new();
    let source = ScriptedAsyncSource::new(vec![
        SourceStep::Data,
        SourceStep::Err,
        SourceStep::Data,
        SourceStep::Done,
    ]);
    let test_handle = test_flow! {
        name: "rate_limiter_async_error_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_async_error_no_charge")),

        stages: {
            src = async_source!(RateLimiterTestEvent => source with [
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
    let delayed = rate_limiter_delayed_total_from_runtime_context(&src_journal).await?;
    assert_eq!(
        delayed, 0,
        "FLOWIP-114m: async source error must not consume a rate-limiter token"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_sync_finite_does_not_charge_source_error() -> Result<()> {
    let (sink, count) = CountingSink::<RateLimiterTestEvent>::new();
    let source = ScriptedSyncSource::new(vec![
        SourceStep::Data,
        SourceStep::Err,
        SourceStep::Data,
        SourceStep::Done,
    ]);
    let test_handle = test_flow! {
        name: "rate_limiter_sync_error_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_sync_error_no_charge")),

        stages: {
            src = source!(RateLimiterTestEvent => source with [
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
    let delayed = rate_limiter_delayed_total_from_runtime_context(&src_journal).await?;
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
}

impl SingleRefSource {
    fn new() -> Self {
        Self { emitted: false }
    }
}

impl TypedFiniteSourceHandler for SingleRefSource {
    type Output = RefPayload;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![RefPayload { id: 1 }]))
    }
}

#[derive(Clone, Debug)]
struct TwoStreamEventsSource {
    emitted: usize,
}

impl TwoStreamEventsSource {
    fn new() -> Self {
        Self { emitted: 0 }
    }
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for TwoStreamEventsSource {
    type Output = StreamPayload;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted >= 2 {
            return Ok(None);
        }
        let id = self.emitted as u64;
        self.emitted += 1;
        tokio::task::yield_now().await;
        Ok(Some(vec![StreamPayload { id }]))
    }
}

#[derive(Clone, Debug)]
struct PassthroughJoin;

impl TypedJoinHandler for PassthroughJoin {
    type State = ();
    type ReferenceKey = u64;
    type Reference = RefPayload;
    type Stream = StreamPayload;
    type Output = EnrichedPayload;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(&self, reference: &Self::Reference) -> Result<u64, HandlerError> {
        Ok(reference.id)
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, u64, RefPayload>,
        stream: StreamPayload,
    ) -> Result<Vec<EnrichedPayload>, HandlerError> {
        Ok(vec![EnrichedPayload {
            source: "stream".to_string(),
            id: stream.id,
        }])
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_join_stage_rejects_rate_limit_middleware() -> Result<()> {
    let (sink, _count) = CountingSink::<EnrichedPayload>::new();
    let reference_source = SingleRefSource::new();
    let stream_source = TwoStreamEventsSource::new();
    let joiner = PassthroughJoin;
    let result = test_flow! {
        name: "rate_limiter_join_support",
        journals: disk_journals(unique_journal_dir("rate_limiter_join_support")),

        stages: {
            ref_src = source!(RefPayload => reference_source);
            stream_src = async_source!(StreamPayload => stream_source);
            joiner = join!(catalog ref_src: RefPayload, StreamPayload -> EnrichedPayload => joiner, observers: [
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
        err.contains("'observers:' accepts observer middleware only"),
        "expected FLOWIP-115s join observer-authority rejection, got: {err}"
    );

    Ok(())
}

/// FLOWIP-115d (AC51/AC52): a built-in rate limiter on a pure-sync transform
/// fails the build. The transform shell has no live I/O to pace, so the shared
/// FLOWIP-120c H1 guard rejects it and names the legitimate destinations.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_transform_stage_rejects_rate_limit_middleware() -> Result<()> {
    let (sink, _count) = CountingSink::<RateLimiterTestEvent>::new();
    let source = SequenceSource::new(2);
    let passthrough = PassthroughTransform;
    let result = test_flow! {
        name: "rate_limiter_transform_reject",
        journals: disk_journals(unique_journal_dir("rate_limiter_transform_reject")),

        stages: {
            src = source!(RateLimiterTestEvent => source);
            throttled = transform!(RateLimiterTestEvent -> RateLimiterTestEvent => passthrough, observers: [
                rate_limit_with_burst(1.0, 3.0)
            ]);
            snk = sink!(RateLimiterTestEvent => sink);
        },

        topology: {
            src |> throttled;
            throttled |> snk;
        }
    }
    .await;

    let err = match result {
        Ok(_) => return Err(anyhow!("rate limiter on a transform must fail the build")),
        Err(err) => format!("{err:?}"),
    };
    assert!(
        err.contains("'observers:' accepts observer middleware only"),
        "expected FLOWIP-115s transform observer-authority rejection, got: {err}"
    );

    Ok(())
}

/// FLOWIP-115d (AC51/AC52): a built-in rate limiter on a pure-sync stateful stage
/// fails the build through the same FLOWIP-120c H1 guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_stateful_stage_rejects_rate_limit_middleware() -> Result<()> {
    let (sink, _count) = CountingSink::<RateLimiterTestEvent>::new();
    let source = SequenceSource::new(2);
    let passthrough = PassthroughStateful;
    let result = test_flow! {
        name: "rate_limiter_stateful_reject",
        journals: disk_journals(unique_journal_dir("rate_limiter_stateful_reject")),

        stages: {
            src = source!(RateLimiterTestEvent => source);
            agg = stateful!(RateLimiterTestEvent -> RateLimiterTestEvent => passthrough, observers: [
                rate_limit_with_burst(1.0, 3.0)
            ]);
            snk = sink!(RateLimiterTestEvent => sink);
        },

        topology: {
            src |> agg;
            agg |> snk;
        }
    }
    .await;

    let err = match result {
        Ok(_) => {
            return Err(anyhow!(
                "rate limiter on a stateful stage must fail the build"
            ))
        }
        Err(err) => format!("{err:?}"),
    };
    assert!(
        err.contains("'observers:' accepts observer middleware only"),
        "expected FLOWIP-115s stateful observer-authority rejection, got: {err}"
    );

    Ok(())
}

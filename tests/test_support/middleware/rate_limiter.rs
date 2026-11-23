// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow::effects::Port;
use obzenflow::testing::CountingSink;
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_dsl::{join, sink, source, stateful, test_flow, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    JoinReferenceView, StatefulEmission, TypedAsyncFiniteSourceHandler, TypedFiniteSourceHandler,
    TypedJoinHandler, TypedStatefulHandler, TypedTransformHandler,
};
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
use obzenflow::stages::sources::ValuesSource;
use obzenflow_runtime::effects::{EffectContext, EffectError};
use obzenflow_runtime::stages::common::handlers::source::{FiniteRead, SourceRead};
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
                if let Some(limiter) = envelope
                    .envelope
                    .observability
                    .as_ref()
                    .and_then(|packet| packet.runtime.as_ref())
                    .and_then(|runtime| runtime.rate_limiter.as_ref())
                {
                    delayed_total = delayed_total.max(limiter.delayed_total);
                }
            }
            Ok(None) => return Ok(delayed_total),
            Err(e) => return Err(anyhow!("failed to read stage journal: {e}")),
        }
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
    fn emit(&self, _state: &Self::State) -> StatefulEmission<Self::Output> {
        StatefulEmission::RetainEpoch { outputs: vec![] }
    }

    fn post_emit(&self, _: &mut Self::State) {}
}

fn counting_sink<T>() -> (CountingSink<T>, Arc<AtomicUsize>) {
    let count = Arc::new(AtomicUsize::new(0));
    (CountingSink::new(count.clone()), count)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_low_rate_half_eps_processes_all_events() -> Result<()> {
    let (sink, count) = counting_sink::<RateLimiterTestEvent>();
    let source = sequence(2);
    let passthrough = PassthroughTransform;
    let test_handle = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_low_rate_half_eps",
        journals: disk_journals(unique_journal_dir("rate_limiter_low_rate_half_eps")),

        stages: {
            src = source!(RateLimiterTestEvent with {
                rate_limit(50.0).burst_capacity(1.0)
            } => source);
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
    let (sink, count) = counting_sink::<RateLimiterTestEvent>();
    let source = sequence(1);
    let passthrough = PassthroughTransform;
    let test_handle = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_weighted_default_burst",
        journals: disk_journals(unique_journal_dir("rate_limiter_weighted_default_burst")),

        stages: {
            src = source!(RateLimiterTestEvent with {
                rate_limit(2.0)
                    .cost(5.0)
            } => source);
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
    let source = sequence(1);
    let passthrough = PassthroughTransform;
    let (sink, _count) = counting_sink::<RateLimiterTestEvent>();
    let result = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_invalid",
        journals: disk_journals(unique_journal_dir("rate_limiter_invalid")),

        stages: {
            src = source!(RateLimiterTestEvent with {
                rate_limit(10.0)
                    .burst_capacity(2.0)
                    .cost(5.0)
            } => source);
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
    assert!(err.contains("cost_per_attempt"), "error: {err}");
}

/// Source step in the FLOWIP-114m no-charge tests. `Done` ends the source.
fn sequence(total: u64) -> impl TypedFiniteSourceHandler<Output = RateLimiterTestEvent> {
    ValuesSource::new(
        (0..total)
            .map(|index| RateLimiterTestEvent { index })
            .collect::<Vec<_>>(),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BatchRead {
    batch: usize,
    next_id: u64,
}

#[obzenflow_runtime::effects::port]
#[async_trait]
trait BatchInput {
    #[query(type = "rate_limiter.batches.read")]
    async fn read(
        &self,
        from: &BatchRead,
        ctx: &obzenflow::queries::QueryContext,
    ) -> Result<FiniteRead<RateLimiterTestEvent, BatchRead>, obzenflow::queries::QueryError>;
}

struct Batches {
    sizes: Vec<u64>,
}

#[obzenflow_runtime::effects::system("batches")]
#[async_trait]
impl BatchInput for Batches {
    async fn read(
        &self,
        from: &BatchRead,
        _ctx: &obzenflow::queries::QueryContext,
    ) -> Result<FiniteRead<RateLimiterTestEvent, BatchRead>, obzenflow::queries::QueryError> {
        let Some(size) = self.sizes.get(from.batch).copied() else {
            return Ok(FiniteRead::EndOfInput);
        };
        Ok(FiniteRead::Batch {
            records: (from.next_id..from.next_id + size)
                .map(|index| RateLimiterTestEvent { index })
                .collect(),
            next: BatchRead {
                batch: from.batch + 1,
                next_id: from.next_id + size,
            },
        })
    }
}

struct BatchedSource(obzenflow::queries::QueryPort<dyn BatchInput>);

impl BatchedSource {
    fn new(sizes: Vec<u64>) -> Self {
        Self(obzenflow::queries::QueryPort::to(Batches { sizes }))
    }
}

impl TypedFiniteSourceHandler for BatchedSource {
    type Output = RateLimiterTestEvent;

    fn read(&self) -> SourceRead<RateLimiterTestEvent> {
        SourceRead::finite(
            self.0.clone(),
            BatchRead {
                batch: 0,
                next_id: 0,
            },
            Ok,
        )
    }
}

#[derive(Clone, Copy, Debug)]
enum SourceStep {
    Data,
    Empty,
    Err,
    Done,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ScriptRead {
    next_id: u64,
}

#[obzenflow_runtime::effects::port]
#[async_trait]
trait ScriptedInput {
    #[query(type = "rate_limiter.scripted.read")]
    async fn read(
        &self,
        from: &ScriptRead,
        ctx: &obzenflow::queries::QueryContext,
    ) -> Result<FiniteRead<RateLimiterTestEvent, ScriptRead>, obzenflow::queries::QueryError>;
}

struct Script {
    steps: Vec<SourceStep>,
    played: AtomicUsize,
}

#[obzenflow_runtime::effects::system("scripted")]
#[async_trait]
impl ScriptedInput for Script {
    async fn read(
        &self,
        from: &ScriptRead,
        _ctx: &obzenflow::queries::QueryContext,
    ) -> Result<FiniteRead<RateLimiterTestEvent, ScriptRead>, obzenflow::queries::QueryError> {
        let step = self
            .steps
            .get(self.played.fetch_add(1, Ordering::SeqCst))
            .copied()
            .unwrap_or(SourceStep::Done);
        match step {
            SourceStep::Data => Ok(FiniteRead::Batch {
                records: vec![RateLimiterTestEvent {
                    index: from.next_id,
                }],
                next: ScriptRead {
                    next_id: from.next_id + 1,
                },
            }),
            SourceStep::Empty => Ok(FiniteRead::Batch {
                records: Vec::new(),
                next: from.clone(),
            }),
            SourceStep::Err => Err(obzenflow::queries::QueryError::recoverable(obzenflow::queries::SourcePollErrorKind::Transport,obzenflow::queries::SourceDiagnosticReason::InputUnavailable)),
            SourceStep::Done => Ok(FiniteRead::EndOfInput),
        }
    }
}

fn script(steps: Vec<SourceStep>) -> SourceRead<RateLimiterTestEvent> {
    SourceRead::finite(
        obzenflow::queries::QueryPort::to(Script {
            steps,
            played: AtomicUsize::new(0),
        }),
        ScriptRead { next_id: 0 },
        Ok,
    )
}

struct ScriptedSyncSource(Vec<SourceStep>);

impl ScriptedSyncSource {
    fn new(steps: Vec<SourceStep>) -> Self {
        Self(steps)
    }
}

impl TypedFiniteSourceHandler for ScriptedSyncSource {
    type Output = RateLimiterTestEvent;

    fn read(&self) -> SourceRead<RateLimiterTestEvent> {
        script(self.0.clone())
    }
}

struct ScriptedAsyncSource(Vec<SourceStep>);

impl ScriptedAsyncSource {
    fn new(steps: Vec<SourceStep>) -> Self {
        Self(steps)
    }
}

impl TypedAsyncFiniteSourceHandler for ScriptedAsyncSource {
    type Output = RateLimiterTestEvent;

    fn read(&self) -> SourceRead<RateLimiterTestEvent> {
        script(self.0.clone())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_source_stage_limits_per_poll_and_documents_batching() -> Result<()> {
    let (sink, count) = counting_sink::<RateLimiterTestEvent>();
    let source = BatchedSource::new(vec![2, 2]);
    let passthrough = PassthroughTransform;
    let test_handle = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_source_poll_gating",
        journals: disk_journals(unique_journal_dir("rate_limiter_source_poll_gating")),

        stages: {
            src = source!(RateLimiterTestEvent with {
                rate_limit(50.0).burst_capacity(1.0)
            } => source);
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
// Each test uses `rate_limit(refill_rate).burst_capacity(capacity)` where
// `capacity == expected_admission_count`. If the rule were violated (i.e. the
// rate limiter charged a no-charge poll) the bucket would empty an admission
// early, the next genuine admission would block for at least `1 / refill_rate`
// seconds, and a `Delayed` event would be journaled. The assertions
// `count == expected_admission_count && delayed == 0` therefore fail under
// the buggy path.

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_async_finite_does_not_charge_eof_poll() -> Result<()> {
    let (sink, count) = counting_sink::<RateLimiterTestEvent>();
    let source =
        ScriptedAsyncSource::new(vec![SourceStep::Data, SourceStep::Data, SourceStep::Done]);
    let test_handle = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_async_finite_eof_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_async_finite_eof_no_charge")),

        stages: {
            src = source!(RateLimiterTestEvent with {
                rate_limit(1.0).burst_capacity(2.0)
            } => source);
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
    let (sink, count) = counting_sink::<RateLimiterTestEvent>();
    let source =
        ScriptedSyncSource::new(vec![SourceStep::Data, SourceStep::Data, SourceStep::Done]);
    let test_handle = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_sync_finite_eof_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_sync_finite_eof_no_charge")),

        stages: {
            src = source!(RateLimiterTestEvent with {
                rate_limit(1.0).burst_capacity(2.0)
            } => source);
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
    let (sink, count) = counting_sink::<RateLimiterTestEvent>();
    let source = ScriptedAsyncSource::new(vec![
        SourceStep::Data,
        SourceStep::Empty,
        SourceStep::Data,
        SourceStep::Done,
    ]);
    let test_handle = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_async_empty_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_async_empty_no_charge")),

        stages: {
            src = source!(RateLimiterTestEvent with {
                rate_limit(1.0).burst_capacity(2.0)
            } => source);
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
    let (sink, count) = counting_sink::<RateLimiterTestEvent>();
    let source = ScriptedSyncSource::new(vec![
        SourceStep::Data,
        SourceStep::Empty,
        SourceStep::Data,
        SourceStep::Done,
    ]);
    let test_handle = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_sync_empty_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_sync_empty_no_charge")),

        stages: {
            src = source!(RateLimiterTestEvent with {
                rate_limit(1.0).burst_capacity(2.0)
            } => source);
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
    let (sink, count) = counting_sink::<RateLimiterTestEvent>();
    let source = ScriptedAsyncSource::new(vec![
        SourceStep::Data,
        SourceStep::Err,
        SourceStep::Data,
        SourceStep::Done,
    ]);
    let test_handle = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_async_error_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_async_error_no_charge")),

        stages: {
            src = source!(RateLimiterTestEvent with {
                rate_limit(1.0).burst_capacity(2.0)
            } => source);
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
    let (sink, count) = counting_sink::<RateLimiterTestEvent>();
    let source = ScriptedSyncSource::new(vec![
        SourceStep::Data,
        SourceStep::Err,
        SourceStep::Data,
        SourceStep::Done,
    ]);
    let test_handle = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_sync_error_no_charge",
        journals: disk_journals(unique_journal_dir("rate_limiter_sync_error_no_charge")),

        stages: {
            src = source!(RateLimiterTestEvent with {
                rate_limit(1.0).burst_capacity(2.0)
            } => source);
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
    let (sink, _count) = counting_sink::<EnrichedPayload>();
    let reference_source = ValuesSource::new([RefPayload { id: 1 }]);
    let stream_source = ValuesSource::new([StreamPayload { id: 0 }, StreamPayload { id: 1 }]);
    let joiner = PassthroughJoin;
    let result = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_join_support",
        journals: disk_journals(unique_journal_dir("rate_limiter_join_support")),

        stages: {
            ref_src = source!(RefPayload => reference_source);
            stream_src = source!(StreamPayload => stream_source);
            joiner = join!(catalog ref_src: RefPayload, StreamPayload -> EnrichedPayload
                with rate_limit(1.0).burst_capacity(3.0) => joiner);
            snk = sink!(EnrichedPayload => sink);
        },

        topology: {
            (ref_src, stream_src) |> joiner;
            joiner |> snk;
        }
    }
    .await;

    let err = match result {
        Ok(_) => return Err(anyhow!("rate limiter on a join must fail the build")),
        Err(err) => format!("{err:?}"),
    };
    assert!(
        err.contains("no declared live operation"),
        "expected contextual join control rejection, got: {err}"
    );

    Ok(())
}

/// FLOWIP-115d (AC51/AC52): a built-in rate limiter on a pure-sync transform
/// fails the build. The transform shell has no live I/O to pace, so the shared
/// FLOWIP-120c H1 guard rejects it and names the legitimate destinations.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_transform_stage_rejects_rate_limit_middleware() -> Result<()> {
    let (sink, _count) = counting_sink::<RateLimiterTestEvent>();
    let source = sequence(2);
    let passthrough = PassthroughTransform;
    let result = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_transform_reject",
        journals: disk_journals(unique_journal_dir("rate_limiter_transform_reject")),

        stages: {
            src = source!(RateLimiterTestEvent => source);
            throttled = transform!(RateLimiterTestEvent -> RateLimiterTestEvent with { rate_limit(1.0).burst_capacity(3.0) } => passthrough);
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
        err.contains("no declared live operation"),
        "expected FLOWIP-115s transform observer-authority rejection, got: {err}"
    );

    Ok(())
}

/// FLOWIP-115d (AC51/AC52): a built-in rate limiter on a pure-sync stateful stage
/// fails the build through the same FLOWIP-120c H1 guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rate_limiter_stateful_stage_rejects_rate_limit_middleware() -> Result<()> {
    let (sink, _count) = counting_sink::<RateLimiterTestEvent>();
    let source = sequence(2);
    let passthrough = PassthroughStateful;
    let result = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "rate_limiter_stateful_reject",
        journals: disk_journals(unique_journal_dir("rate_limiter_stateful_reject")),

        stages: {
            src = source!(RateLimiterTestEvent => source);
            agg = stateful!(RateLimiterTestEvent -> RateLimiterTestEvent with { rate_limit(1.0).burst_capacity(3.0) } => passthrough);
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
        err.contains("no declared live operation"),
        "expected FLOWIP-115s stateful observer-authority rejection, got: {err}"
    );

    Ok(())
}

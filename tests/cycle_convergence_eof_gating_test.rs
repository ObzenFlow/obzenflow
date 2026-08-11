// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::TypedPayload;
use obzenflow_core::{CycleDepth, StageOutputs};
use obzenflow_dsl::{
    async_infinite_source, async_source, flow, sink, source, test_flow, transform, FlowDefinition,
};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkHandler, TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler,
    TypedFiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use obzenflow_runtime::testing::{JournalProbe, TestClock};
use serde::{Deserialize, Serialize};

/// File-local payload for the cycle-convergence test. The JSON shape
/// fingerprints the stage contract per FLOWIP-114c. Individual cycle
/// phases are encoded in the payload `kind`, not as separate event types.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SeedEvent {
    kind: String,
    depth: u64,
    target: u64,
}

impl TypedPayload for SeedEvent {
    const EVENT_TYPE: &'static str = "cycle.seed";
}

const KIND_SEED: &str = "seed";
const KIND_ITER: &str = "iter";
const KIND_DONE: &str = "done";
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Notify;

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

fn single_flow_run_dir(base: &Path) -> Result<PathBuf> {
    let flows_dir = base.join("flows");
    let mut dirs: Vec<PathBuf> = fs::read_dir(&flows_dir)?
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();

    anyhow::ensure!(
        dirs.len() == 1,
        "expected exactly one flow run dir under {:?}, got {}",
        flows_dir,
        dirs.len()
    );

    Ok(dirs.pop().expect("dirs is non-empty"))
}

fn any_error_log_contains(run_dir: &Path, needle: &str) -> Result<bool> {
    for entry in fs::read_dir(run_dir)? {
        let path = entry?.path();
        if !path.is_file() {
            continue;
        }
        if path.file_name().and_then(|n| n.to_str()) == Some("system.log") {
            continue;
        }
        if path.extension().and_then(|ext| ext.to_str()) != Some("log") {
            continue;
        }
        if !path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.contains("_error_"))
        {
            continue;
        }

        let contents = fs::read_to_string(&path)?;
        if contents.contains(needle) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[derive(Clone, Debug)]
struct SingleSeedSource {
    emitted: bool,
    target: u64,
}

impl SingleSeedSource {
    fn new(target: u64) -> Self {
        Self {
            emitted: false,
            target,
        }
    }
}

impl TypedFiniteSourceHandler for SingleSeedSource {
    type Output = SeedEvent;

    fn next(&mut self) -> std::result::Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;

        Ok(Some(vec![SeedEvent {
            kind: KIND_SEED.to_string(),
            depth: 0,
            target: self.target,
        }]))
    }
}

#[derive(Clone, Debug)]
struct SeedThenEofSource {
    state: u8,
    target: u64,
    iteration_started: Arc<Notify>,
}

impl SeedThenEofSource {
    fn new(target: u64, iteration_started: Arc<Notify>) -> Self {
        Self {
            state: 0,
            target,
            iteration_started,
        }
    }
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for SeedThenEofSource {
    type Output = SeedEvent;

    async fn next(&mut self) -> std::result::Result<Option<Vec<Self::Output>>, SourceError> {
        match self.state {
            0 => {
                self.state = 1;
                Ok(Some(vec![SeedEvent {
                    kind: KIND_SEED.to_string(),
                    depth: 0,
                    target: self.target,
                }]))
            }
            1 => {
                self.iteration_started.notified().await;
                self.state = 2;
                Ok(None)
            }
            _ => Ok(None),
        }
    }
}

#[derive(Clone, Debug)]
struct EntryConvergeTransform {
    processed_data: Arc<AtomicU64>,
}

impl EntryConvergeTransform {
    fn new(processed_data: Arc<AtomicU64>) -> Self {
        Self { processed_data }
    }
}

impl TypedTransformHandler for EntryConvergeTransform {
    type Input = SeedEvent;
    type Output = StageOutputs<SeedEvent>;

    fn process(
        &self,
        mut event: SeedEvent,
    ) -> std::result::Result<StageOutputs<SeedEvent>, HandlerError> {
        self.processed_data.fetch_add(1, Ordering::Relaxed);

        if event.depth >= event.target {
            event.kind = KIND_DONE.to_string();
            Ok(StageOutputs::one(event))
        } else if event.kind == KIND_SEED || event.kind == KIND_ITER {
            event.kind = KIND_ITER.to_string();
            Ok(StageOutputs::one(event))
        } else {
            Ok(StageOutputs::none())
        }
    }
}

#[derive(Clone, Debug)]
struct IterationTransform {
    processed_iterations: Arc<AtomicU64>,
    iteration_started: Option<Arc<Notify>>,
}

impl IterationTransform {
    fn new(processed_iterations: Arc<AtomicU64>, iteration_started: Option<Arc<Notify>>) -> Self {
        Self {
            processed_iterations,
            iteration_started,
        }
    }
}

impl TypedTransformHandler for IterationTransform {
    type Input = SeedEvent;
    type Output = StageOutputs<SeedEvent>;

    fn process(
        &self,
        mut event: SeedEvent,
    ) -> std::result::Result<StageOutputs<SeedEvent>, HandlerError> {
        if event.kind != KIND_ITER {
            return Ok(StageOutputs::none());
        }

        self.processed_iterations.fetch_add(1, Ordering::Relaxed);
        if let Some(iteration_started) = &self.iteration_started {
            iteration_started.notify_one();
        }

        event.depth = event.depth.saturating_add(1);
        Ok(StageOutputs::one(event))
    }
}

#[derive(Clone, Debug)]
struct DoneCounterSink {
    done_events: Arc<AtomicU64>,
}

impl DoneCounterSink {
    fn new() -> (Self, Arc<AtomicU64>) {
        let done_events = Arc::new(AtomicU64::new(0));
        (
            Self {
                done_events: done_events.clone(),
            },
            done_events,
        )
    }
}

#[async_trait]
impl SinkHandler for DoneCounterSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        if let ChainEventContent::Data { payload, .. } = &event.content {
            if payload.get("kind").and_then(|v| v.as_str()) == Some(KIND_DONE) {
                self.done_events.fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Count".to_string()),
            None,
        ))
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn cycle_buffers_external_eof_until_scc_quiescent() -> Result<()> {
    let clock = TestClock::new().await.expect("paused runtime");

    let journal_root = unique_journal_dir("cycle_buffers_external_eof");

    let target_iterations = 5u64;
    let entry_processed = Arc::new(AtomicU64::new(0));
    let iter_processed = Arc::new(AtomicU64::new(0));
    let entry_processed_for_flow = entry_processed.clone();
    let iter_processed_for_flow = iter_processed.clone();
    let (sink, done_count) = DoneCounterSink::new();
    let iteration_started = Arc::new(Notify::new());
    let source = SeedThenEofSource::new(target_iterations, iteration_started.clone());
    let entry = EntryConvergeTransform::new(entry_processed_for_flow);
    let iter = IterationTransform::new(iter_processed_for_flow, Some(iteration_started));

    let harness = test_flow! {
        name: "cycle_buffers_external_eof_until_scc_quiescent",
        journals: disk_journals(journal_root),

        stages: {
            src = async_source!(SeedEvent => source);
            entry = transform!(SeedEvent -> SeedEvent => entry);
            iter = transform!(SeedEvent -> SeedEvent => iter);
            snk = sink!(SeedEvent => sink);
        },

        topology: {
            src |> entry;
            entry |> iter;
            entry <| iter;
            entry |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    let probe = JournalProbe::try_on_stage(&harness, "entry")?;
    let handle = harness.into_inner();
    let run = tokio::spawn(handle.run());

    // Drive paused time until the flow terminates.
    for _ in 0..400 {
        if run.is_finished() {
            break;
        }
        clock.advance(Duration::from_millis(50)).await?;
        for _ in 0..16 {
            if run.is_finished() {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    assert!(
        run.is_finished(),
        "flow did not terminate under paused time"
    );
    run.await
        .expect("join handle")
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        done_count.load(Ordering::Relaxed),
        1,
        "expected one converged output"
    );
    assert_eq!(
        iter_processed.load(Ordering::Relaxed),
        target_iterations,
        "expected iter stage to execute the full multi-iteration loop"
    );
    assert_eq!(
        entry_processed.load(Ordering::Relaxed),
        target_iterations.saturating_add(1),
        "expected entry stage to see seed + each returned iteration event"
    );

    // Assert that the SCC entry stage observed the final cycle depth for the converging loop.
    let scc_id = probe
        .expect_event(1)
        .await?
        .envelope()
        .event
        .cycle_scc_id
        .expect("scc id");
    probe
        .expect_event_at_cycle_depth(
            scc_id,
            CycleDepth::new((target_iterations.saturating_add(1)) as u16),
            1,
        )
        .await?;

    Ok(())
}

#[derive(Clone, Debug)]
struct SeedThenDrainSource {
    state: u8,
    target: u64,
}

impl SeedThenDrainSource {
    fn new(target: u64) -> Self {
        Self { state: 0, target }
    }
}

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for SeedThenDrainSource {
    type Output = SeedEvent;

    async fn next(&mut self) -> std::result::Result<Vec<Self::Output>, SourceError> {
        match self.state {
            0 => {
                self.state = 1;
                Ok(vec![SeedEvent {
                    kind: KIND_SEED.to_string(),
                    depth: 0,
                    target: self.target,
                }])
            }
            _ => std::future::pending::<Result<Vec<Self::Output>, SourceError>>().await,
        }
    }
}

#[derive(Clone, Debug)]
struct DualSeedSource {
    emitted: u8,
    converge_target: u64,
    diverge_target: u64,
}

impl DualSeedSource {
    fn new(converge_target: u64, diverge_target: u64) -> Self {
        Self {
            emitted: 0,
            converge_target,
            diverge_target,
        }
    }
}

impl TypedFiniteSourceHandler for DualSeedSource {
    type Output = SeedEvent;

    fn next(&mut self) -> std::result::Result<Option<Vec<Self::Output>>, SourceError> {
        let target = match self.emitted {
            0 => self.converge_target,
            1 => self.diverge_target,
            _ => return Ok(None),
        };
        self.emitted = self.emitted.saturating_add(1);

        Ok(Some(vec![SeedEvent {
            kind: KIND_SEED.to_string(),
            depth: 0,
            target,
        }]))
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn cycle_buffers_drain_until_scc_quiescent() -> Result<()> {
    let clock = TestClock::new().await.expect("paused runtime");

    let journal_root = unique_journal_dir("cycle_buffers_drain");

    let target_iterations = 5u64;
    let entry_processed = Arc::new(AtomicU64::new(0));
    let iter_processed = Arc::new(AtomicU64::new(0));
    let entry_processed_for_flow = entry_processed.clone();
    let iter_processed_for_flow = iter_processed.clone();
    let (sink, done_count) = DoneCounterSink::new();
    let source = SeedThenDrainSource::new(target_iterations);
    let entry = EntryConvergeTransform::new(entry_processed_for_flow);
    let iter = IterationTransform::new(iter_processed_for_flow, None);

    let harness = test_flow! {
        name: "cycle_buffers_drain_until_scc_quiescent",
        journals: disk_journals(journal_root),

        stages: {
            src = async_infinite_source!(SeedEvent => source);
            entry = transform!(SeedEvent -> SeedEvent => entry);
            iter = transform!(SeedEvent -> SeedEvent => iter);
            snk = sink!(SeedEvent => sink);
        },

        topology: {
            src |> entry;
            entry |> iter;
            entry <| iter;
            entry |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    let probe = JournalProbe::try_on_stage(&harness, "entry")?;
    let handle = harness.into_inner();
    handle
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("flow start failed: {e}"))?;

    // Wait until the cycle is in flight, then request the runtime-owned drain.
    for _ in 0..100 {
        if iter_processed.load(Ordering::Relaxed) > 0 {
            break;
        }
        clock.advance(Duration::from_millis(10)).await?;
        tokio::task::yield_now().await;
    }
    assert!(
        iter_processed.load(Ordering::Relaxed) > 0,
        "cycle did not enter its first iteration before drain"
    );
    handle
        .stop_graceful(Duration::from_secs(5))
        .await
        .map_err(|e| anyhow::anyhow!("graceful stop failed: {e}"))?;

    // Drive paused time until the flow terminates.
    for _ in 0..400 {
        if !handle.is_running() {
            break;
        }
        clock.advance(Duration::from_millis(50)).await?;
        for _ in 0..16 {
            if !handle.is_running() {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    assert!(
        !handle.is_running(),
        "flow did not terminate under paused time"
    );
    handle
        .wait_for_completion()
        .await
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        done_count.load(Ordering::Relaxed),
        1,
        "expected one converged output even when drain arrives mid-cycle"
    );
    assert_eq!(
        iter_processed.load(Ordering::Relaxed),
        target_iterations,
        "expected iter stage to execute the full multi-iteration loop"
    );
    assert_eq!(
        entry_processed.load(Ordering::Relaxed),
        target_iterations.saturating_add(1),
        "expected entry stage to see seed + each returned iteration event"
    );

    // Assert the converging loop reached the final cycle depth even with a mid-flight drain.
    let scc_id = probe
        .expect_event(1)
        .await?
        .envelope()
        .event
        .cycle_scc_id
        .expect("scc id");
    probe
        .expect_event_at_cycle_depth(
            scc_id,
            CycleDepth::new((target_iterations.saturating_add(1)) as u16),
            1,
        )
        .await?;

    Ok(())
}

#[tokio::test]
async fn cycle_max_iterations_exceeded_routes_to_error_journal() -> Result<()> {
    let journal_root = unique_journal_dir("cycle_max_iterations_routing");
    let journal_root_for_flow = journal_root.clone();

    let entry_processed = Arc::new(AtomicU64::new(0));
    let iter_processed = Arc::new(AtomicU64::new(0));
    let entry_processed_for_flow = entry_processed.clone();
    let iter_processed_for_flow = iter_processed.clone();
    let (sink, done_count) = DoneCounterSink::new();

    // One correlation converges quickly; one correlation never converges and must be aborted
    // by CycleGuard max_iterations. Default max_iterations=30, so pick a target > 30.
    let converge_target = 2u64;
    let diverge_target = 1_000u64;

    let definition = FlowDefinition::materialize(move |_runtime_config| {
        let source = DualSeedSource::new(converge_target, diverge_target);
        let entry = EntryConvergeTransform::new(entry_processed_for_flow);
        let iter = IterationTransform::new(iter_processed_for_flow, None);

        Ok(flow! {
            name: "cycle_max_iterations_exceeded_routes_to_error_journal",
            journals: disk_journals(journal_root_for_flow),

            stages: {
                src = source!(SeedEvent => source);
                entry = transform!(SeedEvent -> SeedEvent => entry);
                iter = transform!(SeedEvent -> SeedEvent => iter);
                snk = sink!(SeedEvent => sink);
            },

            topology: {
                src |> entry;
                entry |> iter;
                entry <| iter;
                entry |> snk;
            }
        })
    });
    let handle = definition
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("flow run timed out"))?
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        done_count.load(Ordering::Relaxed),
        1,
        "expected the converging correlation to produce one done event"
    );

    let run_dir = single_flow_run_dir(&journal_root)?;
    assert!(
        any_error_log_contains(&run_dir, "Cycle depth")?,
        "expected cycle guard abort to be routed to an error journal; run_dir={run_dir:?}"
    );

    // Sanity: the non-converging correlation should not loop forever.
    assert!(
        iter_processed.load(Ordering::Relaxed) >= converge_target,
        "expected at least the converging loop to execute"
    );
    assert!(
        entry_processed.load(Ordering::Relaxed) > converge_target,
        "expected entry to process multiple events before aborting the diverging loop"
    );

    Ok(())
}

#[tokio::test]
async fn cycle_rejects_sccs_with_multiple_entry_points() {
    let definition = FlowDefinition::materialize(move |_runtime_config| {
        let source_a = SingleSeedSource::new(1);
        let source_b = SingleSeedSource::new(1);
        let transform_a = EntryConvergeTransform::new(Arc::new(AtomicU64::new(0)));
        let transform_b = EntryConvergeTransform::new(Arc::new(AtomicU64::new(0)));
        let (sink, _done_count) = DoneCounterSink::new();

        Ok(flow! {
            name: "cycle_reject_multi_entry_scc",
            journals: disk_journals(unique_journal_dir("cycle_reject_multi_entry_scc")),

            stages: {
                src1 = source!(SeedEvent => source_a);
                src2 = source!(SeedEvent => source_b);
                a = transform!(SeedEvent -> SeedEvent => transform_a);
                b = transform!(SeedEvent -> SeedEvent => transform_b);
                snk = sink!(SeedEvent => sink);
            },

            topology: {
                src1 |> a;
                src2 |> b;
                a |> b;
                a <| b;
                b |> snk;
            }
        })
    });
    let result = definition
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await;

    let err = match result {
        Ok(_) => panic!("expected multi-entry SCC validation to fail"),
        Err(err) => err.to_string(),
    };
    assert!(
        err.contains("must have exactly one entry point"),
        "error: {err}"
    );
}

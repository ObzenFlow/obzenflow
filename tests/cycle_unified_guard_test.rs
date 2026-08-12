// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use async_trait::async_trait;
use obzenflow::typed::stateful as typed_stateful;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_core::{CycleDepth, StageOutputs};
use obzenflow_dsl::{
    async_source, flow, sink, source, stateful, test_flow, transform, FlowDefinition,
};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    StatefulEmission, TypedAsyncFiniteSourceHandler, TypedFiniteSourceHandler,
    TypedStatefulHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::testing::{JournalProbe, TestClock};
use serde::{Deserialize, Serialize};

/// File-local payload for the cycle-guard test. The JSON shape matches
/// what `TestEventSource` emits; the type fingerprints the stage
/// contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SeedEvent {
    n: u64,
}

impl TypedPayload for SeedEvent {
    const EVENT_TYPE: &'static str = "cycle.seed_event";
}
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
struct TestEventSource {
    remaining: usize,
}

impl TestEventSource {
    fn new(count: usize) -> Self {
        Self { remaining: count }
    }
}

impl TypedFiniteSourceHandler for TestEventSource {
    type Output = SeedEvent;

    fn next(&mut self) -> std::result::Result<Option<Vec<Self::Output>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }

        self.remaining -= 1;
        Ok(Some(vec![SeedEvent {
            n: self.remaining as u64,
        }]))
    }
}

#[derive(Clone, Debug)]
struct DelayedEofEventSource {
    emitted: bool,
    eof_delay: Duration,
}

impl DelayedEofEventSource {
    fn new(eof_delay: Duration) -> Self {
        Self {
            emitted: false,
            eof_delay,
        }
    }
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for DelayedEofEventSource {
    type Output = SeedEvent;

    async fn next(&mut self) -> std::result::Result<Option<Vec<Self::Output>>, SourceError> {
        if !self.emitted {
            self.emitted = true;
            return Ok(Some(vec![SeedEvent { n: 0 }]));
        }

        tokio::time::sleep(self.eof_delay).await; // hang-guard: test-only EOF delay under paused time
        Ok(None)
    }
}

#[derive(Debug)]
struct EventCounterSink<T> {
    count: Arc<AtomicU64>,
    _input: std::marker::PhantomData<fn() -> T>,
}

impl<T> Clone for EventCounterSink<T> {
    fn clone(&self) -> Self {
        Self {
            count: Arc::clone(&self.count),
            _input: std::marker::PhantomData,
        }
    }
}

impl<T> EventCounterSink<T> {
    fn new() -> (Self, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
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
impl<T> InlineSink for EventCounterSink<T>
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

#[derive(Clone, Debug)]
struct IdentityTransform<T>(std::marker::PhantomData<fn(T)>);

impl<T> IdentityTransform<T> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> TypedTransformHandler for IdentityTransform<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Input = T;
    type Output = T;

    fn process(&self, event: T) -> std::result::Result<T, HandlerError> {
        Ok(event)
    }
}

#[derive(Clone, Debug)]
struct DropAllTransform;

impl TypedTransformHandler for DropAllTransform {
    type Input = SeedEvent;
    type Output = StageOutputs<SeedEvent>;

    fn process(
        &self,
        _event: SeedEvent,
    ) -> std::result::Result<StageOutputs<SeedEvent>, HandlerError> {
        Ok(StageOutputs::none())
    }
}

#[derive(Clone, Debug)]
struct NoopStateful;

impl TypedStatefulHandler for NoopStateful {
    type State = ();
    type Input = SeedEvent;
    type Output = SeedEvent;

    fn accumulate(&self, _state: &mut Self::State, _event: SeedEvent) {}

    fn initial_state(&self) -> Self::State {}

    fn emit(
        &self,
        _state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: (),
            outputs: Vec::new(),
        })
    }
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

fn count_log_lines(run_dir: &Path) -> Result<usize> {
    let mut total = 0usize;
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
        let contents = fs::read_to_string(&path)?;
        total = total.saturating_add(contents.lines().count());
    }
    Ok(total)
}

#[tokio::test]
async fn cycle_guard_rejects_cycles_with_non_transform_members() {
    let definition = FlowDefinition::materialize(move |_runtime_config| {
        let source = TestEventSource::new(1);
        let stateful = NoopStateful;
        let transform = IdentityTransform::<SeedEvent>::new();
        let (sink, _count) = EventCounterSink::<SeedEvent>::new();

        Ok(flow! {
            name: "cycle_guard_reject_stateful_cycle",
            journals: disk_journals(std::path::PathBuf::from("target/cycle_guard_reject_stateful_cycle")),

            stages: {
                src = source!(SeedEvent => source);
                agg = stateful!(SeedEvent -> SeedEvent => stateful);
                tr = transform!(SeedEvent -> SeedEvent => transform);
                snk = sink!(SeedEvent => sink);
            },

            topology: {
                src |> agg;
                agg |> tr;
                agg <| tr;
                tr |> snk;
            }
        })
    });
    let result = definition
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await;

    let err = match result {
        Ok(_) => panic!("expected cycle topology validation to fail"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("Unsupported cycle topology"), "error: {err}");
    assert!(err.contains("agg (stateful)"), "error: {err}");
}

#[tokio::test]
async fn cycle_guard_rejects_stateful_emit_within_cycle() {
    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct WindowCount {
        count: u64,
    }

    impl TypedPayload for WindowCount {
        const EVENT_TYPE: &'static str = "cycle.stateful_emit_within.count";
    }

    let definition = FlowDefinition::materialize(move |_runtime_config| {
        let source = TestEventSource::new(1);
        let window = typed_stateful::reduce(
            WindowCount::default(),
            |acc: &mut WindowCount, _ev: &SeedEvent| acc.count += 1,
        )
        .emit_within(Duration::from_millis(10));
        let transform = IdentityTransform::<WindowCount>::new();
        let (sink, _count) = EventCounterSink::<WindowCount>::new();

        Ok(flow! {
            name: "cycle_guard_reject_emit_within",
            journals: disk_journals(std::path::PathBuf::from("target/cycle_guard_reject_emit_within")),

            stages: {
                src = source!(SeedEvent => source);
                win = stateful!(SeedEvent -> WindowCount => window);
                tr = transform!(WindowCount -> WindowCount => transform);
                snk = sink!(WindowCount => sink);
            },

            topology: {
                src |> win;
                win |> tr;
                win <| tr;
                tr |> snk;
            }
        })
    });
    let result = definition
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await;

    let err = match result {
        Ok(_) => panic!("expected cycle topology validation to fail"),
        Err(err) => err.to_string(),
    };
    assert!(err.contains("Unsupported cycle topology"), "error: {err}");
    assert!(err.contains("win (stateful)"), "error: {err}");
}

#[tokio::test]
async fn cycle_guard_bounds_flow_signal_backflow() -> Result<()> {
    let base = PathBuf::from("target/cycle_guard_bounds");
    let _ = fs::remove_dir_all(&base);
    let base_for_flow = base.clone();

    let (counter_sink, counter) = EventCounterSink::<SeedEvent>::new();

    let definition = FlowDefinition::materialize(move |_runtime_config| {
        let source = TestEventSource::new(5);
        let transform_a = IdentityTransform::<SeedEvent>::new();
        let transform_b = DropAllTransform;

        Ok(flow! {
            name: "cycle_guard_bounds",
            journals: disk_journals(base_for_flow),

            stages: {
                src = source!(SeedEvent => source);
                a = transform!(SeedEvent -> SeedEvent => transform_a);
                b = transform!(SeedEvent -> SeedEvent => transform_b);
                snk = sink!(SeedEvent => counter_sink);
            },

            topology: {
                src |> a;
                a |> b;
                a <| b;
                b |> snk;
            }
        })
    });
    let handle = definition
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
        .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("flow run timed out (possible cycle amplification)"))?
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        counter.load(Ordering::Relaxed),
        0,
        "expected all data events to be dropped by b"
    );

    let run_dir = single_flow_run_dir(&base)?;
    let total_lines = count_log_lines(&run_dir)?;
    assert!(
        total_lines < 2000,
        "expected bounded journal growth; total log lines={total_lines}, run_dir={:?}",
        run_dir
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn cycle_guard_bounds_data_backflow() -> Result<()> {
    let clock = TestClock::new().await.expect("paused runtime");

    let base = PathBuf::from("target/cycle_guard_bounds_data");
    let _ = fs::remove_dir_all(&base);
    let base_for_flow = base.clone();

    let (counter_sink, counter) = EventCounterSink::<SeedEvent>::new();
    let source = DelayedEofEventSource::new(Duration::from_millis(500));
    let transform_a = IdentityTransform::<SeedEvent>::new();
    let transform_b = IdentityTransform::<SeedEvent>::new();

    let harness = test_flow! {
        name: "cycle_guard_bounds_data",
        journals: disk_journals(base_for_flow),

        stages: {
            src = async_source!(SeedEvent => source);
            a = transform!(SeedEvent -> SeedEvent => transform_a);
            b = transform!(SeedEvent -> SeedEvent => transform_b);
            snk = sink!(SeedEvent => counter_sink);
        },

        topology: {
            src |> a;
            a |> b;
            a <| b;
            b |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    let probe = JournalProbe::try_on_stage(&harness, "a")?;
    let handle = harness.into_inner();
    let run = tokio::spawn(handle.run());

    // Drive paused time until the flow terminates. A fixed number of scheduler
    // yields races the current-thread executor when nextest is running many
    // binaries concurrently, so use an unpaused wall-clock deadline for the
    // harness watchdog while continuing to advance runtime timers virtually.
    let scheduler_deadline = Instant::now() + Duration::from_secs(10);
    while !run.is_finished() && Instant::now() < scheduler_deadline {
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
        "flow did not terminate before the scheduler deadline under paused time \
         (possible data cycle amplification)"
    );
    run.await
        .expect("join handle")
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        counter.load(Ordering::Relaxed),
        30,
        "expected cycle guard to bound the data backflow iterations (default max_iterations=30)"
    );

    // Assert that the SCC entry stage observed the expected max iteration depth.
    let scc_id = probe
        .expect_event(1)
        .await?
        .envelope()
        .event
        .cycle_scc_id
        .expect("scc id");
    probe
        .expect_event_at_cycle_depth(scc_id, CycleDepth::new(30), 1)
        .await?;

    Ok(())
}

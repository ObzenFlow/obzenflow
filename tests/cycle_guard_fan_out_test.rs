// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::StageOutputs;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkDeliveryDeclaration, SinkInputContext, SinkTerminalOutcome, TypedFiniteSourceHandler,
    TypedSinkConsumeReport, TypedSinkHandler, TypedTransformHandler,
};
use serde::{Deserialize, Serialize};

/// File-local payload for the cycle-guard fan-out test. The JSON shape
/// matches what the seed sources emit; the type fingerprints the stage
/// contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SeedEvent {
    kind: String,
    #[serde(default)]
    fan_out: u64,
    #[serde(default)]
    item: u64,
    #[serde(default)]
    iter: u64,
    target: u64,
}

impl TypedPayload for SeedEvent {
    const EVENT_TYPE: &'static str = "test.cycle";
}
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn unique_journal_dir(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    PathBuf::from("target").join(format!("{prefix}_{suffix}"))
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
struct SingleSeedFanOutSource {
    emitted: bool,
    fan_out: u64,
    target: u64,
}

impl SingleSeedFanOutSource {
    fn new(fan_out: u64, target: u64) -> Self {
        Self {
            emitted: false,
            fan_out,
            target,
        }
    }
}

impl TypedFiniteSourceHandler for SingleSeedFanOutSource {
    type Output = SeedEvent;

    fn next(
        &mut self,
    ) -> std::result::Result<
        Option<Vec<Self::Output>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;

        Ok(Some(vec![SeedEvent {
            kind: "seed".to_string(),
            fan_out: self.fan_out,
            item: 0,
            iter: 0,
            target: self.target,
        }]))
    }
}

#[derive(Clone, Debug)]
struct FanOutEntryTransform {
    processed: Arc<AtomicU64>,
}

impl FanOutEntryTransform {
    fn new(processed: Arc<AtomicU64>) -> Self {
        Self { processed }
    }
}

impl TypedTransformHandler for FanOutEntryTransform {
    type Input = SeedEvent;
    type Output = StageOutputs<SeedEvent>;

    fn process(
        &self,
        event: SeedEvent,
    ) -> std::result::Result<StageOutputs<SeedEvent>, HandlerError> {
        self.processed.fetch_add(1, Ordering::Relaxed);

        if event.kind == "seed" {
            let mut outputs = Vec::with_capacity(event.fan_out as usize);
            for item in 0..event.fan_out {
                outputs.push(SeedEvent {
                    kind: "iter".to_string(),
                    fan_out: 0,
                    item,
                    iter: 0,
                    target: event.target,
                });
            }
            return Ok(StageOutputs::many(outputs));
        }

        if event.kind == "iter" {
            if event.iter >= event.target {
                return Ok(StageOutputs::one(SeedEvent {
                    kind: "done".to_string(),
                    ..event
                }));
            }
            return Ok(StageOutputs::one(event));
        }

        Ok(StageOutputs::none())
    }
}

#[derive(Clone, Debug)]
struct PassThroughTransform;

impl TypedTransformHandler for PassThroughTransform {
    type Input = SeedEvent;
    type Output = SeedEvent;

    fn process(&self, event: SeedEvent) -> std::result::Result<SeedEvent, HandlerError> {
        Ok(event)
    }
}

#[derive(Clone, Debug)]
struct IterationTransform {
    processed: Arc<AtomicU64>,
}

impl IterationTransform {
    fn new(processed: Arc<AtomicU64>) -> Self {
        Self { processed }
    }
}

impl TypedTransformHandler for IterationTransform {
    type Input = SeedEvent;
    type Output = StageOutputs<SeedEvent>;

    fn process(
        &self,
        mut event: SeedEvent,
    ) -> std::result::Result<StageOutputs<SeedEvent>, HandlerError> {
        if event.kind != "iter" {
            return Ok(StageOutputs::none());
        }

        self.processed.fetch_add(1, Ordering::Relaxed);
        event.iter = event.iter.saturating_add(1);
        Ok(StageOutputs::one(event))
    }
}

#[derive(Clone, Debug)]
struct DoneCounterSink {
    done_count: Arc<AtomicU64>,
}

impl DoneCounterSink {
    fn new(done_count: Arc<AtomicU64>) -> Self {
        Self { done_count }
    }
}

#[async_trait]
impl TypedSinkHandler for DoneCounterSink {
    type Input = SeedEvent;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        SinkDeliveryDeclaration::undeclared()
    }

    async fn consume(
        &mut self,
        event: SeedEvent,
        _context: SinkInputContext,
    ) -> std::result::Result<TypedSinkConsumeReport, HandlerError> {
        if event.kind == "done" {
            self.done_count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(TypedSinkConsumeReport::terminal(
            SinkTerminalOutcome::success(DeliveryMethod::Custom("Count".to_string()), None),
        ))
    }
}

/// Regression test for FLOWIP-051p: a fan-out at the SCC entry point must not
/// cause sibling conflation in the cycle guard.
///
/// This uses:
/// - fan_out = 10
/// - SCC size = 3 (entry -> pass -> iter -> entry)
/// - required round trips = 4
///
/// With the old correlation_id-keyed guard, each cycle member stage would
/// see 10 visits per round, hit max_iterations=30 at the start of round 4,
/// and abort some siblings before convergence.
#[tokio::test]
async fn cycle_guard_fan_out_siblings_converge_without_spurious_abort() -> Result<()> {
    let journal_root = unique_journal_dir("cycle_guard_fan_out");
    let journal_root_for_flow = journal_root.clone();

    let fan_out = 10u64;
    let target_round_trips = 4u64;

    let entry_processed = Arc::new(AtomicU64::new(0));
    let iter_processed = Arc::new(AtomicU64::new(0));

    let done_count = Arc::new(AtomicU64::new(0));
    let done_count_for_flow = done_count.clone();
    let entry_processed_for_flow = entry_processed.clone();
    let iter_processed_for_flow = iter_processed.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = SingleSeedFanOutSource::new(fan_out, target_round_trips);
        let entry_handler = FanOutEntryTransform::new(entry_processed_for_flow);
        let pass_handler = PassThroughTransform;
        let iter_handler = IterationTransform::new(iter_processed_for_flow);
        let sink_handler = DoneCounterSink::new(done_count_for_flow);

        Ok(flow! {
            name: "cycle_guard_fan_out_siblings_converge_without_spurious_abort",
            journals: disk_journals(journal_root_for_flow),

            stages: {
                src = source!(SeedEvent => source_handler);
                entry = transform!(SeedEvent -> SeedEvent => entry_handler);
                pass = transform!(SeedEvent -> SeedEvent => pass_handler);
                iter = transform!(SeedEvent -> SeedEvent => iter_handler);
                snk = sink!(SeedEvent => sink_handler);
            },

            topology: {
                src |> entry;
                entry |> pass;
                pass |> iter;
                entry <| iter;
                entry |> snk;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("flow run timed out"))?
        .map_err(|e| anyhow::anyhow!("flow run failed: {e}"))?;

    assert_eq!(
        done_count.load(Ordering::Relaxed),
        fan_out,
        "expected every fan-out sibling to converge and emit a done event"
    );
    assert_eq!(
        iter_processed.load(Ordering::Relaxed),
        fan_out.saturating_mul(target_round_trips),
        "expected iter stage to execute the full loop for each sibling"
    );
    assert_eq!(
        entry_processed.load(Ordering::Relaxed),
        1u64.saturating_add(fan_out.saturating_mul(target_round_trips)),
        "expected entry stage to see seed + each returned iteration event"
    );

    let run_dir = single_flow_run_dir(&journal_root)?;
    assert!(
        !any_error_log_contains(&run_dir, "Cycle depth")?,
        "expected no cycle guard aborts; run_dir={run_dir:?}"
    );

    Ok(())
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-010t B6: an `[app]` value in an effect's canonical input is part of
//! that effect's recorded identity. Changing it refuses strict replay and
//! resume catch-up with a descriptor mismatch and zero live calls; reverting
//! it resumes the torn run, and the difference report names the key against
//! the torn run and nothing against the ancestor.

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{ChainPayload, StageFatalReason};
use obzenflow_core::journal::read::RunRecordData;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_transform, flow, infinite_source, sink, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::journal::read::{open_disk_run, read_config_changes, ConfigChanges};
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey, SinkRedeliverySafety,
};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::{AppTable, ResolvedRuntimeConfig};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InlineSink, SinkDescription, SinkWriteFailure,
    TypedInfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use replay_testkit::ReattachingSource;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Reading {
    value: u64,
}

impl TypedPayload for Reading {
    const EVENT_TYPE: &'static str = "app_identity.reading";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Score {
    score: u64,
}

impl TypedPayload for Score {
    const EVENT_TYPE: &'static str = "app_identity.score";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Scored {
    value: u64,
    score: u64,
}

impl TypedPayload for Scored {
    const EVENT_TYPE: &'static str = "app_identity.scored";
}

/// The application's settings; `affinity.threshold` feeds the effect.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    affinity: Affinity,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Affinity {
    threshold: u64,
}

#[derive(Clone, Debug)]
struct BoundedReader {
    next_value: u64,
    remaining: u64,
}

impl TypedInfiniteSourceHandler for BoundedReader {
    type Output = Reading;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        if self.remaining == 0 {
            return Ok(Vec::new());
        }
        self.remaining -= 1;
        let value = self.next_value;
        self.next_value += 1;
        Ok(vec![Reading { value }])
    }
}

type Executed = Arc<Mutex<Vec<u64>>>;

/// The threshold is canonical input, so it is part of the recorded identity.
#[derive(Clone, Debug)]
struct ScoreEffect {
    value: u64,
    threshold: u64,
    executed: Executed,
}

#[async_trait]
impl Effect for ScoreEffect {
    const EFFECT_TYPE: &'static str = "app_identity.score";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;
    type BindingMode = obzenflow_runtime::effects::Portless;

    type Outcome = Score;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

    fn label(&self) -> &str {
        "score"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value, "threshold": self.threshold })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.executed.lock().unwrap().push(self.value);
        Ok(Score {
            score: self.value * self.threshold,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("app-identity:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct ScoreTransform {
    threshold: u64,
    executed: Executed,
}

#[async_trait]
impl EffectfulTransformHandler for ScoreTransform {
    type Input = Reading;
    type Output = obzenflow_core::stage_fact_set![Scored, Score];
    type AllowedEffects = obzenflow_runtime::effect_set![ScoreEffect];

    async fn process(
        &self,
        input: Reading,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let score = fx
            .perform(ScoreEffect {
                value: input.value,
                threshold: self.threshold,
                executed: self.executed.clone(),
            })
            .await?;
        fx.emit(Scored {
            value: input.value,
            score: score.score,
        })
        .await?;
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    delivered: Arc<AtomicU64>,
}

#[async_trait]
impl InlineSink for CountingSink {
    type Input = Scored;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _input: Scored) -> Result<(), SinkWriteFailure> {
        self.delivered.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// What one run's handlers did: the readings whose effect executed live, and
/// the outputs its sink consumed.
#[derive(Default)]
struct Run {
    executed: Executed,
    delivered: Arc<AtomicU64>,
}

impl Run {
    fn executed(&self) -> Vec<u64> {
        self.executed.lock().unwrap().clone()
    }
}

/// A build context whose config file holds `[app.affinity] threshold`.
fn context(threshold: u64) -> FlowBuildContext {
    let table =
        serde_json::Map::from_iter([("affinity".to_string(), json!({ "threshold": threshold }))]);
    FlowBuildContext::new(Arc::new(
        ResolvedRuntimeConfig::builtin_defaults().with_app(AppTable::new("obzenflow.toml", table)),
    ))
}

async fn build(
    journal_base: &Path,
    first_value: u64,
    count: u64,
    threshold: u64,
) -> Result<(FlowHandle, Run)> {
    let run = Run::default();
    let (executed, delivered, base) = (
        run.executed.clone(),
        run.delivered.clone(),
        journal_base.to_path_buf(),
    );
    let handle = FlowDefinition::materialize_with(move |_, settings: Settings| {
        let reader = ReattachingSource(BoundedReader {
            next_value: first_value,
            remaining: count,
        });
        let score_transform = ScoreTransform {
            threshold: settings.affinity.threshold,
            executed,
        };
        let counting_sink = CountingSink { delivered };
        Ok(flow! {
            name: "app_identity",
            journals: disk_journals(base),

            stages: {
                src = infinite_source!(Reading => reader);
                score = effectful_transform!(
                    Reading -> { Scored, Score } uses ScoreEffect => score_transform);
                snk = sink!(Scored => counting_sink);
            },

            topology: {
                src |> score;
                score |> snk;
            }
        })
    })
    .build(context(threshold))
    .await
    .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    Ok((handle, run))
}

async fn wait_for_running(handle: &FlowHandle) -> Result<()> {
    let mut rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if matches!(*rx.borrow(), PipelineState::Running) {
                return Ok(());
            }
            rx.changed()
                .await
                .map_err(|_| anyhow!("pipeline state channel closed"))?;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for pipeline to reach Running"))?
}

/// Run until the sink consumed `delivered` outputs and `executed` effects ran
/// live, then stop.
async fn run_until(
    journal_base: &Path,
    first_value: u64,
    count: u64,
    threshold: u64,
    delivered: u64,
    executed: usize,
) -> Result<Vec<u64>> {
    let (handle, run) = build(journal_base, first_value, count, threshold).await?;
    wait_for_running(&handle).await?;
    tokio::time::timeout(Duration::from_secs(20), async {
        while run.delivered.load(Ordering::SeqCst) < delivered || run.executed().len() < executed {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "timeout waiting for {delivered} deliveries and {executed} executions (saw {} and {:?})",
            run.delivered.load(Ordering::SeqCst),
            run.executed()
        )
    })?;
    handle.stop().await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the run to stop"))??;
    Ok(run.executed())
}

/// A run that refuses at its first recorded effect: no live call, no
/// delivery, and a failed outcome.
async fn run_refused(
    journal_base: &Path,
    first_value: u64,
    count: u64,
    threshold: u64,
) -> Result<()> {
    let (handle, run) = build(journal_base, first_value, count, threshold).await?;
    let outcome = tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the refused run to end"))?;
    assert!(outcome.is_err(), "a changed effect identity fails the run");
    assert_eq!(run.executed(), Vec::<u64>::new(), "zero live calls");
    assert_eq!(run.delivered.load(Ordering::SeqCst), 0);
    Ok(())
}

fn archive(path: PathBuf, verb: ReplayVerb) -> ReplayBootstrap {
    ReplayBootstrap {
        archive_path: path,
        allow_incomplete_archive: true,
        allow_duplicate_sink_delivery: false,
        verb,
    }
}

/// The stage failures a run journalled, as `(stage, reason, detail)`.
async fn stage_fatals(run_dir: &Path) -> Result<Vec<(String, StageFatalReason, String)>> {
    let mut snapshot = open_disk_run(run_dir).await?;
    let mut fatals = Vec::new();
    while let Some(record) = snapshot.next().await? {
        if let RunRecordData::Chain(row) = &record.record {
            if let ChainPayload::Execution(ExecutionPayload::StageFatalRecorded(fatal)) =
                &row.payload
            {
                fatals.push((fatal.stage_key.clone(), fatal.reason, fatal.detail.clone()));
            }
        }
    }
    Ok(fatals)
}

async fn assert_refused_by_descriptor_mismatch(run_dir: &Path) -> Result<()> {
    let fatals = stage_fatals(run_dir).await?;
    assert_eq!(fatals.len(), 1, "{fatals:?}");
    let (stage, reason, detail) = &fatals[0];
    assert_eq!(stage, "score");
    assert_eq!(*reason, StageFatalReason::ReplayDivergence);
    assert!(detail.starts_with("effect descriptor mismatch"), "{detail}");
    Ok(())
}

/// The changed keys against each compared run, as `show` reads them.
fn changed_keys(run_dir: &Path) -> Result<Vec<(PathBuf, Vec<String>)>> {
    Ok(read_config_changes(run_dir)?
        .into_iter()
        .map(|compared| {
            assert!(matches!(compared.changes, ConfigChanges::Recorded(_)));
            let keys = compared
                .changes
                .keys()
                .into_iter()
                .map(str::to_string)
                .collect();
            (compared.run, keys)
        })
        .collect())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_app_value_in_an_effect_identity_refuses_replay_and_resume() -> Result<()> {
    const KEY: &str = "app.affinity.threshold";

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    assert_eq!(
        run_until(&journal_base, 1, 3, 10, 3, 3).await?,
        [1, 2, 3],
        "the recording performs readings 1 to 3 live"
    );
    let r0 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;

    // Strict replay with the changed value refuses at the first recorded effect.
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Replay)).await,
        );
        run_refused(&journal_base, 1, 0, 20).await?;
    }
    let replay = replay_testkit::latest_run_dir(&journal_base);
    assert_refused_by_descriptor_mismatch(&replay).await?;
    assert_eq!(
        changed_keys(&replay)?,
        [(r0.clone(), vec![KEY.to_string()])]
    );

    // Resume catch-up refuses the same way, and the resume stays torn. Its
    // reader offers no live readings, so the torn run holds only the prefix.
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Resume)).await,
        );
        run_refused(&journal_base, 4, 0, 20).await?;
    }
    let r1 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;
    assert_refused_by_descriptor_mismatch(&r1).await?;
    assert_eq!(changed_keys(&r1)?, [(r0.clone(), vec![KEY.to_string()])]);

    // Reverting the value resumes the torn run: it merges the ancestor and
    // reuses every recorded outcome, so only the live readings 4 and 5 execute.
    let r2_executed = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r1.clone(), ReplayVerb::Resume)).await,
        );
        run_until(&journal_base, 4, 2, 10, 2, 2).await?
    };
    assert_eq!(r2_executed, [4, 5]);
    let r2 = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(
        changed_keys(&r2)?,
        [(r1, vec![KEY.to_string()]), (r0, Vec::new())],
        "the key differs from the torn run and from nothing the ancestor recorded"
    );
    Ok(())
}

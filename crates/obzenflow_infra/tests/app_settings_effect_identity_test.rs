// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_adapters::sources::{
    YamlDecodeError, YamlDecoder, YamlRecord, YamlSelection, YamlSource,
};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourceContinuationRefusal,
};
use obzenflow_core::event::{ChainPayload, StageFatalReason};
use obzenflow_core::journal::read::RunRecordData;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::dsl::FlowBuildError;
use obzenflow_dsl::{effectful_transform, flow, infinite_source, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::journal::read::{
    open_disk_run, read_config_changes, read_configuration_refusals, ConfigChanges,
};
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
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Unscored {
    value: u64,
}

impl TypedPayload for Unscored {
    const EVENT_TYPE: &'static str = "app_identity.unscored";
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    affinity: Affinity,
    #[serde(default)]
    rescore: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Affinity {
    threshold: u64,
}

#[derive(Clone, Copy, Debug)]
struct App {
    threshold: u64,
    rescore: bool,
}

const RECORDED: App = App {
    threshold: 10,
    rescore: false,
};

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

type ScoreOutput = obzenflow_core::stage_fact_set![Scored, Score, Unscored];
type ScoreEffects = obzenflow_runtime::effect_set![ScoreEffect];

const RESCORE_OFFSET: u64 = 1_000;

#[derive(Clone, Debug)]
struct ScoreTransform {
    app: App,
    executed: Executed,
}

impl ScoreTransform {
    fn effect(&self, value: u64) -> ScoreEffect {
        ScoreEffect {
            value,
            threshold: self.app.threshold,
            executed: self.executed.clone(),
        }
    }

    async fn score(
        &self,
        value: u64,
        fx: &mut Effects<ScoreOutput, ScoreEffects>,
    ) -> Result<Score, EffectError> {
        let score = fx.perform(self.effect(value)).await?;
        if self.app.rescore {
            fx.perform(self.effect(value + RESCORE_OFFSET)).await?;
        }
        Ok(score)
    }
}

#[async_trait]
impl EffectfulTransformHandler for ScoreTransform {
    type Input = Reading;
    type Output = ScoreOutput;
    type AllowedEffects = ScoreEffects;

    async fn process(
        &self,
        input: Reading,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        match self.score(input.value, fx).await {
            Ok(score) => {
                fx.emit(Scored {
                    value: input.value,
                    score: score.score,
                })
                .await?
            }
            Err(_) => fx.emit(Unscored { value: input.value }).await?,
        }
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

fn context(table: serde_json::Value) -> FlowBuildContext {
    let serde_json::Value::Object(table) = table else {
        panic!("[app] is a table");
    };
    FlowBuildContext::new(Arc::new(
        ResolvedRuntimeConfig::builtin_defaults().with_app(AppTable::new("obzenflow.toml", table)),
    ))
}

async fn build(
    journal_base: &Path,
    first_value: u64,
    count: u64,
    app: App,
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
            app: App {
                threshold: settings.affinity.threshold,
                rescore: settings.rescore,
            },
            executed,
        };
        let counting_sink = CountingSink { delivered };
        Ok(flow! {
            name: "app_identity",
            journals: disk_journals(base),

            stages: {
                src = infinite_source!(Reading => reader);
                score = effectful_transform!(
                    Reading -> { Scored, Score, Unscored } uses ScoreEffect => score_transform);
                snk = sink!(Scored => counting_sink);
            },

            topology: {
                src |> score;
                score |> snk;
            }
        })
    })
    .build(context(json!({
        "affinity": { "threshold": app.threshold },
        "rescore": app.rescore,
    })))
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

async fn run_until(
    journal_base: &Path,
    first_value: u64,
    count: u64,
    app: App,
    delivered: u64,
    executed: usize,
) -> Result<Vec<u64>> {
    let (handle, run) = build(journal_base, first_value, count, app).await?;
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

async fn run_refused(journal_base: &Path, first_value: u64, count: u64, app: App) -> Result<()> {
    let (handle, run) = build(journal_base, first_value, count, app).await?;
    let outcome = tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the refused run to end"))?;
    assert!(outcome.is_err(), "a changed effect history fails the run");
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

async fn source_refusals(run_dir: &Path) -> Result<Vec<(String, SourceContinuationRefusal)>> {
    let mut snapshot = open_disk_run(run_dir).await?;
    let mut refusals = Vec::new();
    while let Some(record) = snapshot.next().await? {
        if let (RunRecordData::Chain(row), Some(stage)) = (&record.record, &record.journal.stage) {
            if let ChainPayload::Execution(ExecutionPayload::SourceContinuationRefused(fact)) =
                &row.payload
            {
                refusals.push((stage.key.clone(), fact.refusal.clone()));
            }
        }
    }
    Ok(refusals)
}

async fn assert_refused(run_dir: &Path, reason: StageFatalReason) -> Result<String> {
    let fatals = stage_fatals(run_dir).await?;
    assert_eq!(fatals.len(), 1, "{fatals:?}");
    let (stage, recorded, detail) = fatals.into_iter().next().unwrap();
    assert_eq!(stage, "score");
    assert_eq!(recorded, reason);
    let unscored = replay_testkit::read_stage_envelopes_appended(run_dir, "score")
        .await
        .iter()
        .filter(|record| Unscored::from_event(&record.authored()).is_some())
        .count();
    assert_eq!(unscored, 0, "the handler's fallback fact never commits");
    Ok(detail)
}

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

fn stages(keys: &[&str]) -> BTreeSet<String> {
    keys.iter().map(|key| key.to_string()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_app_value_in_an_effect_identity_refuses_replay_and_resume() -> Result<()> {
    const KEY: &str = "app.affinity.threshold";
    let changed = App {
        threshold: 20,
        ..RECORDED
    };

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    assert_eq!(
        run_until(&journal_base, 1, 3, RECORDED, 3, 3).await?,
        [1, 2, 3],
        "the recording performs readings 1 to 3 live"
    );
    let r0 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;

    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Replay)).await,
        );
        run_refused(&journal_base, 1, 0, changed).await?;
    }
    let replay = replay_testkit::latest_run_dir(&journal_base);
    let detail = assert_refused(&replay, StageFatalReason::DescriptorMismatch).await?;
    assert!(detail.starts_with("effect descriptor mismatch"), "{detail}");
    assert_eq!(
        read_configuration_refusals(&replay).await?,
        stages(&["score"])
    );
    assert_eq!(
        changed_keys(&replay)?,
        [(r0.clone(), vec![KEY.to_string()])]
    );

    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Resume)).await,
        );
        run_refused(&journal_base, 4, 0, changed).await?;
    }
    let r1 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;
    assert_refused(&r1, StageFatalReason::DescriptorMismatch).await?;
    assert_eq!(read_configuration_refusals(&r1).await?, stages(&["score"]));
    assert_eq!(changed_keys(&r1)?, [(r0.clone(), vec![KEY.to_string()])]);

    let r2_executed = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r1.clone(), ReplayVerb::Resume)).await,
        );
        run_until(&journal_base, 4, 2, RECORDED, 2, 2).await?
    };
    assert_eq!(r2_executed, [4, 5]);
    let r2 = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(
        stage_fatals(&r2).await?,
        Vec::new(),
        "catch-up finds every recorded outcome under the lineage root"
    );
    assert!(read_configuration_refusals(&r2).await?.is_empty());
    assert_eq!(
        changed_keys(&r2)?,
        [(r1, vec![KEY.to_string()]), (r0, Vec::new())],
        "the key differs from the torn run and from nothing the ancestor recorded"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_value_that_adds_an_effect_refuses_replay_and_resume_catch_up() -> Result<()> {
    const KEY: &str = "app.rescore";
    let rescore = App {
        rescore: true,
        ..RECORDED
    };

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    assert_eq!(
        run_until(&journal_base, 1, 3, RECORDED, 3, 3).await?,
        [1, 2, 3]
    );
    let r0 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;

    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Replay)).await,
        );
        run_refused(&journal_base, 1, 0, rescore).await?;
    }
    let replay = replay_testkit::latest_run_dir(&journal_base);
    assert_refused(&replay, StageFatalReason::MissingRecordedEffect).await?;
    assert_eq!(
        read_configuration_refusals(&replay).await?,
        stages(&["score"])
    );
    assert_eq!(
        changed_keys(&replay)?,
        [(r0.clone(), vec![KEY.to_string()])]
    );

    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Resume)).await,
        );
        run_refused(&journal_base, 4, 0, rescore).await?;
    }
    let r1 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;
    assert_refused(&r1, StageFatalReason::MissingRecordedEffect).await?;
    assert_eq!(read_configuration_refusals(&r1).await?, stages(&["score"]));
    assert_eq!(changed_keys(&r1)?, [(r0, vec![KEY.to_string()])]);
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Account {
    id: u64,
}

impl TypedPayload for Account {
    const EVENT_TYPE: &'static str = "app_identity.account";
}

#[derive(Clone, Debug)]
struct AccountYaml;

impl YamlDecoder for AccountYaml {
    type Output = Account;

    fn decode(&self, record: YamlRecord<'_>) -> Result<Account, YamlDecodeError> {
        record.deserialize()
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AccountsSettings {
    accounts_file: PathBuf,
}

const ACCOUNTS: u64 = 8;

fn write_accounts(path: &Path) -> Result<()> {
    let rows: String = (1..=ACCOUNTS).map(|id| format!("  - id: {id}\n")).collect();
    std::fs::write(path, format!("accounts:\n{rows}"))?;
    Ok(())
}

async fn build_accounts(
    journal_base: &Path,
    accounts_file: &Path,
    rate: f64,
    burst: f64,
    delivered: Arc<AtomicUsize>,
) -> Result<FlowHandle> {
    let base = journal_base.to_path_buf();
    FlowDefinition::materialize_with(move |_, settings: AccountsSettings| {
        let accounts_source = YamlSource::builder(AccountYaml)
            .path(&settings.accounts_file)
            .selection(YamlSelection::SequenceAt("/accounts".into()))
            .batch_size(1)
            .build()
            .map_err(|error| FlowBuildError::StageResourcesFailed(error.to_string()))?;
        let collect_handler = replay_testkit::CountDeliveries::<Account>::new(delivered);
        Ok(flow! {
            name: "app_accounts",
            journals: disk_journals(base),

            stages: {
                accounts = source!(Account => accounts_source with {
                    rate_limit(rate).burst_capacity(burst)
                });
                collect = sink!(Account => collect_handler);
            },

            topology: {
                accounts |> collect;
            }
        })
    })
    .build(context(json!({ "accounts_file": accounts_file })))
    .await
    .map_err(|e| anyhow!("flow failed to build: {e:?}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_app_path_refuses_an_interrupted_source_and_reaches_the_footer() -> Result<()> {
    const KEY: &str = "app.accounts_file";

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let recorded_file = temp.path().join("accounts.yaml");
    let staging_file = temp.path().join("staging_accounts.yaml");
    write_accounts(&recorded_file)?;
    write_accounts(&staging_file)?;

    {
        let delivered = Arc::new(AtomicUsize::new(0));
        let handle =
            build_accounts(&journal_base, &recorded_file, 0.5, 3.0, delivered.clone()).await?;
        wait_for_running(&handle).await?;
        tokio::time::timeout(Duration::from_secs(20), async {
            while delivered.load(Ordering::SeqCst) < 3 {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| anyhow!("timeout waiting for 3 accounts"))?;
        handle.stop_graceful(Duration::from_secs(10)).await?;
        tokio::time::timeout(Duration::from_secs(20), handle.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for the recording to stop"))??;
    }
    let r0 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;

    let outcome = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Resume)).await,
        );
        let handle = build_accounts(
            &journal_base,
            &staging_file,
            0.5,
            3.0,
            Arc::new(AtomicUsize::new(0)),
        )
        .await?;
        tokio::time::timeout(Duration::from_secs(20), handle.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for the refused resume to end"))?
    };
    assert!(outcome.is_err(), "a changed input refuses the resume");
    let r1 = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(
        source_refusals(&r1).await?,
        [(
            "accounts".to_string(),
            SourceContinuationRefusal::InputChanged {
                part: "file".to_string()
            }
        )]
    );
    assert_eq!(
        read_configuration_refusals(&r1).await?,
        stages(&["accounts"])
    );
    assert_eq!(changed_keys(&r1)?, [(r0, vec![KEY.to_string()])]);
    Ok(())
}

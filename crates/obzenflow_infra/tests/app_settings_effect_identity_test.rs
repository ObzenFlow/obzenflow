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
use obzenflow_core::event::payloads::effect_payload::{
    EffectInputPosition, EffectOrdinal, EffectType, SETTLEMENT_EFFECT_TYPE,
};
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourceContinuationRefusal,
};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{
    ChainPayload, Departure, DepartureAction, StageFatalReason, StageFatalRecorded,
};
use obzenflow_core::journal::archive_actions::FailureEvidence;
use obzenflow_core::journal::read::RunRecordData;
use obzenflow_core::{EventType, TypedPayload};
use obzenflow_dsl::dsl::FlowBuildError;
use obzenflow_dsl::{effectful_transform, flow, infinite_source, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::journal::read::{
    open_disk_run, read_config_changes, read_configuration_refusals, ConfigChanges,
};
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
use obzenflow_runtime::effects::{
    Call, CallRequest, EffectContext, EffectError, Effects, IdempotencyKey, Keyed, RequiresKey,
    SinkRedeliverySafety, System,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Noted {
    value: u64,
}

impl TypedPayload for Noted {
    const EVENT_TYPE: &'static str = "app_identity.noted";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Capped {
    value: u64,
}

impl TypedPayload for Capped {
    const EVENT_TYPE: &'static str = "app_identity.capped";
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Settings {
    affinity: Affinity,
    #[serde(default)]
    rescore: bool,
    #[serde(default)]
    annotate: bool,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default)]
    quiet: Option<u64>,
    #[serde(default)]
    failing: Option<u64>,
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
    annotate: bool,
    limit: Option<u64>,
    quiet: Option<u64>,
    failing: Option<u64>,
}

const RECORDED: App = App {
    threshold: 10,
    rescore: false,
    annotate: false,
    limit: None,
    quiet: None,
    failing: None,
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

#[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
struct Scorer {
    executed: Executed,
}

#[derive(Clone, Debug, Serialize)]
struct ScoreRequest {
    value: u64,
    threshold: u64,
}

#[async_trait]
impl CallRequest for ScoreRequest {
    const EFFECT_TYPE: &'static str = "app_identity.score";
    type Safety = RequiresKey;
    type Port = Scorer;
    type Outcome = Score;

    async fn call(&self, scorer: &Scorer, _ctx: &mut EffectContext) -> Result<Score, EffectError> {
        scorer.executed.lock().unwrap().push(self.value);
        Ok(Score {
            score: self.value * self.threshold,
        })
    }
}

impl Keyed for ScoreRequest {
    fn idempotency_key(&self) -> IdempotencyKey {
        IdempotencyKey(format!("app-identity:{}", self.value))
    }
}

type ScoreEffect = Call<ScoreRequest>;

type ScoreOutput = obzenflow_core::stage_fact_set![Scored, Score, Unscored, Noted, Capped];
type ScoreEffects = obzenflow_runtime::effect_set![ScoreEffect];

const RESCORE_OFFSET: u64 = 1_000;

#[derive(Clone, Debug, obzenflow_runtime::effects::EffectHandler)]
#[effect_handler(effects = obzenflow_runtime::effects)]
struct ScoreTransform {
    app: App,
    #[system]
    scorer: System<Scorer>,
}

impl ScoreTransform {
    fn request(&self, value: u64) -> ScoreRequest {
        ScoreRequest {
            value,
            threshold: self.app.threshold,
        }
    }

    async fn score(
        &self,
        value: u64,
        fx: &mut Effects<ScoreOutput, ScoreEffects>,
    ) -> Result<Score, EffectError> {
        let score = fx.call(self.request(value)).await?;
        if self.app.rescore {
            fx.call(self.request(value + RESCORE_OFFSET)).await?;
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
        if self.app.limit.is_some_and(|limit| input.value > limit) {
            fx.emit(Capped { value: input.value }).await?;
            return Ok(fx.complete()?);
        }
        if self.app.quiet == Some(input.value) {
            return Ok(fx.complete_empty()?);
        }
        if self.app.failing == Some(input.value) {
            return Err(HandlerError::PermanentFailure(format!(
                "reading {} is unreadable",
                input.value
            )));
        }
        match self.score(input.value, fx).await {
            Ok(score) => {
                fx.emit(Scored {
                    value: input.value,
                    score: score.score,
                })
                .await?;
                if self.app.annotate {
                    fx.emit(Noted { value: input.value }).await?;
                }
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
                annotate: settings.annotate,
                limit: settings.limit,
                quiet: settings.quiet,
                failing: settings.failing,
            },
            scorer: System::eager("scorer", Arc::new(Scorer { executed }))?,
        };
        let counting_sink = CountingSink { delivered };
        Ok(flow! {
            name: "app_identity",
            journals: disk_journals(base),

            stages: {
                src = infinite_source!(Reading => reader);
                score = effectful_transform!(
                    Reading -> { Scored, Score, Unscored, Noted, Capped }
                        => score_transform);
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
        "annotate": app.annotate,
        "limit": app.limit,
        "quiet": app.quiet,
        "failing": app.failing,
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
        allow_incomplete_archive: false,
        allow_duplicate_sink_delivery: false,
        verb,
    }
}

async fn failure(run: &Path, verb: ReplayVerb) -> Result<FailureEvidence> {
    let opened = match verb {
        ReplayVerb::Resume => DiskReplayArchive::open_for_resume(run.to_path_buf(), false).await?,
        ReplayVerb::Replay => DiskReplayArchive::open(run.to_path_buf(), false).await?,
    };
    Ok(opened.evidence().failure)
}

fn recorded_override(run: &Path) -> Result<bool> {
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(run.join("run_manifest.json"))?)?;
    Ok(manifest["replay"]["allow_incomplete_archive"] == json!(true))
}

async fn stage_fatals(run_dir: &Path) -> Result<Vec<StageFatalRecorded>> {
    let mut snapshot = open_disk_run(run_dir).await?;
    let mut fatals = Vec::new();
    while let Some(record) = snapshot.next().await? {
        if let RunRecordData::Chain(row) = &record.record {
            if let ChainPayload::Execution(ExecutionPayload::StageFatalRecorded(fatal)) =
                &row.payload
            {
                fatals.push(fatal.clone());
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

async fn assert_refused(run_dir: &Path, reason: StageFatalReason) -> Result<StageFatalRecorded> {
    let fatals = stage_fatals(run_dir).await?;
    assert_eq!(fatals.len(), 1, "{fatals:?}");
    let fatal = fatals.into_iter().next().unwrap();
    assert_eq!(fatal.stage_key, "score");
    assert_eq!(fatal.reason, reason);
    let unscored = replay_testkit::read_stage_envelopes_appended(run_dir, "score")
        .await
        .iter()
        .filter(|record| Unscored::from_event(&record.authored()).is_some())
        .count();
    assert_eq!(unscored, 0, "the handler's fallback fact never commits");
    Ok(fatal)
}

fn score_effect(input: u64, ordinal: u32) -> DepartureAction {
    DepartureAction::Effect {
        effect_type: EffectType::from(ScoreRequest::EFFECT_TYPE),
        input_position: EffectInputPosition::new(input),
        effect_ordinal: EffectOrdinal::new(ordinal),
    }
}

async fn recorded_fact<T: TypedPayload>(run_dir: &Path) -> DepartureAction {
    let event = replay_testkit::read_stage_envelopes_appended(run_dir, "score")
        .await
        .iter()
        .map(|row| row.authored())
        .find(|event| T::from_event(event).is_some())
        .expect("the recording holds the fact");
    DepartureAction::Fact {
        event_type: EventType::from(event.event_type()),
        event_id: event.id,
    }
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
    let refusal = assert_refused(&replay, StageFatalReason::DescriptorMismatch).await?;
    assert!(
        refusal.detail.starts_with("effect descriptor mismatch"),
        "{}",
        refusal.detail
    );
    assert_eq!(
        refusal.departure, None,
        "a descriptor mismatch departs from no action"
    );
    assert_eq!(
        failure(&replay, ReplayVerb::Replay).await?,
        FailureEvidence::Other,
        "a failed replay is not a resume, so it keeps the override"
    );
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
    assert_eq!(
        failure(&r1, ReplayVerb::Resume).await?,
        FailureEvidence::DepartedFromArchive
    );

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
    assert!(
        !recorded_override(&r2)?,
        "the plain remedy records no override"
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
    let refusal = assert_refused(&replay, StageFatalReason::MissingRecordedEffect).await?;
    assert_eq!(
        refusal.departure, None,
        "strict replay checks no archived actions"
    );
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
    let refusal = assert_refused(&r1, StageFatalReason::MissingRecordedEffect).await?;
    assert_eq!(
        refusal.departure,
        Some(Departure {
            offered: Some(score_effect(1, 1)),
            expected: recorded_fact::<Scored>(&r0).await,
        })
    );
    assert_eq!(read_configuration_refusals(&r1).await?, stages(&["score"]));
    assert_eq!(changed_keys(&r1)?, [(r0, vec![KEY.to_string()])]);
    Ok(())
}

async fn run_departed(journal_base: &Path, app: App) -> Result<()> {
    let (handle, run) = build(journal_base, 4, 0, app).await?;
    let outcome = tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the departed resume to end"))?;
    assert!(outcome.is_err(), "a departing action fails the resume");
    assert_eq!(run.executed(), Vec::<u64>::new(), "zero live calls");
    Ok(())
}

async fn assert_crossing_after_archived_actions(run_dir: &Path) {
    let rows = replay_testkit::read_stage_envelopes_appended(run_dir, "score").await;
    let marker = rows
        .iter()
        .position(|row| {
            matches!(
                &row.payload,
                ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete { .. })
            )
        })
        .expect("the resumed stage crosses");
    let (rebuilt, live) = rows.split_at(marker);
    assert!(rebuilt
        .iter()
        .filter(|row| row.consumes_data_credit())
        .all(|row| row.authored().replay_context.is_some()));
    let live: Vec<_> = live
        .iter()
        .filter(|row| row.consumes_data_credit())
        .collect();
    assert!(!live.is_empty(), "live work follows the crossing");
    assert!(live
        .iter()
        .all(|row| row.authored().replay_context.is_none()));
}

async fn record_readings(journal_base: &Path, app: App) -> Result<PathBuf> {
    run_until(journal_base, 1, 3, app, 3, 3).await?;
    Ok(std::fs::canonicalize(replay_testkit::latest_run_dir(
        journal_base,
    ))?)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_value_that_adds_a_fact_refuses_resume_until_the_recorded_value_returns(
) -> Result<()> {
    const KEY: &str = "app.annotate";
    let annotate = App {
        annotate: true,
        ..RECORDED
    };

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let r0 = record_readings(&journal_base, RECORDED).await?;

    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Resume)).await,
        );
        run_departed(&journal_base, annotate).await?;
    }
    let r1 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;
    let refusal = assert_refused(&r1, StageFatalReason::MissingRecordedFact).await?;
    assert_eq!(
        refusal.detail,
        "stage 'score' emitted Noted at input 1, but its recording's next action is a \
         Score effect"
    );
    let departure = refusal.departure.expect("the refusal names both actions");
    assert!(
        matches!(
            &departure.offered,
            Some(DepartureAction::Fact { event_type, .. })
                if Noted::matches_event_type(event_type.as_str())
        ),
        "{departure:?}"
    );
    assert_eq!(departure.expected, score_effect(2, 0));
    assert_eq!(changed_keys(&r1)?, [(r0.clone(), vec![KEY.to_string()])]);

    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r1.clone(), ReplayVerb::Resume)).await,
        );
        assert_eq!(
            run_until(&journal_base, 4, 2, RECORDED, 2, 2).await?,
            [4, 5]
        );
    }
    let r2 = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(stage_fatals(&r2).await?, Vec::new());
    assert_crossing_after_archived_actions(&r2).await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_value_that_skips_a_recorded_fact_refuses_resume() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let annotate = App {
        annotate: true,
        ..RECORDED
    };
    let r0 = record_readings(&journal_base, annotate).await?;

    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Resume)).await,
        );
        run_departed(&journal_base, RECORDED).await?;
    }
    let r1 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;
    let refusal = assert_refused(&r1, StageFatalReason::UnusedFactHistory).await?;
    assert_eq!(
        refusal.detail,
        "stage 'score' skipped a Noted fact its recording holds"
    );
    assert_eq!(
        refusal.departure,
        Some(Departure {
            offered: Some(score_effect(2, 0)),
            expected: recorded_fact::<Noted>(&r0).await,
        })
    );
    assert_eq!(changed_keys(&r1)?, [(r0, vec!["app.annotate".to_string()])]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_value_that_turns_a_settled_input_into_a_call_refuses_resume() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let capped = App {
        limit: Some(2),
        ..RECORDED
    };
    let r0 = {
        let (handle, run) = build(&journal_base, 1, 3, capped).await?;
        wait_for_running(&handle).await?;
        let r0 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let capped_rows = replay_testkit::read_stage_envelopes_appended(&r0, "score")
                    .await
                    .iter()
                    .filter(|row| Capped::from_event(&row.authored()).is_some())
                    .count();
                if capped_rows == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .map_err(|_| anyhow!("timeout waiting for reading 3 to be capped"))?;
        handle.stop().await?;
        tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for the run to stop"))??;
        assert_eq!(
            run.executed(),
            [1, 2],
            "the recording settles reading 3 without a call"
        );
        r0
    };

    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Resume)).await,
        );
        run_departed(&journal_base, RECORDED).await?;
    }
    let r1 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;
    let refusal = assert_refused(&r1, StageFatalReason::MissingRecordedEffect).await?;
    assert_eq!(
        refusal.detail,
        "stage 'score' would perform Score at input 3, but its recording's next action is a \
         Capped fact"
    );
    assert_eq!(
        refusal.departure,
        Some(Departure {
            offered: Some(score_effect(3, 0)),
            expected: recorded_fact::<Capped>(&r0).await,
        })
    );
    assert_eq!(changed_keys(&r1)?, [(r0, vec!["app.limit".to_string()])]);
    Ok(())
}

async fn settlements(run_dir: &Path) -> Vec<(u64, bool)> {
    replay_testkit::read_stage_envelopes_appended(run_dir, "score")
        .await
        .iter()
        .filter_map(|row| {
            let event = row.authored();
            let provenance = event.effect_provenance.as_ref()?;
            (provenance.descriptor.effect_type.as_str() == SETTLEMENT_EFFECT_TYPE).then(|| {
                (
                    provenance.cursor.input_seq.get(),
                    event.replay_context.is_some(),
                )
            })
        })
        .collect()
}

async fn record_a_settlement_without_an_action(journal_base: &Path, app: App) -> Result<PathBuf> {
    let (handle, run) = build(journal_base, 1, 3, app).await?;
    wait_for_running(&handle).await?;
    let r0 = std::fs::canonicalize(replay_testkit::latest_run_dir(journal_base))?;
    tokio::time::timeout(Duration::from_secs(20), async {
        while settlements(&r0).await.is_empty() {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for reading 3 to settle"))?;
    handle.stop().await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the run to stop"))??;
    assert_eq!(run.executed(), [1, 2]);
    assert_eq!(settlements(&r0).await, [(3, false)]);
    Ok(r0)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_changed_value_that_acts_on_an_input_settled_without_an_action_refuses_resume(
) -> Result<()> {
    for (key, settled) in [
        (
            "app.quiet",
            App {
                quiet: Some(3),
                ..RECORDED
            },
        ),
        (
            "app.failing",
            App {
                failing: Some(3),
                ..RECORDED
            },
        ),
    ] {
        let temp = tempfile::tempdir()?;
        let journal_base = temp.path().join("journals");
        let r0 = record_a_settlement_without_an_action(&journal_base, settled).await?;

        {
            let _bootstrap = install_bootstrap_config(
                replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Replay))
                    .await,
            );
            let (handle, run) = build(&journal_base, 1, 0, settled).await?;
            tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
                .await
                .map_err(|_| anyhow!("timeout waiting for the replay"))??;
            assert_eq!(run.executed(), Vec::<u64>::new(), "replay makes no call");
        }
        let replay = replay_testkit::latest_run_dir(&journal_base);
        assert_eq!(
            settlements(&replay).await,
            [(3, true)],
            "{key}: strict replay copies the settlement"
        );

        {
            let _bootstrap = install_bootstrap_config(
                replay_testkit::bootstrap_with_archive(archive(r0.clone(), ReplayVerb::Resume))
                    .await,
            );
            run_departed(&journal_base, RECORDED).await?;
        }
        let r1 = std::fs::canonicalize(replay_testkit::latest_run_dir(&journal_base))?;
        let refusal = assert_refused(&r1, StageFatalReason::MissingRecordedEffect).await?;
        assert_eq!(
            refusal.detail,
            "stage 'score' would perform Score at input 3, but its recording settled that \
             input without an action",
            "{key}"
        );
        assert_eq!(
            refusal.departure,
            Some(Departure {
                offered: Some(score_effect(3, 0)),
                expected: DepartureAction::Settlement {
                    input_position: EffectInputPosition::new(3),
                },
            }),
            "{key}"
        );
        assert_eq!(changed_keys(&r1)?, [(r0.clone(), vec![key.to_string()])]);

        {
            let _bootstrap = install_bootstrap_config(
                replay_testkit::bootstrap_with_archive(archive(r1.clone(), ReplayVerb::Resume))
                    .await,
            );
            assert_eq!(
                run_until(&journal_base, 4, 2, settled, 2, 2).await?,
                [4, 5],
                "{key}"
            );
        }
        let r2 = replay_testkit::latest_run_dir(&journal_base);
        assert_eq!(stage_fatals(&r2).await?, Vec::new(), "{key}");
        assert_eq!(
            settlements(&r2).await,
            [(3, true)],
            "{key}: the recorded value reproduces the settlement"
        );
        assert_crossing_after_archived_actions(&r2).await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn strict_replay_with_a_changed_value_proceeds_and_verify_reports_the_difference(
) -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let r0 = record_readings(&journal_base, RECORDED).await?;
    let annotate = App {
        annotate: true,
        ..RECORDED
    };

    {
        let plain = ReplayBootstrap {
            allow_incomplete_archive: false,
            ..archive(r0.clone(), ReplayVerb::Replay)
        };
        let _bootstrap =
            install_bootstrap_config(replay_testkit::bootstrap_with_archive(plain).await);
        let (handle, run) = build(&journal_base, 1, 0, annotate).await?;
        tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for the replay"))??;
        assert_eq!(run.executed(), Vec::<u64>::new(), "replay makes no call");
    }
    let replay = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(stage_fatals(&replay).await?, Vec::new());
    let verification = obzenflow_infra::verify::verify_run_dirs(
        &r0,
        &replay,
        &obzenflow_infra::verify::VerifyOptions {
            write_report: false,
            ..obzenflow_infra::verify::VerifyOptions::default()
        },
    )?;
    assert_eq!(
        verification.exit_code(),
        1,
        "{}",
        obzenflow_infra::verify::render_verdict(&verification)
    );
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
                accounts = source!(Account with {
                    rate_limit(rate).burst_capacity(burst)
                } => accounts_source);
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
    assert_eq!(
        failure(&r1, ReplayVerb::Resume).await?,
        FailureEvidence::DepartedFromArchive
    );

    let r1 = std::fs::canonicalize(r1)?;
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(archive(r1.clone(), ReplayVerb::Resume)).await,
        );
        let handle = build_accounts(
            &journal_base,
            &recorded_file,
            0.5,
            3.0,
            Arc::new(AtomicUsize::new(0)),
        )
        .await?;
        tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for the remedy to complete"))??;
    }
    let r2 = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(source_refusals(&r2).await?, Vec::new());
    assert_eq!(stage_fatals(&r2).await?, Vec::new());
    Ok(())
}

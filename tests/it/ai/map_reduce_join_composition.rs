// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::fake_ai;

use fake_ai::FakeChatModels;
use obzenflow::ai::{
    self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec, Chunk, Partials,
};
use obzenflow_core::ai::{
    AiProviderError, AiProviderFailureKind, AiUnavailable, AiUnavailableCause,
};
use obzenflow_core::journal::RunManifest;
use obzenflow_core::{outcome, Outcomes, TypedPayload};
use obzenflow_dsl::{ai, flow, join, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, Verdict, VerifyOptions};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkWriteFailure, SourceError, TypedFiniteSourceHandler, TypedTransformHandler,
};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Seed {
    id: u64,
    key: u64,
    values: Vec<u64>,
}

impl TypedPayload for Seed {
    const EVENT_TYPE: &'static str = "test.ai_join.seed";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SeedValue {
    seed: u64,
    key: u64,
    value: u64,
}

impl TypedPayload for SeedValue {
    const EVENT_TYPE: &'static str = "test.ai_join.seed_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Scale {
    key: u64,
    factor: u64,
}

impl TypedPayload for Scale {
    const EVENT_TYPE: &'static str = "test.ai_join.scale";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Partial {
    total: u64,
}

impl TypedPayload for Partial {
    const EVENT_TYPE: &'static str = "test.ai_join.partial";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Row {
    id: u64,
    key: u64,
    total: u64,
}

impl TypedPayload for Row {
    const EVENT_TYPE: &'static str = "test.ai_join.row";
}

#[derive(Debug, Default)]
struct SourceReads(AtomicUsize);

#[derive(Clone, Debug)]
struct FixtureSource<T> {
    rows: VecDeque<T>,
    reads: Arc<SourceReads>,
}

impl<T: TypedPayload + Clone + Send + Sync + 'static> TypedFiniteSourceHandler
    for FixtureSource<T>
{
    type Output = T;

    fn next(&mut self) -> Result<Option<Vec<T>>, SourceError> {
        self.reads.0.fetch_add(1, Ordering::SeqCst);
        Ok(self.rows.pop_front().map(|row| vec![row]))
    }
}

#[derive(Clone, Debug)]
struct Explode;

impl TypedTransformHandler for Explode {
    type Input = Seed;
    type Output = Outcomes<SeedValue>;

    fn process(&self, seed: Seed) -> Result<Outcomes<SeedValue>, HandlerError> {
        Ok(Outcomes::many(seed.values.iter().map(|value| SeedValue {
            seed: seed.id,
            key: seed.key,
            value: *value,
        })))
    }
}

#[derive(Clone, Copy)]
struct SumMap;

impl AiRole<Chunk<SeedValue>> for SumMap {
    type Effect = ChatCompletion;
    type Out = Partial;

    fn stage_logic_version(&self) -> &str {
        "sum-map-v1"
    }

    fn prepare(&self, chunk: &Chunk<SeedValue>) -> Result<ChatRequestSpec, HandlerError> {
        let values: Vec<u64> = chunk.iter().map(|item| item.value).collect();
        Ok(ChatRequestSpec::new("Sum.", format!("sum {values:?}")))
    }

    fn interpret(
        &self,
        chunk: Chunk<SeedValue>,
        call: AiOutcome<ChatCompletionReply>,
    ) -> Result<Partial, HandlerError> {
        call.replied()?;
        Ok(Partial {
            total: chunk.iter().map(|item| item.value).sum(),
        })
    }
}

#[derive(Clone, Copy)]
struct SumReduce;

impl AiRole<Partials<SeedValue, Partial>> for SumReduce {
    type Effect = ChatCompletion;
    type Out = Row;

    fn prepare(
        &self,
        input: &Partials<SeedValue, Partial>,
    ) -> Result<ChatRequestSpec, HandlerError> {
        Ok(ChatRequestSpec::new(
            "Reduce.",
            format!("reduce {}", input.items()[0].seed),
        ))
    }

    fn interpret(
        &self,
        input: Partials<SeedValue, Partial>,
        call: AiOutcome<ChatCompletionReply>,
    ) -> Result<Row, HandlerError> {
        call.replied()?;
        let first = &input.items()[0];
        if first.seed == 4 {
            return Err(HandlerError::Domain("deliberate fixture failure".into()));
        }
        Ok(Row {
            id: first.seed,
            key: first.key,
            total: input.partials().iter().map(|partial| partial.total).sum(),
        })
    }
}

fn seed_of(item: &SeedValue) -> u64 {
    item.seed
}

#[derive(Clone, Copy, Debug)]
enum Placement {
    Before,
}

impl Placement {
    fn expected(self) -> Vec<Row> {
        match self {
            Self::Before => vec![
                Row {
                    id: 1,
                    key: 7,
                    total: 12,
                },
                Row {
                    id: 3,
                    key: 7,
                    total: 18,
                },
            ],
        }
    }
}

fn seed_rows() -> VecDeque<Seed> {
    [
        Seed {
            id: 1,
            key: 7,
            values: vec![1, 2, 3],
        },
        Seed {
            id: 2,
            key: 8,
            values: vec![],
        },
        Seed {
            id: 3,
            key: 7,
            values: vec![4, 5],
        },
        Seed {
            id: 4,
            key: 9,
            values: vec![6],
        },
    ]
    .into()
}

fn scale_rows() -> VecDeque<Scale> {
    [
        Scale { key: 7, factor: 2 },
        Scale { key: 8, factor: 3 },
        Scale { key: 9, factor: 4 },
    ]
    .into()
}

fn manifest(run: &Path) -> RunManifest {
    serde_json::from_slice(&std::fs::read(run.join("run_manifest.json")).unwrap()).unwrap()
}

fn run_dir(root: &Path) -> PathBuf {
    let paths: Vec<_> = std::fs::read_dir(root.join("flows"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(paths.len(), 1, "each run has a separate journal root");
    paths[0].clone()
}

#[derive(Clone)]
struct CollectRows {
    delivered: Arc<Mutex<Vec<Row>>>,
}

#[async_trait::async_trait]
impl obzenflow_runtime::stages::sink::InlineSink for CollectRows {
    type Input = Row;
    fn describe(&self) -> obzenflow_runtime::stages::sink::SinkDescription {
        obzenflow_runtime::stages::sink::SinkDescription::method(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Custom(
                "collect_rows".into(),
            ),
        )
        .with_redelivery_safety(obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat)
    }
    async fn write(&mut self, input: Row) -> Result<(), SinkWriteFailure> {
        self.delivered.lock().unwrap().push(input);
        Ok(())
    }
}

impl std::fmt::Debug for CollectRows {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CollectRows")
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct HeldValue {
    seed: u64,
    value: u64,
    unavailable: AiUnavailable,
}

impl TypedPayload for HeldValue {
    const EVENT_TYPE: &'static str = "test.ai_join.held_value";
}

#[derive(Clone)]
struct TriageRole {
    version: String,
    interpreted: Arc<AtomicUsize>,
}

impl AiRole<SeedValue> for TriageRole {
    type Effect = ChatCompletion;
    type Out = outcome![SeedValue | HeldValue];

    fn stage_logic_version(&self) -> &str {
        &self.version
    }

    fn prepare(&self, item: &SeedValue) -> Result<ChatRequestSpec, HandlerError> {
        Ok(ChatRequestSpec::new(
            "Triage.",
            format!("triage {}", item.value),
        ))
    }

    fn interpret(
        &self,
        item: SeedValue,
        call: AiOutcome<ChatCompletionReply>,
    ) -> Result<Self::Out, HandlerError> {
        self.interpreted.fetch_add(1, Ordering::SeqCst);
        if let Some(unavailable) = call.unavailable() {
            return Ok(outcome(HeldValue {
                seed: item.seed,
                value: item.value,
                unavailable: unavailable.clone(),
            }));
        }
        call.replied()?;
        Ok(outcome(item))
    }
}

#[derive(Clone)]
struct CollectHeld {
    held: Arc<Mutex<Vec<HeldValue>>>,
}

#[async_trait::async_trait]
impl obzenflow_runtime::stages::sink::InlineSink for CollectHeld {
    type Input = HeldValue;
    fn describe(&self) -> obzenflow_runtime::stages::sink::SinkDescription {
        obzenflow_runtime::stages::sink::SinkDescription::method(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Custom(
                "collect_held".into(),
            ),
        )
        .with_redelivery_safety(obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat)
    }
    async fn write(&mut self, input: HeldValue) -> Result<(), SinkWriteFailure> {
        self.held.lock().unwrap().push(input);
        Ok(())
    }
}

impl std::fmt::Debug for CollectHeld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CollectHeld")
    }
}

#[derive(Clone, Default)]
struct TriageRun {
    interpreted: Arc<AtomicUsize>,
    delivered: Arc<Mutex<Vec<Row>>>,
    held: Arc<Mutex<Vec<HeldValue>>>,
}

fn build_triage_flow(root: PathBuf, run: TriageRun) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let reads = Arc::new(SourceReads::default());
        let seeds = FixtureSource {
            rows: seed_rows(),
            reads: reads.clone(),
        };
        let scales = FixtureSource {
            rows: scale_rows(),
            reads,
        };
        let joined = obzenflow::stages::joins::inner(
            |scale: &Scale| scale.key,
            |seed: &Seed| seed.key,
            |scale: Scale, mut seed: Seed| {
                seed.values
                    .iter_mut()
                    .for_each(|value| *value *= scale.factor);
                seed
            },
        );
        let explode = Explode;
        let triage = ai::infer(TriageRole {
            version: "triage-v1".to_string(),
            interpreted: run.interpreted.clone(),
        });
        let digest = ai::map(SumMap)
            .key_by(seed_of as fn(&SeedValue) -> u64)
            .chunks(1)
            .reduce(SumReduce)
            .on_eof();
        let held = CollectHeld {
            held: run.held.clone(),
        };
        let output = CollectRows {
            delivered: run.delivered.clone(),
        };

        Ok(flow! {
            name: "ai_infer_into_ai_map_reduce",
            journals: disk_journals(root),
            stages: {
                seeds = source!(Seed => seeds);
                scales = source!(Scale => scales);
                joined = join!(catalog scales: Scale, Seed -> Seed => joined);
                explode = transform!(Seed -> SeedValue => explode);
                triage = ai!(SeedValue -> { SeedValue, HeldValue } => triage);
                digest = ai!(SeedValue -> Row => digest);
                held = sink!(HeldValue => held);
                output = sink!(Row => output);
            },
            topology: {
                (scales, seeds) |> joined;
                joined |> explode;
                explode |> triage;
                triage |> digest;
                triage |> held;
                digest |> output;
            }
        })
    })
}

fn triage_config(root: &Path) -> PathBuf {
    let config = root.with_extension("toml");
    std::fs::write(
        &config,
        "[journal]\nsync = \"off\"\n\n[ai.models]\nmodel = \"deterministic\"\n",
    )
    .expect("write the run configuration");
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn a_multi_fact_infer_feeds_the_map_reduce_through_a_selected_feed_and_replays() {
    let temp = tempfile::tempdir().unwrap();
    let expected_rows = Placement::Before.expected();
    let expected_held = vec![HeldValue {
        seed: 4,
        value: 24,
        unavailable: AiUnavailable::new(
            AiUnavailableCause::Provider {
                failure: AiProviderFailureKind::Timeout,
            },
            "endpoint down",
        ),
    }];

    let live_root = temp.path().join("live");
    let live = TriageRun::default();
    let models = FakeChatModels::with_reply(|request| {
        if fake_ai::last_user_message(request) == "triage 24" {
            Err(AiProviderError::Timeout {
                message: "endpoint down".to_string(),
            })
        } else {
            Ok(fake_ai::response("ok"))
        }
    });
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(triage_config(&live_root))
        .with_ai_models(models.resolvers())
        .run_async(build_triage_flow(live_root.clone(), live.clone()))
        .await
        .expect("the composition runs live");
    assert_eq!(*live.delivered.lock().unwrap(), expected_rows);
    assert_eq!(*live.held.lock().unwrap(), expected_held);
    assert_eq!(
        models.call_count(),
        13,
        "six triage calls, five map calls and two reduce calls"
    );
    let live_archive = run_dir(&live_root);
    assert_eq!(
        manifest(&live_archive).stages["triage"].stage_logic_version,
        "triage-v1"
    );

    let replay_root = temp.path().join("replay");
    let replayed = TriageRun::default();
    let replay = FakeChatModels::forbidden();
    FlowApplication::builder()
        .with_cli_args([
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.clone().into_os_string(),
        ])
        .with_config_file(triage_config(&replay_root))
        .with_ai_models(replay.resolvers())
        .run_async(build_triage_flow(replay_root.clone(), replayed.clone()))
        .await
        .expect("the composition replays");
    assert_eq!(*replayed.delivered.lock().unwrap(), expected_rows);
    assert_eq!(*replayed.held.lock().unwrap(), expected_held);
    assert_eq!(replay.connect_count() + replay.call_count(), 0);

    let outcome = verify_run_dirs(
        &live_archive,
        &run_dir(&replay_root),
        &VerifyOptions {
            write_report: false,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(outcome.verdict(), Verdict::CertifiedMatch, "{outcome:?}");
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime and journal proofs for joins adjacent to a map-reduce `ai!` stage
//! keyed by seed. All inputs and chat responses are local fixtures; replay
//! cannot connect a client.

mod replay_testkit;

#[path = "support/fake_ai.rs"]
pub mod fake_ai;

use fake_ai::FakeChatModels;
use obzenflow::ai::{
    self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec, Chunk, Partials,
};
use obzenflow_core::ai::{
    AiMapReduceGroupingEnded, AiMapReduceMapInput, AiMapReduceReduceFailed,
};
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::payloads::system_payload::SystemFeedRole;
use obzenflow_core::event::provenance::CompositeActivationContext;
use obzenflow_core::event::{ChainEvent, ChainPayload, JournalRecord};
use obzenflow_core::journal::RunManifest;
use obzenflow_core::{StageId, StageOutputs, TypedPayload, WriterId};
use obzenflow_dsl::{ai, flow, join, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, Verdict, VerifyOptions};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkWriteFailure, SourceError, TypedFiniteSourceHandler, TypedTransformHandler,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    type Output = StageOutputs<SeedValue>;

    fn process(&self, seed: Seed) -> Result<StageOutputs<SeedValue>, HandlerError> {
        Ok(StageOutputs::many(seed.values.iter().map(|value| SeedValue {
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

    fn prepare(&self, chunk: &Chunk<SeedValue>) -> Result<ChatRequestSpec, HandlerError> {
        let values: Vec<u64> = chunk.iter().map(|item| item.value).collect();
        Ok(ChatRequestSpec::new("Sum.", format!("sum {values:?}")))
    }

    fn interpret(
        &self,
        chunk: Chunk<SeedValue>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<Partial, HandlerError> {
        outcome.replied()?;
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
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<Row, HandlerError> {
        outcome.replied()?;
        let first = &input.items()[0];
        // A different payload family in the same reduce journal must never be
        // decoded as Row by either side of a downstream join.
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
    Stream,
    Catalog,
}

impl Placement {
    fn result_stage(self) -> &'static str {
        if matches!(self, Self::Before) {
            "digest__reduce"
        } else {
            "joined"
        }
    }

    fn expected(self) -> Vec<Row> {
        match self {
            Self::Before | Self::Stream => vec![
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
            Self::Catalog => vec![
                Row {
                    id: 101,
                    key: 7,
                    total: 9,
                },
                Row {
                    id: 103,
                    key: 7,
                    total: 9,
                },
            ],
        }
    }
}

fn build_flow(root: PathBuf, placement: Placement, reads: Arc<SourceReads>, delivered: Arc<Mutex<Vec<Row>>>) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let digest = ai::map(SumMap)
            .key_by(seed_of as fn(&SeedValue) -> u64)
            .chunks(1)
            .reduce(SumReduce)
            .on_eof();
        let explode = Explode;
        let seeds = FixtureSource {
            rows: [
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
            .into(),
            reads: reads.clone(),
        };
        let scales = FixtureSource {
            rows: [
                Scale { key: 7, factor: 2 },
                Scale { key: 8, factor: 3 },
                Scale { key: 9, factor: 4 },
            ]
            .into(),
            reads: reads.clone(),
        };
        let queries = FixtureSource {
            rows: [
                Row {
                    id: 101,
                    key: 7,
                    total: 999,
                },
                Row {
                    id: 102,
                    key: 8,
                    total: 999,
                },
                Row {
                    id: 103,
                    key: 7,
                    total: 999,
                },
                Row {
                    id: 104,
                    key: 9,
                    total: 999,
                },
            ]
            .into(),
            reads,
        };
        let output = CollectRows { delivered };

        Ok(match placement {
            Placement::Before => {
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
                flow! {
                    name: "join_before_ai",
                    journals: disk_journals(root),
                    stages: {
                        seeds = source!(Seed => seeds);
                        scales = source!(Scale => scales);
                        joined = join!(catalog scales: Scale, Seed -> Seed => joined);
                        explode = transform!(Seed -> SeedValue => explode);
                        digest = ai!(SeedValue -> Row => digest);
                        output = sink!(Row => output);
                    },
                    topology: {
                        (scales, seeds) |> joined;
                        joined |> explode;
                        explode |> digest;
                        digest |> output;
                    }
                }
            }
            Placement::Stream => {
                let joined = obzenflow::stages::joins::inner(
                    |scale: &Scale| scale.key,
                    |row: &Row| row.key,
                    |scale: Scale, mut row: Row| {
                        row.total *= scale.factor;
                        row
                    },
                );
                flow! {
                    name: "ai_as_join_stream",
                    journals: disk_journals(root),
                    stages: {
                        seeds = source!(Seed => seeds);
                        scales = source!(Scale => scales);
                        explode = transform!(Seed -> SeedValue => explode);
                        digest = ai!(SeedValue -> Row => digest);
                        joined = join!(catalog scales: Scale, Row -> Row => joined);
                        output = sink!(Row => output);
                    },
                    topology: {
                        seeds |> explode;
                        explode |> digest;
                        (scales, digest) |> joined;
                        joined |> output;
                    }
                }
            }
            Placement::Catalog => {
                let joined = obzenflow::stages::joins::inner(
                    |reference: &Row| reference.key,
                    |query: &Row| query.key,
                    |reference: Row, mut query: Row| {
                        query.total = reference.total;
                        query
                    },
                );
                flow! {
                    name: "ai_as_join_catalog",
                    journals: disk_journals(root),
                    stages: {
                        seeds = source!(Seed => seeds);
                        queries = source!(Row => queries);
                        explode = transform!(Seed -> SeedValue => explode);
                        digest = ai!(SeedValue -> Row => digest);
                        joined = join!(catalog digest: Row, Row -> Row => joined);
                        output = sink!(Row => output);
                    },
                    topology: {
                        seeds |> explode;
                        explode |> digest;
                        (digest, queries) |> joined;
                        joined |> output;
                    }
                }
            }
        })
    })
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

async fn stage_records(run: &Path, stage: &str) -> Vec<JournalRecord<ChainPayload>> {
    let manifest = manifest(run);
    replay_testkit::read_journal_envelopes_appended::<ChainEvent>(
        &run.join(&manifest.stages[stage].data_journal_file),
    )
    .await
}

fn typed<T: TypedPayload>(
    records: &[JournalRecord<ChainPayload>],
) -> Vec<(T, &JournalRecord<ChainPayload>)> {
    records
        .iter()
        .filter_map(|row| T::from_event(&row.authored()).map(|value| (value, row)))
        .collect()
}

fn activation_set(row: &JournalRecord<ChainPayload>) -> BTreeSet<String> {
    row.composite_activations()
        .iter()
        .map(|activation| format!("{activation:?}"))
        .collect()
}

fn expected_set<'a>(activations: impl IntoIterator<Item = &'a CompositeActivationContext>) -> BTreeSet<String> {
    activations
        .into_iter()
        .map(|activation| format!("{activation:?}"))
        .collect()
}

async fn assert_journals(run: &Path, placement: Placement) -> Vec<Row> {
    let manifest = manifest(run);
    let item_records = stage_records(run, "explode").await;
    let items = typed::<SeedValue>(&item_records);
    assert_eq!(items.len(), 6, "the empty seed contributes no item");
    let mut item_activations: BTreeMap<u64, Vec<CompositeActivationContext>> = BTreeMap::new();
    for (item, row) in &items {
        item_activations
            .entry(item.seed)
            .or_default()
            .push(CompositeActivationContext::new(
                obzenflow_core::id::CompositeId::new("ai_map_reduce:digest"),
                row.envelope.provenance.event.id,
                "in",
                row.envelope.provenance.event.processing.event_time,
            ));
    }

    let chunks = stage_records(run, "digest__chunk").await;
    let groups = typed::<AiMapReduceMapInput<SeedValue>>(&chunks);
    assert_eq!(groups.len(), 6, "one group per item at chunks(1)");
    for (group, record) in &groups {
        let seed = group.items[0].seed;
        assert_eq!(group.key, Some(serde_json::json!(seed)), "groups never mix keys");
        assert_eq!(activation_set(record).len(), 1);
        assert!(expected_set(&item_activations[&seed]).is_superset(&activation_set(record)));
    }
    let ended = typed::<AiMapReduceGroupingEnded>(&chunks);
    let group_counts: BTreeMap<String, u64> = ended
        .iter()
        .map(|(ended, _)| {
            (
                serde_json::to_string(&ended.key).unwrap(),
                ended.group_count,
            )
        })
        .collect();
    assert_eq!(
        group_counts,
        BTreeMap::from([
            ("1".to_string(), 3),
            ("3".to_string(), 2),
            ("4".to_string(), 1),
        ])
    );

    let reduce = stage_records(run, "digest__reduce").await;
    let reduced = typed::<Row>(&reduce);
    assert_eq!(reduced.len(), 2);
    for (row, record) in &reduced {
        assert_eq!(
            activation_set(record),
            expected_set(&item_activations[&row.id]),
            "a reduced row carries exactly its own seed's contributions"
        );
    }
    let failed = typed::<AiMapReduceReduceFailed>(&reduce);
    assert_eq!(failed.len(), 1, "the deliberate failure remains durable");
    assert_eq!(failed[0].0.key, Some(serde_json::json!(4)));
    assert_eq!(
        activation_set(failed[0].1),
        expected_set(&item_activations[&4])
    );

    let results = stage_records(run, placement.result_stage()).await;
    let output = typed::<Row>(&results);
    let values: Vec<_> = output.iter().map(|(value, _)| value.clone()).collect();
    assert_eq!(values, placement.expected());
    for (value, record) in &output {
        let selected = if matches!(placement, Placement::Catalog) {
            3
        } else {
            value.id
        };
        assert_eq!(
            activation_set(record),
            expected_set(&item_activations[&selected])
        );
    }

    let joins = stage_records(run, "joined").await;
    let writer = WriterId::from(
        manifest.stages["joined"]
            .stage_id
            .parse::<StageId>()
            .unwrap(),
    );
    let mut finals = Vec::new();
    for row in &joins {
        if row.envelope.provenance.event.writer_id != writer {
            continue;
        }
        if let ChainPayload::FlowControl(FlowControlPayload::ConsumptionFinal {
            pass,
            eof_seen,
            reader_seq,
            advertised_writer_seq,
            consumed_count,
            ..
        }) = &row.payload
        {
            assert!(*pass && *eof_seen);
            assert_eq!(Some(*reader_seq), *advertised_writer_seq);
            assert_eq!(reader_seq.0, consumed_count.0);
            finals.push(consumed_count.0);
        }
    }
    finals.sort();
    assert_eq!(
        finals,
        match placement {
            Placement::Before => vec![3, 4],
            Placement::Stream => vec![2, 3],
            Placement::Catalog => vec![2, 4],
        }
    );

    // Check locally authored EOFs, rather than forwarded upstream EOF copies.
    for (stage, kind, count) in [
        (
            "joined",
            if matches!(placement, Placement::Before) {
                Seed::descriptor()
            } else {
                Row::descriptor()
            },
            if matches!(placement, Placement::Before) {
                4
            } else {
                2
            },
        ),
        ("digest__reduce", Row::descriptor(), 2),
    ] {
        let writer = WriterId::from(manifest.stages[stage].stage_id.parse::<StageId>().unwrap());
        let records = stage_records(run, stage).await;
        let eofs: Vec<_> = records
            .iter()
            .filter(|row| row.envelope.provenance.event.writer_id == writer)
            .filter_map(|row| {
                if let ChainPayload::FlowControl(FlowControlPayload::Eof {
                    kind,
                    writer_seq_by_event_type,
                    ..
                }) = &row.payload
                {
                    Some((kind, writer_seq_by_event_type))
                } else {
                    None
                }
            })
            .collect();
        assert_eq!(eofs.len(), 1);
        assert_eq!(*eofs[0].0, EofKind::Natural);
        assert_eq!(eofs[0].1[&kind].0, count);
        if stage == "digest__reduce" {
            assert_eq!(eofs[0].1[&AiMapReduceReduceFailed::descriptor()].0, 1);
        }
    }

    let receipts = stage_records(run, "output").await;
    let parents: Vec<_> = receipts
        .iter()
        .filter_map(|row| {
            if let ChainPayload::Delivery(delivery) = &row.payload {
                assert_eq!(
                    serde_json::to_value(&delivery.result).unwrap(),
                    serde_json::json!({"result": "success"})
                );
                Some(row.envelope.provenance.event.causality.parent_ids.clone())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        parents,
        output
            .iter()
            .map(|(_, row)| vec![row.envelope.provenance.event.id])
            .collect::<Vec<_>>()
    );

    let mut join_feeds = Vec::new();
    let join_id = manifest.stages["joined"]
        .stage_id
        .parse::<StageId>()
        .unwrap();
    for record in stage_records(run, "joined").await {
        if let ChainPayload::Execution(ExecutionPayload::ContractStatus {
            reader,
            upstream,
            selected_event_type,
            feed_role,
            pass,
            reader_seq,
            advertised_writer_seq,
            ..
        }) = &record.payload
        {
            if *reader == join_id {
                assert!(*pass);
                assert_eq!(reader_seq, advertised_writer_seq);
                join_feeds.push((
                    *upstream,
                    selected_event_type.clone().unwrap(),
                    feed_role.unwrap(),
                    reader_seq.unwrap().0,
                ));
            }
        }
    }
    let (reference, reference_type, reference_count, stream, stream_type, stream_count) =
        match placement {
            Placement::Before => (
                "scales",
                Scale::descriptor(),
                3,
                "seeds",
                Seed::descriptor(),
                4,
            ),
            Placement::Stream => (
                "scales",
                Scale::descriptor(),
                3,
                "digest__reduce",
                Row::descriptor(),
                2,
            ),
            Placement::Catalog => (
                "digest__reduce",
                Row::descriptor(),
                2,
                "queries",
                Row::descriptor(),
                4,
            ),
        };
    assert_eq!(join_feeds.len(), 2);
    for (stage, kind, role, count) in [
        (
            reference,
            reference_type,
            SystemFeedRole::Reference,
            reference_count,
        ),
        (stream, stream_type, SystemFeedRole::Stream, stream_count),
    ] {
        assert!(
            join_feeds.contains(&(
                manifest.stages[stage].stage_id.parse().unwrap(),
                kind,
                role,
                count
            )),
            "missing exact {role:?} contract for {stage}"
        );
    }
    values
}

async fn exercise(placement: Placement) {
    let temp = tempfile::tempdir().unwrap();
    let mut live: Option<PathBuf> = None;
    let mut live_values = Vec::new();
    for mode in ["live", "replay"] {
        let replay = mode == "replay";
        let root = temp.path().join(mode);
        let reads = Arc::new(SourceReads::default());
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let models = if replay {
            FakeChatModels::forbidden()
        } else {
            FakeChatModels::replying(|_| "ok".to_string())
        };
        let mut args = vec![OsString::from("obzenflow")];
        if let Some(archive) = &live {
            args.push("--replay-from".into());
            args.push(OsString::from(archive));
        }
        let config = root.with_extension("toml");
        std::fs::write(
            &config,
            "[runtime.backpressure.flow]\nmode = \"enforce\"\nwindow = 3\nstall_timeout_ms = 3000\n\n\
             [ai.models]\nmodel = \"deterministic\"\n",
        )
        .expect("write the run configuration");
        tokio::time::timeout(
            Duration::from_secs(30),
            FlowApplication::builder()
                .with_cli_args(args)
                .with_config_file(config)
                .with_ai_models(models.providers())
                .run_async(build_flow(
                    root.clone(),
                    placement,
                    reads.clone(),
                    delivered.clone(),
                )),
        )
        .await
        .expect("composition must release enforced backpressure")
        .expect("composition run completes");
        assert_eq!(
            models.call_count(),
            if replay { 0 } else { 9 },
            "six map calls and three reduce calls; replay performs neither"
        );
        if replay {
            assert_eq!(models.connect_count(), 0);
        }
        assert_eq!(reads.0.load(Ordering::SeqCst) == 0, replay);
        assert_eq!(*delivered.lock().unwrap(), placement.expected());
        let archive = run_dir(&root);
        let values = assert_journals(&archive, placement).await;
        if let Some(baseline) = &live {
            assert_eq!(values, live_values);
            let outcome = verify_run_dirs(
                baseline,
                &archive,
                &VerifyOptions {
                    write_report: false,
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(outcome.verdict(), Verdict::CertifiedMatch, "{outcome:?}");
        } else {
            live = Some(archive);
            live_values = values;
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn join_before_ai_map_reduce_preserves_keys_backpressure_and_replay() {
    exercise(Placement::Before).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ai_map_reduce_as_join_stream_preserves_contributions_and_filters_failures() {
    exercise(Placement::Stream).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ai_map_reduce_as_join_catalog_selects_latest_contribution_and_replays() {
    exercise(Placement::Catalog).await;
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

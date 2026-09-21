// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime and journal proofs for joins adjacent to the generated AI composite.
//! All inputs and chat responses are local fixtures; replay cannot resolve a client.

use async_trait::async_trait;
use obzenflow_adapters::ai::{ChatBindingEvidence, ChatCompletion, CHAT_CLIENT};
use obzenflow_adapters::middleware::control::ai_resilience;
use obzenflow_core::ai::{
    AiClientError, AiFinaliseRole, AiMapReduceFinaliseFailed, AiMapReducePlanningManifest,
    AiMapRole, AiRoleLogicFailure, ChatClient, ChatCompletionReply, ChatMessage, ChatParams,
    ChatRequest, ChatRequestSpec, ChatResponse, ChatTarget, ChunkInfo, HeuristicTokenEstimator,
    Many, ResolvedTokenEstimator, TokenCount, TokenEstimatorFallbackReason,
    TokenEstimatorResolutionInfo,
};
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::payloads::system_payload::SystemFeedRole;
use obzenflow_core::event::provenance::CompositeActivationContext;
use obzenflow_core::event::{ChainEvent, ChainPayload, JournalRecord, SystemEvent, SystemPayload};
use obzenflow_core::journal::{Journal, RunManifest};
use obzenflow_core::{EventId, JournalOwner, StageId, SystemId, TypedPayload, WriterId};
use obzenflow_dsl::dsl::backpressure_clause::enforced;
use obzenflow_dsl::{ai_map_reduce, flow, join, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_infra::verify::{verify_run_dirs, Verdict, VerifyOptions};
use obzenflow_runtime::effects::{
    EffectBinding, EffectRegistrationBuilder, LogicalEffectBindingName, ResolvedEffectPort,
};
use obzenflow_runtime::stages::common::handlers::{SourceError, TypedFiniteSourceHandler};
use obzenflow_runtime::stages::sink::SinkTyped;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
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
struct Counters {
    source_reads: AtomicUsize,
    resolutions: AtomicUsize,
    chat_calls: AtomicUsize,
}

#[derive(Clone, Debug)]
struct FixtureSource<T> {
    rows: VecDeque<T>,
    counters: Arc<Counters>,
}

impl<T: TypedPayload + Clone + Send + Sync + 'static> TypedFiniteSourceHandler
    for FixtureSource<T>
{
    type Output = T;

    fn next(&mut self) -> Result<Option<Vec<T>>, SourceError> {
        self.counters.source_reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.rows.pop_front().map(|row| vec![row]))
    }
}

struct DummyChat {
    target: ChatTarget,
    counters: Arc<Counters>,
    forbidden: bool,
}

#[async_trait]
impl ChatClient for DummyChat {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        self.counters.chat_calls.fetch_add(1, Ordering::SeqCst);
        assert!(!self.forbidden, "replay called the chat client");
        Ok(ChatResponse {
            text: "ok".into(),
            tool_calls: Vec::new(),
            usage: None,
            raw: None,
        })
    }
}

fn chat_binding(counters: Arc<Counters>, forbidden: bool) -> EffectBinding<ChatCompletion> {
    let target = ChatTarget::new("fixture", "deterministic");
    let estimator = ResolvedTokenEstimator::new(
        Arc::new(HeuristicTokenEstimator::default()),
        TokenEstimatorResolutionInfo::heuristic(
            "deterministic",
            TokenEstimatorFallbackReason::ExplicitHeuristic,
            None,
        ),
    );
    EffectRegistrationBuilder::<ChatCompletion>::new(
        LogicalEffectBindingName::new("chat").unwrap(),
        ChatBindingEvidence::new(target.clone(), estimator).unwrap(),
    )
    .bind_deferred_with_metadata(
        CHAT_CLIENT,
        Arc::new(move || {
            counters.resolutions.fetch_add(1, Ordering::SeqCst);
            assert!(!forbidden, "replay resolved the chat client");
            let client: Arc<dyn ChatClient> = Arc::new(DummyChat {
                target: target.clone(),
                counters: counters.clone(),
                forbidden,
            });
            Ok(ResolvedEffectPort::new(client, Arc::new(target.clone())))
        }),
    )
    .unwrap()
    .finish()
    .unwrap()
}

fn request(message: String) -> ChatRequestSpec {
    ChatRequestSpec {
        messages: vec![ChatMessage::user(message)],
        params: ChatParams::default(),
        tools: Vec::new(),
        response_format: None,
    }
}

struct SumMap;

impl AiMapRole<u64, Partial> for SumMap {
    fn prepare(
        &self,
        items: &[u64],
        _chunk: &ChunkInfo,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Ok(request(format!("sum {items:?}")))
    }

    fn interpret(
        &self,
        items: Vec<u64>,
        _chunk: ChunkInfo,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
    ) -> Result<Partial, AiRoleLogicFailure> {
        Ok(Partial {
            total: items.into_iter().sum(),
        })
    }
}

struct SumFinalise;

impl AiFinaliseRole<Seed, Many<Partial>, Row> for SumFinalise {
    fn prepare(
        &self,
        seed: &Seed,
        _collected: &Many<Partial>,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Ok(request(format!("finalise {}", seed.id)))
    }

    fn interpret(
        &self,
        seed: Seed,
        collected: Many<Partial>,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
    ) -> Result<Row, AiRoleLogicFailure> {
        // A different payload family in the same finalise journal must never
        // be decoded as Row by either side of a downstream join.
        if seed.id == 4 {
            return Err(AiRoleLogicFailure::Parse {
                message: "deliberate fixture failure".into(),
            });
        }
        Ok(Row {
            id: seed.id,
            key: seed.key,
            total: collected.items.into_iter().map(|p| p.total).sum(),
        })
    }
}

macro_rules! digest {
    ($chat:expr) => {{
        let chat = $chat;
        let map_role = SumMap;
        let finalise_role = SumFinalise;
        ai_map_reduce!(
            Seed -> Row => {
                map: [u64] -> Partial
                    uses at_least_once(ChatCompletion) via chat with ai_resilience() => map_role,
                reduce: (Seed, [Partial]) -> Row
                    uses at_least_once(ChatCompletion) via chat with ai_resilience() => finalise_role,
            },
            chunking: by_budget {
                items: |seed: &Seed| seed.values.clone(),
                render: |item: &u64, _ctx| item.to_string(),
                budget: TokenCount::new(100),
                max_items: Some(1),
                oversize: error,
            }
        )
    }};
}

#[derive(Clone, Copy, Debug)]
enum Placement {
    Before,
    Stream,
    Catalog,
}

impl Placement {
    fn seed_stage(self) -> &'static str {
        if matches!(self, Self::Before) {
            "joined"
        } else {
            "seeds"
        }
    }

    fn result_stage(self) -> &'static str {
        if matches!(self, Self::Before) {
            "digest__finalize"
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
                    id: 2,
                    key: 8,
                    total: 0,
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
                    id: 102,
                    key: 8,
                    total: 0,
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

fn build_flow(
    root: PathBuf,
    placement: Placement,
    counters: Arc<Counters>,
    delivered: Arc<Mutex<Vec<Row>>>,
    replay: bool,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let chat = chat_binding(counters.clone(), replay);
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
            counters: counters.clone(),
        };
        let scales = FixtureSource {
            rows: [
                Scale { key: 7, factor: 2 },
                Scale { key: 8, factor: 3 },
                Scale { key: 9, factor: 4 },
            ]
            .into(),
            counters: counters.clone(),
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
            counters,
        };
        let output = SinkTyped::new(move |row: Row| {
            let delivered = delivered.clone();
            async move {
                delivered.lock().unwrap().push(row);
            }
        })
        .idempotent();

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
                    backpressure: enforced(3).stall_timeout_ms(3_000),
                    stages: {
                        seeds = source!(Seed => seeds);
                        scales = source!(Scale => scales);
                        joined = join!(catalog scales: Scale, Seed -> Seed => joined);
                        digest = digest!(chat);
                        output = sink!(Row => output);
                    },
                    topology: {
                        (scales, seeds) |> joined;
                        joined |> digest;
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
                    backpressure: enforced(3).stall_timeout_ms(3_000),
                    stages: {
                        seeds = source!(Seed => seeds);
                        scales = source!(Scale => scales);
                        digest = digest!(chat);
                        joined = join!(catalog scales: Scale, Row -> Row => joined);
                        output = sink!(Row => output);
                    },
                    topology: {
                        seeds |> digest;
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
                    backpressure: enforced(3).stall_timeout_ms(3_000),
                    stages: {
                        seeds = source!(Seed => seeds);
                        queries = source!(Row => queries);
                        digest = digest!(chat);
                        joined = join!(catalog digest: Row, Row -> Row => joined);
                        output = sink!(Row => output);
                    },
                    topology: {
                        seeds |> digest;
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
    let journal = DiskJournal::<ChainEvent>::with_owner(
        run.join(&manifest.stages[stage].data_journal_file),
        JournalOwner::stage(StageId::new()),
    )
    .unwrap();
    let mut reader = journal.reader().await.unwrap();
    let mut rows = Vec::new();
    while let Some(row) = reader.next().await.unwrap() {
        rows.push(row);
    }
    rows
}

fn typed<T: TypedPayload>(
    records: &[JournalRecord<ChainPayload>],
) -> Vec<(T, &JournalRecord<ChainPayload>)> {
    records
        .iter()
        .filter_map(|row| T::from_event(&row.authored()).map(|value| (value, row)))
        .collect()
}

fn assert_activation(row: &JournalRecord<ChainPayload>, expected: &CompositeActivationContext) {
    assert_eq!(
        row.composite_activations(),
        std::slice::from_ref(expected),
        "exact job contribution, without unrelated catalog entries"
    );
}

async fn assert_journals(run: &Path, placement: Placement) -> Vec<Row> {
    let manifest = manifest(run);
    let seed_records = stage_records(run, placement.seed_stage()).await;
    let seeds = typed::<Seed>(&seed_records);
    assert_eq!(seeds.len(), 4);
    let activations: HashMap<_, _> = seeds
        .iter()
        .map(|(seed, row)| {
            (
                seed.id,
                CompositeActivationContext::new(
                    obzenflow_core::id::CompositeId::new("ai_map_reduce:digest"),
                    row.envelope.provenance.event.id,
                    "in",
                    row.envelope.provenance.event.processing.event_time,
                ),
            )
        })
        .collect();
    let by_event: HashMap<EventId, _> = seeds
        .iter()
        .map(|(seed, row)| (row.envelope.provenance.event.id, seed))
        .collect();

    let chunks = stage_records(run, "digest__chunk").await;
    let plans = typed::<AiMapReducePlanningManifest>(&chunks);
    assert_eq!(
        plans.len(),
        4,
        "one plan per joined or source seed, including the empty job"
    );
    let mut counts = BTreeMap::new();
    for (plan, record) in plans {
        let seed = by_event[&plan.job_key];
        assert_eq!(plan.seed_payload, serde_json::to_value(seed).unwrap());
        assert_eq!(plan.chunk_count, seed.values.len());
        assert_activation(record, &activations[&seed.id]);
        counts.insert(seed.id, plan.chunk_count);
    }
    assert_eq!(counts, BTreeMap::from([(1, 3), (2, 0), (3, 2), (4, 1)]));

    let finalise = stage_records(run, "digest__finalize").await;
    let reduced = typed::<Row>(&finalise);
    assert_eq!(reduced.len(), 3);
    for (row, record) in &reduced {
        assert_activation(record, &activations[&row.id]);
    }
    let failed = typed::<AiMapReduceFinaliseFailed>(&finalise);
    assert_eq!(failed.len(), 1, "the deliberate failure remains durable");
    assert_eq!(failed[0].0.job_key, activations[&4].activation);
    assert_activation(failed[0].1, &activations[&4]);

    let results = stage_records(run, placement.result_stage()).await;
    let output = typed::<Row>(&results);
    let values: Vec<_> = output.iter().map(|(value, _)| value.clone()).collect();
    assert_eq!(values, placement.expected());
    for (value, record) in &output {
        let selected = if matches!(placement, Placement::Catalog) {
            if value.key == 7 {
                3
            } else {
                2
            }
        } else {
            value.id
        };
        assert_activation(record, &activations[&selected]);
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
        if matches!(placement, Placement::Stream) {
            vec![3, 3]
        } else {
            vec![3, 4]
        }
    );

    // Check locally authored EOFs, rather than forwarded upstream EOF copies.
    for (stage, kind, count) in [
        (
            "joined",
            if matches!(placement, Placement::Before) {
                Seed::versioned_event_type()
            } else {
                Row::versioned_event_type()
            },
            if matches!(placement, Placement::Before) {
                4
            } else {
                3
            },
        ),
        ("digest__finalize", Row::versioned_event_type(), 3),
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
        assert_eq!(eofs[0].1[&obzenflow_core::EventType::from(kind)].0, count);
        if stage == "digest__finalize" {
            assert_eq!(
                eofs[0].1[&obzenflow_core::EventType::from(
                    AiMapReduceFinaliseFailed::versioned_event_type()
                )]
                    .0,
                1
            );
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

    let system = DiskJournal::<SystemEvent>::with_owner(
        run.join(&manifest.system_journal_file),
        JournalOwner::system(SystemId::new()),
    )
    .unwrap();
    let mut reader = system.reader().await.unwrap();
    let mut join_feeds = Vec::new();
    let join_id = manifest.stages["joined"]
        .stage_id
        .parse::<StageId>()
        .unwrap();
    while let Some(record) = reader.next().await.unwrap() {
        if let SystemPayload::ContractStatus {
            reader,
            upstream,
            selected_event_type,
            feed_role,
            pass,
            reader_seq,
            advertised_writer_seq,
            ..
        } = &record.payload
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
                Scale::versioned_event_type(),
                3,
                "seeds",
                Seed::versioned_event_type(),
                4,
            ),
            Placement::Stream => (
                "scales",
                Scale::versioned_event_type(),
                3,
                "digest__finalize",
                Row::versioned_event_type(),
                3,
            ),
            Placement::Catalog => (
                "digest__finalize",
                Row::versioned_event_type(),
                3,
                "queries",
                Row::versioned_event_type(),
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
                kind.into(),
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
        let counters = Arc::new(Counters::default());
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let mut args = vec![OsString::from("obzenflow")];
        if let Some(archive) = &live {
            args.push("--replay-from".into());
            args.push(OsString::from(archive));
        }
        tokio::time::timeout(
            Duration::from_secs(30),
            FlowApplication::builder()
                .with_cli_args(args)
                .run_async(build_flow(
                    root.clone(),
                    placement,
                    counters.clone(),
                    delivered.clone(),
                    replay,
                )),
        )
        .await
        .expect("composition must release enforced backpressure")
        .expect("composition run completes");
        assert_eq!(
            counters.resolutions.load(Ordering::SeqCst),
            usize::from(!replay)
        );
        assert_eq!(
            counters.chat_calls.load(Ordering::SeqCst),
            if replay { 0 } else { 10 },
            "six map calls and four finalise calls; replay performs neither"
        );
        assert_eq!(counters.source_reads.load(Ordering::SeqCst) == 0, replay);
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
async fn join_before_ai_map_reduce_preserves_jobs_backpressure_and_replay() {
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

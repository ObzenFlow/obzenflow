// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Map-reduce `ai!` stages through the ordinary flow builder: composite
//! subgraph membership reaches edge validation, the generated members run
//! with a fake chat provider, and joins resolve references through the
//! stage's boundary.

#[path = "support/fake_ai.rs"]
pub mod fake_ai;

use async_trait::async_trait;
use fake_ai::{build_context, build_context_with, chat_model, enforced_backpressure, FakeChatModels};
use obzenflow::ai::{
    self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec, Chunk, Keyed,
    Partials, TriggeredMapReduce, Unkeyed,
};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{ai, flow, join, sink, source, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, JoinReferenceView, SinkDescription, SinkWriteFailure, TypedFiniteSourceHandler,
    TypedJoinHandler,
};
use obzenflow_runtime::typing::SourceTyping;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BuildOnlyItem {
    value: u64,
}
impl TypedPayload for BuildOnlyItem {
    const EVENT_TYPE: &'static str = "regression.amr.item";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct BuildOnlyPartial {
    value: u64,
}
impl TypedPayload for BuildOnlyPartial {
    const EVENT_TYPE: &'static str = "regression.amr.partial";
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
struct BuildOnlyOut {
    total: u64,
}
impl TypedPayload for BuildOnlyOut {
    const EVENT_TYPE: &'static str = "regression.amr.out";
}

#[derive(Clone, Copy)]
struct SumMap;

impl AiRole<Chunk<BuildOnlyItem>> for SumMap {
    type Effect = ChatCompletion;
    type Out = BuildOnlyPartial;

    fn prepare(&self, chunk: &Chunk<BuildOnlyItem>) -> Result<ChatRequestSpec, HandlerError> {
        let values: Vec<String> = chunk.iter().map(|item| item.value.to_string()).collect();
        Ok(ChatRequestSpec::new("Sum the items.", values.join(" ")))
    }

    fn interpret(
        &self,
        chunk: Chunk<BuildOnlyItem>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<BuildOnlyPartial, HandlerError> {
        outcome.replied()?;
        Ok(BuildOnlyPartial {
            value: chunk.iter().map(|item| item.value).sum(),
        })
    }
}

#[derive(Clone, Copy)]
struct SumReduce;

impl AiRole<Partials<BuildOnlyItem, BuildOnlyPartial>> for SumReduce {
    type Effect = ChatCompletion;
    type Out = BuildOnlyOut;

    fn prepare(
        &self,
        input: &Partials<BuildOnlyItem, BuildOnlyPartial>,
    ) -> Result<ChatRequestSpec, HandlerError> {
        Ok(ChatRequestSpec::new(
            "Sum the partials.",
            format!("{} partials", input.partials().len()),
        ))
    }

    fn interpret(
        &self,
        input: Partials<BuildOnlyItem, BuildOnlyPartial>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<BuildOnlyOut, HandlerError> {
        outcome.replied()?;
        Ok(BuildOnlyOut {
            total: input.partials().iter().map(|partial| partial.value).sum(),
        })
    }
}

type Digest = TriggeredMapReduce<SumMap, SumReduce, BuildOnlyItem, Unkeyed>;

fn digest() -> Digest {
    ai::map(SumMap).chunks(1).reduce(SumReduce).on_eof()
}

fn models() -> FakeChatModels {
    FakeChatModels::replying(|_| "ok".to_string())
}

#[derive(Clone, Debug)]
struct NoEventSource;
impl SourceTyping for NoEventSource {
    type Output = BuildOnlyItem;
}
impl TypedFiniteSourceHandler for NoEventSource {
    type Output = BuildOnlyItem;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
struct ItemSource {
    next: u64,
    last: u64,
}

impl ItemSource {
    fn one_to(last: u64) -> Self {
        Self { next: 1, last }
    }
}

impl SourceTyping for ItemSource {
    type Output = BuildOnlyItem;
}

impl TypedFiniteSourceHandler for ItemSource {
    type Output = BuildOnlyItem;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next > self.last {
            return Ok(None);
        }
        let value = self.next;
        self.next += 1;
        Ok(Some(vec![BuildOnlyItem { value }]))
    }
}

#[derive(Debug)]
struct Collect<T> {
    rows: Arc<Mutex<Vec<T>>>,
}

impl<T> Clone for Collect<T> {
    fn clone(&self) -> Self {
        Self {
            rows: Arc::clone(&self.rows),
        }
    }
}

impl<T> Collect<T> {
    fn new() -> (Self, Arc<Mutex<Vec<T>>>) {
        let rows = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                rows: Arc::clone(&rows),
            },
            rows,
        )
    }
}

#[async_trait]
impl<T> InlineSink for Collect<T>
where
    T: TypedPayload + Send + Sync + std::fmt::Debug + 'static,
{
    type Input = T;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("Collect".to_string()))
    }

    async fn write(&mut self, row: T) -> Result<(), SinkWriteFailure> {
        self.rows.lock().expect("collected rows").push(row);
        Ok(())
    }
}

#[tokio::test]
async fn ordinary_flow_builder_accepts_ai_map_reduce_with_subgraph_attached() {
    // If edge validation ran without composite subgraph membership, the
    // members' mixed internal feeds would surface as a SingleEdge mismatch.
    let result = FlowDefinition::materialize(|_runtime_config| {
        let items = NoEventSource;
        let (out, _) = Collect::<BuildOnlyOut>::new();
        let digest = digest();

        Ok(flow! {
            name: "amr_build_only",
            journals: memory_journals(),
            stages: {
                items = source!(BuildOnlyItem => items);
                digest = ai!(BuildOnlyItem -> BuildOnlyOut => digest);
                out = sink!(BuildOnlyOut => out);
            },

            topology: {
                items |> digest;
                digest |> out;
            }
        })
    })
    .build(build_context(&models(), "deterministic"))
    .await;

    result.unwrap_or_else(|err| {
        panic!(
            "the ordinary flow builder must accept a map-reduce ai! stage under the \
             composite-internal-edge rule: {err:?}"
        )
    });
}

#[tokio::test]
async fn ai_map_reduce_runtime_commits_framework_internal_transport_events() {
    let metrics_model =
        std::sync::Arc::new(obzenflow_adapters::monitoring::MetricsReadModel::default());
    let fake = models();
    let context = build_context_with(
        &fake,
        std::iter::once(chat_model("deterministic")).chain(enforced_backpressure(3)),
    )
    .with_metrics_exporter(metrics_model.clone());
    let (out, rows) = Collect::<BuildOnlyOut>::new();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let items = ItemSource::one_to(5);
        let digest = digest();

        Ok(flow! {
            name: "amr_runtime_internal_contracts",
            journals: memory_journals(),
            stages: {
                items = source!(BuildOnlyItem => items);
                digest = ai!(BuildOnlyItem -> BuildOnlyOut => digest);
                out = sink!(BuildOnlyOut => out);
            },

            topology: {
                items |> digest;
                digest |> out;
            }
        })
    })
    .build(context)
    .await
    .expect("map-reduce ai! flow should build");

    handle
        .run()
        .await
        .expect("the generated members should commit their protocol facts");

    assert_eq!(
        *rows.lock().unwrap(),
        vec![BuildOnlyOut { total: 15 }],
        "one window reduces once at the natural end of input"
    );
    assert_eq!(fake.call_count(), 6, "five groups and one reduce");

    let rendered = obzenflow_adapters::monitoring::projections::PrometheusProjection::new()
        .render(&metrics_model.snapshot())
        .expect("terminal backpressure metrics should render");
    let in_flight: Vec<&str> = rendered
        .lines()
        .filter(|line| {
            line.starts_with("obzenflow_backpressure_in_flight{")
                && line.contains("flow=\"amr_runtime_internal_contracts\"")
        })
        .collect();
    assert!(!in_flight.is_empty(), "the run should export edge debt");
    for line in in_flight {
        let debt = line
            .split_whitespace()
            .last()
            .expect("metric value")
            .parse::<u64>()
            .expect("integer in-flight value");
        assert_eq!(debt, 0, "terminal physical debt must be zero: {line}");
    }
}

#[tokio::test]
async fn a_map_only_stage_emits_one_output_per_group() {
    let fake = models();
    let (out, rows) = Collect::<BuildOnlyPartial>::new();
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let items = ItemSource::one_to(5);
        let tag = ai::map(SumMap).chunks(2);

        Ok(flow! {
            name: "amr_map_only",
            journals: memory_journals(),
            stages: {
                items = source!(BuildOnlyItem => items);
                tag = ai!(BuildOnlyItem -> BuildOnlyPartial => tag);
                out = sink!(BuildOnlyPartial => out);
            },

            topology: {
                items |> tag;
                tag |> out;
            }
        })
    })
    .build(build_context(&fake, "deterministic"))
    .await
    .expect("a map-only ai! flow should build");
    handle.run().await.expect("a map-only ai! flow should run");

    assert_eq!(
        *rows.lock().unwrap(),
        vec![
            BuildOnlyPartial { value: 3 },
            BuildOnlyPartial { value: 7 },
            BuildOnlyPartial { value: 5 },
        ],
        "groups close at two items, and the end of input flushes the last"
    );
    assert_eq!(fake.call_count(), 3);
}

#[tokio::test]
async fn a_rolling_trigger_reduces_once_per_k_chunks_and_the_remainder_at_end_of_input() {
    let fake = models();
    let (out, rows) = Collect::<BuildOnlyOut>::new();
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let items = ItemSource::one_to(5);
        let digest = ai::map(SumMap).chunks(1).reduce(SumReduce).every_n_chunks(2);

        Ok(flow! {
            name: "amr_every_n_chunks",
            journals: memory_journals(),
            stages: {
                items = source!(BuildOnlyItem => items);
                digest = ai!(BuildOnlyItem -> BuildOnlyOut => digest);
                out = sink!(BuildOnlyOut => out);
            },

            topology: {
                items |> digest;
                digest |> out;
            }
        })
    })
    .build(build_context(&fake, "deterministic"))
    .await
    .expect("a rolling map-reduce ai! flow should build");
    handle.run().await.expect("a rolling map-reduce ai! flow should run");

    assert_eq!(
        *rows.lock().unwrap(),
        vec![
            BuildOnlyOut { total: 3 },
            BuildOnlyOut { total: 7 },
            BuildOnlyOut { total: 5 },
        ]
    );
    assert_eq!(fake.call_count(), 8, "five groups and three reduces");
}

type ByParity = TriggeredMapReduce<
    SumMap,
    SumReduce,
    BuildOnlyItem,
    Keyed<u64, fn(&BuildOnlyItem) -> u64>,
>;

fn parity(item: &BuildOnlyItem) -> u64 {
    item.value % 2
}

#[tokio::test]
async fn a_keyed_stage_never_mixes_keys_in_a_group_or_window() {
    let fake = models();
    let (out, rows) = Collect::<BuildOnlyOut>::new();
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let items = ItemSource::one_to(5);
        let by_parity: ByParity = ai::map(SumMap)
            .key_by(parity as fn(&BuildOnlyItem) -> u64)
            .chunks(2)
            .reduce(SumReduce)
            .on_eof();

        Ok(flow! {
            name: "amr_keyed",
            journals: memory_journals(),
            stages: {
                items = source!(BuildOnlyItem => items);
                by_parity = ai!(BuildOnlyItem -> BuildOnlyOut => by_parity);
                out = sink!(BuildOnlyOut => out);
            },

            topology: {
                items |> by_parity;
                by_parity |> out;
            }
        })
    })
    .build(build_context(&fake, "deterministic"))
    .await
    .expect("a keyed map-reduce ai! flow should build");
    handle.run().await.expect("a keyed map-reduce ai! flow should run");

    let mut totals: Vec<u64> = rows.lock().unwrap().iter().map(|row| row.total).collect();
    totals.sort_unstable();
    assert_eq!(totals, vec![6, 9], "even keys sum to 6 and odd keys to 9");
    assert_eq!(
        fake.call_count(),
        5,
        "odd items 1, 3 and 5 form two groups, even items 2 and 4 one, and each key reduces once"
    );
}

#[tokio::test]
async fn boundary_type_mismatch_diagnostic_names_composite_and_port() {
    let result = FlowDefinition::materialize(|_runtime_config| {
        let items = NoEventSource;
        let (wrong, _) = Collect::<BuildOnlyItem>::new();
        let digest = digest();

        Ok(flow! {
            name: "amr_boundary_mismatch",
            journals: memory_journals(),
            stages: {
                items = source!(BuildOnlyItem => items);
                digest = ai!(BuildOnlyItem -> BuildOnlyOut => digest);
                // No output port carries BuildOnlyItem, so the edge binds the
                // default `out` port and must fail edge typing there.
                wrong = sink!(BuildOnlyItem => wrong);
            },

            topology: {
                items |> digest;
                digest |> wrong;
            }
        })
    })
    .build(build_context(&models(), "deterministic"))
    .await;

    let err = match result {
        Ok(_) => panic!("expected an edge-typing failure at the composite boundary"),
        Err(err) => err,
    };
    let message = format!("{err:?}");
    assert!(
        message.contains("digest__reduce"),
        "error should name the member stage: {message}"
    );
    assert!(
        message.contains("composite 'digest' boundary port 'out'"),
        "error should name the composite and port: {message}"
    );
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JoinStreamP {
    key: u64,
}
impl TypedPayload for JoinStreamP {
    const EVENT_TYPE: &'static str = "regression.amr.join_stream";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JoinedP {
    key: u64,
}
impl TypedPayload for JoinedP {
    const EVENT_TYPE: &'static str = "regression.amr.joined";
}

#[derive(Clone, Debug)]
struct LocalNoopJoin;

impl TypedJoinHandler for LocalNoopJoin {
    type State = ();
    type ReferenceKey = ();
    type Reference = BuildOnlyOut;
    type Stream = JoinStreamP;
    type Output = JoinedP;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(&self, _reference: &Self::Reference) -> Result<(), HandlerError> {
        Ok(())
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, (), BuildOnlyOut>,
        _stream: JoinStreamP,
    ) -> std::result::Result<Vec<JoinedP>, HandlerError> {
        Ok(vec![])
    }
}

#[derive(Clone, Debug)]
struct NoStreamSource;
impl SourceTyping for NoStreamSource {
    type Output = JoinStreamP;
}
impl TypedFiniteSourceHandler for NoStreamSource {
    type Output = JoinStreamP;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }
}

/// A join reference naming the `ai!` stage resolves through its boundary by
/// the join's declared reference type.
#[tokio::test]
async fn join_reference_resolves_through_composite_boundary_port() {
    let result = FlowDefinition::materialize(|_runtime_config| {
        let items = NoEventSource;
        let stream_handler = NoStreamSource;
        let join_handler = LocalNoopJoin;
        let (joined, _) = Collect::<JoinedP>::new();
        let digest = digest();

        Ok(flow! {
            name: "amr_join_reference",
            journals: memory_journals(),
            stages: {
                items = source!(BuildOnlyItem => items);
                digest = ai!(BuildOnlyItem -> BuildOnlyOut => digest);
                stream_src = source!(JoinStreamP => stream_handler);
                enrich = join!(catalog digest: BuildOnlyOut, JoinStreamP -> JoinedP => join_handler);
                joined = sink!(JoinedP => joined);
            },

            topology: {
                items |> digest;
                (digest, stream_src) |> enrich;
                enrich |> joined;
            }
        })
    })
    .build(build_context(&models(), "deterministic"))
    .await;

    result.unwrap_or_else(|err| {
        panic!(
            "a join referencing the ai! stage must resolve through its typed output port: {err:?}"
        )
    });
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn ordinary_rust_bindings_remain_visible_to_test_flow() {
    let fake = models();
    let result = async {
        let items = NoEventSource;
        let (out, _) = Collect::<BuildOnlyOut>::new();
        let digest = digest();

        obzenflow_dsl::test_flow! {
            build_context: build_context(&fake, "deterministic"),
            name: "amr_test_flow_binding_hygiene",
            journals: memory_journals(),
            stages: {
                items = source!(BuildOnlyItem => items);
                digest = ai!(BuildOnlyItem -> BuildOnlyOut => digest);
                out = sink!(BuildOnlyOut => out);
            },

            topology: {
                items |> digest;
                digest |> out;
            }
        }
        .await
    }
    .await;

    let _harness = result.expect("ordinary Rust locals must remain visible to test_flow!");
}

#[tokio::test]
async fn built_flow_serializes_canonical_boundary_payload_types_exactly_once() {
    let handle = FlowDefinition::materialize(|_runtime_config| {
        let items = NoEventSource;
        let (out, _) = Collect::<BuildOnlyOut>::new();
        let digest = digest();

        Ok(flow! {
            name: "amr_boundary_payload_contract",
            journals: memory_journals(),
            stages: {
                items = source!(BuildOnlyItem => items);
                digest = ai!(BuildOnlyItem -> BuildOnlyOut => digest);
                out = sink!(BuildOnlyOut => out);
            },

            topology: {
                items |> digest;
                digest |> out;
            }
        })
    })
    .build(build_context(&models(), "deterministic"))
    .await
    .expect("typed map-reduce ai! flow should build");

    let topology = handle.topology().expect("built flow exposes topology");
    let digest = topology
        .subgraphs()
        .iter()
        .find(|subgraph| subgraph.subgraph_id == "ai_map_reduce:digest")
        .expect("digest subgraph");
    let input = digest
        .boundary_ports
        .iter()
        .find(|port| port.name == "in")
        .expect("input port");
    let output = digest
        .boundary_ports
        .iter()
        .find(|port| port.name == "out")
        .expect("output port");

    assert_eq!(
        input.payload_event_types,
        vec![BuildOnlyItem::event_type_name()]
    );
    assert_eq!(
        output.payload_event_types,
        vec![BuildOnlyOut::event_type_name()]
    );

    let serialized = serde_json::to_string(&*topology).expect("topology serializes");
    assert!(serialized.contains("regression.amr.item"));
    assert!(serialized.contains("regression.amr.out"));
    assert!(
        !serialized.contains(".v1.v1"),
        "boundary payload event types must already be canonical: {serialized}"
    );
}

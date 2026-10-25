// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "support/fake_ai.rs"]
pub mod fake_ai;

use async_trait::async_trait;
use fake_ai::{
    build_context, build_context_with, chat_model, enforced_backpressure, FakeChatModels,
};
use obzenflow::ai::{
    self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec, Chunk, Keyed,
    Partials, TriggeredMapReduce, Unkeyed,
};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{ai, flow, infinite_source, join, sink, source, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, JoinReferenceView, SinkDescription, SinkWriteFailure, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler, TypedJoinHandler,
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
    let result = FlowDefinition::materialize(|_| {
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

    let handle = FlowDefinition::materialize(move |_| {
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
    let handle = FlowDefinition::materialize(move |_| {
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
    let handle = FlowDefinition::materialize(move |_| {
        let items = ItemSource::one_to(5);
        let digest = ai::map(SumMap)
            .chunks(1)
            .reduce(SumReduce)
            .every_n_chunks(2);

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
    handle
        .run()
        .await
        .expect("a rolling map-reduce ai! flow should run");

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

type ByParity =
    TriggeredMapReduce<SumMap, SumReduce, BuildOnlyItem, Keyed<u64, fn(&BuildOnlyItem) -> u64>>;

fn parity(item: &BuildOnlyItem) -> u64 {
    item.value % 2
}

#[tokio::test]
async fn a_keyed_stage_never_mixes_keys_in_a_group_or_window() {
    let fake = models();
    let (out, rows) = Collect::<BuildOnlyOut>::new();
    let handle = FlowDefinition::materialize(move |_| {
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
    handle
        .run()
        .await
        .expect("a keyed map-reduce ai! flow should run");

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
    let result = FlowDefinition::materialize(|_| {
        let items = NoEventSource;
        let (wrong, _) = Collect::<BuildOnlyItem>::new();
        let digest = digest();

        Ok(flow! {
            name: "amr_boundary_mismatch",
            journals: memory_journals(),
            stages: {
                items = source!(BuildOnlyItem => items);
                digest = ai!(BuildOnlyItem -> BuildOnlyOut => digest);
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

#[tokio::test]
async fn join_reference_resolves_through_composite_boundary_port() {
    let result = FlowDefinition::materialize(|_| {
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
    let handle = FlowDefinition::materialize(|_| {
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

#[derive(Clone, Copy)]
struct TagOne;

impl AiRole<BuildOnlyItem> for TagOne {
    type Effect = ChatCompletion;
    type Out = BuildOnlyPartial;

    fn prepare(&self, item: &BuildOnlyItem) -> Result<ChatRequestSpec, HandlerError> {
        Ok(ChatRequestSpec::new(
            "Tag the item.",
            item.value.to_string(),
        ))
    }

    fn interpret(
        &self,
        item: BuildOnlyItem,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<BuildOnlyPartial, HandlerError> {
        outcome.replied()?;
        Ok(BuildOnlyPartial { value: item.value })
    }
}

#[tokio::test]
async fn infer_map_and_map_reduce_lower_to_one_two_and_four_members() {
    let handle = FlowDefinition::materialize(|_| {
        let items = NoEventSource;
        let brief = ai::infer(TagOne);
        let tag = ai::map(SumMap).chunks(2);
        let digest = digest();
        let (briefs, _) = Collect::<BuildOnlyPartial>::new();
        let (tags, _) = Collect::<BuildOnlyPartial>::new();
        let (digests, _) = Collect::<BuildOnlyOut>::new();

        Ok(flow! {
            name: "amr_shapes",
            journals: memory_journals(),
            stages: {
                items = source!(BuildOnlyItem => items);
                brief = ai!(BuildOnlyItem -> BuildOnlyPartial => brief);
                tag = ai!(BuildOnlyItem -> BuildOnlyPartial => tag);
                digest = ai!(BuildOnlyItem -> BuildOnlyOut => digest);
                briefs = sink!(BuildOnlyPartial => briefs);
                tags = sink!(BuildOnlyPartial => tags);
                digests = sink!(BuildOnlyOut => digests);
            },

            topology: {
                items |> brief;
                items |> tag;
                items |> digest;
                brief |> briefs;
                tag |> tags;
                digest |> digests;
            }
        })
    })
    .build(build_context(&models(), "deterministic"))
    .await
    .expect("every ai! shape builds");

    let topology = handle
        .topology()
        .expect("a built flow carries its topology");
    let names = topology
        .stages()
        .map(|stage| (stage.id, stage.name.clone()))
        .collect::<std::collections::HashMap<_, _>>();
    assert!(names.values().any(|name| name == "brief"));
    let members = |binding: &str| {
        let subgraph = topology
            .subgraphs()
            .iter()
            .find(|subgraph| subgraph.binding == binding)
            .unwrap_or_else(|| panic!("`{binding}` lowers to a composite"));
        let mut members = subgraph
            .member_stage_ids
            .iter()
            .map(|id| names[id].clone())
            .collect::<Vec<_>>();
        members.sort();
        members
    };
    assert!(
        topology
            .subgraphs()
            .iter()
            .all(|subgraph| subgraph.binding != "brief"),
        "an infer stage is its one generated member"
    );
    assert_eq!(members("tag"), ["tag__chunk", "tag__map"]);
    assert_eq!(
        members("digest"),
        [
            "digest__chunk",
            "digest__collect",
            "digest__map",
            "digest__reduce"
        ]
    );
}

#[tokio::test]
async fn a_zero_reduce_trigger_fails_the_build_naming_the_stage() {
    let result = FlowDefinition::materialize(|_| {
        let items = NoEventSource;
        let (out, _) = Collect::<BuildOnlyOut>::new();
        let digest = ai::map(SumMap)
            .chunks(1)
            .reduce(SumReduce)
            .every_n_chunks(0);

        Ok(flow! {
            name: "amr_zero_trigger",
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

    let error = match result {
        Ok(_) => panic!("a window that never reduces cannot build"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains("stage `digest`: `.every_n_chunks(0)` can never reduce; use 1 or more"),
        "{error}"
    );
}

#[derive(Clone, Copy)]
struct SumMapExceptThree;

impl AiRole<Chunk<BuildOnlyItem>> for SumMapExceptThree {
    type Effect = ChatCompletion;
    type Out = BuildOnlyPartial;

    fn prepare(&self, chunk: &Chunk<BuildOnlyItem>) -> Result<ChatRequestSpec, HandlerError> {
        if chunk.iter().any(|item| item.value == 3) {
            return Err(HandlerError::Validation(
                "item 3 cannot be summed".to_string(),
            ));
        }
        SumMap.prepare(chunk)
    }

    fn interpret(
        &self,
        chunk: Chunk<BuildOnlyItem>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<BuildOnlyPartial, HandlerError> {
        SumMap.interpret(chunk, outcome)
    }
}

#[tokio::test]
async fn a_failed_group_fails_only_its_rolling_window() {
    let fake = models();
    let (out, rows) = Collect::<BuildOnlyOut>::new();
    let handle = FlowDefinition::materialize(move |_| {
        let items = ItemSource::one_to(5);
        let digest = ai::map(SumMapExceptThree)
            .chunks(1)
            .reduce(SumReduce)
            .every_n_chunks(2);

        Ok(flow! {
            name: "amr_failed_window",
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
    handle
        .run()
        .await
        .expect("a failed group fails its window, not the stage");

    assert_eq!(
        *rows.lock().unwrap(),
        vec![BuildOnlyOut { total: 3 }, BuildOnlyOut { total: 5 }],
        "the window holding item 3 fails, and the windows before and after it reduce"
    );
    assert_eq!(
        fake.call_count(),
        6,
        "four groups call the model, and two windows reduce"
    );
}

#[tokio::test]
async fn a_build_without_a_chat_provider_fails_naming_the_stage() {
    let result = FlowDefinition::materialize(|_| {
        let items = NoEventSource;
        let (out, _) = Collect::<BuildOnlyOut>::new();
        let digest = digest();

        Ok(flow! {
            name: "amr_no_provider",
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
    .build(obzenflow_runtime::run_context::FlowBuildContext::new(
        fake_ai::runtime_config([chat_model("deterministic")], Default::default()),
    ))
    .await;

    let error = match result {
        Ok(_) => panic!("an ai! stage needs a chat model provider"),
        Err(error) => error.to_string(),
    };
    assert!(
        error.contains(
            "stage `digest`: this build has no chat model provider; enable the `ai` feature or install one with FlowBuildContext::with_ai_models"
        ),
        "{error}"
    );
}

#[derive(Clone, Debug)]
struct EndlessItems {
    next: u64,
    emit: u64,
}

impl TypedInfiniteSourceHandler for EndlessItems {
    type Output = BuildOnlyItem;

    fn next(&mut self) -> Result<Vec<BuildOnlyItem>, SourceError> {
        if self.next >= self.emit {
            std::thread::sleep(std::time::Duration::from_millis(10));
            return Ok(Vec::new());
        }
        self.next += 1;
        Ok(vec![BuildOnlyItem { value: self.next }])
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_infinite_source_reduces_once_per_k_chunks_without_an_end_of_input() {
    let fake = models();
    let (out, rows) = Collect::<BuildOnlyOut>::new();
    let handle = FlowDefinition::materialize(move |_| {
        let items = EndlessItems { next: 0, emit: 8 };
        let digest = ai::map(SumMap)
            .chunks(1)
            .reduce(SumReduce)
            .every_n_chunks(2);

        Ok(flow! {
            name: "amr_infinite_rolling",
            journals: memory_journals(),
            stages: {
                items = infinite_source!(BuildOnlyItem => items);
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
    .expect("a rolling map-reduce over an infinite source should build");
    handle.start().await.expect("the flow starts");
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while rows.lock().unwrap().len() < 4 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("four windows reduce while the source stays open");
    handle.stop().await.expect("the flow stops");
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        handle.wait_for_completion(),
    )
    .await
    .expect("the stopped flow terminates")
    .expect("a stopped flow ends cleanly");

    assert_eq!(
        *rows.lock().unwrap(),
        vec![
            BuildOnlyOut { total: 3 },
            BuildOnlyOut { total: 7 },
            BuildOnlyOut { total: 11 },
            BuildOnlyOut { total: 15 },
        ]
    );
    assert_eq!(fake.call_count(), 12, "eight groups and four reduces");
}

fn reduce_prompts(fake: &FakeChatModels) -> Vec<String> {
    fake.recorded
        .lock()
        .unwrap()
        .iter()
        .map(|call| fake_ai::last_user_message(&call.request))
        .filter(|prompt| prompt.ends_with("partials"))
        .collect()
}

#[tokio::test]
async fn twelve_items_in_groups_of_three_reduce_in_two_windows_of_two_groups() {
    let fake = models();
    let (out, rows) = Collect::<BuildOnlyOut>::new();
    let handle = FlowDefinition::materialize(move |_| {
        let items = ItemSource::one_to(12);
        let digest = ai::map(SumMap)
            .chunks(3)
            .reduce(SumReduce)
            .every_n_chunks(2);

        Ok(flow! {
            name: "amr_two_windows",
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
    handle
        .run()
        .await
        .expect("a rolling map-reduce ai! flow should run");

    assert_eq!(
        *rows.lock().unwrap(),
        vec![BuildOnlyOut { total: 21 }, BuildOnlyOut { total: 57 }]
    );
    assert_eq!(reduce_prompts(&fake), ["2 partials", "2 partials"]);
}

#[tokio::test]
async fn a_rolling_trigger_fires_per_key() {
    let fake = models();
    let (out, rows) = Collect::<BuildOnlyOut>::new();
    let handle = FlowDefinition::materialize(move |_| {
        let items = ItemSource::one_to(8);
        let by_parity = ai::map(SumMap)
            .key_by(parity as fn(&BuildOnlyItem) -> u64)
            .chunks(1)
            .reduce(SumReduce)
            .every_n_chunks(2);

        Ok(flow! {
            name: "amr_keyed_rolling",
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
    .expect("a keyed rolling map-reduce ai! flow should build");
    handle
        .run()
        .await
        .expect("a keyed rolling map-reduce ai! flow should run");

    let mut totals: Vec<u64> = rows.lock().unwrap().iter().map(|row| row.total).collect();
    totals.sort_unstable();
    assert_eq!(
        totals,
        vec![4, 6, 12, 14],
        "odd keys reduce 1+3 and 5+7, even keys 2+4 and 6+8"
    );
    assert_eq!(fake.call_count(), 12, "eight groups and four reduces");
}

#[tokio::test]
async fn a_failed_group_fails_only_its_keys_window() {
    let fake = models();
    let (out, rows) = Collect::<BuildOnlyOut>::new();
    let handle = FlowDefinition::materialize(move |_| {
        let items = ItemSource::one_to(8);
        let by_parity = ai::map(SumMapExceptThree)
            .key_by(parity as fn(&BuildOnlyItem) -> u64)
            .chunks(1)
            .reduce(SumReduce)
            .every_n_chunks(2);

        Ok(flow! {
            name: "amr_keyed_failed_window",
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
    .expect("a keyed rolling map-reduce ai! flow should build");
    handle
        .run()
        .await
        .expect("a failed group fails its window, not the stage");

    let mut totals: Vec<u64> = rows.lock().unwrap().iter().map(|row| row.total).collect();
    totals.sort_unstable();
    assert_eq!(
        totals,
        vec![6, 12, 14],
        "the odd key's window holding item 3 fails, and every other window reduces"
    );
    assert_eq!(fake.call_count(), 10, "seven groups and three reduces");
}

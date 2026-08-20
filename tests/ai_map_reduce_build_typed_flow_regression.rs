// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-114c regression test: the ordinary flow builder must attach composite
//! subgraph membership to the topology it passes to `validate_edge_typing`.
//!
//! Before the closing-PR fix, the build path assembled `topology_stages`
//! from `TopologyStageInfo::new(...)` without `.with_subgraph(...)`, and the
//! validator's composite-internal-edge skip rule could not fire. Any flow
//! using `ai_map_reduce!` would surface its mixed internal selected feeds as a
//! `SingleEdge` mismatch (the map consumes both chunks and manifests; the
//! collector consumes forwarded manifests plus tagged partials). This test wires a minimal
//! `ai_map_reduce!` composite end-to-end through `flow!`, awaits the build,
//! and asserts `Ok(_)`.
//!
//! The source and sink below are stubs for the build-only cases. Those tests
//! exercise DSL expansion, composite lowering, topology assembly, and edge
//! validation without driving the runtime.

use async_trait::async_trait;
use obzenflow_adapters::ai::{
    ChatBindingEvidence, ChatBindingEvidenceBuildError, ChatCompletion, CHAT_CLIENT,
};
use obzenflow_core::ai::{
    AiClientError, AiFinaliseRole, AiMapRole, AiRoleLogicFailure, ChatClient, ChatCompletionReply,
    ChatMessage, ChatParams, ChatRequest, ChatRequestSpec, ChatResponse, ChatTarget,
    HeuristicTokenEstimator, Many, ResolvedTokenEstimator, TokenCount,
    TokenEstimatorFallbackReason, TokenEstimatorResolutionInfo,
};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{ai_map_reduce, flow, join, sink, source, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::effects::{
    EffectBinding, EffectRegistrationBuilder, LogicalEffectBindingName, ResolvedEffectPort,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, JoinReferenceView, SinkDescription, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, TypedFiniteSourceHandler, TypedJoinHandler,
};
use obzenflow_runtime::typing::SourceTyping;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

// ── Typed payloads for each stage of the ai_map_reduce composite ──────────

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct BuildOnlySeed {
    n: u64,
}
impl TypedPayload for BuildOnlySeed {
    const EVENT_TYPE: &'static str = "regression.amr.seed";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BuildOnlyItem {
    value: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BuildOnlyPartial {
    value: u64,
}
impl TypedPayload for BuildOnlyPartial {
    const EVENT_TYPE: &'static str = "regression.amr.partial";
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct BuildOnlyOut {
    total: u64,
}
impl TypedPayload for BuildOnlyOut {
    const EVENT_TYPE: &'static str = "regression.amr.out";
}

fn test_target() -> ChatTarget {
    ChatTarget::new("test", "deterministic")
}

struct BuildMapRole;

impl AiMapRole<BuildOnlyItem, BuildOnlyPartial> for BuildMapRole {
    fn prepare(
        &self,
        items: &[BuildOnlyItem],
        _chunk: &obzenflow_core::ai::ChunkInfo,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Ok(ChatRequestSpec {
            messages: vec![ChatMessage::user(format!("{} items", items.len()))],
            params: ChatParams::default(),
            tools: Vec::new(),
            response_format: None,
        })
    }

    fn interpret(
        &self,
        items: Vec<BuildOnlyItem>,
        _chunk: obzenflow_core::ai::ChunkInfo,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
    ) -> Result<BuildOnlyPartial, AiRoleLogicFailure> {
        Ok(BuildOnlyPartial {
            value: items.into_iter().map(|item| item.value).sum(),
        })
    }
}

struct BuildFinaliseRole;

impl AiFinaliseRole<BuildOnlySeed, Many<BuildOnlyPartial>, BuildOnlyOut> for BuildFinaliseRole {
    fn prepare(
        &self,
        _seed: &BuildOnlySeed,
        collected: &Many<BuildOnlyPartial>,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Ok(ChatRequestSpec {
            messages: vec![ChatMessage::user(format!(
                "{} partials",
                collected.items.len()
            ))],
            params: ChatParams::default(),
            tools: Vec::new(),
            response_format: None,
        })
    }

    fn interpret(
        &self,
        _seed: BuildOnlySeed,
        collected: Many<BuildOnlyPartial>,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
    ) -> Result<BuildOnlyOut, AiRoleLogicFailure> {
        Ok(BuildOnlyOut {
            total: collected
                .items
                .into_iter()
                .map(|partial| partial.value)
                .sum(),
        })
    }
}

struct DeterministicChatClient {
    target: ChatTarget,
}

#[async_trait]
impl ChatClient for DeterministicChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        Ok(ChatResponse {
            text: "ok".to_string(),
            tool_calls: Vec::new(),
            usage: None,
            raw: None,
        })
    }
}

fn test_binding(estimator_model: &str) -> EffectBinding<ChatCompletion> {
    let client: Arc<dyn ChatClient> = Arc::new(DeterministicChatClient {
        target: test_target(),
    });
    let evidence = ChatBindingEvidence::new(test_target(), test_estimator(estimator_model))
        .expect("test chat target and estimator models agree");
    EffectRegistrationBuilder::<ChatCompletion>::new(
        LogicalEffectBindingName::new("chat").unwrap(),
        evidence,
    )
    .bind_eager_with_metadata(
        CHAT_CLIENT,
        ResolvedEffectPort::new(client.clone(), Arc::new(client.target().clone())),
    )
    .unwrap()
    .finish()
    .unwrap()
}

fn test_estimator(model: &str) -> ResolvedTokenEstimator {
    ResolvedTokenEstimator::new(
        Arc::new(HeuristicTokenEstimator::default()),
        TokenEstimatorResolutionInfo::heuristic(
            model,
            TokenEstimatorFallbackReason::ExplicitHeuristic,
            None,
        ),
    )
}

macro_rules! generated_digest {
    ($chat:expr) => {{
        let chat = $chat;
        let map_role = BuildMapRole;
        let finalise_role = BuildFinaliseRole;
        ai_map_reduce!(
            BuildOnlySeed -> BuildOnlyOut => {
                map: [BuildOnlyItem] -> BuildOnlyPartial
                    uses at_least_once(ChatCompletion)
                        via chat
                        with obzenflow_adapters::middleware::control::ai_resilience()
                    => map_role,
                reduce: (BuildOnlySeed, [BuildOnlyPartial]) -> BuildOnlyOut
                    uses at_least_once(ChatCompletion)
                        via chat
                        with obzenflow_adapters::middleware::control::ai_resilience()
                    => finalise_role,
            },
            chunking: by_budget {
                items: |seed: &BuildOnlySeed| {
                    (1..=seed.n)
                        .map(|value| BuildOnlyItem { value })
                        .collect::<Vec<_>>()
                },
                render: |item: &BuildOnlyItem, _ctx| item.value.to_string(),
                budget: TokenCount::new(100),
                max_items: Some(1),
                oversize: error,
            }
        )
    }};
}

// ── Stub handlers (build-only; never invoked under this test's run path) ──

#[derive(Clone, Debug)]
struct NoEventSource;
impl SourceTyping for NoEventSource {
    type Output = BuildOnlySeed;
}
impl TypedFiniteSourceHandler for NoEventSource {
    type Output = BuildOnlySeed;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        Ok(None)
    }
}

#[derive(Debug)]
struct NoopSink<T>(std::marker::PhantomData<fn() -> T>);

impl<T> Clone for NoopSink<T> {
    fn clone(&self) -> Self {
        Self(std::marker::PhantomData)
    }
}

impl<T> NoopSink<T> {
    fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

#[async_trait]
impl<T> InlineSink for NoopSink<T>
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
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        )))
    }
}

#[derive(Clone, Debug)]
struct CountingOutSink {
    delivered: Arc<AtomicUsize>,
    total: Arc<AtomicU64>,
}

impl CountingOutSink {
    fn new(delivered: Arc<AtomicUsize>, total: Arc<AtomicU64>) -> Self {
        Self { delivered, total }
    }
}

#[async_trait]
impl InlineSink for CountingOutSink {
    type Input = BuildOnlyOut;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        out: BuildOnlyOut,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        self.delivered.fetch_add(1, Ordering::SeqCst);
        self.total.store(out.total, Ordering::SeqCst);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("CountingOut".to_string()),
            None,
        )))
    }
}

#[derive(Clone, Debug)]
struct OneSeedSource {
    emitted: bool,
}

impl OneSeedSource {
    fn new() -> Self {
        Self { emitted: false }
    }
}

impl SourceTyping for OneSeedSource {
    type Output = BuildOnlySeed;
}

impl TypedFiniteSourceHandler for OneSeedSource {
    type Output = BuildOnlySeed;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![BuildOnlySeed { n: 5 }]))
    }
}

#[tokio::test]
async fn ordinary_flow_builder_accepts_ai_map_reduce_with_subgraph_attached() {
    // The build is the assertion. If `validate_edge_typing` ran against a
    // topology without composite subgraph membership, the mixed selected
    // internal feeds would surface as
    // `FlowBuildError::EdgeTypingMismatch { kind: SingleEdge, .. }` and this
    // `.await` would resolve to `Err`. With the FLOWIP-114c closing-PR fix in
    // the ordinary builder, subgraph membership is attached before the
    // validator runs and the three composite-internal edges are recognized.
    let result = FlowDefinition::materialize(|_runtime_config| {
        let seed_handler = NoEventSource;
        let sink_handler = NoopSink::<BuildOnlyOut>::new();
        let chat = test_binding("deterministic");

        Ok(flow! {
            name: "amr_build_only",
            journals: memory_journals(),
            stages: {
                seed = source!(BuildOnlySeed => seed_handler);
                digest = generated_digest!(chat);
                sink_stage = sink!(BuildOnlyOut => sink_handler);
            },

            topology: {
                seed |> digest;
                digest |> sink_stage;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    let _handle = result.unwrap_or_else(|err| {
        panic!(
            "FLOWIP-114c regression: the ordinary flow builder rejected an ai_map_reduce flow that \
             should validate cleanly under the composite-internal-edge skip rule. If you see \
             a SingleEdge mismatch on `digest__chunk -> digest__collect`, the subgraph \
             membership map is being applied AFTER `validate_edge_typing` instead of before. \
             See `obzenflow_dsl/src/dsl/flow_builder.rs` around the topology_stages assembly. Error: \
             {err:?}"
        )
    });
}

#[tokio::test]
async fn materializer_scope_remains_visible_to_ai_effect_bindings() {
    let result = FlowDefinition::materialize(|_runtime_config| {
        let chat = test_binding("deterministic");
        let bound_map_role = BuildMapRole;
        let bound_finalise_role = BuildFinaliseRole;
        let seed_handler = NoEventSource;
        let sink_handler = NoopSink::<BuildOnlyOut>::new();

        Ok(flow! {
            name: "amr_flow_materializer_hygiene",
            journals: memory_journals(),
            stages: {
                seed = source!(BuildOnlySeed => seed_handler);
                digest = ai_map_reduce!(
                    BuildOnlySeed -> BuildOnlyOut => {
                        map: [BuildOnlyItem] -> BuildOnlyPartial
                            uses at_least_once(ChatCompletion)
                                via chat
                                with obzenflow_adapters::middleware::control::ai_resilience()
                            => bound_map_role,
                        reduce: (BuildOnlySeed, [BuildOnlyPartial]) -> BuildOnlyOut
                            uses at_least_once(ChatCompletion)
                                via chat
                                with obzenflow_adapters::middleware::control::ai_resilience()
                            => bound_finalise_role,
                    },
                    chunking: by_budget {
                        items: |seed: &BuildOnlySeed| {
                            (1..=seed.n)
                                .map(|value| BuildOnlyItem { value })
                                .collect::<Vec<_>>()
                        },
                        render: |item: &BuildOnlyItem, _ctx| item.value.to_string(),
                        budget: TokenCount::new(100),
                        max_items: Some(1),
                        oversize: error,
                    }
                );
                sink_stage = sink!(BuildOnlyOut => sink_handler);
            },

            topology: {
                seed |> digest;
                digest |> sink_stage;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    let _handle = result.expect("materializer locals must remain visible to the inner flow");
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn ordinary_rust_bindings_remain_visible_to_test_flow() {
    let result = async {
        let chat = test_binding("deterministic");
        let bound_map_role = BuildMapRole;
        let bound_finalise_role = BuildFinaliseRole;
        let seed_handler = NoEventSource;
        let sink_handler = NoopSink::<BuildOnlyOut>::new();

        obzenflow_dsl::test_flow! {
            name: "amr_test_flow_binding_hygiene",
            journals: memory_journals(),
            stages: {
                seed = source!(BuildOnlySeed => seed_handler);
                digest = ai_map_reduce!(
                    BuildOnlySeed -> BuildOnlyOut => {
                        map: [BuildOnlyItem] -> BuildOnlyPartial
                            uses at_least_once(ChatCompletion)
                                via chat
                                with obzenflow_adapters::middleware::control::ai_resilience()
                            => bound_map_role,
                        reduce: (BuildOnlySeed, [BuildOnlyPartial]) -> BuildOnlyOut
                            uses at_least_once(ChatCompletion)
                                via chat
                                with obzenflow_adapters::middleware::control::ai_resilience()
                            => bound_finalise_role,
                    },
                    chunking: by_budget {
                        items: |seed: &BuildOnlySeed| {
                            (1..=seed.n)
                                .map(|value| BuildOnlyItem { value })
                                .collect::<Vec<_>>()
                        },
                        render: |item: &BuildOnlyItem, _ctx| item.value.to_string(),
                        budget: TokenCount::new(100),
                        max_items: Some(1),
                        oversize: error,
                    }
                );
                sink_stage = sink!(BuildOnlyOut => sink_handler);
            },

            topology: {
                seed |> digest;
                digest |> sink_stage;
            }
        }
        .await
    }
    .await;

    let _harness = result.expect("ordinary Rust locals must remain visible to test_flow!");
}

#[test]
fn estimator_mismatch_fails_before_journal_or_effect_port_evaluation() {
    let journals_evaluated = Arc::new(AtomicBool::new(false));
    let ports_evaluated = Arc::new(AtomicBool::new(false));
    let journal_probe = Arc::clone(&journals_evaluated);
    let port_probe = Arc::clone(&ports_evaluated);

    let result = (|| {
        let evidence = ChatBindingEvidence::new(test_target(), test_estimator("different-model"))?;
        journal_probe.store(true, Ordering::SeqCst);
        port_probe.store(true, Ordering::SeqCst);
        Ok::<_, ChatBindingEvidenceBuildError>(evidence)
    })();
    let error = result.expect_err("an estimator for a different model must fail construction");

    assert!(matches!(
        error,
        ChatBindingEvidenceBuildError::EstimatorModelMismatch
    ));
    assert!(
        !journals_evaluated.load(Ordering::SeqCst),
        "journal construction must remain outside a rejected chat contract"
    );
    assert!(
        !ports_evaluated.load(Ordering::SeqCst),
        "effect-port registration must remain outside a rejected chat contract"
    );
}

#[tokio::test]
async fn built_flow_serializes_canonical_boundary_payload_types_exactly_once() {
    let handle = FlowDefinition::materialize(|_runtime_config| {
        let seed_handler = NoEventSource;
        let sink_handler = NoopSink::<BuildOnlyOut>::new();
        let chat = test_binding("deterministic");

        Ok(flow! {
            name: "amr_boundary_payload_contract",
            journals: memory_journals(),
            stages: {
                seed = source!(BuildOnlySeed => seed_handler);
                digest = generated_digest!(chat);
                sink_stage = sink!(BuildOnlyOut => sink_handler);
            },

            topology: {
                seed |> digest;
                digest |> sink_stage;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .expect("typed ai_map_reduce flow should build");

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
        vec![BuildOnlySeed::versioned_event_type()]
    );
    assert_eq!(
        output.payload_event_types,
        vec![BuildOnlyOut::versioned_event_type()]
    );

    let serialized = serde_json::to_string(&*topology).expect("topology serializes");
    assert!(serialized.contains("regression.amr.seed.v1"));
    assert!(serialized.contains("regression.amr.out.v1"));
    assert!(
        !serialized.contains(".v1.v1"),
        "boundary payload event types must already be canonical: {serialized}"
    );
}

#[tokio::test]
async fn ai_map_reduce_runtime_commits_framework_internal_transport_events() {
    let delivered = Arc::new(AtomicUsize::new(0));
    let delivered_for_flow = delivered.clone();
    let total = Arc::new(AtomicU64::new(0));
    let total_for_flow = total.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let seed_handler = OneSeedSource::new();
        let sink_handler = CountingOutSink::new(delivered_for_flow, total_for_flow);
        let chat = test_binding("deterministic");

        Ok(flow! {
            name: "amr_runtime_internal_contracts",
            journals: memory_journals(),
            backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(3)
                .stall_timeout_ms(3_000),
            stages: {
                seed = source!(BuildOnlySeed => seed_handler);
                digest = generated_digest!(chat);
                sink_stage = sink!(BuildOnlyOut => sink_handler);
            },

            topology: {
                seed |> digest;
                digest |> sink_stage;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .expect("ai_map_reduce runtime flow should build");

    let metrics = handle
        .run_with_metrics()
        .await
        .expect("ai_map_reduce should commit planning manifests and tagged partials")
        .expect("test flow should expose its terminal metrics snapshot");

    assert_eq!(
        delivered.load(Ordering::SeqCst),
        1,
        "ai_map_reduce should deliver one reduced output to the downstream sink"
    );
    assert_eq!(
        total.load(Ordering::SeqCst),
        15,
        "collector should route tagged partials and finalise their sum"
    );

    let rendered = metrics
        .render_metrics()
        .expect("terminal backpressure metrics should render");
    let duration_count = rendered
        .lines()
        .find_map(|line| {
            if line.starts_with("obzenflow_composite_boundary_duration_seconds_count{")
                && line.contains("composite=\"ai_map_reduce:digest\"")
                && line.contains("entry_port=\"in\"")
                && line.contains("exit_port=\"out\"")
            {
                line.split_whitespace().last()?.parse::<u64>().ok()
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            panic!(
                "runtime resource wiring must stamp the digest input activation and project its final output:\n{rendered}"
            )
        });
    assert_eq!(
        duration_count, 1,
        "one admitted seed and one final output form one paired boundary duration"
    );
    assert!(
        !rendered.lines().any(|line| {
            line.starts_with("obzenflow_composite_boundary_duration_invalid_total{")
                && line.contains("composite=\"ai_map_reduce:digest\"")
        }),
        "the canonical ai_map_reduce boundary must not produce rejected duration evidence"
    );
    let in_flight: Vec<&str> = rendered
        .lines()
        .filter(|line| {
            line.starts_with("obzenflow_backpressure_in_flight{")
                && line.contains("flow=\"amr_runtime_internal_contracts\"")
        })
        .collect();
    assert!(
        !in_flight.is_empty(),
        "window-2 run should export physical edge debt"
    );
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

/// FLOWIP-128a D1 diagnostics: a downstream whose type no boundary port
/// carries binds the default port, and the resulting edge-typing error names
/// the composite and the port rather than only the mangled member stage.
#[tokio::test]
async fn boundary_type_mismatch_diagnostic_names_composite_and_port() {
    let result = FlowDefinition::materialize(|_runtime_config| {
        let seed_handler = NoEventSource;
        let sink_handler = NoopSink::<BuildOnlySeed>::new();
        let chat = test_binding("deterministic");

        Ok(flow! {
            name: "amr_boundary_mismatch",
            journals: memory_journals(),
            stages: {
                seed = source!(BuildOnlySeed => seed_handler);
                digest = generated_digest!(chat);
                // Wrong type: no output port carries BuildOnlySeed, so the edge
                // binds the default `out` port and must fail edge typing there.
                sink_stage = sink!(BuildOnlySeed => sink_handler);
            },

            topology: {
                seed |> digest;
                digest |> sink_stage;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    let err = match result {
        Ok(_) => panic!("expected an edge-typing failure at the composite boundary"),
        Err(err) => err,
    };
    let message = format!("{err:?}");
    assert!(
        message.contains("digest__finalize"),
        "error should name the member stage: {message}"
    );
    assert!(
        message.contains("composite 'digest' boundary port 'out'"),
        "error should name the composite and port (FLOWIP-128a D1): {message}"
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

/// FLOWIP-128a A4: a join reference variable naming a composite resolves
/// through the boundary by the join's declared reference type, with
/// default-port fallback. Before the fix this build failed with
/// "references unknown stage variable 'digest'".
#[tokio::test]
async fn join_reference_resolves_through_composite_boundary_port() {
    let result = FlowDefinition::materialize(|_runtime_config| {
        let seed_handler = NoEventSource;
        let stream_handler = NoStreamSource;
        let join_handler = LocalNoopJoin;
        let sink_handler = NoopSink::<JoinedP>::new();
        let chat = test_binding("deterministic");

        Ok(flow! {
            name: "amr_join_reference",
            journals: memory_journals(),
            stages: {
                seed = source!(BuildOnlySeed => seed_handler);
                digest = generated_digest!(chat);
                stream_src = source!(JoinStreamP => stream_handler);
                enrich = join!(catalog digest: BuildOnlyOut, JoinStreamP -> JoinedP => join_handler);
                joined_sink = sink!(JoinedP => sink_handler);
            },

            topology: {
                seed |> digest;
                stream_src |> enrich;
                enrich |> joined_sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    result.unwrap_or_else(|err| {
        panic!(
            "FLOWIP-128a A4: a join referencing the composite binding must resolve \
             through the boundary's typed output port. Error: {err:?}"
        )
    });
}

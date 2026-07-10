// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-114c regression test: `build_typed_flow!` must attach composite
//! subgraph membership to the topology it passes to `validate_edge_typing`.
//!
//! Before the closing-PR fix, `build_typed_flow!` built `topology_stages`
//! from `TopologyStageInfo::new(...)` without `.with_subgraph(...)`, and the
//! validator's composite-internal-edge skip rule could not fire. Any flow
//! using `ai_map_reduce!` would surface its mixed internal selected feeds as a
//! `SingleEdge` mismatch (the map consumes both chunks and manifests; the
//! collector consumes forwarded manifests plus tagged partials). This test wires a minimal
//! `ai_map_reduce!` composite end-to-end through `flow!`, awaits the build,
//! and asserts `Ok(_)`.
//!
//! The handlers below are stubs: the source emits no events, the transforms
//! return empty vectors, the sink is a no-op. The test only exercises the
//! build path (DSL expansion + composite lowering + topology assembly +
//! validate_edge_typing); it does not drive the runtime.

use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventContent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_dsl::{ai_map_reduce, flow, join, sink, source};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncTransformHandler, FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::stateful::CollectByInput;
use obzenflow_runtime::typing::{SourceTyping, TransformTyping};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
struct BuildOnlyChunk {
    chunk_index: usize,
    chunk_count: usize,
    value: u64,
}
impl TypedPayload for BuildOnlyChunk {
    const EVENT_TYPE: &'static str = "regression.amr.chunk";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct BuildOnlyPartial {
    value: u64,
}
impl TypedPayload for BuildOnlyPartial {
    const EVENT_TYPE: &'static str = "regression.amr.partial";
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct BuildOnlyCollected {
    values: Vec<u64>,
}
impl TypedPayload for BuildOnlyCollected {
    const EVENT_TYPE: &'static str = "regression.amr.collected";
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct BuildOnlyOut {
    total: u64,
}
impl TypedPayload for BuildOnlyOut {
    const EVENT_TYPE: &'static str = "regression.amr.out";
}

// ── Stub handlers (build-only; never invoked under this test's run path) ──

#[derive(Clone, Debug)]
struct NoEventSource;
impl SourceTyping for NoEventSource {
    type Output = BuildOnlySeed;
}
impl FiniteSourceHandler for NoEventSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
struct NoopChunker;
impl TransformTyping for NoopChunker {
    type Input = BuildOnlySeed;
    type Output = BuildOnlyChunk;
}
#[async_trait]
impl TransformHandler for NoopChunker {
    fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct NoopMap;
#[async_trait]
impl AsyncTransformHandler for NoopMap {
    async fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct NoopFinalize;
#[async_trait]
impl AsyncTransformHandler for NoopFinalize {
    async fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct NoopSink;
#[async_trait]
impl SinkHandler for NoopSink {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        ))
    }
}

#[derive(Clone, Debug)]
struct CountingOutSink {
    delivered: Arc<AtomicUsize>,
    total: Arc<AtomicU64>,
}

impl CountingOutSink {
    fn new() -> (Self, Arc<AtomicUsize>, Arc<AtomicU64>) {
        let delivered = Arc::new(AtomicUsize::new(0));
        let total = Arc::new(AtomicU64::new(0));
        (
            Self {
                delivered: Arc::clone(&delivered),
                total: Arc::clone(&total),
            },
            delivered,
            total,
        )
    }
}

#[async_trait]
impl SinkHandler for CountingOutSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if let ChainEventContent::Data {
            event_type,
            payload,
        } = &event.content
        {
            if BuildOnlyOut::event_type_matches(event_type) {
                let out: BuildOnlyOut = serde_json::from_value(payload.clone()).map_err(|err| {
                    HandlerError::Deserialization(format!(
                        "counting sink output decode failed: {err}"
                    ))
                })?;
                self.delivered.fetch_add(1, Ordering::SeqCst);
                self.total.store(out.total, Ordering::SeqCst);
            }
        }

        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("CountingOut".to_string()),
            None,
        ))
    }
}

#[derive(Clone, Debug)]
struct OneSeedSource {
    emitted: bool,
    writer_id: WriterId,
}

impl OneSeedSource {
    fn new() -> Self {
        Self {
            emitted: false,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl SourceTyping for OneSeedSource {
    type Output = BuildOnlySeed;
}

impl FiniteSourceHandler for OneSeedSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            BuildOnlySeed::EVENT_TYPE,
            json!(BuildOnlySeed { n: 2 }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct RuntimeChunker;

impl TransformTyping for RuntimeChunker {
    type Input = BuildOnlySeed;
    type Output = BuildOnlyChunk;
}

#[async_trait]
impl TransformHandler for RuntimeChunker {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let chunk_count = 5;
        let mut out = Vec::new();
        for chunk_index in 0..chunk_count {
            out.push(ChainEventFactory::derived_data_event(
                event.writer_id,
                &event,
                BuildOnlyChunk::EVENT_TYPE,
                json!(BuildOnlyChunk {
                    chunk_index,
                    chunk_count,
                    value: (chunk_index as u64) + 1,
                }),
                obzenflow_core::config::LineagePolicy::default(),
            ));
        }
        Ok(out)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct RuntimeMap;

#[async_trait]
impl AsyncTransformHandler for RuntimeMap {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let ChainEventContent::Data { payload, .. } = &event.content else {
            return Ok(Vec::new());
        };
        let chunk: BuildOnlyChunk = serde_json::from_value(payload.clone()).map_err(|err| {
            HandlerError::Deserialization(format!("runtime map chunk decode failed: {err}"))
        })?;
        Ok(vec![ChainEventFactory::derived_data_event(
            event.writer_id,
            &event,
            BuildOnlyPartial::EVENT_TYPE,
            json!(BuildOnlyPartial { value: chunk.value }),
            obzenflow_core::config::LineagePolicy::default(),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct RuntimeFinalize;

#[async_trait]
impl AsyncTransformHandler for RuntimeFinalize {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let ChainEventContent::Data { payload, .. } = &event.content else {
            return Ok(Vec::new());
        };
        let collected: BuildOnlyCollected =
            serde_json::from_value(payload.clone()).map_err(|err| {
                HandlerError::Deserialization(format!("runtime finalize decode failed: {err}"))
            })?;
        let total = collected.values.iter().copied().sum();
        Ok(vec![ChainEventFactory::derived_data_event(
            event.writer_id,
            &event,
            BuildOnlyOut::EVENT_TYPE,
            json!(BuildOnlyOut { total }),
            obzenflow_core::config::LineagePolicy::default(),
        )])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[tokio::test]
async fn build_typed_flow_accepts_ai_map_reduce_with_subgraph_attached() {
    // The build is the assertion. If `validate_edge_typing` ran against a
    // topology without composite subgraph membership, the mixed selected
    // internal feeds would surface as
    // `FlowBuildError::EdgeTypingMismatch { kind: SingleEdge, .. }` and this
    // `.await` would resolve to `Err`. With the FLOWIP-114c closing-PR fix in
    // `build_typed_flow!`, subgraph membership is attached before the
    // validator runs and the three composite-internal edges are recognized.
    let result = flow! {
        name: "amr_build_only",
        journals: memory_journals(),
        middleware: [],

        stages: {
            seed = source!(BuildOnlySeed => NoEventSource);
            digest = ai_map_reduce!(
                chunk: BuildOnlySeed -> BuildOnlyChunk => NoopChunker,
                map: BuildOnlyChunk -> BuildOnlyPartial => NoopMap,
                collect: BuildOnlyPartial -> BuildOnlyCollected => CollectByInput::new(
                    BuildOnlyCollected::default(),
                    |acc: &mut BuildOnlyCollected, partial: &BuildOnlyPartial| {
                        acc.values.push(partial.value);
                    },
                ),
                reduce: BuildOnlyCollected -> BuildOnlyOut => NoopFinalize,
            );
            sink_stage = sink!(BuildOnlyOut => NoopSink);
        },

        topology: {
            seed |> digest;
            digest |> sink_stage;
        }
    }
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    let _handle = result.unwrap_or_else(|err| {
        panic!(
            "FLOWIP-114c regression: `build_typed_flow!` rejected an ai_map_reduce flow that \
             should validate cleanly under the composite-internal-edge skip rule. If you see \
             a SingleEdge mismatch on `digest__chunk -> digest__collect`, the subgraph \
             membership map is being applied AFTER `validate_edge_typing` instead of before. \
             See `obzenflow_dsl/src/dsl/dsl.rs` around the topology_stages assembly. Error: \
             {err:?}"
        )
    });
}

#[tokio::test]
async fn ai_map_reduce_runtime_commits_framework_internal_transport_events() {
    let (sink_handler, delivered, total) = CountingOutSink::new();

    let handle = flow! {
        name: "amr_runtime_internal_contracts",
        journals: memory_journals(),
        middleware: [],
        backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(2)
            .stall_timeout_ms(3_000),

        stages: {
            seed = source!(BuildOnlySeed => OneSeedSource::new());
            digest = ai_map_reduce!(
                chunk: BuildOnlySeed -> BuildOnlyChunk => RuntimeChunker,
                map: BuildOnlyChunk -> BuildOnlyPartial => RuntimeMap,
                collect: BuildOnlyPartial -> BuildOnlyCollected => CollectByInput::new(
                    BuildOnlyCollected::default(),
                    |acc: &mut BuildOnlyCollected, partial: &BuildOnlyPartial| {
                        acc.values.push(partial.value);
                    },
                ),
                reduce: BuildOnlyCollected -> BuildOnlyOut => RuntimeFinalize,
            );
            sink_stage = sink!(BuildOnlyOut => sink_handler);
        },

        topology: {
            seed |> digest;
            digest |> sink_stage;
        }
    }
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
    let result = flow! {
        name: "amr_boundary_mismatch",
        journals: memory_journals(),
        middleware: [],

        stages: {
            seed = source!(BuildOnlySeed => NoEventSource);
            digest = ai_map_reduce!(
                chunk: BuildOnlySeed -> BuildOnlyChunk => NoopChunker,
                map: BuildOnlyChunk -> BuildOnlyPartial => NoopMap,
                collect: BuildOnlyPartial -> BuildOnlyCollected => CollectByInput::new(
                    BuildOnlyCollected::default(),
                    |acc: &mut BuildOnlyCollected, partial: &BuildOnlyPartial| {
                        acc.values.push(partial.value);
                    },
                ),
                reduce: BuildOnlyCollected -> BuildOnlyOut => NoopFinalize,
            );
            // Wrong type: no output port carries BuildOnlySeed, so the edge
            // binds the default `out` port and must fail edge typing there.
            sink_stage = sink!(BuildOnlySeed => NoopSink);
        },

        topology: {
            seed |> digest;
            digest |> sink_stage;
        }
    }
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

#[async_trait]
impl obzenflow_runtime::stages::common::handlers::JoinHandler for LocalNoopJoin {
    type State = ();

    fn initial_state(&self) -> Self::State {}

    fn process_event(
        &self,
        _state: &mut Self::State,
        _event: ChainEvent,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![])
    }

    fn on_source_eof(
        &self,
        _state: &mut Self::State,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![])
    }
}

#[derive(Clone, Debug)]
struct NoStreamSource;
impl SourceTyping for NoStreamSource {
    type Output = JoinStreamP;
}
impl FiniteSourceHandler for NoStreamSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        Ok(None)
    }
}

/// FLOWIP-128a A4: a join reference variable naming a composite resolves
/// through the boundary by the join's declared reference type, with
/// default-port fallback. Before the fix this build failed with
/// "references unknown stage variable 'digest'".
#[tokio::test]
async fn join_reference_resolves_through_composite_boundary_port() {
    let result = flow! {
        name: "amr_join_reference",
        journals: memory_journals(),
        middleware: [],

        stages: {
            seed = source!(BuildOnlySeed => NoEventSource);
            digest = ai_map_reduce!(
                chunk: BuildOnlySeed -> BuildOnlyChunk => NoopChunker,
                map: BuildOnlyChunk -> BuildOnlyPartial => NoopMap,
                collect: BuildOnlyPartial -> BuildOnlyCollected => CollectByInput::new(
                    BuildOnlyCollected::default(),
                    |acc: &mut BuildOnlyCollected, partial: &BuildOnlyPartial| {
                        acc.values.push(partial.value);
                    },
                ),
                reduce: BuildOnlyCollected -> BuildOnlyOut => NoopFinalize,
            );
            stream_src = source!(JoinStreamP => NoStreamSource);
            enrich = join!(catalog digest: BuildOnlyOut, JoinStreamP -> JoinedP => LocalNoopJoin);
            joined_sink = sink!(JoinedP => NoopSink);
        },

        topology: {
            seed |> digest;
            stream_src |> enrich;
            enrich |> joined_sink;
        }
    }
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    result.unwrap_or_else(|err| {
        panic!(
            "FLOWIP-128a A4: a join referencing the composite binding must resolve \
             through the boundary's typed output port. Error: {err:?}"
        )
    });
}

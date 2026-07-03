// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-114c regression test: `build_typed_flow!` must attach composite
//! subgraph membership to the topology it passes to `validate_edge_typing`.
//!
//! Before the closing-PR fix, `build_typed_flow!` built `topology_stages`
//! from `TopologyStageInfo::new(...)` without `.with_subgraph(...)`, and the
//! validator's composite-internal-edge skip rule could not fire. Any flow
//! using `ai_map_reduce!` would surface as a `SingleEdge` mismatch on the
//! `chunk -> collect` manifest edge (the chunker emits `Chunk`, the collect
//! stage declares `Partial` as its typed input). This test wires a minimal
//! `ai_map_reduce!` composite end-to-end through `flow!`, awaits the build,
//! and asserts `Ok(_)`. Without the fix it returns
//! `Err(FlowBuildError::EdgeTypingMismatch { kind: SingleEdge, .. })` on the
//! `digest__chunk -> digest__collect` manifest edge.
//!
//! The handlers below are stubs: the source emits no events, the transforms
//! return empty vectors, the sink is a no-op. The test only exercises the
//! build path (DSL expansion + composite lowering + topology assembly +
//! validate_edge_typing); it does not drive the runtime.

use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventContent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{id::StageId, TypedPayload, WriterId};
use obzenflow_dsl::{ai_map_reduce, flow, sink, source};
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
        let chunk_count = 2;
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
    // topology without composite subgraph membership, the `chunk -> collect`
    // manifest edge would surface as
    // `FlowBuildError::EdgeTypingMismatch { kind: SingleEdge, .. }` and this
    // `.await` would resolve to `Err`. With the FLOWIP-114c closing-PR fix in
    // `build_typed_flow!`, subgraph membership is attached before the
    // validator runs and the four composite-internal edges are skipped.
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

    handle
        .run()
        .await
        .expect("ai_map_reduce should commit planning manifests and tagged partials");

    assert_eq!(
        delivered.load(Ordering::SeqCst),
        1,
        "ai_map_reduce should deliver one reduced output to the downstream sink"
    );
    assert_eq!(
        total.load(Ordering::SeqCst),
        3,
        "collector should route tagged partials and finalise their sum"
    );
}

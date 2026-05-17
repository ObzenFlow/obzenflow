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
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{ChainEvent, TypedPayload};
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
    index: usize,
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
            "noop",
            DeliveryMethod::Custom("Noop".to_string()),
            None,
        ))
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

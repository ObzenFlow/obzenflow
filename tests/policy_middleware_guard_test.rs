// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120c H1: policy middleware attaches to live I/O units only.
//!
//! A circuit breaker or rate limiter on a pure sync surface (sync transform,
//! sync stateful, join) is a flow build error: a deterministic handler shell
//! has no unreliable call to protect. The deprecated async-non-effectful
//! surface still builds with a warning, because the canonical AI example
//! rides it until FLOWIP-120p moves those legs onto effects and FLOWIP-120f
//! deletes the surface.

use async_trait::async_trait;
use obzenflow_adapters::middleware::circuit_breaker;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::TypedPayload;
use obzenflow_core::WriterId;
use obzenflow_dsl::{async_transform, flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncTransformHandler, FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GuardEvent {
    sequence: u64,
}

impl TypedPayload for GuardEvent {
    const EVENT_TYPE: &'static str = "policy_guard.event";
}

#[derive(Clone, Debug)]
struct OneShotSource {
    emitted: bool,
    writer_id: WriterId,
}

impl FiniteSourceHandler for OneShotSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            GuardEvent::EVENT_TYPE,
            json!({ "sequence": 1 }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct SyncPassthrough;

#[async_trait]
impl TransformHandler for SyncPassthrough {
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct AsyncPassthrough;

#[async_trait]
impl AsyncTransformHandler for AsyncPassthrough {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct NullSink;

#[async_trait]
impl SinkHandler for NullSink {
    async fn consume(
        &mut self,
        _event: ChainEvent,
    ) -> Result<obzenflow_core::event::payloads::delivery_payload::DeliveryPayload, HandlerError>
    {
        Ok(
            obzenflow_core::event::payloads::delivery_payload::DeliveryPayload::success(
                "null_sink",
                obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
                None,
            ),
        )
    }
}

#[tokio::test]
async fn flow_level_policy_middleware_is_rejected_at_build() {
    let result = flow! {
        name: "policy_guard_flow_scope",
        journals: disk_journals(std::path::PathBuf::from(
            "target/policy-guard-logs/flow-scope",
        )),
        middleware: [
            circuit_breaker(3)
        ],

        stages: {
            guard_source = source!(GuardEvent => OneShotSource {
                emitted: false,
                writer_id: WriterId::from(obzenflow_core::StageId::new()),
            });
            guarded = transform!(GuardEvent -> GuardEvent => SyncPassthrough);
            guard_sink = sink!(GuardEvent => NullSink);
        },

        topology: {
            guard_source |> guarded;
            guarded |> guard_sink;
        }
    }
    .await;

    let err = match result {
        Ok(_) => panic!("flow-level policy middleware must fail the build"),
        Err(err) => format!("{err:?}"),
    };
    assert!(
        err.contains("PolicyMiddlewareOnFlowScope") || err.contains("Flow-level policy"),
        "expected the FLOWIP-120c flow-scope rejection, got: {err}"
    );
}

#[tokio::test]
async fn policy_middleware_on_pure_sync_stage_is_rejected_at_build() {
    let result = flow! {
        name: "policy_guard_pure_sync",
        journals: disk_journals(std::path::PathBuf::from(
            "target/policy-guard-logs/pure-sync",
        )),
        middleware: [],

        stages: {
            guard_source = source!(GuardEvent => OneShotSource {
                emitted: false,
                writer_id: WriterId::from(obzenflow_core::StageId::new()),
            });
            guarded = transform!(GuardEvent -> GuardEvent => SyncPassthrough, [
                circuit_breaker(3)
            ]);
            guard_sink = sink!(GuardEvent => NullSink);
        },

        topology: {
            guard_source |> guarded;
            guarded |> guard_sink;
        }
    }
    .await;

    let err = match result {
        Ok(_) => panic!("policy middleware on a pure sync transform must fail the build"),
        Err(err) => format!("{err:?}"),
    };
    assert!(
        err.contains("PolicyMiddlewareOnPureStage") || err.contains("pure sync surface"),
        "expected the FLOWIP-120c H1 rejection, got: {err}"
    );
}

#[tokio::test]
async fn policy_middleware_on_async_surface_still_builds() {
    // H1 keeps the deprecated async-non-effectful surface building (with a
    // warning) so the canonical AI example survives until FLOWIP-120p/120f.
    let flow_handle = flow! {
        name: "policy_guard_async",
        journals: disk_journals(std::path::PathBuf::from(
            "target/policy-guard-logs/async-surface",
        )),
        middleware: [],

        stages: {
            guard_source = source!(GuardEvent => OneShotSource {
                emitted: false,
                writer_id: WriterId::from(obzenflow_core::StageId::new()),
            });
            guarded = async_transform!(GuardEvent -> GuardEvent => AsyncPassthrough, [
                circuit_breaker(3)
            ]);
            guard_sink = sink!(GuardEvent => NullSink);
        },

        topology: {
            guard_source |> guarded;
            guarded |> guard_sink;
        }
    }
    .await
    .expect("the deprecated async surface must keep building under H1");

    flow_handle
        .run_with_metrics()
        .await
        .expect("one-shot flow should run to completion");
}

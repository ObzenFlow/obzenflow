// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120c H1: policy middleware attaches to live I/O units only.
//!
//! A circuit breaker or rate limiter on a pure sync surface (sync transform,
//! sync stateful, join, or async non-effect handler) is a flow build error: a
//! handler shell has no typed live-I/O unit to protect. FLOWIP-115n seals the
//! only remaining structural shell to the AI map-reduce factories assigned to
//! FLOWIP-128g.

use async_trait::async_trait;
use obzenflow_adapters::middleware::CircuitBreaker;
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

fn breaker(failures: u32) -> CircuitBreaker {
    CircuitBreaker::builder()
        .consecutive_failures(failures)
        .build()
        .expect("test breaker configuration")
}

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
            breaker(3)
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
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
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
                breaker(3)
            ]);
            guard_sink = sink!(GuardEvent => NullSink);
        },

        topology: {
            guard_source |> guarded;
            guarded |> guard_sink;
        }
    }
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
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
async fn policy_middleware_on_async_non_effect_stage_is_rejected_at_build() {
    let result = flow! {
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
                breaker(3)
            ]);
            guard_sink = sink!(GuardEvent => NullSink);
        },

        topology: {
            guard_source |> guarded;
            guarded |> guard_sink;
        }
    }
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    let err = match result {
        Ok(_) => panic!("policy middleware on an async non-effect stage must fail the build"),
        Err(err) => format!("{err:?}"),
    };
    assert!(
        err.contains("PolicyMiddlewareOnPureStage")
            && err.contains("guarded")
            && err.contains("circuit_breaker"),
        "expected the FLOWIP-115n typed-surface rejection, got: {err}"
    );
}

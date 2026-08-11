// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120c H1: policy middleware attaches to live I/O units only.
//!
//! A circuit breaker or rate limiter on a pure sync surface (sync transform,
//! sync stateful, or join) is a flow build error: a handler shell has no typed
//! live-I/O unit to protect.

use async_trait::async_trait;
use obzenflow_adapters::middleware::CircuitBreaker;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkDeliveryDeclaration, SinkInputContext, SinkTerminalOutcome, TypedFiniteSourceHandler,
    TypedSinkConsumeReport, TypedSinkHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};

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
}

impl TypedFiniteSourceHandler for OneShotSource {
    type Output = GuardEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![GuardEvent { sequence: 1 }]))
    }
}

#[derive(Clone, Debug)]
struct SyncPassthrough;

impl TypedTransformHandler for SyncPassthrough {
    type Input = GuardEvent;
    type Output = GuardEvent;

    fn process(&self, event: GuardEvent) -> Result<GuardEvent, HandlerError> {
        Ok(event)
    }
}

#[derive(Clone, Debug)]
struct NullSink;

#[async_trait]
impl TypedSinkHandler for NullSink {
    type Input = GuardEvent;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        SinkDeliveryDeclaration::undeclared()
    }

    async fn consume(
        &mut self,
        _event: GuardEvent,
        _context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError> {
        Ok(TypedSinkConsumeReport::terminal(
            SinkTerminalOutcome::success(
                obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
                None,
            ),
        ))
    }
}

#[tokio::test]
async fn policy_middleware_on_pure_sync_stage_is_rejected_at_build() {
    let result = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = OneShotSource { emitted: false };
        let transform_handler = SyncPassthrough;
        let sink_handler = NullSink;

        Ok(flow! {
            name: "policy_guard_pure_sync",
            journals: disk_journals(std::path::PathBuf::from(
                "target/policy-guard-logs/pure-sync",
            )),

            stages: {
                guard_source = source!(GuardEvent => source_handler);
                guarded = transform!(GuardEvent -> GuardEvent => transform_handler, observers: [
                    breaker(3)
                ]);
                guard_sink = sink!(GuardEvent => sink_handler);
            },

            topology: {
                guard_source |> guarded;
                guarded |> guard_sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    let err = match result {
        Ok(_) => panic!("policy middleware on a pure sync transform must fail the build"),
        Err(err) => format!("{err:?}"),
    };
    assert!(
        err.contains("'observers:' accepts observer middleware only"),
        "expected the FLOWIP-115s observer-authority rejection, got: {err}"
    );
}

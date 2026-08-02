// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Regression for the FLOWIP-115n total factory facade.
//!
//! The former test installed a breaker through the generic async-handler shell
//! and then asserted handler-authored poison-EOF shutdown behaviour. That shell
//! is no longer a supported breaker surface. The same declaration must now fail
//! deterministically before a run or shutdown contract can be produced.

use async_trait::async_trait;
use obzenflow_adapters::middleware::CircuitBreaker;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{TypedPayload, WriterId};
use obzenflow_dsl::{async_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncTransformHandler, FiniteSourceHandler, SinkHandler,
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
struct BreakerTestEvent {
    index: u64,
}

impl TypedPayload for BreakerTestEvent {
    const EVENT_TYPE: &'static str = "circuit_breaker_shutdown.event";
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
            BreakerTestEvent::EVENT_TYPE,
            json!({ "index": 0 }),
        )]))
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
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
    }
}

#[tokio::test]
async fn breaker_handler_shell_is_rejected_before_shutdown_contracts_exist() {
    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let one_shot_source = OneShotSource {
            emitted: false,
            writer_id: WriterId::from(obzenflow_core::StageId::new()),
        };
        let async_passthrough = AsyncPassthrough;
        let null_sink = NullSink;
        let breaker_policy = breaker(2);

        Ok(flow! {
            name: "breaker_shutdown_contracts",
            journals: disk_journals(std::path::PathBuf::from(
                "target/breaker_shutdown_contracts",
            )),
            middleware: [],

            stages: {
                source = source!(BreakerTestEvent => one_shot_source);
                failing_transform = async_transform!(BreakerTestEvent -> BreakerTestEvent => async_passthrough, [
                    breaker_policy
                ]);
                sink = sink!(BreakerTestEvent => null_sink);
            },

            topology: {
                source |> failing_transform;
                failing_transform |> sink;
            }
        })
    });

    let result = flow_definition
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await;

    let error = match result {
        Ok(_) => panic!("a breaker must not regain the retired async-handler shell"),
        Err(error) => format!("{error:?}"),
    };
    assert!(
        error.contains("PolicyMiddlewareOnPureStage")
            && error.contains("failing_transform")
            && error.contains("circuit_breaker"),
        "expected the deterministic typed-surface diagnostic, got: {error}"
    );
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::dsl::typing::{
    PlaceholderAsyncSource, PlaceholderFiniteSource, PlaceholderInfiniteSource, PlaceholderJoin,
    PlaceholderSink, PlaceholderStateful, PlaceholderTransform,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_runtime::__private::{TypedJoinHandlerAdapter, UnifiedJoinHandler};
use obzenflow_runtime::stages::common::handlers::source::traits::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler,
};
use obzenflow_runtime::stages::common::handlers::{
    SinkHandler, TransformHandler, TypedStatefulHandler,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[test]
fn placeholder_finite_source_signals_eof() {
    let mut handler = PlaceholderFiniteSource::<u8>::new(None);
    let next = FiniteSourceHandler::next(&mut handler).expect("finite source next");
    assert!(next.is_none());
}

#[test]
fn placeholder_infinite_source_idles_with_empty_batch() {
    let mut handler = PlaceholderInfiniteSource::<u8>::new(None);
    let next = InfiniteSourceHandler::next(&mut handler).expect("infinite source next");
    assert!(next.is_empty());
}

#[test]
fn placeholder_transform_drops_data_events() {
    let handler = PlaceholderTransform::<u8, u16>::new(None);
    let event = ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "test.event",
        json!({"hello": "world"}),
    );
    let outputs = TransformHandler::process(&handler, event).expect("transform process");
    assert!(outputs.is_empty());
}

#[tokio::test]
async fn placeholder_transform_drains_cleanly() {
    let mut handler = PlaceholderTransform::<u8, u16>::new(None);
    TransformHandler::drain(&mut handler)
        .await
        .expect("transform drain");
}

#[tokio::test]
async fn placeholder_async_source_is_safe_in_both_modes() {
    let mut handler = PlaceholderAsyncSource::<u8>::new(None);

    let finite = <PlaceholderAsyncSource<u8> as AsyncFiniteSourceHandler>::next(&mut handler)
        .await
        .expect("async finite next");
    assert!(finite.is_none());

    let infinite = <PlaceholderAsyncSource<u8> as AsyncInfiniteSourceHandler>::next(&mut handler)
        .await
        .expect("async infinite next");
    assert!(infinite.is_empty());
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PlaceholderInput;

impl TypedPayload for PlaceholderInput {
    const EVENT_TYPE: &'static str = "test.placeholder.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PlaceholderOutput;

impl TypedPayload for PlaceholderOutput {
    const EVENT_TYPE: &'static str = "test.placeholder.output";
}

#[test]
fn placeholder_stateful_emits_nothing_and_drains() {
    let handler = PlaceholderStateful::<PlaceholderInput, PlaceholderOutput>::new(None);

    TypedStatefulHandler::accumulate(&handler, &mut (), PlaceholderInput);
    let emission = TypedStatefulHandler::emit(&handler, &()).expect("stateful emit");
    let outputs = match emission {
        obzenflow_runtime::stages::stateful::StatefulEmission::RetainEpoch { outputs, .. }
        | obzenflow_runtime::stages::stateful::StatefulEmission::ResetEpoch { outputs, .. } => {
            outputs
        }
    };
    assert!(outputs.is_empty());

    let drained = TypedStatefulHandler::drain(&handler, &()).expect("stateful drain");
    assert!(drained.is_empty());
}

#[tokio::test]
async fn placeholder_sink_acks_and_flushes_safely() {
    let mut handler = PlaceholderSink::<u8>::new(None);
    let event = ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "test.event",
        json!({"hello": "world"}),
    );

    let payload = SinkHandler::consume(&mut handler, event)
        .await
        .expect("sink consume");

    assert!(matches!(payload.delivery_method, DeliveryMethod::Noop));
    assert!(matches!(payload.result, DeliveryResult::Success { .. }));

    let flushed = SinkHandler::flush(&mut handler).await.expect("sink flush");
    assert!(flushed.is_none());
}

#[tokio::test]
async fn placeholder_join_discards_and_drains_safely() {
    let writer_id = WriterId::from(StageId::new());
    let mut handler = TypedJoinHandlerAdapter::new(PlaceholderJoin::<
        PlaceholderInput,
        PlaceholderInput,
        PlaceholderOutput,
    >::new(None));
    UnifiedJoinHandler::install_writer_id(&mut handler, writer_id);
    let mut state = UnifiedJoinHandler::initial_state(&handler);

    let reference_outputs = UnifiedJoinHandler::process_reference(
        &handler,
        &mut state,
        PlaceholderInput.to_event(writer_id),
        StageId::new(),
        writer_id,
        obzenflow_core::MiddlewareExecutionScope::default(),
    )
    .expect("join reference");
    assert!(reference_outputs.is_empty());

    let invocation = UnifiedJoinHandler::process_stream(
        &handler,
        &mut state,
        PlaceholderInput.to_event(writer_id),
        StageId::new(),
        writer_id,
        obzenflow_core::MiddlewareExecutionScope::default(),
    )
    .expect("join stream");
    let (outputs, framework_eof) = invocation.into_parts();
    assert!(outputs.is_empty());
    assert!(framework_eof.is_none());

    let eof_outputs = UnifiedJoinHandler::on_stream_eof(
        &handler,
        &mut state,
        ChainEventFactory::eof_event(writer_id, true),
        StageId::new(),
        writer_id,
    )
    .expect("join on_stream_eof");
    assert!(eof_outputs.is_empty());

    let drained = UnifiedJoinHandler::drain(&handler, &state, None)
        .await
        .expect("join drain");
    assert!(drained.is_empty());
}

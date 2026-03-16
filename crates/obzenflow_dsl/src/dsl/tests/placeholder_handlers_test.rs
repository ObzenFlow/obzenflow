// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::dsl::typing::{
    PlaceholderAsyncSource, PlaceholderAsyncTransform, PlaceholderFiniteSource,
    PlaceholderInfiniteSource, PlaceholderJoin, PlaceholderSink, PlaceholderStateful,
    PlaceholderTransform,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::stages::common::handlers::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, AsyncTransformHandler,
    FiniteSourceHandler, InfiniteSourceHandler, JoinHandler, SinkHandler, StatefulHandler,
    TransformHandler,
};
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

#[tokio::test]
async fn placeholder_async_transform_drops_data_events_and_drains() {
    let mut handler = PlaceholderAsyncTransform::<u8, u16>::new(None);
    let event = ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "test.event",
        json!({"hello": "world"}),
    );

    let outputs = AsyncTransformHandler::process(&handler, event)
        .await
        .expect("async transform process");
    assert!(outputs.is_empty());

    AsyncTransformHandler::drain(&mut handler)
        .await
        .expect("async transform drain");
}

#[tokio::test]
async fn placeholder_stateful_emits_nothing_and_drains() {
    let mut handler = PlaceholderStateful::<u8, u16>::new(None);
    let event = ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "test.event",
        json!({"hello": "world"}),
    );

    StatefulHandler::accumulate(&mut handler, &mut (), event);
    let outputs = StatefulHandler::create_events(&handler, &()).expect("stateful create_events");
    assert!(outputs.is_empty());

    let drained = StatefulHandler::drain(&handler, &())
        .await
        .expect("stateful drain");
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
    let handler = PlaceholderJoin::<u8, u16, u32>::new(None);
    let event = ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "test.event",
        json!({"hello": "world"}),
    );

    let outputs = JoinHandler::process_event(
        &handler,
        &mut (),
        event,
        StageId::new(),
        WriterId::from(StageId::new()),
    )
    .expect("join process_event");
    assert!(outputs.is_empty());

    let eof_outputs = JoinHandler::on_source_eof(
        &handler,
        &mut (),
        StageId::new(),
        WriterId::from(StageId::new()),
    )
    .expect("join on_source_eof");
    assert!(eof_outputs.is_empty());

    let drained = JoinHandler::drain(&handler, &()).await.expect("join drain");
    assert!(drained.is_empty());
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[tokio::test]
async fn downstream_wait_preserves_deferred_ack() {
    tokio::time::pause();
    let (mut supervisor, mut ctx, registry, source, transform, sink, upstream, output) =
        build_transform_harness(
            |writer| ExpandHandler {
                writer_id: writer.into(),
            },
            1,
            1,
        )
        .await;
    let upstream_writer = registry.writer(source);
    upstream_writer.reserve(1).unwrap().commit(1);
    upstream
        .append(
            ChainEventFactory::data_event(
                source.into(),
                "bp_test.in",
                std::num::NonZeroU32::MIN,
                json!({}),
            ),
            Default::default(),
        )
        .await
        .unwrap();
    let state = TransformState::<ExpandHandler>::Running;
    let mut turn = tokio_test::task::spawn(supervisor.dispatch_state(&state, &mut ctx));
    assert_pending!(turn.poll());
    assert_eq!(upstream_writer.min_downstream_credit(), 0);
    registry.reader(transform, sink).ack_consumed(1);
    assert!(matches!(
        assert_ready!(turn.poll()).unwrap(),
        EventLoopDirective::Continue
    ));
    drop(turn);
    assert_eq!(ctx.resources_mut().unwrap().pending_outputs.len(), 1);
    assert_eq!(upstream_writer.min_downstream_credit(), 0);

    let mut turn = tokio_test::task::spawn(supervisor.dispatch_state(&state, &mut ctx));
    // Retry commits the retained output and acknowledges the input, then reaches
    // the existing empty-input sleep. Cancelling this wait preserves the commit.
    assert_pending!(turn.poll());
    assert_eq!(upstream_writer.min_downstream_credit(), 1);
    drop(turn);
    assert!(ctx.resources_mut().unwrap().pending_outputs.is_empty());
    let committed = output.read_causally_ordered().await.unwrap();
    assert_eq!(
        committed
            .iter()
            .filter(|row| row.event_type() == "bp_test.expand_out")
            .count(),
        2
    );
}

#[tokio::test]
async fn draining_completes_running_deferred_ack_once() {
    tokio::time::pause();
    let (mut supervisor, mut ctx, registry, source, transform, sink, upstream, output) =
        build_transform_harness(
            |writer| ExpandHandler {
                writer_id: writer.into(),
            },
            1,
            1,
        )
        .await;
    let upstream_writer = registry.writer(source);
    upstream_writer.reserve(1).unwrap().commit(1);
    upstream
        .append(
            ChainEventFactory::data_event(
                source.into(),
                "bp_test.in",
                std::num::NonZeroU32::MIN,
                json!({}),
            ),
            Default::default(),
        )
        .await
        .unwrap();
    let running = TransformState::<ExpandHandler>::Running;
    let mut turn = tokio_test::task::spawn(supervisor.dispatch_state(&running, &mut ctx));
    assert_pending!(turn.poll());
    assert_eq!(upstream_writer.min_downstream_credit(), 0);
    registry.reader(transform, sink).ack_consumed(1);
    assert!(matches!(
        assert_ready!(turn.poll()).unwrap(),
        EventLoopDirective::Continue
    ));
    drop(turn);
    assert_eq!(ctx.resources_mut().unwrap().pending_outputs.len(), 1);
    assert_eq!(upstream_writer.min_downstream_credit(), 0);

    // The same input completes in a different dispatch/state. It must not
    // acknowledge the input twice or publish duplicate output.
    let draining = TransformState::<ExpandHandler>::Draining;
    let result = supervisor
        .dispatch_state(&draining, &mut ctx)
        .await
        .unwrap();
    assert!(matches!(
        result,
        EventLoopDirective::Transition(TransformEvent::DrainComplete)
    ));
    assert_eq!(upstream_writer.min_downstream_credit(), 1);
    assert!(ctx.resources_mut().unwrap().pending_outputs.is_empty());
    assert_eq!(
        output
            .read_causally_ordered()
            .await
            .unwrap()
            .iter()
            .filter(|row| row.event_type() == "bp_test.expand_out")
            .count(),
        2
    );
}

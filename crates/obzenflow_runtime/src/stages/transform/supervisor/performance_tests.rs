// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

struct RecordedSpan {
    name: &'static str,
    parent: Option<u64>,
    references: usize,
}

#[derive(Default)]
struct Recording {
    spans: Vec<RecordedSpan>,
    entered: Vec<u64>,
}

struct Capture(Arc<Mutex<Recording>>);

#[derive(Default)]
struct SummaryFields(serde_json::Map<String, serde_json::Value>);

impl Visit for SummaryFields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), format!("{value:?}").into());
    }
}

struct CaptureSummaries(Arc<Mutex<Vec<SummaryFields>>>);

impl Subscriber for CaptureSummaries {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event() && metadata.target() == "obzenflow::supervisor_timing"
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        unreachable!()
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = SummaryFields::default();
        event.record(&mut fields);
        if fields.0.contains_key("dispatch_count") {
            self.0.lock().unwrap().push(fields);
        }
    }
    fn enter(&self, _: &Id) {
        unreachable!()
    }
    fn exit(&self, _: &Id) {
        unreachable!()
    }
}

impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_span() && metadata.target() == "obzenflow::performance"
    }

    fn new_span(&self, attributes: &Attributes<'_>) -> Id {
        let mut recording = self.0.lock().unwrap();
        let parent = attributes.parent().map(Id::into_u64).or_else(|| {
            attributes
                .is_contextual()
                .then(|| recording.entered.last().copied())
                .flatten()
        });
        recording.spans.push(RecordedSpan {
            name: attributes.metadata().name(),
            parent,
            references: 1,
        });
        Id::from_u64(recording.spans.len() as u64)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, span: &Id) {
        self.0.lock().unwrap().entered.push(span.into_u64());
    }

    fn exit(&self, span: &Id) {
        assert_eq!(self.0.lock().unwrap().entered.pop(), Some(span.into_u64()));
    }

    fn clone_span(&self, id: &Id) -> Id {
        self.0.lock().unwrap().spans[id.into_u64() as usize - 1].references += 1;
        id.clone()
    }

    fn try_close(&self, id: Id) -> bool {
        let mut recording = self.0.lock().unwrap();
        let references = &mut recording.spans[id.into_u64() as usize - 1].references;
        *references -= 1;
        *references == 0
    }
}

#[tokio::test]
async fn transform_spans_separate_downstream_wait_from_poll_and_preserve_deferred_ack() {
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
    let recording = Arc::new(Mutex::new(Recording::default()));
    let dispatch = tracing::Dispatch::new(Capture(recording.clone()));
    let state = TransformState::<ExpandHandler>::Running;
    let mut turn = tokio_test::task::spawn(
        supervisor
            .dispatch_state(&state, &mut ctx)
            .with_subscriber(dispatch.clone()),
    );
    assert_pending!(turn.poll());
    assert_eq!(upstream_writer.min_downstream_credit(), 0);
    {
        let recording = recording.lock().unwrap();
        assert!(recording.entered.is_empty(), "Pending exits every span");
        for name in [
            "transform_upstream_poll",
            "transform_handler",
            "transform_handler_invoke",
            "transform_output_prepare",
        ] {
            let span = recording
                .spans
                .iter()
                .find(|span| span.name == name)
                .unwrap();
            assert_eq!(
                span.references, 0,
                "{name} ended before the downstream wait"
            );
        }
        let publish = recording
            .spans
            .iter()
            .find(|span| span.name == "transform_output_publish" && span.references > 0)
            .expect("second output is parked under its publication boundary");
        let parent = &recording.spans[publish.parent.unwrap() as usize - 1];
        assert_eq!(parent.name, "transform_running_dispatch");
        let credit_wait = recording
            .spans
            .iter()
            .find(|span| span.name == "backpressure_credit_wait" && span.references > 0)
            .expect("only the actual downstream suspension is a credit wait");
        let mut ancestors = vec![credit_wait.name];
        let mut parent = credit_wait.parent;
        while let Some(id) = parent {
            let span = &recording.spans[id as usize - 1];
            ancestors.push(span.name);
            parent = span.parent;
        }
        assert_eq!(
            ancestors,
            [
                "backpressure_credit_wait",
                "backpressure_drain",
                "transform_output_publish",
                "transform_running_dispatch",
            ]
        );
        assert!(!recording
            .spans
            .iter()
            .any(|span| span.name == "transform_input_ack"));
    }
    registry.reader(transform, sink).ack_consumed(1);
    assert!(matches!(
        assert_ready!(turn.poll()).unwrap(),
        EventLoopDirective::Continue
    ));
    drop(turn);
    assert_eq!(ctx.resources_mut().unwrap().pending_outputs.len(), 1);
    assert_eq!(upstream_writer.min_downstream_credit(), 0);

    let mut turn = tokio_test::task::spawn(
        supervisor
            .dispatch_state(&state, &mut ctx)
            .with_subscriber(dispatch),
    );
    // Retry commits the retained output and acknowledges the input, then reaches
    // the existing empty-input sleep. Cancellation must close that open span.
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
    let recording = recording.lock().unwrap();
    assert!(recording.entered.is_empty());
    assert!(recording.spans.iter().all(|span| span.references == 0));
    for name in [
        "transform_pending_output_drain",
        "transform_input_ack",
        "transform_empty_input_wait",
    ] {
        let span = recording
            .spans
            .iter()
            .find(|span| span.name == name)
            .unwrap();
        assert_eq!(
            recording.spans[span.parent.unwrap() as usize - 1].name,
            "transform_running_dispatch"
        );
    }
}

#[tokio::test]
async fn transform_cycle_allocation_conserves_running_and_draining_deferred_ack() {
    use crate::supervised_base::loop_timing;
    use obzenflow_core::event::payloads::supervisor_descriptor::{SupervisionMode, SupervisorKind};

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
    let summaries = Arc::new(Mutex::new(Vec::new()));
    // Deliberately reject all spans: the coarse clock must stand on its own.
    let dispatch = tracing::Dispatch::new(CaptureSummaries(summaries.clone()));
    let running = TransformState::<ExpandHandler>::Running;
    let mut turn = tokio_test::task::spawn(
        loop_timing::scope(
            "timed-transform",
            SupervisorKind::Transform,
            transform.into(),
            SupervisionMode::HandlerSupervised,
            supervisor.dispatch_state(&running, &mut ctx),
        )
        .with_subscriber(dispatch.clone()),
    );
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
    // become a second business input or charge ack to the earlier cycle.
    let draining = TransformState::<ExpandHandler>::Draining;
    let result = loop_timing::scope(
        "timed-transform",
        SupervisorKind::Transform,
        transform.into(),
        SupervisionMode::HandlerSupervised,
        supervisor.dispatch_state(&draining, &mut ctx),
    )
    .with_subscriber(dispatch)
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

    let summaries = summaries.lock().unwrap();
    assert_eq!(summaries.len(), 2);
    for summary in summaries.iter() {
        let fields = &summary.0;
        let allocated: u64 = [
            "read_ns",
            "handler_ns",
            "prepare_ns",
            "publish_ns",
            "credit_wait_ns",
            "acknowledge_ns",
            "control_ns",
            "idle_ns",
            "residual_ns",
        ]
        .iter()
        .map(|key| fields[*key].as_u64().unwrap())
        .sum();
        assert_eq!(allocated, fields["elapsed_ns"].as_u64().unwrap());
        assert_eq!(fields["dispatch_count"], 1);
        assert_eq!(fields["outcome"], "completed");
        assert_eq!(fields["conservation_failures"], 0);
        assert_eq!(fields["max_conservation_error_ns"], 0);
    }
    let first = &summaries
        .iter()
        .find(|row| row.0["state"] == "Running")
        .unwrap()
        .0;
    assert_eq!(first["business_input_count"], 1);
    assert_eq!(first["acknowledgement_count"], 0);
    assert_eq!(first["acknowledge_ns"], 0);
    assert!(first["handler_ns"].as_u64().unwrap() > 0);
    assert!(first["credit_wait_ns"].as_u64().unwrap() > 0);
    let second = &summaries
        .iter()
        .find(|row| row.0["state"] == "Draining")
        .unwrap()
        .0;
    assert_eq!(second["business_input_count"], 0);
    assert_eq!(second["acknowledgement_count"], 1);
    assert_eq!(second["handler_ns"], 0);
    assert_eq!(second["credit_wait_ns"], 0);
}

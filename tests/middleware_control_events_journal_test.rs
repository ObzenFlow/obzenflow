//! Test that middleware control events are properly written to journal
//!
//! This test verifies that control events emitted by middleware during
//! transform processing are properly written to the journal and can be
//! collected via subscriptions.

use obzenflow_adapters::middleware::{
    Middleware, MiddlewareAction, MiddlewareContext, TransformHandlerExt,
};
use obzenflow_core::{ChainEvent, EventId};
use obzenflow_infra::journal::MemoryJournal;
use obzenflow_runtime_services::messaging::reactive_journal::ReactiveJournal;
use obzenflow_runtime_services::stages::common::handlers::TransformHandler;
use serde_json::json;
use std::sync::Arc;

struct SimpleTransform;

impl TransformHandler for SimpleTransform {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        event.payload["transformed"] = json!(true);
        vec![event]
    }
}

struct MetricsEmittingMiddleware;

impl Middleware for MetricsEmittingMiddleware {
    fn pre_handle(&self, _event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Emit metrics state control event
        ctx.write_control_event(ChainEvent::control(
            ChainEvent::CONTROL_METRICS_STATE,
            json!({
                "queue_depth": 10,
                "in_flight": 5
            }),
        ));
        MiddlewareAction::Continue
    }

    fn post_handle(
        &self,
        _event: &ChainEvent,
        _results: &[ChainEvent],
        ctx: &mut MiddlewareContext,
    ) {
        // Emit middleware state control event
        ctx.write_control_event(ChainEvent::control(
            ChainEvent::CONTROL_MIDDLEWARE_STATE,
            json!({
                "middleware": "test",
                "state_transition": {
                    "from": "processing",
                    "to": "complete"
                }
            }),
        ));
    }
}

#[tokio::test]
async fn test_middleware_control_events_flow_to_journal() {
    // Create journal without observer - using subscription pattern now
    let base_journal = Arc::new(MemoryJournal::new());
    let journal = ReactiveJournal::new(base_journal);

    // Create subscription for control events
    let mut subscription = journal.subscribe(
        obzenflow_runtime_services::messaging::reactive_journal::SubscriptionFilter::EventTypes {
            event_types: vec![
                ChainEvent::CONTROL_METRICS_STATE.to_string(),
                ChainEvent::CONTROL_MIDDLEWARE_STATE.to_string(),
            ],
        }
    ).await.unwrap();

    // Register a writer for the transform
    let writer_id = journal
        .register_writer(obzenflow_topology::StageId::new(), None)
        .await
        .unwrap();

    // Create handler with middleware
    let handler = SimpleTransform
        .middleware()
        .with(MetricsEmittingMiddleware)
        .build();

    // Create test event
    let event = ChainEvent::new(
        EventId::new(),
        writer_id.clone(),
        "test.data",
        json!({"value": 42}),
    );

    // Process event (simulating what TransformSupervisor does)
    let outputs = handler.process(event);

    // Write all outputs to journal (as TransformSupervisor does)
    for output in outputs {
        journal.write(&writer_id, output, None).await.unwrap();
    }

    // Collect control events from subscription
    let mut control_events = Vec::new();
    // Receive events one at a time
    for _ in 0..2 {
        let batch = subscription.recv_batch().await.unwrap();
        control_events.extend(batch);
    }

    // Should have received 2 control events
    assert_eq!(control_events.len(), 2);

    // Verify metrics state event
    let metrics_event = control_events
        .iter()
        .find(|e| e.event.event_type == ChainEvent::CONTROL_METRICS_STATE)
        .expect("Should have metrics state event");

    assert_eq!(metrics_event.event.payload["queue_depth"], 10);
    assert_eq!(metrics_event.event.payload["in_flight"], 5);

    // Verify middleware state event
    let middleware_event = control_events
        .iter()
        .find(|e| e.event.event_type == ChainEvent::CONTROL_MIDDLEWARE_STATE)
        .expect("Should have middleware state event");

    assert_eq!(middleware_event.event.payload["middleware"], "test");
    assert_eq!(
        middleware_event.event.payload["state_transition"]["from"],
        "processing"
    );
    assert_eq!(
        middleware_event.event.payload["state_transition"]["to"],
        "complete"
    );
}

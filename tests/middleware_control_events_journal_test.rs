//! Test that middleware control events are properly written to journal
//!
//! This test verifies that control events emitted by middleware during
//! transform processing are properly written to the journal and can be
//! observed by reading the journal.

use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    Middleware, MiddlewareAction, MiddlewareContext, TransformHandlerExt,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{JournalOwner, StageId, WriterId};
use obzenflow_infra::journal::MemoryJournal;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::TransformHandler;
use serde_json::json;

struct SimpleTransform;

#[async_trait]
impl TransformHandler for SimpleTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

struct MetricsEmittingMiddleware;

impl Middleware for MetricsEmittingMiddleware {
    fn pre_handle(&self, _event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Emit metrics state control event
        let writer_id = WriterId::from(StageId::new());
        ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
            writer_id.clone(),
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
        // Emit middleware state control event (also modeled as metrics snapshot)
        let writer_id = WriterId::from(StageId::new());
        ctx.write_control_event(ChainEventFactory::metrics_state_snapshot(
            writer_id,
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
    // Create a stage-owned in-memory journal
    let stage_id = StageId::new();
    let owner = JournalOwner::stage(stage_id);
    let journal: MemoryJournal<ChainEvent> = MemoryJournal::with_owner(owner);

    // Create handler with middleware
    let handler = SimpleTransform
        .middleware()
        .with(MetricsEmittingMiddleware)
        .build();

    // Writer for the transform
    let writer_id = WriterId::from(stage_id);

    // Create test event
    let event = ChainEventFactory::data_event(writer_id.clone(), "test.data", json!({"value": 42}));

    // Process event (simulating what TransformSupervisor does)
    let outputs = handler
        .process(event)
        .expect("SimpleTransform should not fail in middleware control-events journal test");

    // Write all outputs to journal (as TransformSupervisor does)
    for output in outputs {
        journal.append(output, None).await.unwrap();
    }

    // Read back all events from journal
    let envelopes = journal.read_causally_ordered().await.unwrap();
    let control_events: Vec<ChainEvent> = envelopes
        .into_iter()
        .map(|env| env.event)
        .filter(|e| e.is_lifecycle())
        .collect();

    // Should have received 2 control events
    assert_eq!(control_events.len(), 2);

    // Verify metrics state event (queue depth snapshot)
    let metrics_event = control_events
        .iter()
        .find(|e| {
            let payload = e.payload();
            payload
                .get("metrics")
                .and_then(|m| m.get("queue_depth"))
                .map(|v| v == &json!(10))
                .unwrap_or(false)
        })
        .expect("Should have metrics state event");

    let metrics_payload = metrics_event.payload();
    assert_eq!(metrics_payload["metrics"]["queue_depth"], 10);
    assert_eq!(metrics_payload["metrics"]["in_flight"], 5);

    // Verify middleware state event (modeled as metrics snapshot)
    let middleware_event = control_events
        .iter()
        .find(|e| {
            let payload = e.payload();
            payload
                .get("metrics")
                .and_then(|m| m.get("middleware"))
                .map(|v| v == &json!("test"))
                .unwrap_or(false)
        })
        .expect("Should have middleware state event");

    let middleware_payload = middleware_event.payload();
    assert_eq!(middleware_payload["metrics"]["middleware"], "test");
    assert_eq!(
        middleware_payload["metrics"]["state_transition"]["from"],
        "processing"
    );
    assert_eq!(
        middleware_payload["metrics"]["state_transition"]["to"],
        "complete"
    );
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::LoggingMiddleware;
use obzenflow_core::event::chain_event::ChainEventContent;
use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope, StageType};
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_runtime::stages::observer::{
    HandlerObserver, HandlerObserverContext, ObserverDeterminism, SinkDeliveryObserver,
    SinkDeliveryObserverContext, SinkDeliveryObserverOutcome,
};
use serde_json::json;

fn is_logging_diagnostic(event: &obzenflow_core::ChainEvent) -> bool {
    matches!(
        &event.content,
        ChainEventContent::Observability(ObservabilityPayload::Middleware(
            MiddlewareLifecycle::User(user)
        )) if user.event_type == "obzenflow.logging"
    )
}

#[test]
fn test_logging_middleware_counts_events() {
    let middleware = LoggingMiddleware::with_prefix("TEST");

    let event = ChainEventFactory::data_event(
        obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
        "test.event",
        json!({ "data": "test" }),
    );

    assert_eq!(middleware.events_processed(), 0);

    let flow_context = FlowContext {
        flow_name: "test_flow".to_string(),
        flow_id: "flow_1".to_string(),
        stage_name: "test_stage".to_string(),
        stage_id: obzenflow_core::StageId::new(),
        stage_type: StageType::Transform,
    };
    let ctx = HandlerObserverContext {
        stage_id: flow_context.stage_id,
        stage_name: &flow_context.stage_name,
        flow_context: &flow_context,
        scope: MiddlewareExecutionScope::LiveHandler,
        input: &event,
        stage_input_position: Some(1),
    };
    let report = HandlerObserver::before_handle(&middleware, &ctx);
    assert_eq!(report.diagnostics.len(), 1);
    assert!(is_logging_diagnostic(&report.diagnostics[0]));
    assert_eq!(middleware.events_processed(), 1);

    let report = HandlerObserver::before_handle(&middleware, &ctx);
    assert_eq!(report.diagnostics.len(), 1);
    assert!(is_logging_diagnostic(&report.diagnostics[0]));
    assert_eq!(middleware.events_processed(), 2);
}

#[test]
fn test_logging_middleware_is_live_only() {
    let middleware = LoggingMiddleware::default();
    assert_eq!(
        HandlerObserver::determinism(&middleware),
        ObserverDeterminism::LiveOnly
    );
}

#[test]
fn test_logging_middleware_observes_sink_delivery() {
    let middleware = LoggingMiddleware::with_prefix("SINK WAZ HERE!");
    let event = ChainEventFactory::data_event(
        obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
        "test.event",
        json!({ "data": "test" }),
    );

    let ctx = SinkDeliveryObserverContext {
        stage_id: obzenflow_core::StageId::new(),
        stage_name: "test_sink",
        scope: MiddlewareExecutionScope::LiveSinkDeliveryBoundary,
        input: &event,
        stage_input_position: Some(1),
        outcome: SinkDeliveryObserverOutcome::Delivered,
    };

    let report = SinkDeliveryObserver::after_sink_delivery(&middleware, &ctx);
    assert_eq!(report.diagnostics.len(), 2);
    assert!(report.diagnostics.iter().all(is_logging_diagnostic));
    assert_eq!(middleware.events_processed(), 1);
}

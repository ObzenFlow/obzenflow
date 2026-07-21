// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{indicator, latency, IndicatorConfig, IndicatorMiddleware};
use crate::middleware::MiddlewareFactory;
use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope, StageType};
use obzenflow_core::event::payloads::observability_payload::{
    IndicatorKind, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::{ChainEventContent, ChainEventFactory};
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::stages::observer::{HandlerObserver, HandlerObserverContext};
use serde_json::json;

fn configured() -> IndicatorMiddleware {
    IndicatorMiddleware::with_config(IndicatorConfig {
        kind: IndicatorKind::Latency,
        operation: Some("payment.authorization".to_string()),
        indicator: Some("authorization.latency".to_string()),
        tags: vec![("dependency".to_string(), "payment_gateway".to_string())],
    })
}

#[test]
fn sample_records_raw_value_with_operation_indicator_kind_and_tags() {
    // The sample carries the raw measurement and identity only: no objective,
    // threshold, or met flag is embedded (those are read-side, FLOWIP-115l).
    let sample = configured().sample(MetricsDuration::from_millis(6_120));
    assert_eq!(sample.kind, IndicatorKind::Latency);
    assert_eq!(sample.operation, "payment.authorization");
    assert_eq!(sample.indicator, "authorization.latency");
    assert_eq!(sample.value_ms, 6_120);
    assert_eq!(sample.tags.len(), 1);
    assert_eq!(sample.tags[0].key, "dependency");
    assert_eq!(sample.tags[0].value, "payment_gateway");
}

#[test]
fn diagnostic_is_a_typed_indicator_wide_event() {
    let stage_id = StageId::new();
    let diagnostic = configured().diagnostic(stage_id, MetricsDuration::from_millis(6_120));
    let ChainEventContent::Observability(ObservabilityPayload::Middleware(
        MiddlewareLifecycle::Indicator(sample),
    )) = diagnostic.content
    else {
        panic!("indicator diagnostic should be a typed Indicator middleware event");
    };
    assert_eq!(sample.operation, "payment.authorization");
    assert_eq!(sample.value_ms, 6_120);
}

#[test]
fn after_handle_emits_exactly_one_sample_per_execution() {
    let stage_id = StageId::new();
    let flow_context = FlowContext {
        flow_name: "payment_gateway".to_string(),
        flow_id: "flow_1".to_string(),
        stage_name: "authorize_payment".to_string(),
        stage_id,
        stage_type: StageType::Transform,
    };
    let input = ChainEventFactory::data_event(
        WriterId::from(stage_id),
        "order.validated.v1",
        json!({ "order_id": "ord_1" }),
    );
    let ctx = HandlerObserverContext {
        stage_id,
        stage_name: "authorize_payment",
        flow_context: &flow_context,
        scope: MiddlewareExecutionScope::LiveHandler,
        input: &input,
        stage_input_position: None,
    };
    let middleware = configured();
    assert!(middleware.before_handle(&ctx).is_empty());

    // Fan-out: many outputs, still exactly one sample.
    let mut outputs = vec![
        ChainEventFactory::data_event(WriterId::from(stage_id), "payment.authorized.v1", json!({})),
        ChainEventFactory::data_event(WriterId::from(stage_id), "order.cancelled.v1", json!({})),
    ];
    let report = middleware.after_handle(&ctx, &mut outputs);
    assert_eq!(
        report.diagnostics.len(),
        1,
        "one sample per operation execution"
    );
}

#[test]
fn latency_is_a_convenience_constructor_for_the_indicator_factory() {
    let factory = latency()
        .operation("payment.authorization")
        .indicator("authorization.latency")
        .tag("dependency", "payment_gateway");

    assert_eq!(factory.label(), "latency");
    let snapshot = factory
        .config_snapshot()
        .expect("indicator exposes a snapshot");
    assert_eq!(snapshot["operation"], "payment.authorization");
    assert_eq!(snapshot["indicator"], "authorization.latency");
    assert_eq!(snapshot["tags"][0]["key"], "dependency");
    // The objective is read-side: no boundary/threshold is embedded in the sample.
    assert!(snapshot.get("boundary").is_none());
}

#[test]
fn indicator_factory_is_hook_bound() {
    let declaration = indicator()
        .operation("op")
        .kind(IndicatorKind::Latency)
        .declaration();
    assert!(
        declaration.is_observer(),
        "indicator declares an observer surface"
    );
    assert!(
        !declaration.is_flowip_128g_legacy_shell(),
        "indicator must not use the sealed AI migration route"
    );
}

#[test]
fn indicator_requires_operation_and_indicator_names() {
    use super::IndicatorConfigError;
    assert_eq!(
        indicator()
            .indicator("authorization.latency")
            .validated_identity(),
        Err(IndicatorConfigError::MissingOperation)
    );
    assert_eq!(
        indicator()
            .operation("payment.authorization")
            .validated_identity(),
        Err(IndicatorConfigError::MissingIndicator)
    );
    assert!(indicator()
        .operation("payment.authorization")
        .indicator("authorization.latency")
        .validated_identity()
        .is_ok());
}

#[test]
fn indicator_rejects_blank_identity() {
    use super::IndicatorConfigError;
    assert_eq!(
        indicator()
            .operation("   ")
            .indicator("authorization.latency")
            .validated_identity(),
        Err(IndicatorConfigError::BlankOperation)
    );
    assert_eq!(
        indicator()
            .operation("payment.authorization")
            .indicator("\t")
            .validated_identity(),
        Err(IndicatorConfigError::BlankIndicator)
    );
}

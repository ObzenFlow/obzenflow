// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{log_event, LoggingMiddleware, LoggingMiddlewareFactory};
use crate::middleware::control::ControlMiddlewareAggregator;
use crate::middleware::{
    materialize_factory_checked, MiddlewareAttachmentRequest, MiddlewareDeclarationIndex,
    MiddlewareFactory, MiddlewareSurface, MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId,
};
use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope, StageType};
use obzenflow_core::event::payloads::observability_payload::{
    LoggingAttribute, LoggingEventName, LoggingLevel, LoggingOccurrence, LoggingSinkAttemptResult,
    LoggingSinkOutcome, LoggingSourceOutcome,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_runtime::stages::observer::{
    HandlerObserver, HandlerObserverContext, JoinCanonicalMergeMetadata, JoinDeliverySnapshot,
    JoinObserver, JoinObserverContext, JoinSide, JoinSignalKind, JoinSignalSnapshot,
    ObserverDeterminism, ObserverEvidence, ObserverReport, SinkDeliveryAttemptResult,
    SinkDeliveryObserver, SinkDeliveryObserverContext, SinkDeliveryObserverOutcome,
    SourcePollObserver, SourcePollObserverContext, SourcePollObserverOutcome, StatefulObserver,
    StatefulObserverContext,
};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;

fn configured(trace_mirror: bool) -> LoggingMiddleware {
    LoggingMiddleware::new(
        LoggingEventName::new("payment.authorization.manual_review_handoff").unwrap(),
        LoggingLevel::Info,
        vec![
            LoggingAttribute::new("operation", "payment.authorization").unwrap(),
            LoggingAttribute::new("handoff.kind", "manual_review").unwrap(),
        ],
        trace_mirror,
    )
    .unwrap()
}

fn flow_context(stage_id: obzenflow_core::StageId, stage_type: StageType) -> FlowContext {
    FlowContext {
        flow_name: "payment_gateway".to_string(),
        flow_id: "flow_1".to_string(),
        stage_name: "manual_review".to_string(),
        stage_id,
        stage_type,
    }
}

fn occurrence(report: ObserverReport) -> LoggingOccurrence {
    assert_eq!(report.diagnostics.len(), 1);
    let ObserverEvidence::Logging(evidence) = report
        .diagnostics
        .into_iter()
        .next()
        .expect("one diagnostic")
        .evidence
    else {
        panic!("expected typed logging evidence");
    };
    evidence.occurrence().clone()
}

#[test]
fn handler_report_is_content_only_and_payload_opaque() {
    let middleware = configured(false);
    let stage_id = obzenflow_core::StageId::new();
    let input = ChainEventFactory::data_event(
        obzenflow_core::WriterId::from(stage_id),
        "payment.authorization_unavailable.v1",
        json!({ "secret": "must-not-appear" }),
    );
    let flow_context = flow_context(stage_id, StageType::Transform);
    let ctx = HandlerObserverContext {
        stage_id,
        stage_name: &flow_context.stage_name,
        flow_context: &flow_context,
        scope: MiddlewareExecutionScope::LiveHandler,
        input: &input,
        stage_input_position: Some(7),
    };

    let report = HandlerObserver::before_handle(&middleware, &ctx);
    assert_eq!(report.diagnostics.len(), 1);
    let ObserverEvidence::Logging(evidence) = &report.diagnostics[0].evidence else {
        panic!("logging observer must return closed logging evidence");
    };
    assert!(matches!(
        evidence.occurrence(),
        LoggingOccurrence::HandlerInputObserved { input }
            if input.stage_input_position == Some(7)
    ));
    assert!(!serde_json::to_string(evidence)
        .unwrap()
        .contains("must-not-appear"));
}

#[test]
fn logging_middleware_is_live_only() {
    assert_eq!(
        HandlerObserver::determinism(&configured(false)),
        ObserverDeterminism::LiveOnly
    );
}

#[test]
fn handler_and_stateful_rows_have_locked_multiplicity_and_signal_behaviour() {
    let middleware = configured(false);
    let stage_id = obzenflow_core::StageId::new();
    let flow = flow_context(stage_id, StageType::Transform);
    let input = ChainEventFactory::data_event(
        obzenflow_core::WriterId::from(stage_id),
        "payment.authorization_unavailable.v1",
        json!({}),
    );
    let outputs = vec![
        ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(stage_id),
            "test.output.v1",
            json!({}),
        ),
        ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(stage_id),
            "test.output.v1",
            json!({}),
        ),
        ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(stage_id),
            "test.output.v1",
            json!({}),
        ),
    ];
    let handler = HandlerObserverContext {
        stage_id,
        stage_name: "manual_review",
        flow_context: &flow,
        scope: MiddlewareExecutionScope::LiveHandler,
        input: &input,
        stage_input_position: Some(11),
    };
    assert!(matches!(
        occurrence(HandlerObserver::before_handle(&middleware, &handler)),
        LoggingOccurrence::HandlerInputObserved { input }
            if input.stage_input_position == Some(11)
    ));
    assert!(matches!(
        occurrence(HandlerObserver::after_handle(
            &middleware,
            &handler,
            &outputs
        )),
        LoggingOccurrence::HandlerOutputObserved {
            output_count: 3,
            ..
        }
    ));

    let stateful_with_input = StatefulObserverContext {
        stage_id,
        stage_name: "manual_review",
        flow_context: &flow,
        scope: MiddlewareExecutionScope::LiveHandler,
        input: Some(&input),
        stage_input_position: Some(12),
    };
    assert!(matches!(
        occurrence(StatefulObserver::before_state_accumulate(
            &middleware,
            &stateful_with_input
        )),
        LoggingOccurrence::StatefulInputObserved { .. }
    ));
    assert!(matches!(
        occurrence(StatefulObserver::after_state_emit(
            &middleware,
            &stateful_with_input,
            &[]
        )),
        LoggingOccurrence::StatefulOutputObserved {
            output_count: 0,
            ..
        }
    ));

    let signal_only = StatefulObserverContext {
        input: None,
        stage_input_position: None,
        ..stateful_with_input
    };
    assert!(StatefulObserver::before_state_accumulate(&middleware, &signal_only).is_empty());
    assert!(StatefulObserver::after_state_emit(&middleware, &signal_only, &outputs).is_empty());
}

#[test]
fn join_rows_require_a_canonical_delivery_and_never_expand_fanout() {
    let middleware = configured(false);
    let stage_id = obzenflow_core::StageId::new();
    let source_stage_id = obzenflow_core::StageId::new();
    let flow = flow_context(stage_id, StageType::Join);
    let input = ChainEventFactory::data_event(
        obzenflow_core::WriterId::from(source_stage_id),
        "payment.authorization_unavailable.v1",
        json!({}),
    );
    let input_envelope =
        obzenflow_core::EventEnvelope::new(obzenflow_core::JournalWriterId::new(), input.clone());
    let mut high_water = VectorClock::new();
    high_water.clocks.insert(source_stage_id.to_string(), 9);
    let delivery = JoinDeliverySnapshot {
        side: JoinSide::Stream,
        delivered_source_stage_id: source_stage_id,
        delivered_stage_input_position: 17,
        input_envelope,
        reference_high_water: high_water.clone(),
        canonical_merge: Some(JoinCanonicalMergeMetadata {
            selected_feed: Some("stream".to_string()),
            reader_index: Some(2),
        }),
    };
    let ctx = JoinObserverContext {
        stage_id,
        stage_name: "manual_review",
        flow_context: &flow,
        scope: MiddlewareExecutionScope::LiveHandler,
        input: Some(&input),
        delivery: Some(&delivery),
        signal: None,
    };
    assert!(matches!(
        occurrence(JoinObserver::before_join_input(&middleware, &ctx)),
        LoggingOccurrence::JoinInputObserved { input, delivery }
            if input.stage_input_position == Some(17)
                && delivery.reference_high_water == high_water
                && delivery.canonical_merge.as_ref().and_then(|m| m.reader_index) == Some(2)
    ));
    let outputs = vec![input.clone(), input.clone(), input.clone(), input.clone()];
    assert!(matches!(
        occurrence(JoinObserver::after_join_output(&middleware, &ctx, &outputs)),
        LoggingOccurrence::JoinOutputObserved {
            output_count: 4,
            ..
        }
    ));

    let signal = JoinSignalSnapshot {
        side: Some(JoinSide::Stream),
        signal: JoinSignalKind::Eof,
    };
    let signal_only = JoinObserverContext {
        input: None,
        delivery: None,
        signal: Some(&signal),
        ..ctx
    };
    assert!(JoinObserver::before_join_input(&middleware, &signal_only).is_empty());
    assert!(JoinObserver::after_join_output(&middleware, &signal_only, &outputs).is_empty());
}

#[test]
fn source_poll_maps_every_closed_outcome_to_exactly_one_row() {
    let middleware = configured(false);
    let stage_id = obzenflow_core::StageId::new();
    let flow = flow_context(stage_id, StageType::FiniteSource);
    let outputs = vec![
        ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(stage_id),
            "test.output.v1",
            json!({}),
        ),
        ChainEventFactory::drain_event(obzenflow_core::WriterId::from(stage_id)),
    ];
    let cases = [
        SourcePollObserverOutcome::Batch { events: 4 },
        SourcePollObserverOutcome::Eof,
        SourcePollObserverOutcome::Error {
            kind: ErrorKind::Timeout,
        },
        SourcePollObserverOutcome::Rejected {
            policy: Some("source_breaker".to_string()),
        },
    ];

    for case in cases {
        let expected = match &case {
            SourcePollObserverOutcome::Batch { events } => LoggingSourceOutcome::Batch {
                events: *events as u64,
            },
            SourcePollObserverOutcome::Eof => LoggingSourceOutcome::Eof,
            SourcePollObserverOutcome::Error { kind } => {
                LoggingSourceOutcome::Error { kind: kind.clone() }
            }
            SourcePollObserverOutcome::Rejected { policy } => LoggingSourceOutcome::Rejected {
                policy: policy.clone(),
            },
        };
        let ctx = SourcePollObserverContext {
            stage_id,
            stage_name: "source",
            flow_context: &flow,
            scope: MiddlewareExecutionScope::LiveSourceBoundary,
            poll_duration: Duration::from_millis(12),
            outcome: case.clone(),
        };
        let occurrence = occurrence(SourcePollObserver::after_source_poll(
            &middleware,
            &ctx,
            &outputs,
        ));
        let LoggingOccurrence::SourcePollObserved {
            poll_duration_ms,
            output_count,
            data_event_count,
            outcome,
        } = occurrence
        else {
            panic!("expected source-poll occurrence");
        };
        assert_eq!(poll_duration_ms, 12);
        assert_eq!(output_count, 2);
        assert_eq!(data_event_count, 1);
        assert_eq!(outcome, expected);
    }
}

#[test]
fn logging_factory_excludes_effect_and_lifecycle_surfaces() {
    let declaration = log_event("payment.authorization.manual_review_handoff").declaration();
    assert!(declaration.is_observer());
    for surface in [
        MiddlewareSurfaceKind::Handler,
        MiddlewareSurfaceKind::Stateful,
        MiddlewareSurfaceKind::Join,
        MiddlewareSurfaceKind::SourcePoll,
        MiddlewareSurfaceKind::SinkDelivery,
    ] {
        assert!(declaration.supports(surface));
    }
    assert!(!declaration.supports(MiddlewareSurfaceKind::Effect));
    assert!(!declaration.supports(MiddlewareSurfaceKind::StageLifecycle));
}

fn materialization_error(factory: LoggingMiddlewareFactory) -> String {
    let stage_id = obzenflow_core::StageId::new();
    let config = obzenflow_runtime::pipeline::config::StageConfig {
        stage_id,
        name: "manual_review".to_string(),
        flow_name: "payment_gateway".to_string(),
        cycle_guard: None,
        lineage: obzenflow_core::config::LineagePolicy::default(),
        effective_config: Arc::new(
            obzenflow_runtime::runtime_config::FlowEffectiveConfig::default(),
        ),
    };
    let surface = MiddlewareSurface::Handler { stage_id };
    let protected_unit = ProtectedUnitId {
        stage_id,
        unit: ProtectedUnit::Handler,
    };
    let request = MiddlewareAttachmentRequest {
        surface: &surface,
        protected_unit: &protected_unit,
        declaration_index: MiddlewareDeclarationIndex::observers(0),
    };
    let result = materialize_factory_checked(
        &factory,
        request,
        &config,
        StageType::Transform,
        &Arc::new(ControlMiddlewareAggregator::new()),
    );
    match result {
        Ok(_) => panic!("invalid logging configuration unexpectedly materialised"),
        Err(error) => error.to_string(),
    }
}

#[test]
fn invalid_logging_configuration_fails_during_materialization() {
    let cases = [
        (
            log_event("manual_review"),
            "logging event name 'manual_review' must match",
        ),
        (
            log_event("payment.manual_review").tag("Invalid", "value"),
            "logging attribute key 'Invalid' must match",
        ),
        (
            log_event("payment.manual_review")
                .tag("operation", "one")
                .tag("operation", "two"),
            "logging attribute 'operation' is duplicated",
        ),
        (
            (0..17).fold(log_event("payment.manual_review"), |factory, index| {
                factory.tag(format!("tag_{index}"), "value")
            }),
            "logging accepts at most 16 attributes",
        ),
        (
            log_event("payment.manual_review").tag("a".repeat(65), "value"),
            "logging attribute key",
        ),
        (
            log_event("payment.manual_review").tag("operation", "x".repeat(257)),
            "value exceeds 256 bytes",
        ),
        (
            log_event("payment.manual_review").tag("operation", "contains\nnewline"),
            "value contains a control character",
        ),
    ];

    for (factory, expected) in cases {
        let error = materialization_error(factory);
        assert!(
            error.contains(expected),
            "expected materialisation error containing {expected:?}, got {error:?}"
        );
    }
}

#[test]
fn sink_delivery_uses_exact_boundary_taxonomy_and_canonical_body() {
    let middleware = configured(true);
    let event = ChainEventFactory::data_event(
        obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
        "payment.authorization_unavailable.v1",
        json!({}),
    );
    let cases = vec![
        (
            SinkDeliveryObserverOutcome::Attempted {
                result: SinkDeliveryAttemptResult::ReportedSuccess,
            },
            LoggingSinkOutcome::Attempted {
                result: LoggingSinkAttemptResult::ReportedSuccess,
            },
        ),
        (
            SinkDeliveryObserverOutcome::Attempted {
                result: SinkDeliveryAttemptResult::ReportedPartial {
                    successful_count: 2,
                    failed_count: 1,
                },
            },
            LoggingSinkOutcome::Attempted {
                result: LoggingSinkAttemptResult::ReportedPartial {
                    successful_count: 2,
                    failed_count: 1,
                },
            },
        ),
        (
            SinkDeliveryObserverOutcome::Attempted {
                result: SinkDeliveryAttemptResult::ReportedBuffered,
            },
            LoggingSinkOutcome::Attempted {
                result: LoggingSinkAttemptResult::ReportedBuffered,
            },
        ),
        (
            SinkDeliveryObserverOutcome::Attempted {
                result: SinkDeliveryAttemptResult::ReportedFailure {
                    final_attempt: true,
                },
            },
            LoggingSinkOutcome::Attempted {
                result: LoggingSinkAttemptResult::ReportedFailure {
                    final_attempt: true,
                },
            },
        ),
        (
            SinkDeliveryObserverOutcome::Attempted {
                result: SinkDeliveryAttemptResult::HandlerError {
                    kind: ErrorKind::Remote,
                },
            },
            LoggingSinkOutcome::Attempted {
                result: LoggingSinkAttemptResult::HandlerError {
                    kind: ErrorKind::Remote,
                },
            },
        ),
        (
            SinkDeliveryObserverOutcome::Attempted {
                result: SinkDeliveryAttemptResult::HandlerPanicked,
            },
            LoggingSinkOutcome::Attempted {
                result: LoggingSinkAttemptResult::HandlerPanicked,
            },
        ),
        (
            SinkDeliveryObserverOutcome::Rejected {
                policy: Some("circuit_breaker".to_string()),
            },
            LoggingSinkOutcome::Rejected {
                policy: Some("circuit_breaker".to_string()),
            },
        ),
        (
            SinkDeliveryObserverOutcome::Rejected { policy: None },
            LoggingSinkOutcome::Rejected { policy: None },
        ),
    ];

    for (outcome, expected) in cases {
        let ctx = SinkDeliveryObserverContext {
            stage_id: obzenflow_core::StageId::new(),
            stage_name: "manual_review",
            scope: MiddlewareExecutionScope::LiveSinkDeliveryBoundary,
            input: &event,
            stage_input_position: Some(7),
            outcome,
        };
        let report = SinkDeliveryObserver::after_sink_delivery(&middleware, &ctx);
        let ObserverEvidence::Logging(evidence) = &report.diagnostics[0].evidence else {
            panic!("expected typed logging evidence");
        };
        assert!(matches!(
            evidence.occurrence(),
            LoggingOccurrence::SinkDeliveryBoundaryObserved { input, .. }
                if input.stage_input_position == Some(7)
        ));
        let LoggingOccurrence::SinkDeliveryBoundaryObserved { outcome, .. } = evidence.occurrence()
        else {
            unreachable!("occurrence was checked above");
        };
        assert_eq!(outcome, &expected);
        if matches!(
            evidence.occurrence(),
            LoggingOccurrence::SinkDeliveryBoundaryObserved {
                outcome: LoggingSinkOutcome::Attempted {
                    result: LoggingSinkAttemptResult::ReportedSuccess,
                },
                ..
            }
        ) {
            assert_eq!(
                evidence.body(),
                Some("manual-review handoff attempt reported success")
            );
        }
        let mirror = report.diagnostics[0]
            .local_trace
            .as_ref()
            .expect("trace mirror requested");
        assert_eq!(mirror.body, evidence.body().unwrap());
    }
}

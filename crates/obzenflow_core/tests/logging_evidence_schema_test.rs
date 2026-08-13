// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::observability_payload::{
    LoggingAttribute, LoggingEventName, LoggingEvidence, LoggingInputReference, LoggingLevel,
    LoggingOccurrence, LoggingSchemaError, LoggingSinkAttemptResult, LoggingSinkOutcome,
    LoggingSourceOutcome, MiddlewareLifecycle,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::EventId;
use serde_json::json;

fn input_reference() -> LoggingInputReference {
    LoggingInputReference {
        event_id: EventId::new(),
        event_type: "payment.authorization_unavailable.v1".to_string(),
        stage_input_position: Some(7),
    }
}

fn payment_evidence(occurrence: LoggingOccurrence) -> LoggingEvidence {
    LoggingEvidence::new(
        LoggingEventName::new("payment.authorization.manual_review_handoff").unwrap(),
        LoggingLevel::Info,
        occurrence,
        vec![
            LoggingAttribute::new("operation", "payment.authorization").unwrap(),
            LoggingAttribute::new("handoff.kind", "manual_review").unwrap(),
        ],
    )
    .unwrap()
}

#[test]
fn event_name_grammar_is_exact() {
    for valid in ["a.b", "payment.authorization", "a1.b_2.c3"] {
        assert_eq!(LoggingEventName::new(valid).unwrap().as_str(), valid);
    }

    for invalid in [
        "",
        "single",
        ".leading",
        "trailing.",
        "empty..segment",
        "Upper.case",
        "a.Digit",
        "1a.b",
        "a-b.c",
        "évent.name",
    ] {
        assert!(matches!(
            LoggingEventName::new(invalid),
            Err(LoggingSchemaError::InvalidEventName { .. })
        ));
    }
}

#[test]
fn attribute_bounds_fail_without_rewriting_or_truncation() {
    for key in ["operation", "handoff.kind", "event", "stage_id", "flow_id"] {
        assert_eq!(LoggingAttribute::new(key, "value").unwrap().key(), key);
    }

    assert!(matches!(
        LoggingAttribute::new("Bad.key", "value"),
        Err(LoggingSchemaError::InvalidAttributeKey { .. })
    ));
    assert!(matches!(
        LoggingAttribute::new("a".repeat(65), "value"),
        Err(LoggingSchemaError::AttributeKeyTooLong { .. })
    ));
    assert!(matches!(
        LoggingAttribute::new("key", "é".repeat(129)),
        Err(LoggingSchemaError::AttributeValueTooLong { .. })
    ));
    assert!(matches!(
        LoggingAttribute::new("key", "line\nbreak"),
        Err(LoggingSchemaError::AttributeValueContainsControl { .. })
    ));

    let duplicate = vec![
        LoggingAttribute::new("same", "one").unwrap(),
        LoggingAttribute::new("same", "two").unwrap(),
    ];
    assert!(matches!(
        LoggingEvidence::validate_attributes(&duplicate),
        Err(LoggingSchemaError::DuplicateAttribute { key }) if key == "same"
    ));

    let too_many = (0..17)
        .map(|index| LoggingAttribute::new(format!("key_{index}"), "value").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        LoggingEvidence::validate_attributes(&too_many),
        Err(LoggingSchemaError::TooManyAttributes)
    );
}

#[test]
fn canonical_payment_payload_has_the_locked_wire_form() {
    let evidence = payment_evidence(LoggingOccurrence::SinkDeliveryBoundaryObserved {
        input: input_reference(),
        outcome: LoggingSinkOutcome::Attempted {
            result: LoggingSinkAttemptResult::ReportedSuccess,
        },
    });
    let actual = serde_json::to_value(MiddlewareLifecycle::Logging(evidence)).unwrap();

    // Use the actual occurrence id while keeping every field and nesting level
    // explicit. Event IDs are opaque and intentionally nondeterministic.
    let actual_event_id = actual["details"]["occurrence"]["input"]["event_id"].clone();
    assert_eq!(
        actual,
        json!({
            "middleware_event": "logging",
            "details": {
                "event": "payment.authorization.manual_review_handoff",
                "level": "info",
                "occurrence": {
                    "kind": "sink_delivery_boundary_observed",
                    "input": {
                        "event_id": actual_event_id,
                        "event_type": "payment.authorization_unavailable.v1",
                        "stage_input_position": 7
                    },
                    "outcome": {
                        "kind": "attempted",
                        "result": { "kind": "reported_success" }
                    }
                },
                "attributes": [
                    { "key": "operation", "value": "payment.authorization" },
                    { "key": "handoff.kind", "value": "manual_review" }
                ],
                "body": "manual-review handoff attempt reported success"
            }
        })
    );
}

#[test]
fn canonical_looking_attributes_stay_non_authoritative() {
    let evidence = LoggingEvidence::new(
        LoggingEventName::new("actual.event").unwrap(),
        LoggingLevel::Warn,
        LoggingOccurrence::HandlerInputObserved {
            input: input_reference(),
        },
        vec![
            LoggingAttribute::new("event", "spoofed.event").unwrap(),
            LoggingAttribute::new("stage_id", "spoofed-stage").unwrap(),
            LoggingAttribute::new("flow_id", "spoofed-flow").unwrap(),
        ],
    )
    .unwrap();
    let value = serde_json::to_value(&evidence).unwrap();

    assert_eq!(value["event"], "actual.event");
    assert_eq!(value["attributes"][0]["key"], "event");
    assert_eq!(value["attributes"][0]["value"], "spoofed.event");
    assert!(value.get("stage_id").is_none());
    assert!(value.get("flow_id").is_none());
}

#[test]
fn every_closed_source_and_sink_outcome_round_trips() {
    let source_outcomes = vec![
        LoggingSourceOutcome::Batch { events: 3 },
        LoggingSourceOutcome::Eof,
        LoggingSourceOutcome::Error {
            kind: ErrorKind::Timeout,
        },
        LoggingSourceOutcome::Rejected {
            policy: Some("source_breaker".to_string()),
        },
        LoggingSourceOutcome::Rejected { policy: None },
    ];
    for outcome in source_outcomes {
        let evidence = payment_evidence(LoggingOccurrence::SourcePollObserved {
            poll_duration_ms: 12,
            output_count: 3,
            data_event_count: 2,
            outcome,
        });
        let encoded = serde_json::to_vec(&evidence).unwrap();
        let decoded: LoggingEvidence = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, evidence);
    }

    let sink_results = vec![
        LoggingSinkAttemptResult::ReportedSuccess,
        LoggingSinkAttemptResult::ReportedPartial {
            successful_count: 2,
            failed_count: 1,
        },
        LoggingSinkAttemptResult::ReportedBuffered,
        LoggingSinkAttemptResult::ReportedFailure {
            final_attempt: false,
        },
        LoggingSinkAttemptResult::HandlerError {
            kind: ErrorKind::Remote,
        },
        LoggingSinkAttemptResult::HandlerPanicked,
    ];
    let mut outcomes = sink_results
        .into_iter()
        .map(|result| LoggingSinkOutcome::Attempted { result })
        .collect::<Vec<_>>();
    outcomes.push(LoggingSinkOutcome::Rejected {
        policy: Some("circuit_breaker".to_string()),
    });
    outcomes.push(LoggingSinkOutcome::Rejected { policy: None });

    for outcome in outcomes {
        let evidence = payment_evidence(LoggingOccurrence::SinkDeliveryBoundaryObserved {
            input: input_reference(),
            outcome,
        });
        let encoded = serde_json::to_vec(&evidence).unwrap();
        let decoded: LoggingEvidence = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, evidence);
    }
}

#[test]
fn decoding_rejects_noncanonical_or_extra_runtime_text() {
    let evidence = payment_evidence(LoggingOccurrence::SinkDeliveryBoundaryObserved {
        input: input_reference(),
        outcome: LoggingSinkOutcome::Attempted {
            result: LoggingSinkAttemptResult::ReportedSuccess,
        },
    });
    let mut value = serde_json::to_value(evidence).unwrap();
    value["body"] = json!("secret=sk_live_canary; raw handler error");
    assert!(serde_json::from_value::<LoggingEvidence>(value).is_err());

    let mut with_payload =
        serde_json::to_value(payment_evidence(LoggingOccurrence::HandlerInputObserved {
            input: input_reference(),
        }))
        .unwrap();
    with_payload["occurrence"]["input"]["payload"] =
        json!({ "secret": "sk_live_canary", "error": "raw failure" });
    assert!(serde_json::from_value::<LoggingEvidence>(with_payload).is_err());

    let impossible = json!({
        "event": "payment.authorization.manual_review_handoff",
        "level": "info",
        "occurrence": {
            "kind": "sink_delivery_boundary_observed",
            "input": {
                "event_id": EventId::new(),
                "event_type": "payment.authorization_unavailable.v1",
                "stage_input_position": 7
            },
            "outcome": {
                "kind": "attempted",
                "policy": "never_attempted",
                "result": { "kind": "reported_success" }
            }
        },
        "attributes": [],
        "body": "manual-review handoff attempt reported success"
    });
    assert!(serde_json::from_value::<LoggingEvidence>(impossible).is_err());
}

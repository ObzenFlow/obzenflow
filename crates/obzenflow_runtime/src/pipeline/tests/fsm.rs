// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Control reduction and explicit lifecycle admission.
use crate::pipeline::fsm::context::{StopIntent, StopRequestOutcome};
use crate::pipeline::FlowStopMode;
use crate::stages::common::stage_handle::{STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP};
use std::time::Duration;

#[test]
fn stop_intent_cancel_sets_defaults() {
    let mut intent = StopIntent::default();
    let outcome = intent.apply_request(FlowStopMode::Cancel, None);

    assert!(intent.requested);
    assert!(matches!(intent.mode, Some(FlowStopMode::Cancel)));
    assert_eq!(intent.reason.as_deref(), Some(STOP_REASON_USER_STOP));
    assert!(intent.deadline.is_none());

    match outcome {
        StopRequestOutcome::Applied { reason_label, .. } => {
            assert_eq!(reason_label, STOP_REASON_USER_STOP);
        }
        StopRequestOutcome::Ignored => {
            panic!("cancel request should never be ignored");
        }
    }
}

#[test]
fn stop_intent_graceful_sets_deadline() {
    let mut intent = StopIntent::default();
    let timeout = Duration::from_secs(3);
    let before = std::time::Instant::now();

    let _ = intent.apply_request(FlowStopMode::Graceful { timeout }, None);
    let after = std::time::Instant::now();

    assert!(intent.requested);
    assert!(matches!(
        intent.mode,
        Some(FlowStopMode::Graceful { timeout: t }) if t == timeout
    ));
    assert_eq!(intent.reason.as_deref(), Some(STOP_REASON_USER_STOP));

    let deadline = intent
        .deadline
        .expect("graceful stop should set a deadline");
    assert!(deadline >= before + timeout);
    assert!(deadline <= after + timeout);
}

#[test]
fn stop_intent_cancel_overrides_graceful() {
    let mut intent = StopIntent::default();
    let _ = intent.apply_request(
        FlowStopMode::Graceful {
            timeout: Duration::from_secs(5),
        },
        None,
    );
    assert!(intent.deadline.is_some());

    let _ = intent.apply_request(FlowStopMode::Cancel, None);
    assert!(matches!(intent.mode, Some(FlowStopMode::Cancel)));
    assert!(intent.deadline.is_none());
}

#[test]
fn stop_intent_graceful_is_ignored_after_cancel() {
    let mut intent = StopIntent::default();
    let _ = intent.apply_request(FlowStopMode::Cancel, None);
    let outcome = intent.apply_request(
        FlowStopMode::Graceful {
            timeout: Duration::from_secs(1),
        },
        None,
    );

    assert!(matches!(outcome, StopRequestOutcome::Ignored));
    assert!(matches!(intent.mode, Some(FlowStopMode::Cancel)));
    assert!(intent.deadline.is_none());
}

#[test]
fn stop_intent_expired_timeout_preserves_deadline() {
    let mut intent = StopIntent::default();
    let _ = intent.apply_request(
        FlowStopMode::Graceful {
            timeout: Duration::ZERO,
        },
        Some("first_reason".to_string()),
    );
    assert_eq!(intent.reason.as_deref(), Some("first_reason"));
    let original_deadline = intent.deadline;

    let _ = intent.apply_request(FlowStopMode::Cancel, Some(STOP_REASON_TIMEOUT.to_string()));
    assert_eq!(intent.reason.as_deref(), Some(STOP_REASON_TIMEOUT));
    assert_eq!(
        intent.deadline, original_deadline,
        "timeout escalation must preserve the graceful-stop deadline"
    );
}

#[test]
fn first_graceful_deadline_wins_in_both_duration_orders() {
    for (first, second) in [(1, 60), (60, 1)] {
        let mut intent = StopIntent::default();
        intent.apply_request(
            FlowStopMode::Graceful {
                timeout: Duration::from_secs(first),
            },
            Some("first".into()),
        );
        let first_deadline = intent.deadline;
        assert!(matches!(
            intent.apply_request(
                FlowStopMode::Graceful {
                    timeout: Duration::from_secs(second)
                },
                Some("second".into()),
            ),
            StopRequestOutcome::Ignored
        ));
        assert_eq!(intent.deadline, first_deadline);
        assert_eq!(intent.reason.as_deref(), Some("first"));
    }
}

#[test]
fn cancel_is_absorbing_including_reason_and_admission_time() {
    let mut intent = StopIntent::default();
    intent.apply_request(FlowStopMode::Cancel, Some("explicit_cancel".into()));
    for (mode, reason) in [
        (FlowStopMode::Cancel, "duplicate"),
        (FlowStopMode::Cancel, STOP_REASON_TIMEOUT),
        (
            FlowStopMode::Graceful {
                timeout: Duration::ZERO,
            },
            "late_grace",
        ),
    ] {
        assert!(matches!(
            intent.apply_request(mode, Some(reason.into())),
            StopRequestOutcome::Ignored
        ));
        assert_eq!(intent.reason.as_deref(), Some("explicit_cancel"));
    }
}

#[test]
fn timeout_cancel_requires_an_expired_graceful_stop() {
    let mut intent = StopIntent::default();
    assert!(matches!(
        intent.apply_request(FlowStopMode::Cancel, Some(STOP_REASON_TIMEOUT.into())),
        StopRequestOutcome::Ignored
    ));
    assert!(!intent.requested);
    intent.apply_request(
        FlowStopMode::Graceful {
            timeout: Duration::from_secs(60),
        },
        None,
    );
    let deadline = intent.deadline;
    assert!(matches!(
        intent.apply_request(FlowStopMode::Cancel, Some(STOP_REASON_TIMEOUT.into())),
        StopRequestOutcome::Ignored
    ));
    assert_eq!(intent.deadline, deadline);
    assert!(matches!(intent.mode, Some(FlowStopMode::Graceful { .. })));
}

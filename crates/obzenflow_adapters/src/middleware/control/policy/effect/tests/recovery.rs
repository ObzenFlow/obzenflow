// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Recovery gates: raw eligibility, classification veto, exhaustion, and the
//! raw-success rule.

use super::support::*;

#[tokio::test]
async fn retrying_breaker_recovers_inside_one_boundary_invocation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(
        CircuitBreaker::opens_after(3)
            .retry(
                Retry::fixed(Duration::ZERO)
                    .attempts(3)
                    .max_delay(Duration::from_secs(1))
                    .start_window(Duration::from_secs(5)),
            )
            .build(),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let operation = scripted_operation(calls.clone(), |call| {
        if call <= 2 {
            Err(EffectError::Timeout("scripted timeout".to_string()))
        } else {
            Ok(Vec::new())
        }
    });

    let report = boundary
        .around_repeatable_effect(&identity_for("effect.retry"), &data_event(), operation)
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    let events = retry_events(&report);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, CircuitBreakerEvent::RetryScheduled { .. }))
            .count(),
        2
    );
    assert!(events.iter().any(|event| matches!(
        event,
        CircuitBreakerEvent::RetrySucceeded {
            total_attempts: 3,
            terminal_classification: CircuitBreakerHealthClassification::Success,
            ..
        }
    )));
}

#[tokio::test]
async fn stateful_classifier_runs_once_per_attempt_and_one_result_drives_settlement() {
    let classifier_calls = Arc::new(AtomicUsize::new(0));
    let classifier_calls_for_breaker = classifier_calls.clone();
    let (breaker, control, stage_id) = retrying_breaker_fixture(
        CircuitBreaker::opens_after(1)
            .retry(Retry::fixed(Duration::ZERO).attempts(2))
            .with_failure_classification(move |_, _| {
                match classifier_calls_for_breaker.fetch_add(1, Ordering::SeqCst) {
                    0 => FailureClassification::TransientFailure,
                    1 => FailureClassification::Success,
                    _ => FailureClassification::PermanentFailure,
                }
            })
            .build(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let report = boundary_with_chain(vec![breaker])
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(calls.clone(), |call| {
                if call == 1 {
                    Err(EffectError::Timeout("retry once".to_string()))
                } else {
                    Ok(Vec::new())
                }
            }),
        )
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        classifier_calls.load(Ordering::SeqCst),
        2,
        "the terminal result must not be classified again during settlement"
    );
    assert!(retry_events(&report).iter().any(|event| matches!(
        event,
        CircuitBreakerEvent::RetrySucceeded {
            total_attempts: 2,
            terminal_classification: CircuitBreakerHealthClassification::Success,
            ..
        }
    )));

    let snapshots = control.effect_circuit_breaker_snapshotters(&stage_id);
    let metrics = snapshots[0].1();
    assert_eq!(metrics.requests_total, 1);
    assert_eq!(metrics.successes_total, 1);
    assert_eq!(metrics.failures_total, 0);
}

#[tokio::test]
async fn opaque_execution_failure_is_never_promoted_to_retry() {
    let calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(
        CircuitBreaker::opens_after(3)
            .retry(Retry::fixed(Duration::ZERO).attempts(3))
            .with_failure_classification(|_, _| FailureClassification::TransientFailure)
            .build(),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let operation = scripted_operation(calls.clone(), |_| {
        Err(EffectError::Execution("opaque failure".to_string()))
    });

    let report = boundary
        .around_repeatable_effect(&identity_for("effect.retry"), &data_event(), operation)
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Execution(_)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(retry_events(&report).is_empty());
}

#[tokio::test]
async fn raw_success_is_not_reexecuted_when_classifier_marks_it_transient() {
    let calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(
        CircuitBreaker::opens_after(1)
            .retry(Retry::fixed(Duration::ZERO).attempts(3))
            .with_failure_classification(|_, _| FailureClassification::TransientFailure)
            .build(),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let operation = scripted_operation(calls.clone(), |_| Ok(Vec::new()));

    let report = boundary
        .around_repeatable_effect(&identity_for("effect.retry"), &data_event(), operation)
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(retry_events(&report).is_empty());
}

#[tokio::test]
async fn retry_exhaustion_returns_the_exact_last_failure() {
    let calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(
        CircuitBreaker::opens_after(3)
            .retry(Retry::fixed(Duration::ZERO).attempts(3))
            .build(),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let operation = scripted_operation(calls.clone(), |call| {
        Err(EffectError::Timeout(format!("timeout-{call}")))
    });

    let report = boundary
        .around_repeatable_effect(&identity_for("effect.retry"), &data_event(), operation)
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(ref message)))
            if message == "timeout-3"
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert!(retry_events(&report).iter().any(|event| matches!(
        event,
        CircuitBreakerEvent::RetryExhausted {
            total_attempts: 3,
            reason: CircuitBreakerRetryStopReason::AttemptLimit,
            ..
        }
    )));
}

#[tokio::test]
async fn later_permanent_failure_stops_recovery() {
    let calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(
        CircuitBreaker::opens_after(3)
            .retry(Retry::fixed(Duration::ZERO).attempts(3))
            .build(),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let operation = scripted_operation(calls.clone(), |call| {
        if call == 1 {
            Err(EffectError::Transport("disconnected".to_string()))
        } else {
            Err(EffectError::Permanent("credentials rejected".to_string()))
        }
    });

    let report = boundary
        .around_repeatable_effect(&identity_for("effect.retry"), &data_event(), operation)
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Permanent(_)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(retry_events(&report).iter().any(|event| matches!(
        event,
        CircuitBreakerEvent::RetryStoppedNonRetryable {
            total_attempts: 2,
            ..
        }
    )));
}

#[tokio::test]
async fn custom_classifier_can_veto_but_not_promote_recovery() {
    for classification in [
        FailureClassification::Success,
        FailureClassification::PermanentFailure,
        FailureClassification::PartialSuccess { failed_ratio: 0.5 },
    ] {
        let timeout_calls = Arc::new(AtomicUsize::new(0));
        let vetoing_breaker = retrying_breaker_attachment(
            CircuitBreaker::opens_after(3)
                .retry(Retry::fixed(Duration::ZERO).attempts(3))
                .with_failure_classification(move |_, _| classification.clone())
                .build(),
        );
        let veto_report = boundary_with_chain(vec![vetoing_breaker])
            .around_repeatable_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(timeout_calls.clone(), |_| {
                    Err(EffectError::Timeout("classifier veto".to_string()))
                }),
            )
            .await;
        assert_eq!(timeout_calls.load(Ordering::SeqCst), 1);
        assert!(retry_events(&veto_report).is_empty());
    }

    for error in [
        EffectError::Permanent("permanent".to_string()),
        EffectError::Validation("invalid".to_string()),
        EffectError::Domain("declined".to_string()),
        EffectError::Serialization("bad payload".to_string()),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(
            CircuitBreaker::opens_after(3)
                .retry(Retry::fixed(Duration::ZERO).attempts(3))
                .with_failure_classification(|_, _| FailureClassification::TransientFailure)
                .build(),
        );
        let slot = Arc::new(Mutex::new(Some(error)));
        let report = boundary_with_chain(vec![breaker])
            .around_repeatable_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(calls.clone(), move |_| {
                    Err(slot.lock().unwrap().take().expect("called once"))
                }),
            )
            .await;
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Err(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(retry_events(&report).is_empty());
    }
}

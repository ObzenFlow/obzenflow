// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Concurrent invocations: independent attempt state per cursor, and stops
//! caused by another invocation opening the shared circuit.

use super::support::*;

#[tokio::test]
async fn concurrent_effect_cursors_keep_independent_attempt_state() {
    let breaker = retrying_breaker_attachment(
        CircuitBreaker::opens_after(10)
            .retry(Retry::fixed(Duration::ZERO).attempts(2))
            .build(),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let first_identity = identity_for("effect.retry");
    let second_identity = EffectIdentity {
        cursor: EffectCursor::new("test_flow", "test_stage", 1, 1),
        ..identity_for("effect.retry")
    };
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));
    let first_event = data_event();
    let second_event = data_event();

    let (first_report, second_report) = tokio::join!(
        boundary.around_effect(
            &first_identity,
            &first_event,
            scripted_operation(first_calls.clone(), |call| {
                if call == 1 {
                    Err(EffectError::Timeout("first cursor".to_string()))
                } else {
                    Ok(Vec::new())
                }
            }),
        ),
        boundary.around_effect(
            &second_identity,
            &second_event,
            scripted_operation(second_calls.clone(), |call| {
                if call == 1 {
                    Err(EffectError::Timeout("second cursor".to_string()))
                } else {
                    Ok(Vec::new())
                }
            }),
        ),
    );

    assert_eq!(first_calls.load(Ordering::SeqCst), 2);
    assert_eq!(second_calls.load(Ordering::SeqCst), 2);
    for (report, expected_cursor) in [
        (&first_report, &first_identity.cursor),
        (&second_report, &second_identity.cursor),
    ] {
        let events = retry_events(report);
        assert!(events.iter().any(|event| matches!(
            event,
            CircuitBreakerEvent::RetrySucceeded {
                total_attempts: 2,
                ..
            }
        )));
        assert!(events.iter().all(|event| match event {
            CircuitBreakerEvent::RetryScheduled { cursor, .. }
            | CircuitBreakerEvent::RetrySucceeded { cursor, .. }
            | CircuitBreakerEvent::RetryExhausted { cursor, .. }
            | CircuitBreakerEvent::RetryStoppedNonRetryable { cursor, .. } => {
                cursor == expected_cursor
            }
            _ => unreachable!("retry_events returns only retry evidence"),
        }));
    }
}

#[tokio::test(start_paused = true)]
async fn pending_continuation_stops_when_another_invocation_opens_the_circuit() {
    let (breaker, control, stage_id) = retrying_breaker_fixture(
        CircuitBreaker::opens_after(1)
            .retry(
                Retry::fixed(Duration::from_secs(1))
                    .attempts(2)
                    .max_delay(Duration::from_secs(1))
                    .start_window(Duration::from_secs(5)),
            )
            .build(),
    );
    let boundary = Arc::new(boundary_with_chain(vec![breaker]));
    let first_attempt_finished = Arc::new(tokio::sync::Notify::new());
    let pending_calls = Arc::new(AtomicUsize::new(0));
    let pending_task = {
        let boundary = boundary.clone();
        let first_attempt_finished = first_attempt_finished.clone();
        let pending_calls = pending_calls.clone();
        tokio::spawn(async move {
            boundary
                .around_effect(
                    &identity_for("effect.retry"),
                    &data_event(),
                    scripted_operation(pending_calls, move |_| {
                        first_attempt_finished.notify_one();
                        Err(EffectError::Timeout("pending".to_string()))
                    }),
                )
                .await
        })
    };

    first_attempt_finished.notified().await;
    tokio::task::yield_now().await;

    let opening_calls = Arc::new(AtomicUsize::new(0));
    let opening_report = boundary
        .around_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(opening_calls.clone(), |_| {
                Err(EffectError::Permanent("open now".to_string()))
            }),
        )
        .await;
    assert!(matches!(
        opening_report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Permanent(_)))
    ));
    assert_eq!(opening_calls.load(Ordering::SeqCst), 1);

    tokio::time::advance(Duration::from_secs(1)).await;
    let pending_report = pending_task
        .await
        .expect("pending invocation should finish");
    assert!(matches!(
        pending_report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
    ));
    assert_eq!(pending_calls.load(Ordering::SeqCst), 1);
    assert!(retry_events(&pending_report).iter().any(|event| matches!(
        event,
        CircuitBreakerEvent::RetryExhausted {
            total_attempts: 1,
            reason: CircuitBreakerRetryStopReason::CircuitNoLongerClosed,
            ..
        }
    )));

    let snapshots = control.effect_circuit_breaker_snapshotters(&stage_id);
    let metrics = snapshots[0].1();
    assert_eq!(metrics.requests_total, 1);
    assert_eq!(metrics.failures_total, 1);
}

#[tokio::test(start_paused = true)]
async fn post_sleep_window_exhaustion_settles_breaker_health() {
    let (breaker, control, stage_id) = retrying_breaker_fixture(
        CircuitBreaker::opens_after(5)
            .retry(
                Retry::fixed(Duration::from_millis(100))
                    .attempts(3)
                    .max_delay(Duration::from_secs(1))
                    .start_window(Duration::from_millis(150)),
            )
            .build(),
    );
    let boundary = Arc::new(boundary_with_chain(vec![breaker]));
    let first_attempt_finished = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let task = {
        let boundary = boundary.clone();
        let first_attempt_finished = first_attempt_finished.clone();
        let calls = calls.clone();
        tokio::spawn(async move {
            boundary
                .around_effect(
                    &identity_for("effect.retry"),
                    &data_event(),
                    scripted_operation(calls, move |_| {
                        first_attempt_finished.notify_one();
                        Err(EffectError::Timeout("late".to_string()))
                    }),
                )
                .await
        })
    };

    first_attempt_finished.notified().await;
    tokio::task::yield_now().await;
    // The 100ms delay passed the pre-sleep check (0 + 100 < 150); jumping
    // to 200ms fires the sleep with the window already exceeded, which is
    // the overshoot the post-sleep re-check exists for.
    tokio::time::advance(Duration::from_millis(200)).await;

    let report = task.await.expect("invocation should finish");
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(retry_events(&report).iter().any(|event| matches!(
        event,
        CircuitBreakerEvent::RetryExhausted {
            total_attempts: 1,
            reason: CircuitBreakerRetryStopReason::AttemptStartWindow,
            ..
        }
    )));

    let snapshots = control.effect_circuit_breaker_snapshotters(&stage_id);
    let metrics = snapshots[0].1();
    assert_eq!(metrics.requests_total, 1);
    assert_eq!(metrics.failures_total, 1);
}

struct GateSecondAdmission {
    admissions: AtomicUsize,
    second_admitted: Arc<tokio::sync::Notify>,
    release_second: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl EffectPolicy for GateSecondAdmission {
    fn label(&self) -> &'static str {
        "test.gate_second_admission"
    }

    async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
        if self.admissions.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
            self.second_admitted.notify_one();
            self.release_second.notified().await;
        }
        PolicyAdmission::Admit
    }

    fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}
}

#[tokio::test]
async fn continuation_already_admitted_by_an_inner_policy_completes_after_opening() {
    let breaker = retrying_breaker_attachment(
        CircuitBreaker::opens_after(1)
            .retry(Retry::fixed(Duration::ZERO).attempts(2))
            .build(),
    );
    let second_admitted = Arc::new(tokio::sync::Notify::new());
    let release_second = Arc::new(tokio::sync::Notify::new());
    let gate = EffectPolicyAttachment::neutral(Arc::new(GateSecondAdmission {
        admissions: AtomicUsize::new(0),
        second_admitted: second_admitted.clone(),
        release_second: release_second.clone(),
    }));
    let boundary = Arc::new(boundary_with_chain(vec![breaker, gate]));

    let recovering_calls = Arc::new(AtomicUsize::new(0));
    let recovering_task = {
        let boundary = boundary.clone();
        let recovering_calls = recovering_calls.clone();
        tokio::spawn(async move {
            boundary
                .around_effect(
                    &identity_for("effect.retry"),
                    &data_event(),
                    scripted_operation(recovering_calls, |call| {
                        if call == 1 {
                            Err(EffectError::Timeout("recover".to_string()))
                        } else {
                            Ok(Vec::new())
                        }
                    }),
                )
                .await
        })
    };

    second_admitted.notified().await;
    let opening_calls = Arc::new(AtomicUsize::new(0));
    let opening_report = boundary
        .around_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(opening_calls.clone(), |_| {
                Err(EffectError::Permanent("open now".to_string()))
            }),
        )
        .await;
    assert!(matches!(
        opening_report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Permanent(_)))
    ));
    assert_eq!(opening_calls.load(Ordering::SeqCst), 1);

    release_second.notify_one();
    let recovering_report = recovering_task
        .await
        .expect("admitted continuation should finish");
    assert!(matches!(
        recovering_report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(recovering_calls.load(Ordering::SeqCst), 2);
    assert!(retry_events(&recovering_report)
        .iter()
        .any(|event| matches!(event, CircuitBreakerEvent::RetrySucceeded { .. })));
}

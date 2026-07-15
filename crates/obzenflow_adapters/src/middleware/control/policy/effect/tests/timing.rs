// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Backoff, jitter, floors, the attempt-start window, cancellation, and the
//! slow-call session sample.

use super::support::*;

#[tokio::test(start_paused = true)]
async fn fixed_backoff_waits_exactly_before_each_continuation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(
        CircuitBreakerBuilder::new(3)
            .with_retry_fixed(Duration::from_millis(7), 3)
            .with_retry_limits(RetryLimits {
                max_single_delay: Duration::from_secs(1),
                max_attempt_start_window: Duration::from_secs(1),
            }),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let started = tokio::time::Instant::now();
    let operation = scripted_operation(calls.clone(), |call| {
        if call < 3 {
            Err(EffectError::Timeout("try again".to_string()))
        } else {
            Ok(Vec::new())
        }
    });

    let report = boundary
        .around_effect(&identity_for("effect.retry"), &data_event(), operation)
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(scheduled_delays(&report), [7, 7]);
    assert_eq!(started.elapsed(), Duration::from_millis(14));
}

#[tokio::test(start_paused = true)]
async fn exponential_backoff_applies_jitter_before_the_single_delay_cap() {
    let calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(
        CircuitBreakerBuilder::new(5)
            .with_retry_exponential(4)
            .with_retry_jitter_samples(vec![0.0, 0.0, 0.5])
            .with_retry_limits(RetryLimits {
                max_single_delay: Duration::from_millis(500),
                max_attempt_start_window: Duration::from_secs(5),
            }),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let operation = scripted_operation(calls.clone(), |call| {
        if call < 4 {
            Err(EffectError::Transport("try again".to_string()))
        } else {
            Ok(Vec::new())
        }
    });

    let report = boundary
        .around_effect(&identity_for("effect.retry"), &data_event(), operation)
        .await;

    let delays = scheduled_delays(&report);
    assert_eq!(calls.load(Ordering::SeqCst), 4);
    assert_eq!(delays.len(), 3);
    assert_eq!(delays, [225, 450, 500]);
}

#[tokio::test(start_paused = true)]
async fn raw_rate_limit_floor_cannot_be_shortened_by_classifier_or_backoff_cap() {
    let calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(
        CircuitBreakerBuilder::new(3)
            .with_retry_fixed(Duration::from_millis(1), 2)
            .with_retry_limits(RetryLimits {
                max_single_delay: Duration::from_millis(5),
                max_attempt_start_window: Duration::from_secs(1),
            })
            .with_failure_classification_classifier(|_, _| {
                FailureClassification::RateLimited(Duration::from_millis(10))
            }),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let operation = scripted_operation(calls.clone(), |call| {
        if call == 1 {
            Err(EffectError::RateLimited {
                message: "quota".to_string(),
                retry_after: Duration::from_millis(50),
            })
        } else {
            Ok(Vec::new())
        }
    });

    let report = boundary
        .around_effect(&identity_for("effect.retry"), &data_event(), operation)
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(scheduled_delays(&report), [50]);
}

#[tokio::test(start_paused = true)]
async fn rate_limit_floor_at_start_window_prevents_another_attempt() {
    let calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(
        CircuitBreakerBuilder::new(3)
            .with_retry_fixed(Duration::ZERO, 2)
            .with_retry_limits(RetryLimits {
                max_single_delay: Duration::from_millis(1),
                max_attempt_start_window: Duration::from_millis(100),
            }),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let operation = scripted_operation(calls.clone(), |_| {
        Err(EffectError::RateLimited {
            message: "quota".to_string(),
            retry_after: Duration::from_millis(100),
        })
    });

    let report = boundary
        .around_effect(&identity_for("effect.retry"), &data_event(), operation)
        .await;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(scheduled_delays(&report).is_empty());
    assert!(retry_events(&report).iter().any(|event| matches!(
        event,
        CircuitBreakerEvent::RetryExhausted {
            total_attempts: 1,
            reason: CircuitBreakerRetryStopReason::AttemptStartWindow,
            ..
        }
    )));
}

#[tokio::test]
async fn cancelling_during_backoff_drops_the_sequence_without_a_later_call() {
    let calls = Arc::new(AtomicUsize::new(0));
    let first_call = Arc::new(tokio::sync::Notify::new());
    let breaker = retrying_breaker_attachment(
        CircuitBreakerBuilder::new(3).with_retry_fixed(Duration::from_secs(60), 2),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let operation = {
        let first_call = first_call.clone();
        scripted_operation(calls.clone(), move |_| {
            first_call.notify_one();
            Err(EffectError::Timeout("wait".to_string()))
        })
    };
    let task = tokio::spawn(async move {
        boundary
            .around_effect(&identity_for("effect.retry"), &data_event(), operation)
            .await
    });

    first_call.notified().await;
    tokio::task::yield_now().await;
    task.abort();
    let cancellation = task.await;
    assert!(matches!(cancellation, Err(error) if error.is_cancelled()));
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn recovered_session_settles_breaker_health_once_and_can_be_slow() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (breaker, control, stage_id) = retrying_breaker_fixture(
        CircuitBreakerBuilder::new(3)
            .rate_based_over_last_n_calls(1, 1.0)
            .slow_call(Duration::from_millis(5), 1.0)
            .with_retry_fixed(Duration::from_millis(10), 2),
    );
    let report = boundary_with_chain(vec![breaker])
        .around_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(calls.clone(), |call| {
                if call == 1 {
                    Err(EffectError::Timeout("again".to_string()))
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
    let snapshots = control.effect_circuit_breaker_snapshotters(&stage_id);
    assert_eq!(snapshots.len(), 1);
    let metrics = snapshots[0].1();
    assert_eq!(metrics.requests_total, 1);
    assert_eq!(metrics.successes_total, 1);
    assert_eq!(metrics.failures_total, 0);
    assert_eq!(
        metrics.opened_total, 1,
        "the whole 10 ms session is one slow sample"
    );
}

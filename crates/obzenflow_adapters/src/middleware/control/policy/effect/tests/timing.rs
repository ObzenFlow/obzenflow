// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Backoff, jitter, floors, the attempt-start window, cancellation, and the
//! slow-call session sample.

use super::support::*;
use crate::middleware::{EffectResilience, RateLimiter};

#[tokio::test(start_paused = true)]
async fn fixed_backoff_waits_exactly_before_each_continuation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let breaker = retrying_breaker_attachment(
        CircuitBreaker::opens_after(3)
            .retry(
                Retry::fixed(Duration::from_millis(7))
                    .attempts(3)
                    .max_delay(Duration::from_secs(1))
                    .start_window(Duration::from_secs(1)),
            )
            .build(),
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
        .around_repeatable_effect(&identity_for("effect.retry"), &data_event(), operation)
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
        CircuitBreaker::opens_after(5)
            .retry(
                Retry::exponential()
                    .attempts(4)
                    .jitter_samples(vec![0.0, 0.0, 0.5])
                    .max_delay(Duration::from_millis(500))
                    .start_window(Duration::from_secs(5)),
            )
            .build(),
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
        .around_repeatable_effect(&identity_for("effect.retry"), &data_event(), operation)
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
        CircuitBreaker::opens_after(3)
            .retry(
                Retry::fixed(Duration::from_millis(1))
                    .attempts(2)
                    .max_delay(Duration::from_millis(5))
                    .start_window(Duration::from_secs(1)),
            )
            .with_failure_classification(|_, _| {
                FailureClassification::RateLimited(Duration::from_millis(10))
            })
            .build(),
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
        .around_repeatable_effect(&identity_for("effect.retry"), &data_event(), operation)
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
        CircuitBreaker::opens_after(3)
            .retry(
                Retry::fixed(Duration::ZERO)
                    .attempts(2)
                    .max_delay(Duration::from_millis(1))
                    .start_window(Duration::from_millis(100)),
            )
            .build(),
    );
    let boundary = boundary_with_chain(vec![breaker]);
    let operation = scripted_operation(calls.clone(), |_| {
        Err(EffectError::RateLimited {
            message: "quota".to_string(),
            retry_after: Duration::from_millis(100),
        })
    });

    let report = boundary
        .around_repeatable_effect(&identity_for("effect.retry"), &data_event(), operation)
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
        CircuitBreaker::opens_after(3)
            .retry(Retry::fixed(Duration::from_secs(60)).attempts(2))
            .build(),
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
            .around_repeatable_effect(&identity_for("effect.retry"), &data_event(), operation)
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
        CircuitBreaker::opens_when(
            failure_rate(1.0)
                .over_last_calls(1)
                .or_slow_calls_over(Duration::from_millis(5), 1.0),
        )
        .retry(Retry::fixed(Duration::from_millis(10)).attempts(2))
        .build(),
    );
    let report = boundary_with_chain(vec![breaker])
        .around_repeatable_effect(
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

#[tokio::test(start_paused = true)]
async fn resilience_limiter_wait_is_not_a_slow_dependency_sample() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .count_window(2)
            .minimum_calls(2)
            .slow_call_duration(Duration::from_millis(50))
            .slow_call_rate_threshold(0.5)
            .open_for(Duration::from_secs(5))
            .build()
            .expect("slow-call test breaker"),
    )
    .rate_limit_each_attempt(
        RateLimiter::per_second(1.0)
            .unwrap()
            .with_burst(1.0)
            .unwrap(),
    )
    .build()
    .expect("slow-call resilience aggregate");
    let config = test_stage_config(factory.as_ref());
    let stage_id = config.stage_id;
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let attachment = materialize_effect_attachment(
        factory.as_ref(),
        &config,
        &control,
        0,
        EffectSafety::Idempotent,
    )
    .expect("aggregate should materialize");
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));
    let started = tokio::time::Instant::now();

    let first = boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(calls.clone(), |_| Ok(Vec::new())),
        )
        .await;
    let second = boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(calls.clone(), |_| Ok(Vec::new())),
        )
        .await;

    assert_eq!(started.elapsed(), Duration::from_secs(1));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let attempt = second
        .control_events
        .iter()
        .find_map(|event| match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::AttemptSettled {
                    slow,
                    dependency_elapsed_ms,
                    admission_wait_ms,
                    ..
                }),
            )) => Some((*slow, *dependency_elapsed_ms, *admission_wait_ms)),
            _ => None,
        })
        .expect("second call should publish one physical-attempt row");
    assert_eq!(attempt, (false, 0, 1_000));
    assert!(matches!(
        first.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert!(matches!(
        second.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));

    let breaker = control.effect_circuit_breaker_snapshotters(&stage_id);
    let metrics = breaker[0].1();
    assert_eq!(metrics.requests_total, 2);
    assert_eq!(metrics.successes_total, 2);
    assert_eq!(metrics.failures_total, 0);
    assert_eq!(metrics.opened_total, 0);
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 2);
}

#[tokio::test(start_paused = true)]
async fn final_breaker_admission_cancels_a_stale_limiter_reservation() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(1)
            .open_for(Duration::from_secs(30))
            .build()
            .expect("concurrency test breaker"),
    )
    .rate_limit_each_attempt(
        RateLimiter::per_second(1.0)
            .unwrap()
            .with_burst(1.0)
            .unwrap(),
    )
    .build()
    .expect("concurrency resilience aggregate");
    let config = test_stage_config(factory.as_ref());
    let stage_id = config.stage_id;
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let attachment = materialize_effect_attachment(
        factory.as_ref(),
        &config,
        &control,
        0,
        EffectSafety::Idempotent,
    )
    .expect("aggregate should materialize");
    let boundary = Arc::new(boundary_with_chain(vec![attachment]));
    let first_started = Arc::new(tokio::sync::Notify::new());
    let release_first = Arc::new(tokio::sync::Notify::new());

    let first_task = {
        let boundary = boundary.clone();
        let first_started = first_started.clone();
        let release_first = release_first.clone();
        tokio::spawn(async move {
            boundary
                .around_repeatable_effect(
                    &identity_for("effect.retry"),
                    &data_event(),
                    RepeatableEffectOperation::new(move || {
                        let first_started = first_started.clone();
                        let release_first = release_first.clone();
                        async move {
                            first_started.notify_one();
                            release_first.notified().await;
                            Err(EffectError::Timeout("opens circuit".to_string()))
                        }
                    }),
                )
                .await
        })
    };
    first_started.notified().await;

    let second_calls = Arc::new(AtomicUsize::new(0));
    let second_task = {
        let boundary = boundary.clone();
        let second_calls = second_calls.clone();
        tokio::spawn(async move {
            boundary
                .around_repeatable_effect(
                    &identity_for("effect.retry"),
                    &data_event(),
                    scripted_operation(second_calls, |_| Ok(Vec::new())),
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 1);

    release_first.notify_one();
    let first = first_task.await.unwrap();
    assert!(matches!(
        first.outcome,
        EffectBoundaryOutcome::Executed(Err(_))
    ));

    tokio::time::advance(Duration::from_secs(1)).await;
    let second = second_task.await.unwrap();
    assert!(matches!(second.outcome, EffectBoundaryOutcome::Aborted(_)));
    assert_eq!(second_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        effect_limiter_events(control.as_ref(), stage_id),
        1,
        "the breaker-rejected reservation must not become a committed permit"
    );

    let breaker = control.effect_circuit_breaker_snapshotters(&stage_id);
    let metrics = breaker[0].1();
    assert_eq!(metrics.requests_total, 1);
    assert_eq!(metrics.failures_total, 1);
    assert_eq!(metrics.rejections_total, 1);
}

#[tokio::test]
async fn cancelling_an_in_flight_resilience_attempt_records_no_health_sample() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(2)
            .build()
            .expect("cancellation test breaker"),
    )
    .rate_limit_each_attempt(RateLimiter::per_second(100.0).unwrap())
    .build()
    .expect("cancellation resilience aggregate");
    let config = test_stage_config(factory.as_ref());
    let stage_id = config.stage_id;
    let control = Arc::new(ControlMiddlewareAggregator::new());
    let attachment = materialize_effect_attachment(
        factory.as_ref(),
        &config,
        &control,
        0,
        EffectSafety::Idempotent,
    )
    .expect("aggregate should materialize");
    let boundary = boundary_with_chain(vec![attachment]);
    let started = Arc::new(tokio::sync::Notify::new());
    let task = {
        let started = started.clone();
        tokio::spawn(async move {
            boundary
                .around_repeatable_effect(
                    &identity_for("effect.retry"),
                    &data_event(),
                    RepeatableEffectOperation::new(move || {
                        let started = started.clone();
                        async move {
                            started.notify_one();
                            std::future::pending::<Result<Vec<ChainEvent>, EffectError>>().await
                        }
                    }),
                )
                .await
        })
    };

    started.notified().await;
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 1);
    let breaker = control.effect_circuit_breaker_snapshotters(&stage_id);
    let metrics = breaker[0].1();
    assert_eq!(metrics.requests_total, 1);
    assert_eq!(metrics.successes_total, 0);
    assert_eq!(metrics.failures_total, 0);
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Recovery timing, cancellation, and shared-authority laws for the final
//! `EffectResilience` attachment (FLOWIP-115n).

use super::support::*;
use crate::middleware::{EffectResilience, RateLimiter};
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventContent;

fn scripted_operation(
    calls: Arc<AtomicUsize>,
    result_for_call: impl Fn(usize) -> Result<Vec<ChainEvent>, EffectError> + Send + Sync + 'static,
) -> RepeatableEffectOperation {
    let result_for_call = Arc::new(result_for_call);
    RepeatableEffectOperation::new(move || {
        let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
        let result_for_call = result_for_call.clone();
        async move { result_for_call(call) }
    })
}

fn materialized_resilience(
    factory: Box<dyn crate::middleware::MiddlewareFactory>,
) -> (
    EffectPolicyAttachment,
    Arc<ControlMiddlewareAggregator>,
    StageId,
) {
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
    .expect("resilience aggregate should materialize for an idempotent effect");
    (attachment, control, stage_id)
}

fn retry_delays(report: &obzenflow_runtime::effects::EffectBoundaryReport) -> Vec<u64> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::RetryScheduled {
                    delay_ms,
                    ..
                }),
            )) => Some(*delay_ms),
            _ => None,
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn fixed_backoff_delays_each_physical_continuation() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(4)
            .open_for(Duration::from_secs(5))
            .build()
            .expect("test breaker"),
    )
    .retry(
        Retry::fixed(Duration::from_millis(7))
            .max_attempts(3)
            .max_backoff(Duration::from_secs(1))
            .attempt_start_window(Duration::from_secs(1)),
    )
    .build()
    .expect("retrying resilience aggregate");
    let (attachment, _, _) = materialized_resilience(factory);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));
    let started = tokio::time::Instant::now();

    let report = boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(calls.clone(), |call| {
                if call < 3 {
                    Err(EffectError::Timeout("try again".to_string()))
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
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(retry_delays(&report), [7, 7]);
    assert_eq!(started.elapsed(), Duration::from_millis(14));
}

#[tokio::test]
async fn cancellation_during_backoff_starts_no_later_attempt() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(3)
            .build()
            .expect("test breaker"),
    )
    .retry(Retry::fixed(Duration::from_secs(60)).max_attempts(2))
    .build()
    .expect("retrying resilience aggregate");
    let (attachment, _, _) = materialized_resilience(factory);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));
    let first_call = Arc::new(tokio::sync::Notify::new());
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
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn another_invocation_opening_the_circuit_stops_a_pending_continuation() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(2)
            .open_for(Duration::from_secs(30))
            .build()
            .expect("shared breaker"),
    )
    .retry(
        Retry::fixed(Duration::from_millis(10))
            .max_attempts(2)
            .attempt_start_window(Duration::from_secs(1)),
    )
    .build()
    .expect("retrying resilience aggregate");
    let (attachment, _, _) = materialized_resilience(factory);
    let boundary = Arc::new(boundary_with_chain(vec![attachment]));
    let first_calls = Arc::new(AtomicUsize::new(0));
    let first_settled = Arc::new(tokio::sync::Notify::new());

    let pending = {
        let boundary = boundary.clone();
        let first_settled = first_settled.clone();
        let first_calls = first_calls.clone();
        tokio::spawn(async move {
            boundary
                .around_repeatable_effect(
                    &identity_for("effect.retry"),
                    &data_event(),
                    scripted_operation(first_calls, move |_| {
                        first_settled.notify_one();
                        Err(EffectError::Timeout("first invocation error".to_string()))
                    }),
                )
                .await
        })
    };
    first_settled.notified().await;
    tokio::task::yield_now().await;

    let mut second_identity = identity_for("effect.retry");
    second_identity.cursor = EffectCursor::new("test_flow", "test_stage", 2, 0);
    let opening = boundary
        .around_repeatable_effect(
            &second_identity,
            &data_event(),
            RepeatableEffectOperation::new(|| async {
                Err(EffectError::Timeout("opening invocation error".to_string()))
            }),
        )
        .await;
    assert!(matches!(
        opening.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(ref message)))
            if message == "opening invocation error"
    ));

    tokio::time::advance(Duration::from_millis(10)).await;
    let pending = pending.await.unwrap();
    assert!(matches!(
        pending.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(ref message)))
            if message == "first invocation error"
    ));
    assert_eq!(
        first_calls.load(Ordering::SeqCst),
        1,
        "continuation denial must preserve the last physical error without another call"
    );
}

#[tokio::test]
async fn open_half_open_recovery_and_chronic_failure_share_one_authority() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(1)
            .open_for(Duration::from_millis(2))
            .probes(1)
            .build()
            .expect("recovery test breaker"),
    )
    .build()
    .expect("recovery resilience aggregate");
    let (attachment, control, stage_id) = materialized_resilience(factory);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));

    let first = boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(calls.clone(), |_| {
                Err(EffectError::Timeout("initial outage".to_string()))
            }),
        )
        .await;
    assert!(matches!(
        first.outcome,
        EffectBoundaryOutcome::Executed(Err(_))
    ));

    let rejected = boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(calls.clone(), |_| Ok(Vec::new())),
        )
        .await;
    assert!(matches!(
        rejected.outcome,
        EffectBoundaryOutcome::Aborted(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    tokio::time::sleep(Duration::from_millis(10)).await;
    let probe = boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(calls.clone(), |_| Ok(Vec::new())),
        )
        .await;
    assert!(matches!(
        probe.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));

    let chronic = boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(calls.clone(), |_| {
                Err(EffectError::Transport(
                    "dependency failed again".to_string(),
                ))
            }),
        )
        .await;
    assert!(matches!(
        chronic.outcome,
        EffectBoundaryOutcome::Executed(Err(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 3);

    let breaker = control.effect_circuit_breaker_snapshotters(&stage_id);
    let metrics = breaker[0].1();
    assert_eq!(metrics.requests_total, 3);
    assert_eq!(metrics.successes_total, 1);
    assert_eq!(metrics.failures_total, 2);
    assert_eq!(metrics.rejections_total, 1);
    assert_eq!(metrics.opened_total, 2);
    assert!(matches!(
        metrics.state,
        obzenflow_runtime::control_plane::CircuitBreakerState::Open
    ));
}

#[tokio::test(start_paused = true)]
async fn limiter_wait_is_not_a_slow_dependency_sample() {
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
    .expect("rate-limited resilience aggregate");
    let (attachment, control, stage_id) = materialized_resilience(factory);
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
    let settled = second
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
    assert_eq!(settled, (false, 0, 1_000));
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
    assert_eq!(metrics.slow_total, 0);
    assert_eq!(metrics.opened_total, 0);
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 2);
}

#[tokio::test(start_paused = true)]
async fn circuit_opening_cancels_a_queued_limiter_reservation() {
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
    .expect("concurrent resilience aggregate");
    let (attachment, control, stage_id) = materialized_resilience(factory);
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

#[tokio::test(start_paused = true)]
async fn cancellation_during_limiter_wait_commits_no_permit_or_attempt() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(2)
            .build()
            .expect("cancellation test breaker"),
    )
    .rate_limit_each_attempt(
        RateLimiter::per_second(1.0)
            .unwrap()
            .with_burst(1.0)
            .unwrap(),
    )
    .build()
    .expect("rate-limited resilience aggregate");
    let (attachment, control, stage_id) = materialized_resilience(factory);
    let boundary = Arc::new(boundary_with_chain(vec![attachment]));
    let calls = Arc::new(AtomicUsize::new(0));

    let first = boundary
        .around_repeatable_effect(
            &identity_for("effect.retry"),
            &data_event(),
            scripted_operation(calls.clone(), |_| Ok(Vec::new())),
        )
        .await;
    assert!(matches!(
        first.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));

    let waiting = {
        let boundary = boundary.clone();
        let calls = calls.clone();
        tokio::spawn(async move {
            boundary
                .around_repeatable_effect(
                    &identity_for("effect.retry"),
                    &data_event(),
                    scripted_operation(calls, |_| Ok(Vec::new())),
                )
                .await
        })
    };
    tokio::task::yield_now().await;
    waiting.abort();
    assert!(matches!(waiting.await, Err(error) if error.is_cancelled()));
    tokio::time::advance(Duration::from_secs(2)).await;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 1);
    let breaker = control.effect_circuit_breaker_snapshotters(&stage_id);
    let metrics = breaker[0].1();
    assert_eq!(metrics.requests_total, 1);
    assert_eq!(metrics.successes_total, 1);
    assert_eq!(metrics.failures_total, 0);
}

#[tokio::test]
async fn cancellation_in_flight_records_an_attempt_without_a_health_sample() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(2)
            .build()
            .expect("cancellation test breaker"),
    )
    .rate_limit_each_attempt(RateLimiter::per_second(100.0).unwrap())
    .build()
    .expect("cancellation resilience aggregate");
    let (attachment, control, stage_id) = materialized_resilience(factory);
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

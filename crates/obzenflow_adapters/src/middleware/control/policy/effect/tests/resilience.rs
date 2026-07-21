// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Recovery timing, cancellation, and shared-authority laws for the final
//! `EffectResilience` attachment (FLOWIP-115n).

use super::support::*;
use crate::middleware::{EffectResilience, RateLimiter};
use obzenflow_core::config::{ConfigAddress, ConfigSource};
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, CircuitBreakerRetryStopReason, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventContent;
use obzenflow_runtime::runtime_config::{
    CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
    RESILIENCE_BREAKER_CONSECUTIVE_FAILURES_KEY, RESILIENCE_BREAKER_COUNT_WINDOW_KEY,
    RESILIENCE_BREAKER_FAILURE_RATE_THRESHOLD_KEY, RESILIENCE_BREAKER_MINIMUM_CALLS_KEY,
    RESILIENCE_BREAKER_MODE_KEY, RESILIENCE_RATE_LIMITER_BURST_CAPACITY_KEY,
    RESILIENCE_RETRY_FIXED_DELAY_MS_KEY, RESILIENCE_RETRY_KIND_KEY,
};

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
    materialized_resilience_with_snapshot(factory, &ResolvedRuntimeConfig::builtin_defaults())
}

fn materialized_resilience_with_snapshot(
    factory: Box<dyn crate::middleware::MiddlewareFactory>,
    snapshot: &ResolvedRuntimeConfig,
) -> (
    EffectPolicyAttachment,
    Arc<ControlMiddlewareAggregator>,
    StageId,
) {
    let config = test_stage_config_for_factories_with_snapshot(&[factory.as_ref()], snapshot);
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

fn file_effect_snapshot(entries: &[(&str, ConfigValue)]) -> ResolvedRuntimeConfig {
    let mut candidates = CandidateSet::default();
    for (key_path, value) in entries {
        candidates
            .admit(ScopedCandidate {
                key_path: (*key_path).to_string(),
                address: ConfigAddress::effect("retrying_breaker_test", "effect.retry"),
                source: ConfigSource::File,
                value: value.clone(),
            })
            .expect("test file candidate should be valid");
    }
    ResolvedRuntimeConfig::new(candidates)
}

#[test]
fn consumed_keys_cover_optional_and_mode_dependent_fields_without_creating_components() {
    let breaker_only = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(2)
            .build()
            .expect("breaker-only config"),
    )
    .build()
    .expect("breaker-only aggregate");
    let breaker_keys = breaker_only.consumed_config_keys();
    assert!(breaker_keys.contains(&RESILIENCE_BREAKER_COUNT_WINDOW_KEY));
    assert!(breaker_keys.contains(&RESILIENCE_BREAKER_CONSECUTIVE_FAILURES_KEY));
    assert!(!breaker_keys.contains(&RESILIENCE_RETRY_KIND_KEY));
    assert!(!breaker_keys.contains(&RESILIENCE_RATE_LIMITER_BURST_CAPACITY_KEY));

    let complete = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(2)
            .build()
            .expect("complete config breaker"),
    )
    .retry(Retry::exponential().max_attempts(2))
    .rate_limit_each_attempt(RateLimiter::per_second(10.0).expect("test limiter"))
    .build()
    .expect("complete aggregate");
    let complete_keys = complete.consumed_config_keys();
    assert!(complete_keys.contains(&RESILIENCE_RETRY_FIXED_DELAY_MS_KEY));
    assert!(complete_keys.contains(&RESILIENCE_RATE_LIMITER_BURST_CAPACITY_KEY));
}

#[tokio::test]
async fn file_configuration_can_switch_consecutive_breaker_to_rate_based() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(1)
            .build()
            .expect("consecutive builder"),
    )
    .build()
    .expect("switchable aggregate");
    let snapshot = file_effect_snapshot(&[
        (
            RESILIENCE_BREAKER_MODE_KEY,
            ConfigValue::Text("rate_based".to_string()),
        ),
        (RESILIENCE_BREAKER_COUNT_WINDOW_KEY, ConfigValue::U64(2)),
        (RESILIENCE_BREAKER_MINIMUM_CALLS_KEY, ConfigValue::U64(2)),
        (
            RESILIENCE_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
            ConfigValue::F64(1.0),
        ),
    ]);
    let (attachment, _, _) = materialized_resilience_with_snapshot(factory, &snapshot);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));

    let first = boundary
        .around_repeatable_effect(
            &identity_at(101),
            &data_event(),
            scripted_operation(calls.clone(), |_| {
                Err(EffectError::Timeout("first rate sample".to_string()))
            }),
        )
        .await;
    assert!(matches!(
        first.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
    ));
    let second = boundary
        .around_repeatable_effect(
            &identity_at(102),
            &data_event(),
            scripted_operation(calls.clone(), |_| Ok(Vec::new())),
        )
        .await;
    assert!(matches!(
        second.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn file_configuration_can_switch_rate_based_breaker_to_consecutive() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .count_window(5)
            .minimum_calls(5)
            .failure_rate_threshold(1.0)
            .build()
            .expect("rate-based builder"),
    )
    .build()
    .expect("switchable aggregate");
    let snapshot = file_effect_snapshot(&[
        (
            RESILIENCE_BREAKER_MODE_KEY,
            ConfigValue::Text("consecutive".to_string()),
        ),
        (
            RESILIENCE_BREAKER_CONSECUTIVE_FAILURES_KEY,
            ConfigValue::U64(1),
        ),
    ]);
    let (attachment, _, _) = materialized_resilience_with_snapshot(factory, &snapshot);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));

    let _ = boundary
        .around_repeatable_effect(
            &identity_at(103),
            &data_event(),
            scripted_operation(calls.clone(), |_| {
                Err(EffectError::Timeout("open immediately".to_string()))
            }),
        )
        .await;
    let rejected = boundary
        .around_repeatable_effect(
            &identity_at(104),
            &data_event(),
            scripted_operation(calls.clone(), |_| Ok(Vec::new())),
        )
        .await;
    assert!(matches!(
        rejected.outcome,
        EffectBoundaryOutcome::Aborted(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn file_configuration_can_switch_retry_backoff_kind() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(3)
            .build()
            .expect("retry switch breaker"),
    )
    .retry(Retry::exponential().max_attempts(2))
    .build()
    .expect("switchable retry aggregate");
    let snapshot = file_effect_snapshot(&[
        (
            RESILIENCE_RETRY_KIND_KEY,
            ConfigValue::Text("fixed".to_string()),
        ),
        (RESILIENCE_RETRY_FIXED_DELAY_MS_KEY, ConfigValue::U64(7)),
    ]);
    let (attachment, _, _) = materialized_resilience_with_snapshot(factory, &snapshot);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));

    let report = boundary
        .around_repeatable_effect(
            &identity_at(105),
            &data_event(),
            scripted_operation(calls, |call| {
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
    assert_eq!(retry_delays(&report), [7]);
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

fn recovery_completions(
    report: &obzenflow_runtime::effects::EffectBoundaryReport,
) -> Vec<(u32, u64, u64)> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::RecoveryCompleted {
                    total_attempts,
                    backoff_elapsed_ms,
                    recovery_elapsed_ms,
                    ..
                }),
            )) => Some((*total_attempts, *backoff_elapsed_ms, *recovery_elapsed_ms)),
            _ => None,
        })
        .collect()
}

fn identity_at(input_seq: u64) -> EffectIdentity {
    let mut identity = identity_for("effect.retry");
    identity.cursor = EffectCursor::new("test_flow", "test_stage", input_seq, 0);
    identity
}

fn settled_attempts(report: &obzenflow_runtime::effects::EffectBoundaryReport) -> Vec<u32> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::AttemptSettled {
                    attempt,
                    ..
                }),
            )) => Some(*attempt),
            _ => None,
        })
        .collect()
}

fn scheduled_attempts(report: &obzenflow_runtime::effects::EffectBoundaryReport) -> Vec<u32> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::RetryScheduled {
                    next_attempt,
                    ..
                }),
            )) => Some(*next_attempt),
            _ => None,
        })
        .collect()
}

fn retry_exhaustions(
    report: &obzenflow_runtime::effects::EffectBoundaryReport,
) -> Vec<(u32, CircuitBreakerRetryStopReason)> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::RetryExhausted {
                    total_attempts,
                    reason,
                    ..
                }),
            )) => Some((*total_attempts, *reason)),
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
    assert_eq!(recovery_completions(&report), [(3, 14, 14)]);
}

#[tokio::test(start_paused = true)]
async fn initial_limiter_wait_does_not_consume_attempt_start_window() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(3)
            .build()
            .expect("attempt-window breaker"),
    )
    .retry(
        Retry::fixed(Duration::from_millis(1))
            .max_attempts(2)
            .attempt_start_window(Duration::from_millis(10)),
    )
    .rate_limit_each_attempt(
        RateLimiter::per_second(1.0)
            .unwrap()
            .with_burst(1.0)
            .unwrap(),
    )
    .build()
    .expect("attempt-window resilience aggregate");
    let (attachment, _, _) = materialized_resilience(factory);
    let boundary = boundary_with_chain(vec![attachment]);

    let filler = boundary
        .around_repeatable_effect(
            &identity_at(21),
            &data_event(),
            RepeatableEffectOperation::new(|| async { Ok(Vec::new()) }),
        )
        .await;
    assert!(matches!(
        filler.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));

    let calls = Arc::new(AtomicUsize::new(0));
    let started = tokio::time::Instant::now();
    let report = boundary
        .around_repeatable_effect(
            &identity_at(22),
            &data_event(),
            scripted_operation(calls.clone(), |_| Ok(Vec::new())),
        )
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(started.elapsed(), Duration::from_secs(1));
    assert_eq!(settled_attempts(&report), [1]);
}

#[tokio::test(start_paused = true)]
async fn retry_limiter_wait_crossing_deadline_refunds_and_preserves_error() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(4)
            .build()
            .expect("attempt-window breaker"),
    )
    .retry(
        Retry::fixed(Duration::from_millis(1))
            .max_attempts(2)
            .attempt_start_window(Duration::from_millis(10)),
    )
    .rate_limit_each_attempt(
        RateLimiter::per_second(1.0)
            .unwrap()
            .with_burst(1.0)
            .unwrap(),
    )
    .build()
    .expect("attempt-window resilience aggregate");
    let (attachment, control, stage_id) = materialized_resilience(factory);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));

    let report = boundary
        .around_repeatable_effect(
            &identity_at(23),
            &data_event(),
            scripted_operation(calls.clone(), |_| {
                Err(EffectError::Timeout(
                    "first attempt remains terminal".to_string(),
                ))
            }),
        )
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(ref message)))
            if message == "first attempt remains terminal"
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(settled_attempts(&report), [1]);
    assert_eq!(scheduled_attempts(&report), [2]);
    assert_eq!(
        retry_exhaustions(&report),
        [(1, CircuitBreakerRetryStopReason::AttemptStartWindow)]
    );
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 1);

    let fresh_started = tokio::time::Instant::now();
    let fresh = boundary
        .around_repeatable_effect(
            &identity_at(24),
            &data_event(),
            RepeatableEffectOperation::new(|| async { Ok(Vec::new()) }),
        )
        .await;
    assert!(matches!(
        fresh.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(fresh_started.elapsed(), Duration::ZERO);
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 2);
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

#[tokio::test(start_paused = true)]
async fn stale_initial_reservation_cannot_cross_open_recover_closed_cycle() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(1)
            .open_for(Duration::from_millis(1))
            .probes(1)
            .build()
            .expect("ABA test breaker"),
    )
    .rate_limit_each_attempt(
        RateLimiter::per_second(1.0)
            .unwrap()
            .with_burst(1.0)
            .unwrap(),
    )
    .build()
    .expect("ABA resilience aggregate");
    let (attachment, control, stage_id) = materialized_resilience(factory);
    let target_identity = identity_at(2);
    let gate = FinalAdmissionTestGate::new(target_identity.cursor.clone(), 0);
    let resilience = attachment
        .effect_resilience_policy()
        .expect("aggregate attachment")
        .clone();
    resilience.set_final_admission_test_gate(gate.clone());
    let boundary = Arc::new(boundary_with_chain(vec![attachment]));

    let filler = boundary
        .around_repeatable_effect(
            &identity_at(1),
            &data_event(),
            RepeatableEffectOperation::new(|| async { Ok(Vec::new()) }),
        )
        .await;
    assert!(matches!(
        filler.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));

    let target_calls = Arc::new(AtomicUsize::new(0));
    let target = {
        let boundary = boundary.clone();
        let target_calls = target_calls.clone();
        tokio::spawn(async move {
            boundary
                .around_repeatable_effect(
                    &target_identity,
                    &data_event(),
                    scripted_operation(target_calls, |_| Ok(Vec::new())),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(2), gate.wait_until_reached())
        .await
        .expect("target should pause with an uncommitted limiter reservation");

    let opener = boundary
        .around_repeatable_effect(
            &identity_at(3),
            &data_event(),
            RepeatableEffectOperation::new(|| async {
                Err(EffectError::Timeout("opens intervening epoch".to_string()))
            }),
        )
        .await;
    assert!(matches!(
        opener.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
    ));

    resilience.expire_breaker_cooldown_for_test();
    let probe = boundary
        .around_repeatable_effect(
            &identity_at(4),
            &data_event(),
            RepeatableEffectOperation::new(|| async { Ok(Vec::new()) }),
        )
        .await;
    assert!(matches!(
        probe.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));

    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 3);
    gate.release();
    let target = target.await.expect("target task should complete");
    assert!(matches!(
        target.outcome,
        EffectBoundaryOutcome::Aborted(ref reason)
            if reason.cause.code.as_str() == "circuit_open"
    ));
    assert_eq!(target_calls.load(Ordering::SeqCst), 0);
    assert!(settled_attempts(&target).is_empty());
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 3);

    let fresh_started = tokio::time::Instant::now();
    let fresh = boundary
        .around_repeatable_effect(
            &identity_at(5),
            &data_event(),
            RepeatableEffectOperation::new(|| async { Ok(Vec::new()) }),
        )
        .await;
    assert!(matches!(
        fresh.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(fresh_started.elapsed(), Duration::ZERO);
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 4);
}

#[tokio::test(start_paused = true)]
async fn stale_retry_reservation_preserves_last_physical_error_after_recovery_cycle() {
    let factory = EffectResilience::with_breaker(
        CircuitBreaker::builder()
            .consecutive_failures(2)
            .open_for(Duration::from_millis(1))
            .probes(1)
            .build()
            .expect("retry ABA test breaker"),
    )
    .retry(
        Retry::fixed(Duration::from_millis(1))
            .max_attempts(2)
            .attempt_start_window(Duration::from_secs(10)),
    )
    .rate_limit_each_attempt(
        RateLimiter::per_second(1.0)
            .unwrap()
            .with_burst(1.0)
            .unwrap(),
    )
    .build()
    .expect("retry ABA resilience aggregate");
    let (attachment, control, stage_id) = materialized_resilience(factory);
    let target_identity = identity_at(12);
    let gate = FinalAdmissionTestGate::new(target_identity.cursor.clone(), 1);
    let resilience = attachment
        .effect_resilience_policy()
        .expect("aggregate attachment")
        .clone();
    resilience.set_final_admission_test_gate(gate.clone());
    let boundary = Arc::new(boundary_with_chain(vec![attachment]));

    let filler = boundary
        .around_repeatable_effect(
            &identity_at(11),
            &data_event(),
            RepeatableEffectOperation::new(|| async { Ok(Vec::new()) }),
        )
        .await;
    assert!(matches!(
        filler.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));

    let target_calls = Arc::new(AtomicUsize::new(0));
    let target = {
        let boundary = boundary.clone();
        let target_calls = target_calls.clone();
        tokio::spawn(async move {
            boundary
                .around_repeatable_effect(
                    &target_identity,
                    &data_event(),
                    scripted_operation(target_calls, |_| {
                        Err(EffectError::Timeout("target first error".to_string()))
                    }),
                )
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(3), gate.wait_until_reached())
        .await
        .expect("target retry should pause with an uncommitted reservation");

    let opener = boundary
        .around_repeatable_effect(
            &identity_at(13),
            &data_event(),
            RepeatableEffectOperation::new(|| async {
                Err(EffectError::Timeout("opens intervening epoch".to_string()))
            }),
        )
        .await;
    assert!(matches!(
        opener.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
    ));

    resilience.expire_breaker_cooldown_for_test();
    let probe = boundary
        .around_repeatable_effect(
            &identity_at(14),
            &data_event(),
            RepeatableEffectOperation::new(|| async { Ok(Vec::new()) }),
        )
        .await;
    assert!(matches!(
        probe.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));

    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 4);
    gate.release();
    let target = target.await.expect("target task should complete");
    assert!(matches!(
        target.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(ref message)))
            if message == "target first error"
    ));
    assert_eq!(target_calls.load(Ordering::SeqCst), 1);
    assert_eq!(settled_attempts(&target), [1]);
    assert_eq!(scheduled_attempts(&target), [2]);
    assert_eq!(
        retry_exhaustions(&target),
        [(1, CircuitBreakerRetryStopReason::CircuitNoLongerClosed)]
    );
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 4);

    let fresh_started = tokio::time::Instant::now();
    let fresh = boundary
        .around_repeatable_effect(
            &identity_at(15),
            &data_event(),
            RepeatableEffectOperation::new(|| async { Ok(Vec::new()) }),
        )
        .await;
    assert!(matches!(
        fresh.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(fresh_started.elapsed(), Duration::ZERO);
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 5);
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
    let rejected_completion = recovery_completions(&rejected);
    assert_eq!(rejected_completion.len(), 1);
    assert_eq!(rejected_completion[0].0, 0);
    assert_eq!(rejected_completion[0].1, 0);
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
    assert_eq!(recovery_completions(&second), [(1, 0, 1_000)]);
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

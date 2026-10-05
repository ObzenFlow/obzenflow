// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Recovery timing, cancellation, and shared-authority laws for the final
//! `EffectResilience` attachment (FLOWIP-115n).

use super::support::*;
use crate::middleware::control::resilience::EffectPlanFactory;
use obzenflow_core::event::payloads::execution_payload::RecoveryFact;

use obzenflow_core::config::{ConfigAddress, ConfigScope, ConfigSource};
use obzenflow_core::event::payloads::execution_payload::{
    CircuitBreakerFact, ExecutionPayload, RetryStopReason,
};
use obzenflow_core::event::ChainPayload;
use obzenflow_runtime::runtime_config::{
    CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
    CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY, CIRCUIT_BREAKER_COUNT_WINDOW_KEY,
    CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY, CIRCUIT_BREAKER_MINIMUM_CALLS_KEY,
    CIRCUIT_BREAKER_MODE_KEY, CIRCUIT_BREAKER_OPEN_FOR_MS_KEY, CIRCUIT_BREAKER_PROBES_KEY,
    CIRCUIT_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY, CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS_KEY,
    CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY, RATE_LIMITER_BURST_CAPACITY_KEY,
    RATE_LIMITER_COST_PER_ATTEMPT_KEY, RATE_LIMITER_EVENTS_PER_SECOND_KEY,
    RETRY_ATTEMPT_START_WINDOW_MS_KEY, RETRY_FIXED_DELAY_MS_KEY, RETRY_KIND_KEY,
    RETRY_MAX_ATTEMPTS_KEY, RETRY_MAX_BACKOFF_MS_KEY,
};
use std::collections::BTreeSet;

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

#[tokio::test(start_paused = true)]
async fn breaker_free_retry_has_independent_budget_admission_and_evidence() {
    use crate::middleware::control::{compose_effect_controls, rate_limit, retry};
    use crate::middleware::{MiddlewareAttachmentSite, MiddlewareFactory};

    for limited in [false, true] {
        let mut controls: Vec<(MiddlewareAttachmentSite, Box<dyn MiddlewareFactory>)> = vec![(
            MiddlewareAttachmentSite::Effect,
            Box::new(
                retry()
                    .fixed_delay(Duration::from_millis(7))
                    .max_attempts(3),
            ),
        )];
        if limited {
            controls.push((
                MiddlewareAttachmentSite::Implementation,
                Box::new(rate_limit(1_000.0)),
            ));
        }
        let factory = compose_effect_controls(controls).expect("supported breaker-free plan");
        let (attachment, control, stage_id) = materialized_resilience(factory);
        assert!(control
            .effect_circuit_breaker_snapshotters(&stage_id)
            .is_empty());
        let boundary = boundary_with_chain(vec![attachment]);
        for input_seq in [101, 102] {
            let calls = Arc::new(AtomicUsize::new(0));
            let report = boundary
                .around_repeatable_effect(
                    &identity_at(input_seq),
                    &data_event(),
                    scripted_operation(calls.clone(), |call| {
                        if call < 3 {
                            Err(EffectError::Timeout("retryable dependency".into()))
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
            assert_eq!(recovery_completions(&report).len(), 1);
            assert!(report.control_events.iter().all(|event| !matches!(
                event.payload,
                ChainPayload::Execution(ExecutionPayload::CircuitBreaker(_))
            )));
            assert_eq!(
                report
                    .control_events
                    .iter()
                    .filter(|event| matches!(
                        event.payload,
                        ChainPayload::Execution(ExecutionPayload::Recovery(
                            RecoveryFact::AttemptCompleted { .. }
                        ))
                    ))
                    .count(),
                3
            );
            assert!(report
                .control_events
                .iter()
                .all(|event| !event.consumes_data_credit()));
        }
        if limited {
            assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 6);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn limiter_only_records_each_physical_call_without_retrying_or_breaker_health() {
    use crate::middleware::control::{compose_effect_controls, rate_limit};
    use crate::middleware::MiddlewareAttachmentSite;

    let factory = compose_effect_controls(vec![(
        MiddlewareAttachmentSite::Effect,
        Box::new(rate_limit(1_000.0)),
    )])
    .expect("limiter-only plan");
    let (attachment, control, stage_id) = materialized_resilience(factory);
    let boundary = boundary_with_chain(vec![attachment]);
    assert!(control
        .effect_circuit_breaker_snapshotters(&stage_id)
        .is_empty());
    for succeeds in [false, true] {
        let calls = Arc::new(AtomicUsize::new(0));
        let report = boundary
            .around_repeatable_effect(
                &identity_at(if succeeds { 92 } else { 91 }),
                &data_event(),
                scripted_operation(calls.clone(), move |_| {
                    if succeeds {
                        Ok(Vec::new())
                    } else {
                        Err(EffectError::Timeout("original failure".into()))
                    }
                }),
            )
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(settled_attempts(&report), [1]);
        assert_eq!(recovery_completions(&report).len(), 1);
        assert!(retry_delays(&report).is_empty());
        assert!(report.control_events.iter().all(|event| !matches!(
            event.payload,
            ChainPayload::Execution(ExecutionPayload::CircuitBreaker(_))
        )));
        match report.outcome {
            EffectBoundaryOutcome::Executed(Ok(_)) => assert!(succeeds),
            EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(message))) => {
                assert!(!succeeds);
                assert_eq!(message, "original failure");
            }
            _ => panic!("limiter-only plan must preserve the physical outcome"),
        }
    }
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 2);
}

#[tokio::test(start_paused = true)]
async fn breaker_free_retry_stops_on_attempt_budget_and_preserves_last_error() {
    use crate::middleware::control::{compose_effect_controls, retry};
    use crate::middleware::MiddlewareAttachmentSite;
    let factory = compose_effect_controls(vec![(
        MiddlewareAttachmentSite::Effect,
        Box::new(
            retry()
                .fixed_delay(Duration::from_millis(1))
                .max_attempts(2),
        ),
    )])
    .expect("retry-only plan");
    let (attachment, _, _) = materialized_resilience(factory);
    let calls = Arc::new(AtomicUsize::new(0));
    let report = boundary_with_chain(vec![attachment])
        .around_repeatable_effect(
            &identity_at(99),
            &data_event(),
            scripted_operation(calls.clone(), |call| {
                Err(EffectError::Timeout(format!("attempt {call}")))
            }),
        )
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        matches!(report.outcome, EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(ref message))) if message == "attempt 2")
    );
    assert!(report.control_events.iter().any(|event| matches!(
        event.payload,
        ChainPayload::Execution(ExecutionPayload::Recovery(RecoveryFact::RetryExhausted {
            reason: RetryStopReason::AttemptLimit,
            ..
        }))
    )));
}

#[tokio::test(start_paused = true)]
async fn cancellation_after_limiter_reservation_refunds_before_any_physical_call() {
    for with_breaker in [false, true] {
        let mut plan = EffectPlanFactory::empty();
        plan.breaker = with_breaker
            .then(|| crate::middleware::control::circuit_breaker().consecutive_failures(3));
        let factory = plan
            .retry(crate::middleware::control::retry())
            .rate_limit_each_attempt(
                crate::middleware::control::rate_limit(1.0).burst_capacity(1.0),
            )
            .build()
            .expect("cancellable effect plan");
        let (attachment, control, stage_id) = materialized_resilience(factory);
        let target = identity_at(501);
        let gate = FinalAdmissionTestGate::new(target.cursor.clone(), 0);
        attachment
            .effect_resilience_policy()
            .expect("one recovery owner")
            .set_final_admission_test_gate(gate.clone());
        let boundary = Arc::new(boundary_with_chain(vec![attachment]));
        let calls = Arc::new(AtomicUsize::new(0));
        let pending = {
            let boundary = boundary.clone();
            let calls = calls.clone();
            tokio::spawn(async move {
                boundary
                    .around_repeatable_effect(
                        &target,
                        &data_event(),
                        scripted_operation(calls, |_| Ok(Vec::new())),
                    )
                    .await
            })
        };
        gate.wait_until_reached().await;
        pending.abort();
        assert!(matches!(pending.await, Err(error) if error.is_cancelled()));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 0);
        let start = tokio::time::Instant::now();
        let next = boundary
            .around_repeatable_effect(
                &identity_at(502),
                &data_event(),
                scripted_operation(calls.clone(), |_| Ok(Vec::new())),
            )
            .await;
        assert!(matches!(
            next.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "the abandoned reservation must refund the sole token"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 1);
    }
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

fn file_stage_snapshot(entries: &[(&str, ConfigValue)]) -> ResolvedRuntimeConfig {
    let mut candidates = CandidateSet::default();
    for (key_path, value) in entries {
        candidates
            .admit(ScopedCandidate::unqualified(
                *key_path,
                ConfigScope::stage("retrying_breaker_test"),
                ConfigSource::File,
                value.clone(),
            ))
            .expect("test file candidate should be valid");
    }
    ResolvedRuntimeConfig::new(candidates)
}

fn assert_defaults_are_consumed(factory: &dyn crate::middleware::MiddlewareFactory) {
    let consumed: BTreeSet<_> = factory.consumed_config_keys().into_iter().collect();
    for default in factory.dsl_config_defaults() {
        assert!(
            consumed.contains(default.key_path),
            "factory '{}' emitted default '{}' without declaring it consumable",
            factory.label(),
            default.key_path
        );
    }
}

#[test]
fn consumed_keys_cover_optional_and_mode_dependent_fields_without_creating_components() {
    let breaker_only = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker().consecutive_failures(2),
    )
    .build()
    .expect("breaker-only aggregate");
    assert_defaults_are_consumed(breaker_only.as_ref());
    let breaker_keys: BTreeSet<_> = breaker_only.consumed_config_keys().into_iter().collect();
    let expected_breaker_keys = BTreeSet::from([
        CIRCUIT_BREAKER_MODE_KEY,
        CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY,
        CIRCUIT_BREAKER_COUNT_WINDOW_KEY,
        CIRCUIT_BREAKER_MINIMUM_CALLS_KEY,
        CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
        CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS_KEY,
        CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
        CIRCUIT_BREAKER_OPEN_FOR_MS_KEY,
        CIRCUIT_BREAKER_PROBES_KEY,
        CIRCUIT_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
    ]);
    assert_eq!(breaker_keys, expected_breaker_keys);

    let complete = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker().consecutive_failures(2),
    )
    .retry(crate::middleware::control::retry().max_attempts(2))
    .rate_limit_each_attempt(crate::middleware::control::rate_limit(10.0))
    .build()
    .expect("complete aggregate");
    assert_defaults_are_consumed(complete.as_ref());
    let complete_keys: BTreeSet<_> = complete.consumed_config_keys().into_iter().collect();
    let expected_complete_keys = expected_breaker_keys
        .into_iter()
        .chain([
            RETRY_KIND_KEY,
            RETRY_FIXED_DELAY_MS_KEY,
            RETRY_MAX_ATTEMPTS_KEY,
            RETRY_MAX_BACKOFF_MS_KEY,
            RETRY_ATTEMPT_START_WINDOW_MS_KEY,
            RATE_LIMITER_EVENTS_PER_SECOND_KEY,
            RATE_LIMITER_BURST_CAPACITY_KEY,
            RATE_LIMITER_COST_PER_ATTEMPT_KEY,
        ])
        .collect();
    assert_eq!(complete_keys, expected_complete_keys);

    let other_shapes = [
        EffectPlanFactory::with_breaker(
            crate::middleware::control::circuit_breaker()
                .count_window(10)
                .minimum_calls(5)
                .failure_rate_threshold(0.5),
        )
        .build()
        .expect("rate-based aggregate"),
        EffectPlanFactory::with_breaker(
            crate::middleware::control::circuit_breaker().consecutive_failures(2),
        )
        .retry(crate::middleware::control::retry().fixed_delay(Duration::from_millis(5)))
        .build()
        .expect("fixed retry aggregate"),
        EffectPlanFactory::with_breaker(
            crate::middleware::control::circuit_breaker().consecutive_failures(2),
        )
        .rate_limit_each_attempt(crate::middleware::control::rate_limit(10.0).burst_capacity(25.0))
        .build()
        .expect("explicit burst aggregate"),
    ];
    for factory in other_shapes {
        assert_defaults_are_consumed(factory.as_ref());
    }
}

#[test]
fn exact_configuration_cannot_create_omitted_retry_or_limiter_components() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker().consecutive_failures(2),
    )
    .build()
    .expect("breaker-only aggregate");

    for (key_path, value) in [
        (RETRY_KIND_KEY, ConfigValue::Text("fixed".to_string())),
        (RATE_LIMITER_BURST_CAPACITY_KEY, ConfigValue::F64(3.0)),
    ] {
        let snapshot = file_effect_snapshot(&[(key_path, value)]);
        let error =
            test_effective_config_for_factories_with_snapshot(&[factory.as_ref()], &snapshot)
                .expect_err("an exact subject cannot attach a structurally omitted component");
        assert!(matches!(
            *error,
            obzenflow_runtime::runtime_config::ConfigResolveError::UnattachedEffectSubject {
                key_path: rejected,
                ..
            } if rejected == key_path
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn stage_broadcast_configuration_cannot_create_omitted_retry_or_limiter_components() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker().consecutive_failures(2),
    )
    .build()
    .expect("breaker-only aggregate");
    let snapshot = file_stage_snapshot(&[
        (RETRY_KIND_KEY, ConfigValue::Text("fixed".to_string())),
        (RETRY_FIXED_DELAY_MS_KEY, ConfigValue::U64(1)),
        (RATE_LIMITER_EVENTS_PER_SECOND_KEY, ConfigValue::F64(100.0)),
        (RATE_LIMITER_BURST_CAPACITY_KEY, ConfigValue::F64(10.0)),
        (RATE_LIMITER_COST_PER_ATTEMPT_KEY, ConfigValue::F64(1.0)),
    ]);
    let effective =
        test_effective_config_for_factories_with_snapshot(&[factory.as_ref()], &snapshot)
            .expect("broadcast values for absent components have no resolution point");
    let stage = StageKey::from("retrying_breaker_test");
    let effect_type = obzenflow_core::event::EffectType::from("effect.retry");
    for key_path in [RETRY_KIND_KEY, RATE_LIMITER_EVENTS_PER_SECOND_KEY] {
        assert!(effective
            .effect_value(key_path, &stage, &effect_type)
            .is_none());
    }

    let (attachment, control, stage_id) = materialized_resilience_with_snapshot(factory, &snapshot);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));
    let report = boundary
        .around_repeatable_effect(
            &identity_at(100),
            &data_event(),
            scripted_operation(calls.clone(), |_| {
                Err(EffectError::Timeout("no retry component".to_string()))
            }),
        )
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(retry_delays(&report).is_empty());
    assert!(control
        .effect_rate_limiter_snapshotters(&stage_id)
        .is_empty());
}

#[tokio::test]
async fn file_configuration_can_switch_consecutive_breaker_to_rate_based() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker().consecutive_failures(1),
    )
    .build()
    .expect("switchable aggregate");
    let snapshot = file_effect_snapshot(&[
        (
            CIRCUIT_BREAKER_MODE_KEY,
            ConfigValue::Text("rate_based".to_string()),
        ),
        (CIRCUIT_BREAKER_COUNT_WINDOW_KEY, ConfigValue::U64(2)),
        (CIRCUIT_BREAKER_MINIMUM_CALLS_KEY, ConfigValue::U64(2)),
        (
            CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
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
async fn exact_file_configuration_preserves_a_tiny_positive_failure_rate() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker().consecutive_failures(1),
    )
    .build()
    .expect("switchable aggregate");
    let snapshot = file_effect_snapshot(&[
        (
            CIRCUIT_BREAKER_MODE_KEY,
            ConfigValue::Text("rate_based".to_string()),
        ),
        (CIRCUIT_BREAKER_COUNT_WINDOW_KEY, ConfigValue::U64(2)),
        (CIRCUIT_BREAKER_MINIMUM_CALLS_KEY, ConfigValue::U64(1)),
        (
            CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
            ConfigValue::F64(f64::MIN_POSITIVE),
        ),
    ]);
    let (attachment, _, _) = materialized_resilience_with_snapshot(factory, &snapshot);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));

    let success = boundary
        .around_repeatable_effect(
            &identity_at(107),
            &data_event(),
            scripted_operation(calls.clone(), |_| Ok(Vec::new())),
        )
        .await;
    assert!(matches!(
        success.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));

    let failure = boundary
        .around_repeatable_effect(
            &identity_at(108),
            &data_event(),
            scripted_operation(calls.clone(), |_| {
                Err(EffectError::Timeout("tiny threshold failure".to_string()))
            }),
        )
        .await;
    assert!(matches!(
        failure.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
    ));

    let rejected = boundary
        .around_repeatable_effect(
            &identity_at(109),
            &data_event(),
            scripted_operation(calls.clone(), |_| Ok(Vec::new())),
        )
        .await;
    assert!(matches!(
        rejected.outcome,
        EffectBoundaryOutcome::Aborted(_)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn file_configuration_can_switch_rate_based_breaker_to_consecutive() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker()
            .count_window(5)
            .minimum_calls(5)
            .failure_rate_threshold(1.0),
    )
    .build()
    .expect("switchable aggregate");
    let snapshot = file_effect_snapshot(&[
        (
            CIRCUIT_BREAKER_MODE_KEY,
            ConfigValue::Text("consecutive".to_string()),
        ),
        (
            CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY,
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
async fn file_configuration_can_supply_omitted_rate_based_slow_call_fields() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker()
            .count_window(2)
            .minimum_calls(1)
            .failure_rate_threshold(1.0),
    )
    .build()
    .expect("switchable aggregate");
    let snapshot = file_effect_snapshot(&[
        (
            CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS_KEY,
            ConfigValue::U64(10),
        ),
        (
            CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
            ConfigValue::F64(1.0),
        ),
    ]);
    let (attachment, _, _) = materialized_resilience_with_snapshot(factory, &snapshot);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));
    let slow_calls = calls.clone();

    let first = boundary
        .around_repeatable_effect(
            &identity_at(105),
            &data_event(),
            RepeatableEffectOperation::new(move || {
                slow_calls.fetch_add(1, Ordering::SeqCst);
                async {
                    tokio::time::sleep(Duration::from_millis(11)).await;
                    Ok(Vec::new())
                }
            }),
        )
        .await;
    assert!(matches!(
        first.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));

    let rejected = boundary
        .around_repeatable_effect(
            &identity_at(106),
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
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker().consecutive_failures(3),
    )
    .retry(crate::middleware::control::retry().max_attempts(2))
    .build()
    .expect("switchable retry aggregate");
    let snapshot = file_effect_snapshot(&[
        (RETRY_KIND_KEY, ConfigValue::Text("fixed".to_string())),
        (RETRY_FIXED_DELAY_MS_KEY, ConfigValue::U64(7)),
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

#[tokio::test(start_paused = true)]
async fn file_configuration_can_switch_fixed_retry_to_exponential() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker().consecutive_failures(3),
    )
    .retry(
        crate::middleware::control::retry()
            .fixed_delay(Duration::from_millis(7))
            .max_attempts(2),
    )
    .build()
    .expect("switchable retry aggregate");
    let snapshot =
        file_effect_snapshot(&[(RETRY_KIND_KEY, ConfigValue::Text("exponential".to_string()))]);
    let (attachment, _, _) = materialized_resilience_with_snapshot(factory, &snapshot);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));

    let report = boundary
        .around_repeatable_effect(
            &identity_at(106),
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
    let delays = retry_delays(&report);
    assert_eq!(delays.len(), 1);
    assert!(
        (225..=275).contains(&delays[0]),
        "the switched exponential retry should apply ±10% jitter to its 250 ms initial delay, got {} ms",
        delays[0]
    );
}

async fn assert_resilience_limiter_uses_supplied_implicit_burst(snapshot: &ResolvedRuntimeConfig) {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker().consecutive_failures(10),
    )
    .rate_limit_each_attempt(crate::middleware::control::rate_limit(1.0))
    .build()
    .expect("burst proof aggregate");
    let (attachment, control, stage_id) = materialized_resilience_with_snapshot(factory, snapshot);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));
    let started = tokio::time::Instant::now();

    for input_seq in 201..204 {
        let report = boundary
            .around_repeatable_effect(
                &identity_at(input_seq),
                &data_event(),
                scripted_operation(calls.clone(), |_| Ok(Vec::new())),
            )
            .await;
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
    }

    assert_eq!(started.elapsed(), Duration::ZERO);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 3);
}

#[tokio::test(start_paused = true)]
async fn exact_file_configuration_supplies_resilience_limiter_implicit_burst() {
    let snapshot =
        file_effect_snapshot(&[(RATE_LIMITER_BURST_CAPACITY_KEY, ConfigValue::F64(3.0))]);
    assert_resilience_limiter_uses_supplied_implicit_burst(&snapshot).await;
}

#[tokio::test(start_paused = true)]
async fn stage_broadcast_configuration_supplies_resilience_limiter_implicit_burst() {
    let snapshot = file_stage_snapshot(&[(RATE_LIMITER_BURST_CAPACITY_KEY, ConfigValue::F64(3.0))]);
    assert_resilience_limiter_uses_supplied_implicit_burst(&snapshot).await;
}

fn retry_delays(report: &obzenflow_runtime::effects::EffectBoundaryReport) -> Vec<u64> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(RecoveryFact::RetryScheduled {
                delay_ms,
                ..
            })) => Some(*delay_ms),
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
        .filter_map(|event| match &event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(
                RecoveryFact::RecoveryCompleted {
                    total_attempts,
                    backoff_elapsed_ms,
                    recovery_elapsed_ms,
                    ..
                },
            )) => Some((*total_attempts, *backoff_elapsed_ms, *recovery_elapsed_ms)),
            _ => None,
        })
        .collect()
}

fn recovery_completion_cursors(
    report: &obzenflow_runtime::effects::EffectBoundaryReport,
) -> Vec<EffectCursor> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(
                RecoveryFact::RecoveryCompleted { cursor, .. },
            )) => Some(cursor.clone()),
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
        .filter_map(|event| match &event.payload {
            ChainPayload::Execution(ExecutionPayload::CircuitBreaker(
                CircuitBreakerFact::AttemptSettled { attempt, .. },
            ))
            | ChainPayload::Execution(ExecutionPayload::Recovery(
                RecoveryFact::AttemptCompleted { attempt, .. },
            )) => Some(*attempt),
            _ => None,
        })
        .collect()
}

fn scheduled_attempts(report: &obzenflow_runtime::effects::EffectBoundaryReport) -> Vec<u32> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(RecoveryFact::RetryScheduled {
                next_attempt,
                ..
            })) => Some(*next_attempt),
            _ => None,
        })
        .collect()
}

fn retry_exhaustions(
    report: &obzenflow_runtime::effects::EffectBoundaryReport,
) -> Vec<(u32, RetryStopReason)> {
    report
        .control_events
        .iter()
        .filter_map(|event| match &event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(RecoveryFact::RetryExhausted {
                total_attempts,
                reason,
                ..
            })) => Some((*total_attempts, *reason)),
            _ => None,
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn fixed_backoff_delays_each_physical_continuation() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker()
            .consecutive_failures(4)
            .open_for(Duration::from_secs(5)),
    )
    .retry(
        crate::middleware::control::retry()
            .fixed_delay(Duration::from_millis(7))
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
async fn recovery_clock_measures_backoff_overshoot_not_the_planned_delay_sum() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker().consecutive_failures(3),
    )
    .retry(
        crate::middleware::control::retry()
            .fixed_delay(Duration::from_millis(7))
            .max_attempts(2)
            .attempt_start_window(Duration::from_secs(1)),
    )
    .build()
    .expect("overshoot resilience aggregate");
    let (attachment, _, _) = materialized_resilience(factory);
    let boundary = boundary_with_chain(vec![attachment]);
    let calls = Arc::new(AtomicUsize::new(0));
    let first_call = Arc::new(tokio::sync::Notify::new());
    let release_failure = Arc::new(tokio::sync::Notify::new());
    let operation = {
        let calls = calls.clone();
        let first_call = first_call.clone();
        let release_failure = release_failure.clone();
        RepeatableEffectOperation::new(move || {
            let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
            let first_call = first_call.clone();
            let release_failure = release_failure.clone();
            async move {
                if call == 1 {
                    first_call.notify_one();
                    release_failure.notified().await;
                    Err(EffectError::Timeout("retry after overshoot".to_string()))
                } else {
                    Ok(Vec::new())
                }
            }
        })
    };
    let task = tokio::spawn(async move {
        boundary
            .around_repeatable_effect(&identity_for("effect.retry"), &data_event(), operation)
            .await
    });

    first_call.notified().await;
    release_failure.notify_one();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(25)).await;
    let report = task.await.expect("overshoot boundary task");

    assert_eq!(retry_delays(&report), [7]);
    let completions = recovery_completions(&report);
    let [completion] = completions.as_slice() else {
        panic!("one recovery completion expected");
    };
    assert_eq!(completion.0, 2);
    assert!(
        completion.1 >= 25,
        "actual backoff must retain scheduler overshoot: {completion:?}"
    );
    assert!(completion.2 >= completion.1);
}

#[tokio::test(start_paused = true)]
async fn initial_limiter_wait_does_not_consume_attempt_start_window() {
    for with_breaker in [false, true] {
        let factory = {
            let mut plan = EffectPlanFactory::empty();
            plan.breaker = with_breaker
                .then(|| crate::middleware::control::circuit_breaker().consecutive_failures(3));
            plan
        }
        .retry(
            crate::middleware::control::retry()
                .fixed_delay(Duration::from_millis(1))
                .max_attempts(2)
                .attempt_start_window(Duration::from_millis(10)),
        )
        .rate_limit_each_attempt(crate::middleware::control::rate_limit(1.0).burst_capacity(1.0))
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
}

#[tokio::test(start_paused = true)]
async fn retry_limiter_wait_crossing_deadline_refunds_and_preserves_error() {
    for with_breaker in [false, true] {
        let factory = {
            let mut plan = EffectPlanFactory::empty();
            plan.breaker = with_breaker
                .then(|| crate::middleware::control::circuit_breaker().consecutive_failures(4));
            plan
        }
        .retry(
            crate::middleware::control::retry()
                .fixed_delay(Duration::from_millis(1))
                .max_attempts(2)
                .attempt_start_window(Duration::from_millis(10)),
        )
        .rate_limit_each_attempt(crate::middleware::control::rate_limit(1.0).burst_capacity(1.0))
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
            [(1, RetryStopReason::AttemptStartWindow)]
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
}

#[tokio::test]
async fn cancellation_during_backoff_starts_no_later_attempt() {
    for with_breaker in [false, true] {
        let factory = {
            let mut plan = EffectPlanFactory::empty();
            plan.breaker = with_breaker
                .then(|| crate::middleware::control::circuit_breaker().consecutive_failures(3));
            plan
        }
        .retry(
            crate::middleware::control::retry()
                .fixed_delay(Duration::from_secs(60))
                .max_attempts(2),
        )
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
}

#[tokio::test(start_paused = true)]
async fn another_invocation_opening_the_circuit_stops_a_pending_continuation() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker()
            .consecutive_failures(2)
            .open_for(Duration::from_secs(30)),
    )
    .retry(
        crate::middleware::control::retry()
            .fixed_delay(Duration::from_millis(10))
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
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker()
            .consecutive_failures(1)
            .open_for(Duration::from_millis(1))
            .probes(1),
    )
    .rate_limit_each_attempt(crate::middleware::control::rate_limit(1.0).burst_capacity(1.0))
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
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker()
            .consecutive_failures(2)
            .open_for(Duration::from_millis(1))
            .probes(1),
    )
    .retry(
        crate::middleware::control::retry()
            .fixed_delay(Duration::from_millis(1))
            .max_attempts(2)
            .attempt_start_window(Duration::from_secs(10)),
    )
    .rate_limit_each_attempt(crate::middleware::control::rate_limit(1.0).burst_capacity(1.0))
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
        [(1, RetryStopReason::CircuitNoLongerClosed)]
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
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker()
            .consecutive_failures(1)
            .open_for(Duration::from_millis(2))
            .probes(1),
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
    let metrics = breaker[0].1().expect("measurement captured");
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

#[tokio::test]
async fn probe_busy_rejection_has_no_attempt_or_committed_permit() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker()
            .consecutive_failures(1)
            .open_for(Duration::from_secs(30))
            .probes(1),
    )
    .rate_limit_each_attempt(crate::middleware::control::rate_limit(1_000.0).burst_capacity(3.0))
    .build()
    .expect("probe-busy resilience aggregate");
    let (attachment, control, stage_id) = materialized_resilience(factory);
    let resilience = attachment
        .effect_resilience_policy()
        .expect("aggregate attachment")
        .clone();
    let boundary = Arc::new(boundary_with_chain(vec![attachment]));

    let opening = boundary
        .around_repeatable_effect(
            &identity_at(1),
            &data_event(),
            RepeatableEffectOperation::new(|| async {
                Err(EffectError::Timeout(
                    "open for probe-busy proof".to_string(),
                ))
            }),
        )
        .await;
    assert!(matches!(
        opening.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
    ));
    resilience.expire_breaker_cooldown_for_test();

    let probe_started = Arc::new(tokio::sync::Notify::new());
    let release_probe = Arc::new(tokio::sync::Notify::new());
    let probe = {
        let boundary = boundary.clone();
        let probe_started = probe_started.clone();
        let release_probe = release_probe.clone();
        tokio::spawn(async move {
            boundary
                .around_repeatable_effect(
                    &identity_at(2),
                    &data_event(),
                    RepeatableEffectOperation::new(move || {
                        let probe_started = probe_started.clone();
                        let release_probe = release_probe.clone();
                        async move {
                            probe_started.notify_one();
                            release_probe.notified().await;
                            Ok(Vec::new())
                        }
                    }),
                )
                .await
        })
    };
    probe_started.notified().await;

    let rejected_calls = Arc::new(AtomicUsize::new(0));
    let rejected = boundary
        .around_repeatable_effect(
            &identity_at(3),
            &data_event(),
            scripted_operation(rejected_calls.clone(), |_| Ok(Vec::new())),
        )
        .await;
    assert!(matches!(
        rejected.outcome,
        EffectBoundaryOutcome::Aborted(ref reason)
            if reason.cause.source.as_str() == "circuit_breaker"
                && reason.cause.code.as_str() == "probe_in_progress"
    ));
    assert_eq!(rejected_calls.load(Ordering::SeqCst), 0);
    assert!(settled_attempts(&rejected).is_empty());
    let rejected_completions = recovery_completions(&rejected);
    let [completion] = rejected_completions.as_slice() else {
        panic!("probe-busy rejection must emit one completion row");
    };
    assert_eq!(completion.0, 0);
    assert_eq!(completion.1, 0);
    assert_eq!(
        recovery_completion_cursors(&rejected),
        [identity_at(3).cursor],
        "the zero-attempt completion must remain keyed to the rejected invocation"
    );
    assert_eq!(
        effect_limiter_events(control.as_ref(), stage_id),
        2,
        "only the opening call and active probe may commit permits"
    );

    release_probe.notify_one();
    let probe = probe.await.expect("active probe task should complete");
    assert!(matches!(
        probe.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));

    let breaker = control.effect_circuit_breaker_snapshotters(&stage_id);
    let metrics = breaker[0].1().expect("measurement captured");
    assert_eq!(metrics.requests_total, 2);
    assert_eq!(metrics.failures_total, 1);
    assert_eq!(metrics.successes_total, 1);
    assert_eq!(metrics.rejections_total, 1);
}

#[tokio::test(start_paused = true)]
async fn limiter_wait_is_not_a_slow_dependency_sample() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker()
            .count_window(2)
            .minimum_calls(2)
            .slow_call_duration(Duration::from_millis(50))
            .slow_call_rate_threshold(0.5)
            .open_for(Duration::from_secs(5)),
    )
    .rate_limit_each_attempt(crate::middleware::control::rate_limit(1.0).burst_capacity(1.0))
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
        .find_map(|event| match &event.payload {
            ChainPayload::Execution(ExecutionPayload::CircuitBreaker(
                CircuitBreakerFact::AttemptSettled {
                    slow,
                    dependency_elapsed_ms,
                    admission_wait_ms,
                    ..
                },
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
    let metrics = breaker[0].1().expect("measurement captured");
    assert_eq!(metrics.requests_total, 2);
    assert_eq!(metrics.successes_total, 2);
    assert_eq!(metrics.failures_total, 0);
    assert_eq!(metrics.slow_total, 0);
    assert_eq!(metrics.opened_total, 0);
    assert_eq!(effect_limiter_events(control.as_ref(), stage_id), 2);
}

#[tokio::test(start_paused = true)]
async fn circuit_opening_cancels_a_queued_limiter_reservation() {
    let factory = EffectPlanFactory::with_breaker(
        crate::middleware::control::circuit_breaker()
            .consecutive_failures(1)
            .open_for(Duration::from_secs(30)),
    )
    .rate_limit_each_attempt(crate::middleware::control::rate_limit(1.0).burst_capacity(1.0))
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
    let metrics = breaker[0].1().expect("measurement captured");
    assert_eq!(metrics.requests_total, 1);
    assert_eq!(metrics.failures_total, 1);
    assert_eq!(metrics.rejections_total, 1);
}

#[tokio::test(start_paused = true)]
async fn cancellation_during_limiter_wait_commits_no_permit_or_attempt() {
    for with_breaker in [false, true] {
        let factory = {
            let mut plan = EffectPlanFactory::empty();
            plan.breaker = with_breaker
                .then(|| crate::middleware::control::circuit_breaker().consecutive_failures(2));
            plan
        }
        .rate_limit_each_attempt(crate::middleware::control::rate_limit(1.0).burst_capacity(1.0))
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
        if with_breaker {
            let metrics = breaker[0].1().expect("measurement captured");
            assert_eq!(metrics.requests_total, 1);
            assert_eq!(metrics.successes_total, 1);
            assert_eq!(metrics.failures_total, 0);
        } else {
            assert!(breaker.is_empty());
        }
    }
}

#[tokio::test]
async fn cancellation_in_flight_records_an_attempt_without_a_health_sample() {
    for with_breaker in [false, true] {
        let factory = {
            let mut plan = EffectPlanFactory::empty();
            plan.breaker = with_breaker
                .then(|| crate::middleware::control::circuit_breaker().consecutive_failures(2));
            plan
        }
        .rate_limit_each_attempt(crate::middleware::control::rate_limit(100.0))
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
        if with_breaker {
            let metrics = breaker[0].1().expect("measurement captured");
            assert_eq!(metrics.requests_total, 1);
            assert_eq!(metrics.successes_total, 0);
            assert_eq!(metrics.failures_total, 0);
        } else {
            assert!(breaker.is_empty());
        }
    }
}

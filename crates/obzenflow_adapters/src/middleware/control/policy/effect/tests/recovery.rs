// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::payloads::observability_payload::{RetryInvocation, RetryProtectedUnit};

#[tokio::test]
async fn concurrent_retry_invocations_keep_cursor_context_and_attempts_isolated() {
    let owner = effect_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
    let admissions = Arc::new(AtomicUsize::new(0));
    let log = Arc::new(Mutex::new(Vec::new()));
    let boundary = Arc::new(retry_effect_boundary(owner, admissions.clone(), log));
    let attempt_barrier = Arc::new(Barrier::new(2));
    let ordinals_a = Arc::new(Mutex::new(Vec::new()));
    let ordinals_b = Arc::new(Mutex::new(Vec::new()));

    let run_a = {
        let boundary = boundary.clone();
        let attempt_barrier = attempt_barrier.clone();
        let ordinals = ordinals_a.clone();
        async move {
            let mut identity = identity_for("effect.retry_test");
            identity.cursor = EffectCursor::new("test_flow", "test_stage", 11, 0);
            let expected_cursor = identity.cursor.clone();
            let mut executor = LockstepEffectExecutor {
                ordinals,
                attempt_barrier,
            };
            let report = boundary
                .around_retryable_effect(
                    &identity,
                    &data_event(),
                    &mut executor,
                    BoundaryStopReceiver::default(),
                )
                .await;
            (expected_cursor, report)
        }
    };
    let run_b = {
        let boundary = boundary.clone();
        let attempt_barrier = attempt_barrier.clone();
        let ordinals = ordinals_b.clone();
        async move {
            let mut identity = identity_for("effect.retry_test");
            identity.cursor = EffectCursor::new("test_flow", "test_stage", 22, 0);
            let expected_cursor = identity.cursor.clone();
            let mut executor = LockstepEffectExecutor {
                ordinals,
                attempt_barrier,
            };
            let report = boundary
                .around_retryable_effect(
                    &identity,
                    &data_event(),
                    &mut executor,
                    BoundaryStopReceiver::default(),
                )
                .await;
            (expected_cursor, report)
        }
    };

    let ((cursor_a, report_a), (cursor_b, report_b)) = tokio::join!(run_a, run_b);

    assert_eq!(*ordinals_a.lock().unwrap(), vec![1, 2]);
    assert_eq!(*ordinals_b.lock().unwrap(), vec![1, 2]);
    assert_eq!(admissions.load(Ordering::SeqCst), 4);

    let mut invocation_contexts = Vec::new();
    for (expected_cursor, report) in [(cursor_a, report_a), (cursor_b, report_b)] {
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
        assert_eq!(report.control_events.len(), 2);

        let failed_context = match retry_event(&report.control_events[0]) {
            RetryEvent::AttemptFailed {
                context: Some(context),
                attempt_number: 1,
                ..
            } => context.clone(),
            other => panic!("expected first-attempt failure, got {other:?}"),
        };
        let succeeded_context = match retry_event(&report.control_events[1]) {
            RetryEvent::SucceededAfterRetry {
                context: Some(context),
                total_attempts: 2,
                ..
            } => context.clone(),
            other => panic!("expected second-attempt recovery, got {other:?}"),
        };
        assert_eq!(failed_context, succeeded_context);
        assert_eq!(
            failed_context.invocation,
            RetryInvocation::Effect {
                cursor: expected_cursor
            }
        );
        invocation_contexts.push(failed_context);
    }

    assert_ne!(invocation_contexts[0], invocation_contexts[1]);
}

#[tokio::test]
async fn retryable_effect_transient_then_success_uses_fresh_context_and_terminal_rows() {
    let owner = effect_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
    let admissions = Arc::new(AtomicUsize::new(0));
    let log = Arc::new(Mutex::new(Vec::new()));
    let boundary = retry_effect_boundary(owner.clone(), admissions.clone(), log.clone());
    let identity = identity_for("effect.retry_test");
    let parent = data_event();
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let mut executor = SequenceEffectExecutor {
        outcomes: VecDeque::from([
            Err(EffectError::TransientExecution("temporary".to_string())),
            Ok(vec![data_event()]),
        ]),
        ordinals: ordinals.clone(),
    };

    let report = boundary
        .around_retryable_effect(
            &identity,
            &parent,
            &mut executor,
            BoundaryStopReceiver::default(),
        )
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(ref events)) if events.len() == 1
    ));
    assert_eq!(*ordinals.lock().unwrap(), vec![1, 2]);
    assert_eq!(admissions.load(Ordering::SeqCst), 2);
    assert_eq!(
        *log.lock().unwrap(),
        vec![
            "admit:1",
            "commit",
            "observe:error",
            "admit:2",
            "commit",
            "observe:ok"
        ]
    );
    assert_eq!(report.control_events.len(), 2);

    let context = match retry_event(&report.control_events[0]) {
        RetryEvent::AttemptFailed {
            context: Some(context),
            attempt_number,
            max_attempts,
            error_kind,
            delay_ms,
            ..
        } => {
            assert_eq!(*attempt_number, 1);
            assert_eq!(*max_attempts, 3);
            assert_eq!(*error_kind, Some(ErrorKind::Remote));
            assert_eq!(*delay_ms, Some(0));
            assert_eq!(context.stage_id, owner.stage_id);
            assert_eq!(context.attachment_id, owner.attachment_id.as_ulid());
            assert_eq!(
                context.protected_unit,
                RetryProtectedUnit::Effect {
                    effect_type: "effect.retry_test".into()
                }
            );
            assert_eq!(
                context.invocation,
                RetryInvocation::Effect {
                    cursor: identity.cursor.clone()
                }
            );
            assert_eq!(
                report.control_events[0].causality.parent_ids,
                vec![parent.id]
            );
            context.clone()
        }
        other => panic!("expected attempt_failed as the first row, got {other:?}"),
    };
    match retry_event(&report.control_events[1]) {
        RetryEvent::SucceededAfterRetry {
            context: Some(success_context),
            total_attempts,
            ..
        } => {
            assert_eq!(*total_attempts, 2);
            assert_eq!(success_context, &context);
        }
        other => panic!("expected succeeded_after_retry as the terminal row, got {other:?}"),
    }
}

#[tokio::test]
async fn retryable_effect_transient_then_permanent_has_exact_terminal_attempt_count() {
    let owner = effect_retry_owner(4, Duration::ZERO, Duration::from_secs(1));
    let boundary = retry_effect_boundary(
        owner,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(Mutex::new(Vec::new())),
    );
    let identity = identity_for("effect.retry_test");
    let parent = data_event();
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let mut executor = SequenceEffectExecutor {
        outcomes: VecDeque::from([
            Err(EffectError::TransientExecution("temporary".to_string())),
            Err(EffectError::Execution("permanent".to_string())),
        ]),
        ordinals: ordinals.clone(),
    };

    let report = boundary
        .around_retryable_effect(
            &identity,
            &parent,
            &mut executor,
            BoundaryStopReceiver::default(),
        )
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::Execution(ref message)))
            if message == "permanent"
    ));
    assert_eq!(*ordinals.lock().unwrap(), vec![1, 2]);
    assert_eq!(report.control_events.len(), 2);
    assert!(matches!(
        retry_event(&report.control_events[0]),
        RetryEvent::AttemptFailed {
            attempt_number: 1,
            ..
        }
    ));
    match retry_event(&report.control_events[1]) {
        RetryEvent::Exhausted {
            context: Some(context),
            total_attempts,
            exhaustion_cause,
            last_error_kind,
            ..
        } => {
            assert_eq!(*total_attempts, 2);
            assert_eq!(
                *exhaustion_cause,
                Some(RetryExhaustionCause::TerminalFailure)
            );
            assert_eq!(*last_error_kind, Some(ErrorKind::Unknown));
            assert_eq!(
                context.invocation,
                RetryInvocation::Effect {
                    cursor: identity.cursor.clone()
                }
            );
        }
        other => panic!("expected terminal_failure exhaustion, got {other:?}"),
    }
}

#[tokio::test]
async fn effect_retry_after_above_the_ceiling_exhausts_without_another_call() {
    let boundary = retry_effect_boundary(
        effect_retry_owner(3, Duration::from_millis(5), Duration::from_millis(40)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(Mutex::new(Vec::new())),
    );
    let identity = identity_for("effect.retry_test");
    let parent = data_event();
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let mut executor = SequenceEffectExecutor {
        outcomes: VecDeque::from([Err(EffectError::RateLimited {
            message: "retry later".to_string(),
            retry_after: Some(Duration::from_millis(41)),
        })]),
        ordinals: ordinals.clone(),
    };

    let report = boundary
        .around_retryable_effect(
            &identity,
            &parent,
            &mut executor,
            BoundaryStopReceiver::default(),
        )
        .await;

    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert_eq!(report.control_events.len(), 1);
    match retry_event(&report.control_events[0]) {
        RetryEvent::Exhausted {
            total_attempts,
            exhaustion_cause,
            last_error_kind,
            ..
        } => {
            assert_eq!(*total_attempts, 1);
            assert_eq!(
                *exhaustion_cause,
                Some(RetryExhaustionCause::RetryHintExceedsLimit)
            );
            assert_eq!(*last_error_kind, Some(ErrorKind::RateLimited));
        }
        other => panic!("expected retry-hint exhaustion, got {other:?}"),
    }
}

#[tokio::test]
async fn threshold_one_breaker_keeps_rate_limited_effect_retryable_by_default() {
    let owner = effect_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
    let admissions = Arc::new(AtomicUsize::new(0));
    let breaker = Arc::new(CircuitBreakerMiddleware::new(1));
    let mut chains = HashMap::new();
    chains.insert(
        "effect.retry_test",
        Arc::new(vec![
            EffectPolicyAttachment::event_aware(breaker),
            EffectPolicyAttachment::neutral(Arc::new(EffectRetryOwnerPolicy {
                owner,
                admissions: admissions.clone(),
                log: Arc::new(Mutex::new(Vec::new())),
                observed: None,
            })),
        ]),
    );
    let boundary = PerEffectPolicyBoundary::new(chains);
    let identity = identity_for("effect.retry_test");
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let mut executor = SequenceEffectExecutor {
        outcomes: VecDeque::from([
            Err(EffectError::RateLimited {
                message: "slow down".to_string(),
                retry_after: None,
            }),
            Ok(Vec::new()),
        ]),
        ordinals: ordinals.clone(),
    };

    let report = boundary
        .around_retryable_effect(
            &identity,
            &data_event(),
            &mut executor,
            BoundaryStopReceiver::default(),
        )
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Ok(_))
    ));
    assert_eq!(*ordinals.lock().unwrap(), [1, 2]);
    assert_eq!(admissions.load(Ordering::SeqCst), 2);
    assert!(matches!(
        retry_event(&report.control_events[1]),
        RetryEvent::SucceededAfterRetry {
            total_attempts: 2,
            ..
        }
    ));
}

#[tokio::test]
async fn half_open_rate_limited_effect_probe_is_single_shot() {
    let owner = effect_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
    let admissions = Arc::new(AtomicUsize::new(0));
    let breaker = Arc::new(CircuitBreakerMiddleware::new(1));
    breaker.force_half_open_for_test();
    let mut chains = HashMap::new();
    chains.insert(
        "effect.retry_test",
        Arc::new(vec![
            EffectPolicyAttachment::event_aware(breaker.clone()),
            EffectPolicyAttachment::neutral(Arc::new(EffectRetryOwnerPolicy {
                owner,
                admissions: admissions.clone(),
                log: Arc::new(Mutex::new(Vec::new())),
                observed: None,
            })),
        ]),
    );
    let boundary = PerEffectPolicyBoundary::new(chains);
    let identity = identity_for("effect.retry_test");
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let mut executor = SequenceEffectExecutor {
        outcomes: VecDeque::from([
            Err(EffectError::RateLimited {
                message: "slow down".to_string(),
                retry_after: None,
            }),
            Ok(Vec::new()),
        ]),
        ordinals: ordinals.clone(),
    };

    let report = boundary
        .around_retryable_effect(
            &identity,
            &data_event(),
            &mut executor,
            BoundaryStopReceiver::default(),
        )
        .await;

    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::RateLimited { .. }))
    ));
    assert_eq!(*ordinals.lock().unwrap(), [1]);
    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert!(matches!(
        retry_event(report.control_events.last().expect("terminal retry row")),
        RetryEvent::Exhausted {
            total_attempts: 1,
            exhaustion_cause: Some(RetryExhaustionCause::PolicyRejected),
            ..
        }
    ));
}

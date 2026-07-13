// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[tokio::test(start_paused = true)]
async fn retryable_source_transient_then_success_is_one_logical_outcome_with_onion_rows() {
    let owner = source_retry_owner(3, Duration::from_millis(25), Duration::from_secs(1));
    let log = Arc::new(Mutex::new(Vec::new()));
    let admissions = Arc::new(AtomicUsize::new(0));
    let guard_drops = Arc::new(AtomicUsize::new(0));
    let recovery_allowed = Arc::new(AtomicBool::new(true));
    let boundary = retry_boundary(
        owner.clone(),
        log.clone(),
        admissions.clone(),
        guard_drops.clone(),
        recovery_allowed,
        true,
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let ordinals_for_task = ordinals.clone();

    let task = tokio::spawn(async move {
        let mut executor = SequenceSourceExecutor {
            outcomes: VecDeque::from([
                Err(SourceError::Timeout("first attempt".to_string())),
                Ok(SourcePollCompletion::Batch(vec![test_event()])),
            ]),
            ordinals: ordinals_for_task,
        };
        boundary
            .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
            .await
    });

    for _ in 0..10 {
        if guard_drops.load(Ordering::SeqCst) == 1 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        guard_drops.load(Ordering::SeqCst),
        1,
        "attempt guards must drop before the retry delay"
    );
    assert_eq!(*ordinals.lock().unwrap(), vec![1]);

    tokio::time::advance(Duration::from_millis(25)).await;
    let report = task.await.expect("source retry task completes");

    match report.outcome {
        SourceBoundaryOutcome::Polled(SourcePollReport {
            result: Ok(SourcePollCompletion::Batch(batch)),
            ..
        }) => assert_eq!(batch.len(), 1),
        _ => panic!("only the successful terminal poll outcome must escape"),
    }
    assert_eq!(*ordinals.lock().unwrap(), vec![1, 2]);
    assert_eq!(admissions.load(Ordering::SeqCst), 2);
    assert_eq!(guard_drops.load(Ordering::SeqCst), 2);
    assert_eq!(
        *log.lock().unwrap(),
        vec![
            "admit:owner:1",
            "admit:inner",
            "commit:owner",
            "commit:inner",
            "observe:inner:failed",
            "observe:owner:failed",
            "admit:owner:2",
            "admit:inner",
            "commit:owner",
            "commit:inner",
            "observe:inner:delivered",
            "observe:owner:delivered",
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
            assert_eq!(*error_kind, Some(ErrorKind::Timeout));
            assert_eq!(*delay_ms, Some(25));
            assert_eq!(context.stage_id, owner.stage_id);
            assert_eq!(context.attachment_id, owner.attachment_id.as_ulid());
            assert_eq!(context.protected_unit, RetryProtectedUnit::SourcePoll);
            match context.invocation {
                RetryInvocation::SourcePoll { poll_id } => {
                    assert_eq!(report.control_events[0].causality.parent_ids, vec![poll_id]);
                }
                ref other => panic!("expected source-poll invocation, got {other:?}"),
            }
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
async fn threshold_one_breaker_keeps_rate_limited_source_retryable_by_default() {
    let owner = source_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
    let breaker: Arc<dyn SourcePolicy> = Arc::new(CircuitBreakerSourcePolicy {
        breaker: Arc::new(CircuitBreakerMiddleware::new(1)),
    });
    let admissions = Arc::new(AtomicUsize::new(0));
    let boundary = PerSourcePolicyBoundary::new(
        vec![
            breaker,
            Arc::new(RetryOwnerPolicy {
                owner,
                log: Arc::new(Mutex::new(Vec::new())),
                admissions: admissions.clone(),
                guard_drops: Arc::new(AtomicUsize::new(0)),
                recovery_allowed: Arc::new(AtomicBool::new(true)),
                observed: None,
            }),
        ],
        WriterId::from(StageId::new()),
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let mut executor = SequenceSourceExecutor {
        outcomes: VecDeque::from([
            Err(SourceError::RateLimited {
                message: "slow down".to_string(),
                retry_after: None,
            }),
            Ok(SourcePollCompletion::Batch(vec![test_event()])),
        ]),
        ordinals: ordinals.clone(),
    };

    let report = boundary
        .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
        .await;

    assert!(matches!(
        report.outcome,
        SourceBoundaryOutcome::Polled(SourcePollReport {
            result: Ok(SourcePollCompletion::Batch(_)),
            ..
        })
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
async fn half_open_rate_limited_source_probe_is_single_shot() {
    let owner = source_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
    let breaker = Arc::new(CircuitBreakerMiddleware::new(1));
    breaker.force_half_open_for_test();
    let breaker_policy: Arc<dyn SourcePolicy> = Arc::new(CircuitBreakerSourcePolicy {
        breaker: breaker.clone(),
    });
    let admissions = Arc::new(AtomicUsize::new(0));
    let boundary = PerSourcePolicyBoundary::new(
        vec![
            breaker_policy,
            Arc::new(RetryOwnerPolicy {
                owner,
                log: Arc::new(Mutex::new(Vec::new())),
                admissions: admissions.clone(),
                guard_drops: Arc::new(AtomicUsize::new(0)),
                recovery_allowed: Arc::new(AtomicBool::new(true)),
                observed: None,
            }),
        ],
        WriterId::from(StageId::new()),
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let mut executor = SequenceSourceExecutor {
        outcomes: VecDeque::from([
            Err(SourceError::RateLimited {
                message: "slow down".to_string(),
                retry_after: None,
            }),
            Ok(SourcePollCompletion::Batch(vec![test_event()])),
        ]),
        ordinals: ordinals.clone(),
    };

    let report = boundary
        .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
        .await;

    assert!(matches!(
        report.outcome,
        SourceBoundaryOutcome::Polled(SourcePollReport {
            result: Err(SourceError::RateLimited { .. }),
            ..
        })
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

#[tokio::test]
async fn retryable_source_max_attempts_one_emits_only_terminal_exhaustion() {
    let owner = source_retry_owner(1, Duration::ZERO, Duration::from_secs(1));
    let boundary = retry_boundary(
        owner,
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(true)),
        false,
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let mut executor = SequenceSourceExecutor {
        outcomes: VecDeque::from([Err(SourceError::Transport("offline".to_string()))]),
        ordinals: ordinals.clone(),
    };

    let report = boundary
        .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
        .await;

    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert_eq!(report.control_events.len(), 1);
    match retry_event(&report.control_events[0]) {
        RetryEvent::Exhausted {
            context: Some(context),
            total_attempts,
            exhaustion_cause,
            last_error_kind,
            ..
        } => {
            assert_eq!(*total_attempts, 1);
            assert_eq!(*exhaustion_cause, Some(RetryExhaustionCause::MaxAttempts));
            assert_eq!(*last_error_kind, Some(ErrorKind::Remote));
            assert!(matches!(
                context.invocation,
                RetryInvocation::SourcePoll { .. }
            ));
        }
        other => panic!("expected one exhausted row, got {other:?}"),
    }
}

#[tokio::test]
async fn source_policy_opening_after_settlement_suppresses_the_next_physical_poll() {
    let boundary = retry_boundary(
        source_retry_owner(3, Duration::ZERO, Duration::from_secs(1)),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(false)),
        false,
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let mut executor = SequenceSourceExecutor {
        outcomes: VecDeque::from([
            Err(SourceError::Timeout("opens breaker".to_string())),
            Ok(SourcePollCompletion::Batch(vec![test_event()])),
        ]),
        ordinals: ordinals.clone(),
    };

    let report = boundary
        .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
        .await;

    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert_eq!(report.control_events.len(), 1);
    match retry_event(&report.control_events[0]) {
        RetryEvent::Exhausted {
            total_attempts,
            exhaustion_cause,
            ..
        } => {
            assert_eq!(*total_attempts, 1);
            assert_eq!(
                *exhaustion_cause,
                Some(RetryExhaustionCause::PolicyRejected)
            );
        }
        other => panic!("expected policy-rejected exhaustion, got {other:?}"),
    }
}

#[tokio::test]
async fn source_retry_after_above_the_ceiling_never_starts_a_second_attempt() {
    let boundary = retry_boundary(
        source_retry_owner(3, Duration::from_millis(5), Duration::from_millis(50)),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(true)),
        false,
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let mut executor = SequenceSourceExecutor {
        outcomes: VecDeque::from([Err(SourceError::RateLimited {
            message: "slow down".to_string(),
            retry_after: Some(Duration::from_millis(51)),
        })]),
        ordinals: ordinals.clone(),
    };

    let report = boundary
        .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
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

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[tokio::test]
async fn retryable_sink_typed_remote_error_then_success_returns_one_terminal_delivery() {
    let owner = sink_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
    let admissions = Arc::new(AtomicUsize::new(0));
    let log = Arc::new(Mutex::new(Vec::new()));
    let boundary = retry_sink_boundary(owner.clone(), admissions.clone(), log.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let parent_event_id = EventId::from(Ulid::from(0x115_1003_u128));
    let mut executor = SequenceSinkExecutor {
        parent_event_id,
        outcomes: VecDeque::from([
            SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(
                "temporary".to_string(),
            ))),
            SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(
                DeliveryPayload::success(DeliveryMethod::Noop, None),
            )))),
        ]),
        calls: calls.clone(),
    };

    let report = boundary
        .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
        .await;

    assert!(matches!(
        report.outcome,
        SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(_)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(admissions.load(Ordering::SeqCst), 2);
    assert_eq!(
        *log.lock().unwrap(),
        vec![
            "admit:1",
            "commit",
            "observe:failed",
            "admit:2",
            "commit",
            "observe:delivered",
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
                RetryProtectedUnit::SinkDelivery {
                    configured_target_id: None
                }
            );
            assert_eq!(
                context.invocation,
                RetryInvocation::SinkDelivery { parent_event_id }
            );
            assert_eq!(
                report.control_events[0].causality.parent_ids,
                vec![parent_event_id]
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
async fn threshold_one_breaker_keeps_rate_limited_sink_retryable_by_default() {
    let owner = sink_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
    let admissions = Arc::new(AtomicUsize::new(0));
    let breaker: Arc<dyn SinkPolicy> = Arc::new(CircuitBreakerSinkPolicy {
        breaker: Arc::new(CircuitBreakerMiddleware::new(1)),
    });
    let boundary = PerSinkDeliveryPolicyBoundary::new(vec![
        breaker,
        Arc::new(SinkRetryOwnerPolicy {
            owner,
            admissions: admissions.clone(),
            log: Arc::new(Mutex::new(Vec::new())),
            guard_drops: None,
            observed: None,
        }),
    ]);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut executor = SequenceSinkExecutor {
        parent_event_id: EventId::new(),
        outcomes: VecDeque::from([
            SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::RateLimited {
                message: "slow down".to_string(),
                retry_after: None,
            })),
            SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(
                DeliveryPayload::success(DeliveryMethod::Noop, None),
            )))),
        ]),
        calls: calls.clone(),
    };

    let report = boundary
        .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
        .await;

    assert!(matches!(
        report.outcome,
        SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(_)))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
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
async fn half_open_rate_limited_sink_probe_is_single_shot() {
    let owner = sink_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
    let admissions = Arc::new(AtomicUsize::new(0));
    let breaker = Arc::new(CircuitBreakerMiddleware::new(1));
    breaker.force_half_open_for_test();
    let breaker_policy: Arc<dyn SinkPolicy> = Arc::new(CircuitBreakerSinkPolicy {
        breaker: breaker.clone(),
    });
    let boundary = PerSinkDeliveryPolicyBoundary::new(vec![
        breaker_policy,
        Arc::new(SinkRetryOwnerPolicy {
            owner,
            admissions: admissions.clone(),
            log: Arc::new(Mutex::new(Vec::new())),
            guard_drops: None,
            observed: None,
        }),
    ]);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut executor = SequenceSinkExecutor {
        parent_event_id: EventId::new(),
        outcomes: VecDeque::from([
            SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::RateLimited {
                message: "slow down".to_string(),
                retry_after: None,
            })),
            SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(
                DeliveryPayload::success(DeliveryMethod::Noop, None),
            )))),
        ]),
        calls: calls.clone(),
    };

    let report = boundary
        .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
        .await;

    assert!(matches!(
        report.outcome,
        SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Err(
            HandlerError::RateLimited { .. }
        )))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
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
async fn sink_returned_buffered_failed_and_partial_reports_are_terminal_not_retry_signals() {
    let cases = [
        (
            "buffered",
            DeliveryPayload::buffered(DeliveryMethod::Noop, Some(1)),
        ),
        (
            "failed",
            DeliveryPayload::failed(
                DeliveryMethod::Noop,
                "destination",
                "typed terminal failure",
                true,
            ),
        ),
        (
            "partial",
            DeliveryPayload::partial(
                DeliveryMethod::Noop,
                2,
                1,
                "one item failed",
                Some(vec!["item-3".to_string()]),
            ),
        ),
    ];

    for (case, payload) in cases {
        let boundary = retry_sink_boundary(
            sink_retry_owner(3, Duration::ZERO, Duration::from_secs(1)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(Vec::new())),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let mut executor = SequenceSinkExecutor {
            parent_event_id: EventId::new(),
            outcomes: VecDeque::from([SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(
                SinkConsumeReport::new(payload),
            )))]),
            calls: calls.clone(),
        };

        let report = boundary
            .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert!(
            matches!(
                report.outcome,
                SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(
                    _
                )))
            ),
            "{case} remains the logical terminal delivery report"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "{case} is not an executor error and must never be retried"
        );
        assert!(
            report.control_events.is_empty(),
            "{case} emits no retry lifecycle rows"
        );
    }
}

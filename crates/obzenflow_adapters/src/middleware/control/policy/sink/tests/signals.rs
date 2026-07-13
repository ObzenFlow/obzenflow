// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[tokio::test(start_paused = true)]
async fn active_sink_deadline_is_outcome_unknown_after_one_charged_attempt() {
    let mut owner = sink_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
    owner.policy.max_total_wall_time = Duration::from_millis(50);
    let admissions = Arc::new(AtomicUsize::new(0));
    let log = Arc::new(Mutex::new(Vec::new()));
    let boundary = retry_sink_boundary(owner, admissions.clone(), log.clone());
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let calls_for_task = calls.clone();
    let started_for_task = started.clone();
    let dropped_for_task = dropped.clone();

    let task = tokio::spawn(async move {
        let mut executor = PendingSinkExecutor {
            parent_event_id: EventId::new(),
            calls: calls_for_task,
            started: started_for_task,
            dropped: dropped_for_task,
        };
        boundary
            .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
            .await
    });

    started.notified().await;
    tokio::time::advance(Duration::from_millis(50)).await;
    let report = task.await.expect("sink deadline resolves active delivery");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert_eq!(
        *log.lock().unwrap(),
        vec!["admit:1", "commit", "observe:failed"]
    );
    assert!(matches!(
        report.outcome,
        SinkDeliveryBoundaryOutcome::DeadlineOutcomeUnknown { ref message }
            if message
                == "sink deadline expired while the external outcome remained in doubt"
    ));
    assert_eq!(report.control_events.len(), 1);
    assert!(matches!(
        retry_event(&report.control_events[0]),
        RetryEvent::Exhausted {
            total_attempts: 1,
            exhaustion_cause: Some(RetryExhaustionCause::TotalWallTime),
            last_error_kind: Some(ErrorKind::Timeout),
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn force_abort_drops_active_sink_and_admission_guard_without_rows() {
    let admissions = Arc::new(AtomicUsize::new(0));
    let log = Arc::new(Mutex::new(Vec::new()));
    let guard_drops = Arc::new(AtomicUsize::new(0));
    let boundary = retry_sink_boundary_with_guard(
        sink_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
        admissions.clone(),
        log.clone(),
        Some(guard_drops.clone()),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
    let calls_for_task = calls.clone();
    let started_for_task = started.clone();
    let dropped_for_task = dropped.clone();

    let task = tokio::spawn(async move {
        let mut executor = PendingSinkExecutor {
            parent_event_id: EventId::new(),
            calls: calls_for_task,
            started: started_for_task,
            dropped: dropped_for_task,
        };
        boundary
            .around_retryable_sink_delivery(&mut executor, receiver)
            .await
    });

    started.notified().await;
    controller.request_abort();
    let report = task
        .await
        .expect("force abort cancels active sink delivery");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(guard_drops.load(Ordering::SeqCst), 1);
    assert_eq!(*log.lock().unwrap(), vec!["admit:1", "commit"]);
    assert!(matches!(
        report.outcome,
        SinkDeliveryBoundaryOutcome::Rejected(ref rejection)
            if rejection.policy == "retry_coordinator"
                && rejection.reason == "force abort requested during sink delivery"
    ));
    assert!(report.control_events.is_empty());
}

#[tokio::test(start_paused = true)]
async fn graceful_drain_during_active_retryable_sink_starts_no_next_attempt() {
    let admissions = Arc::new(AtomicUsize::new(0));
    let boundary = retry_sink_boundary(
        sink_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
        admissions.clone(),
        Arc::new(Mutex::new(Vec::new())),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
    let calls_for_task = calls.clone();
    let started_for_task = started.clone();
    let release_for_task = release.clone();

    let task = tokio::spawn(async move {
        let mut executor = ControlledSinkExecutor {
            parent_event_id: EventId::new(),
            outcome: Some(SinkDeliveryAttemptOutcome::Delivered(Err(
                HandlerError::Remote("retryable".to_string()),
            ))),
            calls: calls_for_task,
            started: started_for_task,
            release: release_for_task,
        };
        boundary
            .around_retryable_sink_delivery(&mut executor, receiver)
            .await
    });

    started.notified().await;
    controller.request_drain();
    release.notify_one();
    let report = task
        .await
        .expect("active sink delivery settles during drain");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert!(matches!(
        report.outcome,
        SinkDeliveryBoundaryOutcome::Attempted(
            SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(ref message)))
        ) if message == "retryable"
    ));
    assert_eq!(report.control_events.len(), 1);
    assert!(matches!(
        retry_event(&report.control_events[0]),
        RetryEvent::Exhausted {
            total_attempts: 1,
            exhaustion_cause: Some(RetryExhaustionCause::DrainRequested),
            last_error_kind: Some(ErrorKind::Remote),
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn graceful_drain_during_sink_backoff_records_drain_and_starts_no_next_attempt() {
    let observed = Arc::new(Notify::new());
    let admissions = Arc::new(AtomicUsize::new(0));
    let boundary = retry_sink_boundary_with_observer(
        sink_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
        admissions.clone(),
        Arc::new(Mutex::new(Vec::new())),
        observed.clone(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_task = calls.clone();
    let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();

    let task = tokio::spawn(async move {
        let mut executor = SequenceSinkExecutor {
            parent_event_id: EventId::new(),
            outcomes: VecDeque::from([
                SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(
                    "retryable".to_string(),
                ))),
                SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(
                    DeliveryPayload::success(DeliveryMethod::Noop, None),
                )))),
            ]),
            calls: calls_for_task,
        };
        boundary
            .around_retryable_sink_delivery(&mut executor, receiver)
            .await
    });

    observed.notified().await;
    controller.request_drain();
    let report = task.await.expect("drain interrupts sink backoff");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert!(matches!(
        report.outcome,
        SinkDeliveryBoundaryOutcome::Attempted(
            SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(ref message)))
        ) if message == "retryable"
    ));
    assert_eq!(report.control_events.len(), 2);
    assert!(matches!(
        retry_event(&report.control_events[0]),
        RetryEvent::AttemptFailed {
            attempt_number: 1,
            delay_ms: Some(10_000),
            ..
        }
    ));
    assert!(matches!(
        retry_event(&report.control_events[1]),
        RetryEvent::Exhausted {
            total_attempts: 1,
            exhaustion_cause: Some(RetryExhaustionCause::DrainRequested),
            last_error_kind: Some(ErrorKind::Remote),
            ..
        }
    ));
}

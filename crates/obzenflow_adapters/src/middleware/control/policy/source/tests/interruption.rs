// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[tokio::test(start_paused = true)]
async fn active_source_deadline_returns_cancelled_poll_after_one_charged_attempt() {
    let mut owner = source_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
    owner.policy.max_total_wall_time = Duration::from_millis(50);
    let log = Arc::new(Mutex::new(Vec::new()));
    let admissions = Arc::new(AtomicUsize::new(0));
    let guard_drops = Arc::new(AtomicUsize::new(0));
    let boundary = retry_boundary(
        owner,
        log.clone(),
        admissions.clone(),
        guard_drops.clone(),
        Arc::new(AtomicBool::new(true)),
        false,
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let calls_for_task = calls.clone();
    let started_for_task = started.clone();
    let dropped_for_task = dropped.clone();

    let task = tokio::spawn(async move {
        let mut executor = PendingSourceExecutor {
            calls: calls_for_task,
            started: started_for_task,
            dropped: dropped_for_task,
        };
        boundary
            .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
            .await
    });

    started.notified().await;
    tokio::time::advance(Duration::from_millis(50)).await;
    let report = task.await.expect("source deadline resolves active poll");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert_eq!(guard_drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        *log.lock().unwrap(),
        vec!["admit:owner:1", "commit:owner", "observe:owner:failed"]
    );
    assert!(matches!(
        report.outcome,
        SourceBoundaryOutcome::Polled(SourcePollReport {
            result: Err(SourceError::Timeout(ref message)),
            ..
        }) if message == "source_poll_cancelled"
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
async fn force_abort_drops_active_source_and_admission_guard_without_rows() {
    let log = Arc::new(Mutex::new(Vec::new()));
    let admissions = Arc::new(AtomicUsize::new(0));
    let guard_drops = Arc::new(AtomicUsize::new(0));
    let boundary = retry_boundary(
        source_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
        log.clone(),
        admissions.clone(),
        guard_drops.clone(),
        Arc::new(AtomicBool::new(true)),
        false,
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
    let calls_for_task = calls.clone();
    let started_for_task = started.clone();
    let dropped_for_task = dropped.clone();

    let task = tokio::spawn(async move {
        let mut executor = PendingSourceExecutor {
            calls: calls_for_task,
            started: started_for_task,
            dropped: dropped_for_task,
        };
        boundary
            .around_retryable_poll(&mut executor, receiver)
            .await
    });

    started.notified().await;
    controller.request_abort();
    let report = task.await.expect("force abort cancels active source poll");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(guard_drops.load(Ordering::SeqCst), 1);
    assert_eq!(*log.lock().unwrap(), vec!["admit:owner:1", "commit:owner"]);
    assert!(matches!(
        report.outcome,
        SourceBoundaryOutcome::Rejected { ref reason }
            if reason == "force abort requested during source poll"
    ));
    assert!(report.control_events.is_empty());
}

#[tokio::test(start_paused = true)]
async fn graceful_drain_during_active_retryable_source_starts_no_next_attempt() {
    let admissions = Arc::new(AtomicUsize::new(0));
    let guard_drops = Arc::new(AtomicUsize::new(0));
    let boundary = retry_boundary(
        source_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
        Arc::new(Mutex::new(Vec::new())),
        admissions.clone(),
        guard_drops.clone(),
        Arc::new(AtomicBool::new(true)),
        false,
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
    let ordinals_for_task = ordinals.clone();
    let started_for_task = started.clone();
    let release_for_task = release.clone();

    let task = tokio::spawn(async move {
        let mut executor = ControlledSourceExecutor {
            outcome: Some(Err(SourceError::Timeout("retryable".to_string()))),
            ordinals: ordinals_for_task,
            started: started_for_task,
            release: release_for_task,
        };
        boundary
            .around_retryable_poll(&mut executor, receiver)
            .await
    });

    started.notified().await;
    controller.request_drain();
    release.notify_one();
    let report = task.await.expect("active source poll settles during drain");

    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert_eq!(guard_drops.load(Ordering::SeqCst), 1);
    assert!(matches!(
        report.outcome,
        SourceBoundaryOutcome::Polled(SourcePollReport {
            result: Err(SourceError::Timeout(ref message)),
            ..
        }) if message == "retryable"
    ));
    assert_eq!(report.control_events.len(), 1);
    assert!(matches!(
        retry_event(&report.control_events[0]),
        RetryEvent::Exhausted {
            total_attempts: 1,
            exhaustion_cause: Some(RetryExhaustionCause::DrainRequested),
            last_error_kind: Some(ErrorKind::Timeout),
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn graceful_drain_during_source_backoff_records_drain_and_starts_no_next_attempt() {
    let observed = Arc::new(Notify::new());
    let boundary = retry_boundary_with_observer(
        source_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(true)),
        false,
        Some(observed.clone()),
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let ordinals_for_task = ordinals.clone();
    let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();

    let task = tokio::spawn(async move {
        let mut executor = SequenceSourceExecutor {
            outcomes: VecDeque::from([
                Err(SourceError::Timeout("retryable".to_string())),
                Ok(SourcePollCompletion::Batch(Vec::new())),
            ]),
            ordinals: ordinals_for_task,
        };
        boundary
            .around_retryable_poll(&mut executor, receiver)
            .await
    });

    observed.notified().await;
    controller.request_drain();
    let report = task.await.expect("drain interrupts source backoff");

    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert!(matches!(
        report.outcome,
        SourceBoundaryOutcome::Polled(SourcePollReport {
            result: Err(SourceError::Timeout(ref message)),
            ..
        }) if message == "retryable"
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
            last_error_kind: Some(ErrorKind::Timeout),
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn after_poll_deadline_preserves_known_batch_and_observes_every_policy() {
    let mut owner = source_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
    owner.policy.max_total_wall_time = Duration::from_millis(50);
    let skipped_after_poll_calls = Arc::new(AtomicUsize::new(0));
    let outer_observations = Arc::new(AtomicUsize::new(0));
    let owner_observations = Arc::new(AtomicUsize::new(0));
    let after_poll_started = Arc::new(Notify::new());
    let boundary = PerSourcePolicyBoundary::new(
        vec![
            Arc::new(ObserveOnlyAfterDeadlinePolicy {
                after_poll_calls: skipped_after_poll_calls.clone(),
                observations: outer_observations.clone(),
            }),
            Arc::new(BlockingAfterPollRetryOwnerPolicy {
                owner,
                after_poll_started: after_poll_started.clone(),
                observations: owner_observations.clone(),
            }),
        ],
        WriterId::from(StageId::new()),
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let ordinals_for_task = ordinals.clone();

    let task = tokio::spawn(async move {
        let mut executor = SequenceSourceExecutor {
            outcomes: VecDeque::from([Ok(SourcePollCompletion::Batch(vec![test_event()]))]),
            ordinals: ordinals_for_task,
        };
        boundary
            .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
            .await
    });

    after_poll_started.notified().await;
    tokio::time::advance(Duration::from_millis(50)).await;
    let report = task.await.expect("source settlement deadline resolves");

    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert_eq!(skipped_after_poll_calls.load(Ordering::SeqCst), 0);
    assert_eq!(owner_observations.load(Ordering::SeqCst), 1);
    assert_eq!(outer_observations.load(Ordering::SeqCst), 1);
    assert!(matches!(
        report.outcome,
        SourceBoundaryOutcome::Polled(SourcePollReport {
            result: Ok(SourcePollCompletion::Batch(ref batch)),
            ..
        }) if batch.len() == 1
    ));
    assert!(report.control_events.is_empty());
}

#[tokio::test]
async fn graceful_drain_during_after_poll_settles_the_known_batch() {
    let after_poll_started = Arc::new(Notify::new());
    let after_poll_release = Arc::new(Notify::new());
    let after_poll_calls = Arc::new(AtomicUsize::new(0));
    let observations = Arc::new(AtomicUsize::new(0));
    let boundary = PerSourcePolicyBoundary::new(
        vec![Arc::new(ControlledAfterPollRetryOwnerPolicy {
            owner: source_retry_owner(3, Duration::ZERO, Duration::from_secs(1)),
            after_poll_started: after_poll_started.clone(),
            after_poll_release: after_poll_release.clone(),
            after_poll_calls: after_poll_calls.clone(),
            observations: observations.clone(),
        })],
        WriterId::from(StageId::new()),
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let ordinals_for_task = ordinals.clone();
    let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();

    let task = tokio::spawn(async move {
        let mut executor = SequenceSourceExecutor {
            outcomes: VecDeque::from([Ok(SourcePollCompletion::Batch(vec![test_event()]))]),
            ordinals: ordinals_for_task,
        };
        boundary
            .around_retryable_poll(&mut executor, receiver)
            .await
    });

    after_poll_started.notified().await;
    controller.request_drain();
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "graceful drain must not cancel settlement of a known source result"
    );
    after_poll_release.notify_one();
    let report = task
        .await
        .expect("source settlement completes during drain");

    assert_eq!(*ordinals.lock().unwrap(), [1]);
    assert_eq!(after_poll_calls.load(Ordering::SeqCst), 1);
    assert_eq!(observations.load(Ordering::SeqCst), 1);
    assert!(matches!(
        report.outcome,
        SourceBoundaryOutcome::Polled(SourcePollReport {
            result: Ok(SourcePollCompletion::Batch(ref batch)),
            ..
        }) if batch.len() == 1
    ));
    assert!(report.control_events.is_empty());
}

#[tokio::test(start_paused = true)]
async fn source_backoff_deadline_starts_no_next_attempt() {
    let mut owner = source_retry_owner(3, Duration::from_millis(50), Duration::from_millis(50));
    owner.policy.max_total_wall_time = Duration::from_millis(100);
    let observed = Arc::new(Notify::new());
    let boundary = retry_boundary_with_observer(
        owner,
        Arc::new(Mutex::new(Vec::new())),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(true)),
        false,
        Some(observed.clone()),
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let ordinals_for_task = ordinals.clone();
    let task = tokio::spawn(async move {
        let mut executor = SequenceSourceExecutor {
            outcomes: VecDeque::from([
                Err(SourceError::Timeout("retryable".to_string())),
                Ok(SourcePollCompletion::Batch(Vec::new())),
            ]),
            ordinals: ordinals_for_task,
        };
        boundary
            .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
            .await
    });

    observed.notified().await;
    tokio::time::advance(Duration::from_millis(100)).await;
    let report = task.await.expect("deadline interrupts source backoff");

    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert!(matches!(
        report.outcome,
        SourceBoundaryOutcome::Polled(SourcePollReport {
            result: Err(SourceError::Timeout(ref message)),
            ..
        }) if message == "retryable"
    ));
    assert_eq!(report.control_events.len(), 2);
    assert!(matches!(
        retry_event(&report.control_events[0]),
        RetryEvent::AttemptFailed {
            attempt_number: 1,
            delay_ms: Some(50),
            ..
        }
    ));
    assert!(matches!(
        retry_event(&report.control_events[1]),
        RetryEvent::Exhausted {
            total_attempts: 1,
            exhaustion_cause: Some(RetryExhaustionCause::TotalWallTime),
            last_error_kind: Some(ErrorKind::Timeout),
            ..
        }
    ));
}

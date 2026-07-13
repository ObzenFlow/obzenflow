// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;

#[tokio::test(start_paused = true)]
async fn initial_admission_ready_at_the_exact_deadline_expires_without_execution() {
    let mut owner = effect_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
    owner.policy.max_total_wall_time = Duration::from_millis(50);
    let admissions = Arc::new(AtomicUsize::new(0));
    let admission_started = Arc::new(Notify::new());
    let mut chains = HashMap::new();
    chains.insert(
        "effect.retry_test",
        Arc::new(vec![EffectPolicyAttachment::neutral(Arc::new(
            DelayedAdmissionEffectPolicy {
                owner,
                admissions: admissions.clone(),
                started: admission_started.clone(),
                delay: Duration::from_millis(50),
            },
        ))]),
    );
    let boundary = PerEffectPolicyBoundary::new(chains);
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let ordinals_for_task = ordinals.clone();

    let task = tokio::spawn(async move {
        let mut executor = SequenceEffectExecutor {
            outcomes: VecDeque::from([Ok(Vec::new())]),
            ordinals: ordinals_for_task,
        };
        boundary
            .around_retryable_effect(
                &identity_for("effect.retry_test"),
                &data_event(),
                &mut executor,
                BoundaryStopReceiver::default(),
            )
            .await
    });

    admission_started.notified().await;
    tokio::time::advance(Duration::from_millis(50)).await;
    let report = task.await.expect("deadline resolves the admission wait");

    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert!(ordinals.lock().unwrap().is_empty());
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Aborted(ref reason)
            if reason.cause.code == "retry_total_wall_time"
    ));
    assert_eq!(report.control_events.len(), 1);
    assert!(matches!(
        retry_event(&report.control_events[0]),
        RetryEvent::Exhausted {
            total_attempts: 0,
            exhaustion_cause: Some(RetryExhaustionCause::TotalWallTime),
            total_duration_ms: 50,
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn active_effect_deadline_is_outcome_unknown_after_one_charged_attempt() {
    let mut owner = effect_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
    owner.policy.max_total_wall_time = Duration::from_millis(50);
    let admissions = Arc::new(AtomicUsize::new(0));
    let log = Arc::new(Mutex::new(Vec::new()));
    let boundary = retry_effect_boundary(owner, admissions.clone(), log.clone());
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let ordinals_for_task = ordinals.clone();
    let started_for_task = started.clone();

    let task = tokio::spawn(async move {
        let mut executor = ControlledEffectExecutor {
            outcome: Some(Ok(Vec::new())),
            ordinals: ordinals_for_task,
            started: started_for_task,
            release,
        };
        boundary
            .around_retryable_effect(
                &identity_for("effect.retry_test"),
                &data_event(),
                &mut executor,
                BoundaryStopReceiver::default(),
            )
            .await
    });

    started.notified().await;
    tokio::time::advance(Duration::from_millis(50)).await;
    let report = task.await.expect("effect deadline resolves active call");

    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert_eq!(
        *log.lock().unwrap(),
        vec!["admit:1", "commit", "observe:error"]
    );
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Aborted(ref reason)
            if reason.cause.code == "deadline_outcome_unknown"
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
async fn force_abort_drops_active_effect_and_emits_no_buffered_rows() {
    let admissions = Arc::new(AtomicUsize::new(0));
    let log = Arc::new(Mutex::new(Vec::new()));
    let boundary = retry_effect_boundary(
        effect_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
        admissions.clone(),
        log.clone(),
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
    let ordinals_for_task = ordinals.clone();
    let started_for_task = started.clone();
    let dropped_for_task = dropped.clone();

    let task = tokio::spawn(async move {
        let mut executor = PendingEffectExecutor {
            ordinals: ordinals_for_task,
            started: started_for_task,
            dropped: dropped_for_task,
        };
        boundary
            .around_retryable_effect(
                &identity_for("effect.retry_test"),
                &data_event(),
                &mut executor,
                receiver,
            )
            .await
    });

    started.notified().await;
    controller.request_abort();
    let report = task.await.expect("force abort cancels active effect");

    assert_eq!(admissions.load(Ordering::SeqCst), 1);
    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(*log.lock().unwrap(), vec!["admit:1", "commit"]);
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Aborted(ref reason)
            if reason.cause.code == "force_aborted"
    ));
    assert!(report.control_events.is_empty());
}

#[tokio::test(start_paused = true)]
async fn graceful_drain_during_active_retryable_effect_starts_no_next_attempt() {
    let boundary = retry_effect_boundary(
        effect_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(Mutex::new(Vec::new())),
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
    let ordinals_for_task = ordinals.clone();
    let started_for_task = started.clone();
    let release_for_task = release.clone();

    let task = tokio::spawn(async move {
        let mut executor = ControlledEffectExecutor {
            outcome: Some(Err(EffectError::TransientExecution(
                "retryable".to_string(),
            ))),
            ordinals: ordinals_for_task,
            started: started_for_task,
            release: release_for_task,
        };
        boundary
            .around_retryable_effect(
                &identity_for("effect.retry_test"),
                &data_event(),
                &mut executor,
                receiver,
            )
            .await
    });

    started.notified().await;
    controller.request_drain();
    release.notify_one();
    let report = task.await.expect("active effect settles during drain");

    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::TransientExecution(_)))
    ));
    assert_eq!(report.control_events.len(), 1);
    assert!(matches!(
        retry_event(&report.control_events[0]),
        RetryEvent::Exhausted {
            total_attempts: 1,
            exhaustion_cause: Some(RetryExhaustionCause::DrainRequested),
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn graceful_drain_during_effect_backoff_records_drain_and_starts_no_next_attempt() {
    let observed = Arc::new(Notify::new());
    let boundary = retry_effect_boundary_with_observer(
        effect_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(Mutex::new(Vec::new())),
        Some(observed.clone()),
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let ordinals_for_task = ordinals.clone();
    let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();

    let task = tokio::spawn(async move {
        let mut executor = SequenceEffectExecutor {
            outcomes: VecDeque::from([
                Err(EffectError::TransientExecution("retryable".to_string())),
                Ok(Vec::new()),
            ]),
            ordinals: ordinals_for_task,
        };
        boundary
            .around_retryable_effect(
                &identity_for("effect.retry_test"),
                &data_event(),
                &mut executor,
                receiver,
            )
            .await
    });

    observed.notified().await;
    controller.request_drain();
    let report = task.await.expect("drain interrupts effect backoff");

    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert_eq!(report.control_events.len(), 2);
    assert!(matches!(
        retry_event(&report.control_events[0]),
        RetryEvent::AttemptFailed {
            attempt_number: 1,
            ..
        }
    ));
    assert!(matches!(
        retry_event(&report.control_events[1]),
        RetryEvent::Exhausted {
            total_attempts: 1,
            exhaustion_cause: Some(RetryExhaustionCause::DrainRequested),
            ..
        }
    ));
}

#[tokio::test(start_paused = true)]
async fn force_abort_during_effect_backoff_discards_buffered_attempt_row() {
    let observed = Arc::new(Notify::new());
    let boundary = retry_effect_boundary_with_observer(
        effect_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(Mutex::new(Vec::new())),
        Some(observed.clone()),
    );
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let ordinals_for_task = ordinals.clone();
    let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();

    let task = tokio::spawn(async move {
        let mut executor = SequenceEffectExecutor {
            outcomes: VecDeque::from([
                Err(EffectError::TransientExecution("retryable".to_string())),
                Ok(Vec::new()),
            ]),
            ordinals: ordinals_for_task,
        };
        boundary
            .around_retryable_effect(
                &identity_for("effect.retry_test"),
                &data_event(),
                &mut executor,
                receiver,
            )
            .await
    });

    // Observation is synchronous and the coordinator's next await is the
    // backoff, after it has buffered AttemptFailed for attempt one.
    observed.notified().await;
    controller.request_abort();
    let report = task.await.expect("force abort interrupts effect backoff");

    assert_eq!(*ordinals.lock().unwrap(), vec![1]);
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Executed(Err(EffectError::TransientExecution(_)))
    ));
    assert!(report.control_events.is_empty());
}

#[tokio::test(start_paused = true)]
async fn unrepresentable_deadline_rejects_before_admission_and_emits_no_retry_rows() {
    let mut owner = effect_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
    owner.policy.max_total_wall_time = Duration::MAX;
    let admissions = Arc::new(AtomicUsize::new(0));
    let boundary =
        retry_effect_boundary(owner, admissions.clone(), Arc::new(Mutex::new(Vec::new())));
    let ordinals = Arc::new(Mutex::new(Vec::new()));
    let mut executor = SequenceEffectExecutor {
        outcomes: VecDeque::from([Ok(Vec::new())]),
        ordinals: ordinals.clone(),
    };

    let report = boundary
        .around_retryable_effect(
            &identity_for("effect.retry_test"),
            &data_event(),
            &mut executor,
            BoundaryStopReceiver::default(),
        )
        .await;

    assert_eq!(admissions.load(Ordering::SeqCst), 0);
    assert!(ordinals.lock().unwrap().is_empty());
    assert!(report.control_events.is_empty());
    assert!(matches!(
        report.outcome,
        EffectBoundaryOutcome::Aborted(ref reason)
            if reason.cause.code == "retry_deadline_unrepresentable"
    ));
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::payloads::observability_payload::RetryExhaustionCause;
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use std::num::NonZeroU32;
use std::time::Duration;

#[tokio::test(start_paused = true)]
async fn exact_deadline_readiness_tie_favours_expiry() {
    let deadline = tokio::time::Instant::now();
    let mut before_stop = BoundaryStopReceiver::default();
    let before =
        await_before_execution(std::future::ready("admitted"), deadline, &mut before_stop).await;
    assert_eq!(before, Err(RetryWaitError::Deadline));

    let mut active_stop = BoundaryStopReceiver::default();
    let active = await_active_execution(
        || std::future::ready("completed"),
        deadline,
        &mut active_stop,
    )
    .await;
    assert_eq!(active, Err(RetryWaitError::Deadline));
}

#[tokio::test(start_paused = true)]
async fn active_execution_start_is_not_invoked_after_deadline_or_prestart_drain() {
    let deadline_starts = std::cell::Cell::new(0_u32);
    let mut deadline_stop = BoundaryStopReceiver::default();
    let deadline_result = await_active_execution(
        || {
            deadline_starts.set(deadline_starts.get() + 1);
            std::future::ready(())
        },
        tokio::time::Instant::now(),
        &mut deadline_stop,
    )
    .await;
    assert_eq!(deadline_result, Err(RetryWaitError::Deadline));
    assert_eq!(deadline_starts.get(), 0);

    let drain_starts = std::cell::Cell::new(0_u32);
    let (controller, mut drain_stop) = obzenflow_runtime::stages::common::boundary_stop_channel();
    controller.request_drain();
    let drain_result = await_active_execution(
        || {
            drain_starts.set(drain_starts.get() + 1);
            std::future::ready(())
        },
        tokio::time::Instant::now() + Duration::from_secs(1),
        &mut drain_stop,
    )
    .await;
    assert_eq!(drain_result, Err(RetryWaitError::Drain));
    assert_eq!(drain_starts.get(), 0);
}

#[tokio::test(start_paused = true)]
async fn exact_deadline_and_drain_backoff_tie_preserves_deadline_result() {
    let (controller, mut stop) = obzenflow_runtime::stages::common::boundary_stop_channel();
    controller.request_drain();

    let result = await_before_execution(
        std::future::pending::<()>(),
        tokio::time::Instant::now(),
        &mut stop,
    )
    .await;

    assert_eq!(result, Err(RetryWaitError::Deadline));
}

#[tokio::test(start_paused = true)]
async fn provider_hint_at_remaining_deadline_is_not_shortened_or_slept() {
    let policy = BoundaryRetryPolicy {
        max_attempts: NonZeroU32::new(2).unwrap(),
        backoff: BackoffStrategy::Fixed {
            delay: Duration::ZERO,
        },
        max_single_delay: Duration::from_secs(1),
        max_total_wall_time: Duration::from_millis(50),
    };
    let budget = RetryBudget::new(policy).expect("deadline is representable");

    assert_eq!(
        budget.delay_after(NonZeroU32::MIN, Some(Duration::from_millis(50))),
        Err(RetryExhaustionCause::RetryHintExceedsLimit)
    );
    assert_eq!(tokio::time::Instant::now(), budget.started);
}

#[tokio::test(start_paused = true)]
async fn configured_delay_at_remaining_deadline_exhausts_without_sleeping() {
    let policy = BoundaryRetryPolicy {
        max_attempts: NonZeroU32::new(2).unwrap(),
        backoff: BackoffStrategy::Fixed {
            delay: Duration::from_millis(50),
        },
        max_single_delay: Duration::from_secs(1),
        max_total_wall_time: Duration::from_millis(50),
    };
    let budget = RetryBudget::new(policy).expect("deadline is representable");

    assert_eq!(
        budget.delay_after(NonZeroU32::MIN, None),
        Err(RetryExhaustionCause::TotalWallTime)
    );
    assert_eq!(tokio::time::Instant::now(), budget.started);
}

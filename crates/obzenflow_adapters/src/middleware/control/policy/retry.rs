// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Surface-neutral retry coordination facts shared by adapter-owned boundaries.

use crate::middleware::MiddlewareAttachmentId;
use obzenflow_core::event::payloads::observability_payload::{
    RetryExhaustionCause, RetryLifecycleContext,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{
    ChainEventFactory, RetryAttemptFailedEventParams, RetryExhaustedEventParams,
    RetrySucceededAfterRetryEventParams,
};
use obzenflow_core::{ChainEvent, EventId, StageId, WriterId};
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use obzenflow_runtime::stages::common::{BoundaryStopIntent, BoundaryStopReceiver};
use std::future::Future;
use std::num::NonZeroU32;
use std::time::Duration;

/// Materialisation-validated breaker recovery policy.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct BoundaryRetryPolicy {
    pub max_attempts: NonZeroU32,
    pub backoff: BackoffStrategy,
    pub max_single_delay: Duration,
    pub max_total_wall_time: Duration,
}

impl BoundaryRetryPolicy {
    /// Delay before the next one-based attempt. The completed attempt number is
    /// converted to the existing zero-based backoff input.
    pub fn configured_delay_after(&self, completed_attempt: NonZeroU32) -> Duration {
        self.backoff
            .calculate_delay(completed_attempt.get().saturating_sub(1) as usize)
            .min(self.max_single_delay)
    }
}

/// Immutable recovery-owner facts exposed by one breaker policy to its boundary.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct BoundaryRetryOwner {
    pub attachment_id: MiddlewareAttachmentId,
    pub stage_id: StageId,
    pub writer_id: WriterId,
    pub protected_unit_label: String,
    pub sink_configured_target_id: Option<obzenflow_core::Ulid>,
    pub policy: BoundaryRetryPolicy,
}

/// Surface-neutral dynamic result used to decide whether another physical call
/// is eligible. Breaker health classification remains independent.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptDisposition {
    Completed,
    RetryableFailure {
        kind: ErrorKind,
        retry_after: Option<Duration>,
    },
    TerminalFailure {
        kind: ErrorKind,
    },
    NotExecuted,
}

/// One invocation's hard retry budget, anchored before initial admission.
pub(crate) struct RetryBudget {
    pub(crate) policy: BoundaryRetryPolicy,
    started: tokio::time::Instant,
    deadline: tokio::time::Instant,
}

impl RetryBudget {
    pub(crate) fn new(policy: BoundaryRetryPolicy) -> Option<Self> {
        let started = tokio::time::Instant::now();
        let deadline = started.checked_add(policy.max_total_wall_time)?;
        Some(Self {
            policy,
            started,
            deadline,
        })
    }

    pub(crate) fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }

    pub(crate) fn can_start_execution(&self) -> bool {
        tokio::time::Instant::now() < self.deadline
    }

    pub(crate) fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis().min(u64::MAX as u128) as u64
    }

    pub(crate) fn remaining_ms(&self) -> u64 {
        self.deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64
    }

    pub(crate) fn next_attempt(&self, completed: NonZeroU32) -> Option<NonZeroU32> {
        let next = NonZeroU32::new(completed.get().checked_add(1)?)?;
        (next <= self.policy.max_attempts).then_some(next)
    }

    pub(crate) fn delay_after(
        &self,
        completed: NonZeroU32,
        retry_after: Option<Duration>,
    ) -> Result<Duration, RetryExhaustionCause> {
        if retry_after.is_some_and(|hint| hint > self.policy.max_single_delay) {
            return Err(RetryExhaustionCause::RetryHintExceedsLimit);
        }
        let configured = self.policy.configured_delay_after(completed);
        let delay = retry_after.map_or(configured, |hint| configured.max(hint));
        let now = tokio::time::Instant::now();
        if let Some(hint) = retry_after {
            let Some(hinted_start) = now.checked_add(hint) else {
                return Err(RetryExhaustionCause::RetryHintExceedsLimit);
            };
            if hinted_start >= self.deadline {
                return Err(RetryExhaustionCause::RetryHintExceedsLimit);
            }
        }
        let Some(start_at) = now.checked_add(delay) else {
            return Err(RetryExhaustionCause::TotalWallTime);
        };
        if start_at >= self.deadline {
            return Err(RetryExhaustionCause::TotalWallTime);
        }
        Ok(delay)
    }
}

pub(crate) fn attempt_failed_event(
    owner: &BoundaryRetryOwner,
    context: &RetryLifecycleContext,
    attempt: NonZeroU32,
    kind: ErrorKind,
    delay: Duration,
    budget: &RetryBudget,
    cause: Option<EventId>,
) -> ChainEvent {
    ChainEventFactory::retry_attempt_failed(
        owner.writer_id,
        RetryAttemptFailedEventParams {
            context: context.clone(),
            attempt_number: attempt.get(),
            max_attempts: owner.policy.max_attempts.get(),
            error_kind: kind,
            delay_ms: duration_ms(delay),
            elapsed_ms: budget.elapsed_ms(),
            remaining_wall_ms: budget.remaining_ms(),
            cause,
        },
    )
}

pub(crate) fn succeeded_event(
    owner: &BoundaryRetryOwner,
    context: &RetryLifecycleContext,
    attempts: u32,
    budget: &RetryBudget,
    cause: Option<EventId>,
) -> ChainEvent {
    ChainEventFactory::retry_succeeded_after_retry(
        owner.writer_id,
        RetrySucceededAfterRetryEventParams {
            context: context.clone(),
            total_attempts: attempts,
            total_duration_ms: budget.elapsed_ms(),
            cause,
        },
    )
}

pub(crate) fn exhausted_event(
    owner: &BoundaryRetryOwner,
    context: &RetryLifecycleContext,
    attempts: u32,
    exhaustion_cause: RetryExhaustionCause,
    last_error_kind: Option<ErrorKind>,
    budget: &RetryBudget,
    cause: Option<EventId>,
) -> ChainEvent {
    ChainEventFactory::retry_exhausted(
        owner.writer_id,
        RetryExhaustedEventParams {
            context: context.clone(),
            total_attempts: attempts,
            exhaustion_cause,
            last_error_kind,
            total_duration_ms: budget.elapsed_ms(),
            cause,
        },
    )
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryWaitError {
    Deadline,
    Drain,
    Abort,
}

/// Await admission or backoff. Drain and abort both prevent a new execution.
pub(crate) async fn await_before_execution<F, T>(
    future: F,
    deadline: tokio::time::Instant,
    stop: &mut BoundaryStopReceiver,
) -> Result<T, RetryWaitError>
where
    F: Future<Output = T>,
{
    if tokio::time::Instant::now() >= deadline {
        return Err(RetryWaitError::Deadline);
    }
    match stop.intent() {
        BoundaryStopIntent::Drain => return Err(RetryWaitError::Drain),
        BoundaryStopIntent::Abort => return Err(RetryWaitError::Abort),
        BoundaryStopIntent::Running => {}
    }
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(RetryWaitError::Deadline),
            intent = stop.changed() => match intent {
                BoundaryStopIntent::Running => {}
                BoundaryStopIntent::Drain => return Err(RetryWaitError::Drain),
                BoundaryStopIntent::Abort => return Err(RetryWaitError::Abort),
            },
            output = &mut future => return Ok(output),
        }
    }
}

/// Await an already-started physical call. Drain is remembered but does not
/// cancel it; deadline and force abort drop the local future promptly.
pub(crate) async fn await_active_execution<S, F, T>(
    start: S,
    deadline: tokio::time::Instant,
    stop: &mut BoundaryStopReceiver,
) -> Result<(T, bool), RetryWaitError>
where
    S: FnOnce() -> F,
    F: Future<Output = T>,
{
    if tokio::time::Instant::now() >= deadline {
        return Err(RetryWaitError::Deadline);
    }
    match stop.intent() {
        BoundaryStopIntent::Running => {}
        BoundaryStopIntent::Drain => return Err(RetryWaitError::Drain),
        BoundaryStopIntent::Abort => return Err(RetryWaitError::Abort),
    }
    // Invoke the start closure only after the final deadline/stop barrier. The
    // closure owns the physical-call commit point, including any reservation
    // charge and construction of the executor future.
    let future = start();
    let mut drain_requested = false;
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(RetryWaitError::Deadline),
            intent = stop.changed() => match intent {
                BoundaryStopIntent::Running => {}
                BoundaryStopIntent::Drain => drain_requested = true,
                BoundaryStopIntent::Abort => return Err(RetryWaitError::Abort),
            },
            output = &mut future => return Ok((output, drain_requested)),
        }
    }
}

/// Await asynchronous post-call settlement for a result that is already
/// known. Graceful drain is remembered but does not cancel settlement;
/// deadline and force abort remain terminal interruption points.
pub(crate) async fn await_settlement<F, T>(
    future: F,
    deadline: tokio::time::Instant,
    stop: &mut BoundaryStopReceiver,
) -> Result<(T, bool), RetryWaitError>
where
    F: Future<Output = T>,
{
    if tokio::time::Instant::now() >= deadline {
        return Err(RetryWaitError::Deadline);
    }
    let mut drain_requested = matches!(stop.intent(), BoundaryStopIntent::Drain);
    if matches!(stop.intent(), BoundaryStopIntent::Abort) {
        return Err(RetryWaitError::Abort);
    }
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(RetryWaitError::Deadline),
            intent = stop.changed() => match intent {
                BoundaryStopIntent::Running => {}
                BoundaryStopIntent::Drain => drain_requested = true,
                BoundaryStopIntent::Abort => return Err(RetryWaitError::Abort),
            },
            output = &mut future => return Ok((output, drain_requested)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn exact_deadline_readiness_tie_favours_expiry() {
        let deadline = tokio::time::Instant::now();
        let mut before_stop = BoundaryStopReceiver::default();
        let before =
            await_before_execution(std::future::ready("admitted"), deadline, &mut before_stop)
                .await;
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
        let (controller, mut drain_stop) =
            obzenflow_runtime::stages::common::boundary_stop_channel();
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
}

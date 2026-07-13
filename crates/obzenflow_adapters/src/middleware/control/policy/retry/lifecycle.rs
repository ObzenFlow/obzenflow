// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{BoundaryRetryOwner, RetryBudget};
use obzenflow_core::event::payloads::observability_payload::{
    RetryExhaustionCause, RetryLifecycleContext,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{
    ChainEventFactory, RetryAttemptFailedEventParams, RetryExhaustedEventParams,
    RetrySucceededAfterRetryEventParams,
};
use obzenflow_core::{ChainEvent, EventId};
use std::num::NonZeroU32;
use std::time::Duration;

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

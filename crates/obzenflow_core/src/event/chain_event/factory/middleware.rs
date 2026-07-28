// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ChainEventFactory;
use crate::event::chain_event::{
    ChainEvent, CircuitBreakerAttemptSettledEventParams,
    CircuitBreakerRecoveryCompletedEventParams, CircuitBreakerSummaryEventParams,
};
use crate::event::context::causality_context::CausalityContext;
use crate::event::payloads::effect_payload::EffectCursor;
use crate::event::payloads::observability_payload::{
    CircuitBreakerEvent, CircuitBreakerHealthClassification, CircuitBreakerRetryStopReason,
    MiddlewareLifecycle, ObservabilityPayload,
};
use crate::event::types::{EventId, WriterId};

impl ChainEventFactory {
    fn circuit_breaker_retry_event(
        writer_id: WriterId,
        event: CircuitBreakerEvent,
        cause: EventId,
    ) -> ChainEvent {
        let mut event = Self::observability_event(
            writer_id,
            ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(event)),
        );
        event.causality = CausalityContext::with_parent(cause);
        event
    }

    pub fn circuit_breaker_retry_scheduled(
        writer_id: WriterId,
        cursor: EffectCursor,
        next_attempt: u32,
        delay_ms: u64,
        cause: EventId,
    ) -> ChainEvent {
        Self::circuit_breaker_retry_event(
            writer_id,
            CircuitBreakerEvent::RetryScheduled {
                cursor,
                next_attempt,
                delay_ms,
            },
            cause,
        )
    }

    pub fn circuit_breaker_attempt_settled(
        writer_id: WriterId,
        params: CircuitBreakerAttemptSettledEventParams,
        cause: EventId,
    ) -> ChainEvent {
        let CircuitBreakerAttemptSettledEventParams {
            cursor,
            attempt,
            health_classification,
            slow,
            dependency_elapsed_ms,
            admission_wait_ms,
        } = params;
        Self::circuit_breaker_retry_event(
            writer_id,
            CircuitBreakerEvent::AttemptSettled {
                cursor,
                attempt,
                health_classification,
                slow,
                dependency_elapsed_ms,
                admission_wait_ms,
            },
            cause,
        )
    }

    pub fn circuit_breaker_retry_succeeded(
        writer_id: WriterId,
        cursor: EffectCursor,
        total_attempts: u32,
        terminal_classification: CircuitBreakerHealthClassification,
        cause: EventId,
    ) -> ChainEvent {
        Self::circuit_breaker_retry_event(
            writer_id,
            CircuitBreakerEvent::RetrySucceeded {
                cursor,
                total_attempts,
                terminal_classification,
            },
            cause,
        )
    }

    pub fn circuit_breaker_retry_exhausted(
        writer_id: WriterId,
        cursor: EffectCursor,
        total_attempts: u32,
        reason: CircuitBreakerRetryStopReason,
        cause: EventId,
    ) -> ChainEvent {
        Self::circuit_breaker_retry_event(
            writer_id,
            CircuitBreakerEvent::RetryExhausted {
                cursor,
                total_attempts,
                reason,
            },
            cause,
        )
    }

    pub fn circuit_breaker_retry_stopped_non_retryable(
        writer_id: WriterId,
        cursor: EffectCursor,
        total_attempts: u32,
        cause: EventId,
    ) -> ChainEvent {
        Self::circuit_breaker_retry_event(
            writer_id,
            CircuitBreakerEvent::RetryStoppedNonRetryable {
                cursor,
                total_attempts,
            },
            cause,
        )
    }

    pub fn circuit_breaker_recovery_completed(
        writer_id: WriterId,
        params: CircuitBreakerRecoveryCompletedEventParams,
        cause: EventId,
    ) -> ChainEvent {
        let CircuitBreakerRecoveryCompletedEventParams {
            cursor,
            total_attempts,
            backoff_elapsed_ms,
            recovery_elapsed_ms,
        } = params;
        Self::circuit_breaker_retry_event(
            writer_id,
            CircuitBreakerEvent::RecoveryCompleted {
                cursor,
                total_attempts,
                backoff_elapsed_ms,
                recovery_elapsed_ms,
            },
            cause,
        )
    }

    /// Create a circuit breaker opened event
    pub fn circuit_breaker_opened(
        writer_id: WriterId,
        error_rate: f64,
        failure_count: u64,
    ) -> ChainEvent {
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                CircuitBreakerEvent::Opened {
                    error_rate,
                    failure_count,
                    last_error: None,
                },
            )),
        )
    }

    /// Create a circuit breaker summary event
    pub fn circuit_breaker_summary(
        writer_id: WriterId,
        params: CircuitBreakerSummaryEventParams,
    ) -> ChainEvent {
        let CircuitBreakerSummaryEventParams {
            window_duration_s,
            requests_processed,
            requests_rejected,
            state,
            consecutive_failures,
            rejection_rate,
            successes_total,
            failures_total,
            opened_total,
            time_in_closed_seconds,
            time_in_open_seconds,
            time_in_half_open_seconds,
        } = params;
        Self::observability_event(
            writer_id,
            ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                CircuitBreakerEvent::Summary {
                    window_duration_s,
                    requests_processed,
                    requests_rejected,
                    state,
                    consecutive_failures,
                    rejection_rate,
                    successes_total,
                    failures_total,
                    opened_total,
                    time_in_closed_seconds,
                    time_in_open_seconds,
                    time_in_half_open_seconds,
                },
            )),
        )
    }
}

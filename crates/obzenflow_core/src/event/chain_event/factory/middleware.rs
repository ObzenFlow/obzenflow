// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ChainEventFactory;
use crate::event::chain_event::{
    ChainEvent, CircuitBreakerAttemptSettledEventParams, CircuitBreakerOpenedEventParams,
    CircuitBreakerRecoveryCompletedEventParams,
};
use crate::event::payloads::effect_payload::EffectCursor;
use crate::event::payloads::execution_payload::{CircuitBreakerFact, ExecutionPayload};
use crate::event::payloads::execution_payload::{
    CircuitBreakerHealthClassification, CircuitBreakerRetryStopReason,
};
use crate::event::provenance::causality_context::CausalityContext;
use crate::event::types::{EventId, WriterId};

impl ChainEventFactory {
    fn circuit_breaker_retry_event(
        writer_id: WriterId,
        event: CircuitBreakerFact,
        cause: EventId,
    ) -> ChainEvent {
        let mut event = Self::execution_event(writer_id, ExecutionPayload::CircuitBreaker(event));
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
            CircuitBreakerFact::RetryScheduled {
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
            CircuitBreakerFact::AttemptSettled {
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
            CircuitBreakerFact::RetrySucceeded {
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
            CircuitBreakerFact::RetryExhausted {
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
            CircuitBreakerFact::RetryStoppedNonRetryable {
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
            CircuitBreakerFact::RecoveryCompleted {
                cursor,
                total_attempts,
                backoff_elapsed_ms,
                recovery_elapsed_ms,
            },
            cause,
        )
    }

    /// Create a circuit breaker opened event carrying the exact evidence that
    /// caused the state transition.
    pub fn circuit_breaker_opened(
        writer_id: WriterId,
        params: CircuitBreakerOpenedEventParams,
    ) -> ChainEvent {
        let CircuitBreakerOpenedEventParams {
            cooldown_ms,
            trigger,
            observed_calls,
            error_rate,
            failure_count,
            slow_call_rate,
            slow_call_count,
            last_error,
        } = params;
        Self::execution_event(
            writer_id,
            ExecutionPayload::CircuitBreaker(CircuitBreakerFact::Opened {
                cooldown_ms,
                error_rate,
                failure_count,
                trigger,
                observed_calls,
                slow_call_rate,
                slow_call_count,
                last_error,
            }),
        )
    }
}

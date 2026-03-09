// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ChainEventFactory;
use crate::event::chain_event::{ChainEvent, CircuitBreakerSummaryEventParams};
use crate::event::context::causality_context::CausalityContext;
use crate::event::payloads::observability_payload::{
    CircuitBreakerEvent, MiddlewareLifecycle, ObservabilityPayload, RetryEvent,
};
use crate::event::status::processing_status::ErrorKind;
use crate::event::types::{EventId, WriterId};

impl ChainEventFactory {
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

    /// Create a retry exhausted event.
    ///
    /// When `cause` is provided the control event is linked to the input event
    /// that triggered the retry sequence, establishing causal lineage for
    /// downstream observability tooling.
    pub fn retry_exhausted(
        writer_id: WriterId,
        total_attempts: u32,
        last_error: String,
        total_duration_ms: u64,
        cause: Option<EventId>,
    ) -> ChainEvent {
        let mut evt = Self::observability_event(
            writer_id,
            ObservabilityPayload::Middleware(MiddlewareLifecycle::Retry(RetryEvent::Exhausted {
                total_attempts,
                last_error,
                total_duration_ms,
            })),
        );
        if let Some(parent) = cause {
            evt.causality = CausalityContext::with_parent(parent);
        }
        evt
    }

    /// Create a retry attempt failed event.
    ///
    /// When `cause` is provided the control event is linked to the input event
    /// that triggered the retry sequence.
    pub fn retry_attempt_failed(
        writer_id: WriterId,
        attempt_number: u32,
        max_attempts: u32,
        error_kind: Option<ErrorKind>,
        delay_ms: Option<u64>,
        cause: Option<EventId>,
    ) -> ChainEvent {
        let mut evt = Self::observability_event(
            writer_id,
            ObservabilityPayload::Middleware(MiddlewareLifecycle::Retry(
                RetryEvent::AttemptFailed {
                    attempt_number,
                    max_attempts,
                    error_kind,
                    delay_ms,
                },
            )),
        );
        if let Some(parent) = cause {
            evt.causality = CausalityContext::with_parent(parent);
        }
        evt
    }

    /// Create a retry succeeded-after-retry event.
    ///
    /// When `cause` is provided the control event is linked to the input event
    /// that triggered the retry sequence.
    pub fn retry_succeeded_after_retry(
        writer_id: WriterId,
        total_attempts: u32,
        total_duration_ms: u64,
        cause: Option<EventId>,
    ) -> ChainEvent {
        let mut evt = Self::observability_event(
            writer_id,
            ObservabilityPayload::Middleware(MiddlewareLifecycle::Retry(
                RetryEvent::SucceededAfterRetry {
                    total_attempts,
                    total_duration_ms,
                },
            )),
        );
        if let Some(parent) = cause {
            evt.causality = CausalityContext::with_parent(parent);
        }
        evt
    }
}

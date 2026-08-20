// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Admission-only policy seam for one physical sink delivery.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::SinkConsumeReport;
use async_trait::async_trait;
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, MiddlewareLifecycle, RateLimiterEvent,
};
use std::fmt;

pub const MAX_SINK_POLICY_EVIDENCE_ENTRIES: usize = 64;
const MAX_POLICY_TEXT_BYTES: usize = 512;

/// Read-only outcome retained by the runtime after the sole writer call.
pub enum SinkDeliveryAttemptOutcome {
    Delivered(Result<Box<SinkConsumeReport>, HandlerError>),
    Panicked { message: String },
}

#[derive(Debug, Clone)]
pub struct SinkDeliveryRejection {
    policy: String,
    reason: String,
}

fn bounded_policy_text(value: impl Into<String>) -> String {
    let mut value = value
        .into()
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .collect::<String>();
    if value.len() > MAX_POLICY_TEXT_BYTES {
        let mut boundary = MAX_POLICY_TEXT_BYTES;
        while !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        value.truncate(boundary);
    }
    value
}

impl SinkDeliveryRejection {
    pub fn new(policy: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            policy: bounded_policy_text(policy),
            reason: bounded_policy_text(reason),
        }
    }

    pub fn policy(&self) -> &str {
        &self.policy
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// A closed, transient descriptor for middleware lifecycle evidence. It cannot
/// carry delivery, data, progress, settlement, or terminal stage events.
pub struct SinkPolicyEvidence {
    lifecycle: MiddlewareLifecycle,
}

impl fmt::Debug for SinkPolicyEvidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkPolicyEvidence")
            .field("validated", &true)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkPolicyEvidenceError {
    DisallowedCircuitBreakerEvent,
    InvalidDiagnostic,
    CapacityExceeded,
}

impl fmt::Display for SinkPolicyEvidenceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DisallowedCircuitBreakerEvent => {
                f.write_str("circuit-breaker event is not sink-policy evidence")
            }
            Self::InvalidDiagnostic => f.write_str("sink-policy evidence is not finite or bounded"),
            Self::CapacityExceeded => f.write_str("sink-policy evidence batch capacity exceeded"),
        }
    }
}

impl std::error::Error for SinkPolicyEvidenceError {}

fn valid_float(value: f64) -> bool {
    value.is_finite()
}

fn valid_text(value: &str) -> bool {
    value.len() <= MAX_POLICY_TEXT_BYTES && !value.chars().any(char::is_control)
}

impl SinkPolicyEvidence {
    pub fn circuit_breaker(event: CircuitBreakerEvent) -> Result<Self, SinkPolicyEvidenceError> {
        match &event {
            CircuitBreakerEvent::Opened {
                error_rate,
                slow_call_rate,
                last_error,
                ..
            } => {
                if !valid_float(*error_rate)
                    || slow_call_rate.is_some_and(|value| !valid_float(value))
                    || last_error
                        .as_deref()
                        .is_some_and(|value| !valid_text(value))
                {
                    return Err(SinkPolicyEvidenceError::InvalidDiagnostic);
                }
            }
            CircuitBreakerEvent::Closed { .. }
            | CircuitBreakerEvent::Rejected { .. }
            | CircuitBreakerEvent::HalfOpen { .. } => {}
            CircuitBreakerEvent::Summary {
                state,
                rejection_rate,
                time_in_closed_seconds,
                time_in_open_seconds,
                time_in_half_open_seconds,
                ..
            } => {
                if !valid_text(state)
                    || !valid_float(*rejection_rate)
                    || !valid_float(*time_in_closed_seconds)
                    || !valid_float(*time_in_open_seconds)
                    || !valid_float(*time_in_half_open_seconds)
                {
                    return Err(SinkPolicyEvidenceError::InvalidDiagnostic);
                }
            }
            CircuitBreakerEvent::AttemptSettled { .. }
            | CircuitBreakerEvent::RetryScheduled { .. }
            | CircuitBreakerEvent::RetrySucceeded { .. }
            | CircuitBreakerEvent::RetryExhausted { .. }
            | CircuitBreakerEvent::RetryStoppedNonRetryable { .. }
            | CircuitBreakerEvent::RecoveryCompleted { .. } => {
                return Err(SinkPolicyEvidenceError::DisallowedCircuitBreakerEvent);
            }
        }
        Ok(Self {
            lifecycle: MiddlewareLifecycle::CircuitBreaker(event),
        })
    }

    pub fn rate_limiter(event: RateLimiterEvent) -> Result<Self, SinkPolicyEvidenceError> {
        let valid = match &event {
            RateLimiterEvent::Delayed {
                current_rate,
                limit_rate,
                ..
            } => valid_float(*current_rate) && valid_float(*limit_rate),
            RateLimiterEvent::ActivityPulse { limit_rate, .. } => valid_float(*limit_rate),
            RateLimiterEvent::ModeChange {
                mode_from,
                mode_to,
                limit_rate,
            } => valid_text(mode_from) && valid_text(mode_to) && valid_float(*limit_rate),
            RateLimiterEvent::WindowUtilization {
                utilization_percent,
                ..
            } => valid_float(*utilization_percent),
            RateLimiterEvent::ConfigChanged { old_rate, new_rate } => {
                valid_float(*old_rate) && valid_float(*new_rate)
            }
        };
        if !valid {
            return Err(SinkPolicyEvidenceError::InvalidDiagnostic);
        }
        Ok(Self {
            lifecycle: MiddlewareLifecycle::RateLimiter(event),
        })
    }

    pub(crate) fn into_lifecycle(self) -> MiddlewareLifecycle {
        self.lifecycle
    }
}

pub struct SinkPolicyEvidenceBatch {
    entries: Vec<SinkPolicyEvidence>,
}

impl Default for SinkPolicyEvidenceBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for SinkPolicyEvidenceBatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkPolicyEvidenceBatch")
            .field("len", &self.entries.len())
            .finish()
    }
}

impl SinkPolicyEvidenceBatch {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn try_push(
        &mut self,
        evidence: SinkPolicyEvidence,
    ) -> Result<(), SinkPolicyEvidenceError> {
        if self.entries.len() == MAX_SINK_POLICY_EVIDENCE_ENTRIES {
            return Err(SinkPolicyEvidenceError::CapacityExceeded);
        }
        self.entries.push(evidence);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub(crate) fn into_entries(self) -> Vec<SinkPolicyEvidence> {
        self.entries
    }
}

pub enum SinkDeliveryAdmission {
    Admitted(Box<dyn SinkDeliveryPermit>),
    Rejected {
        rejection: SinkDeliveryRejection,
        evidence: SinkPolicyEvidenceBatch,
    },
}

/// Single-use observer permit held across the one physical writer call.
pub trait SinkDeliveryPermit: Send {
    fn observe(self: Box<Self>, outcome: &SinkDeliveryAttemptOutcome) -> SinkPolicyEvidenceBatch;
}

#[async_trait]
pub trait SinkDeliveryBoundary: Send + Sync {
    async fn admit_sink_delivery(&self) -> SinkDeliveryAdmission;
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::effect_payload::EffectCursor;
    use obzenflow_core::event::payloads::observability_payload::{
        CircuitBreakerHealthClassification, CircuitBreakerOpenTrigger,
        CircuitBreakerRejectionReason, CircuitBreakerRetryStopReason,
    };

    fn cursor() -> EffectCursor {
        EffectCursor::new("flow", "stage", 1, 0)
    }

    fn assert_allowed_breaker(event: CircuitBreakerEvent) {
        assert!(SinkPolicyEvidence::circuit_breaker(event).is_ok());
    }

    fn assert_rejected_breaker(event: CircuitBreakerEvent) {
        assert!(matches!(
            SinkPolicyEvidence::circuit_breaker(event),
            Err(SinkPolicyEvidenceError::DisallowedCircuitBreakerEvent)
        ));
    }

    #[test]
    fn every_sink_legal_breaker_and_limiter_variant_is_explicitly_allowed() {
        assert_allowed_breaker(CircuitBreakerEvent::Opened {
            error_rate: 0.5,
            failure_count: 2,
            trigger: CircuitBreakerOpenTrigger::FailureRate,
            observed_calls: 4,
            slow_call_rate: Some(0.25),
            slow_call_count: Some(1),
            last_error: Some("redacted".into()),
        });
        assert_allowed_breaker(CircuitBreakerEvent::Closed {
            success_count: 3,
            recovery_duration_ms: 20,
        });
        assert_allowed_breaker(CircuitBreakerEvent::Rejected {
            reason: CircuitBreakerRejectionReason::CircuitOpen,
            cooldown_remaining_ms: Some(10),
            circuit_open_duration_ms: Some(30),
        });
        assert_allowed_breaker(CircuitBreakerEvent::HalfOpen {
            test_request_count: 1,
        });
        assert_allowed_breaker(CircuitBreakerEvent::Summary {
            window_duration_s: 10,
            requests_processed: 4,
            requests_rejected: 1,
            state: "closed".into(),
            consecutive_failures: 0,
            rejection_rate: 0.25,
            successes_total: 3,
            failures_total: 1,
            opened_total: 1,
            time_in_closed_seconds: 8.0,
            time_in_open_seconds: 1.0,
            time_in_half_open_seconds: 1.0,
        });

        for event in [
            RateLimiterEvent::Delayed {
                delay_ms: 1,
                current_rate: 2.0,
                limit_rate: 1.0,
            },
            RateLimiterEvent::ActivityPulse {
                window_ms: 1000,
                delayed_events: 1,
                delay_ms_total: 2,
                delay_ms_max: 2,
                limit_rate: 1.0,
            },
            RateLimiterEvent::ModeChange {
                mode_from: "steady".into(),
                mode_to: "limited".into(),
                limit_rate: 1.0,
            },
            RateLimiterEvent::WindowUtilization {
                utilization_percent: 50.0,
                events_in_window: 2,
                window_size_ms: 1000,
            },
            RateLimiterEvent::ConfigChanged {
                old_rate: 1.0,
                new_rate: 2.0,
            },
        ] {
            assert!(SinkPolicyEvidence::rate_limiter(event).is_ok());
        }
    }

    #[test]
    fn every_retry_or_settlement_shaped_breaker_variant_is_rejected() {
        assert_rejected_breaker(CircuitBreakerEvent::AttemptSettled {
            cursor: cursor(),
            attempt: 1,
            health_classification: CircuitBreakerHealthClassification::TransientFailure,
            slow: false,
            dependency_elapsed_ms: 10,
            admission_wait_ms: 0,
        });
        assert_rejected_breaker(CircuitBreakerEvent::RetryScheduled {
            cursor: cursor(),
            next_attempt: 2,
            delay_ms: 10,
        });
        assert_rejected_breaker(CircuitBreakerEvent::RetrySucceeded {
            cursor: cursor(),
            total_attempts: 2,
            terminal_classification: CircuitBreakerHealthClassification::Success,
        });
        assert_rejected_breaker(CircuitBreakerEvent::RetryExhausted {
            cursor: cursor(),
            total_attempts: 3,
            reason: CircuitBreakerRetryStopReason::AttemptLimit,
        });
        assert_rejected_breaker(CircuitBreakerEvent::RetryStoppedNonRetryable {
            cursor: cursor(),
            total_attempts: 1,
        });
        assert_rejected_breaker(CircuitBreakerEvent::RecoveryCompleted {
            cursor: cursor(),
            total_attempts: 3,
            backoff_elapsed_ms: 20,
            recovery_elapsed_ms: 30,
        });
    }

    #[test]
    fn evidence_text_numbers_and_batch_capacity_fail_closed() {
        assert!(matches!(
            SinkPolicyEvidence::circuit_breaker(CircuitBreakerEvent::Opened {
                error_rate: f64::NAN,
                failure_count: 1,
                trigger: CircuitBreakerOpenTrigger::FailureRate,
                observed_calls: 1,
                slow_call_rate: None,
                slow_call_count: None,
                last_error: None,
            }),
            Err(SinkPolicyEvidenceError::InvalidDiagnostic)
        ));
        assert!(matches!(
            SinkPolicyEvidence::rate_limiter(RateLimiterEvent::ModeChange {
                mode_from: "credential\nleak".into(),
                mode_to: "limited".into(),
                limit_rate: 1.0,
            }),
            Err(SinkPolicyEvidenceError::InvalidDiagnostic)
        ));

        let mut batch = SinkPolicyEvidenceBatch::new();
        for _ in 0..MAX_SINK_POLICY_EVIDENCE_ENTRIES {
            batch
                .try_push(
                    SinkPolicyEvidence::circuit_breaker(CircuitBreakerEvent::Closed {
                        success_count: 1,
                        recovery_duration_ms: 1,
                    })
                    .unwrap(),
                )
                .unwrap();
        }
        assert_eq!(batch.len(), MAX_SINK_POLICY_EVIDENCE_ENTRIES);
        assert!(matches!(
            batch.try_push(
                SinkPolicyEvidence::circuit_breaker(CircuitBreakerEvent::HalfOpen {
                    test_request_count: 1,
                })
                .unwrap()
            ),
            Err(SinkPolicyEvidenceError::CapacityExceeded)
        ));
    }
}

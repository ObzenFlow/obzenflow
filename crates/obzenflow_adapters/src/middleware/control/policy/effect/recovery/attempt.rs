// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::super::retry::{await_active_execution, await_before_execution, RetryWaitError};
use super::super::attachment::EffectPolicyAttachment;
use super::super::contract::{EffectAttemptOutcome, PolicyAdmission};
use super::termination::{deadline_outcome_unknown_error, interruption_cause, InterruptionPhase};
use crate::middleware::{MiddlewareAbortCause, MiddlewareContext};
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use obzenflow_runtime::effects::{EffectError, EffectExecutor};
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use std::num::NonZeroU32;

pub(super) enum AdmissionExit {
    Synthesized {
        results: Vec<ChainEvent>,
        source: String,
        control_events: Vec<ChainEvent>,
    },
    Rejected {
        cause: MiddlewareAbortCause,
        control_events: Vec<ChainEvent>,
    },
    Interrupted {
        wait_error: RetryWaitError,
        cause: MiddlewareAbortCause,
        control_events: Vec<ChainEvent>,
    },
}

pub(super) enum ActiveExecutionExit {
    Settled(SettledEffectAttempt),
    Drain {
        cause: Box<MiddlewareAbortCause>,
        control_events: Vec<ChainEvent>,
    },
    Abort,
}

pub(super) struct SettledEffectAttempt {
    pub(super) result: Result<Vec<ChainEvent>, EffectError>,
    pub(super) drain_requested: bool,
    pub(super) active_deadline: bool,
    pub(super) recovery_allowed: bool,
    pub(super) control_events: Vec<ChainEvent>,
}

/// The state for exactly one physical effect attempt. Keeping the admitted
/// policy slice beside its context makes reverse-order settlement structural.
pub(super) struct AdmittedEffectAttempt<'a> {
    ctx: MiddlewareContext,
    admitted: Vec<&'a EffectPolicyAttachment>,
}

impl<'a> AdmittedEffectAttempt<'a> {
    pub(super) async fn admit(
        chain: &'a [EffectPolicyAttachment],
        event: &ChainEvent,
        deadline: tokio::time::Instant,
        stop: &mut BoundaryStopReceiver,
    ) -> Result<Self, AdmissionExit> {
        let mut attempt = Self {
            ctx: MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary),
            admitted: Vec::new(),
        };
        for policy in chain {
            let admission =
                await_before_execution(policy.admit(event, &mut attempt.ctx), deadline, stop).await;
            match admission {
                Ok(PolicyAdmission::Admit) => attempt.admitted.push(policy),
                Ok(PolicyAdmission::Synthesize { results, cause: _ }) => {
                    let outcome = EffectAttemptOutcome::SkippedBy(policy.label());
                    attempt.observe_admitted(event, &outcome);
                    return Err(AdmissionExit::Synthesized {
                        results,
                        source: policy.label().to_string(),
                        control_events: attempt.ctx.take_control_events(),
                    });
                }
                Ok(PolicyAdmission::Reject(cause)) => {
                    let outcome = EffectAttemptOutcome::RejectedBy(&cause);
                    attempt.observe_admitted(event, &outcome);
                    return Err(AdmissionExit::Rejected {
                        cause,
                        control_events: attempt.ctx.take_control_events(),
                    });
                }
                Err(wait_error) => {
                    let cause = interruption_cause(wait_error, InterruptionPhase::Admission);
                    let outcome = EffectAttemptOutcome::RejectedBy(&cause);
                    attempt.observe_admitted(event, &outcome);
                    return Err(AdmissionExit::Interrupted {
                        wait_error,
                        cause,
                        control_events: attempt.ctx.take_control_events(),
                    });
                }
            }
        }
        Ok(attempt)
    }

    pub(super) fn reject_before_execution(
        mut self,
        event: &ChainEvent,
        wait_error: RetryWaitError,
    ) -> AdmissionExit {
        let cause = interruption_cause(wait_error, InterruptionPhase::BeforeExecutorStart);
        let outcome = EffectAttemptOutcome::RejectedBy(&cause);
        self.observe_admitted(event, &outcome);
        AdmissionExit::Interrupted {
            wait_error,
            cause,
            control_events: self.ctx.take_control_events(),
        }
    }

    pub(super) async fn execute(
        mut self,
        event: &ChainEvent,
        execute: &mut dyn EffectExecutor,
        attempt: NonZeroU32,
        deadline: tokio::time::Instant,
        stop: &mut BoundaryStopReceiver,
    ) -> ActiveExecutionExit {
        let mut call_started = None;
        let active = await_active_execution(
            || {
                for policy in &self.admitted {
                    policy.commit_execution(&mut self.ctx);
                }
                call_started = Some(tokio::time::Instant::now());
                execute.attempt(attempt)
            },
            deadline,
            stop,
        )
        .await;
        let (result, drain_requested, active_deadline) = match active {
            Ok((result, drain_requested)) => (result, drain_requested, false),
            Err(RetryWaitError::Deadline) => (Err(deadline_outcome_unknown_error()), false, true),
            Err(RetryWaitError::Drain) => {
                let cause = interruption_cause(
                    RetryWaitError::Drain,
                    InterruptionPhase::BeforeExecutorStart,
                );
                let outcome = EffectAttemptOutcome::RejectedBy(&cause);
                self.observe_admitted(event, &outcome);
                return ActiveExecutionExit::Drain {
                    cause: Box::new(cause),
                    control_events: self.ctx.take_control_events(),
                };
            }
            Err(RetryWaitError::Abort) => return ActiveExecutionExit::Abort,
        };
        self.ctx
            .insert::<crate::middleware::context_keys::EffectCallDurationNanos>(
                call_started
                    .expect("successful active execution must record its start")
                    .elapsed()
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
            );
        let outcome = EffectAttemptOutcome::Executed(&result);
        self.observe_admitted(event, &outcome);
        let recovery_allowed = self
            .admitted
            .iter()
            .all(|policy| policy.recovery_allowed_after_settlement(&self.ctx));
        ActiveExecutionExit::Settled(SettledEffectAttempt {
            result,
            drain_requested,
            active_deadline,
            recovery_allowed,
            control_events: self.ctx.take_control_events(),
        })
    }

    fn observe_admitted(&mut self, event: &ChainEvent, outcome: &EffectAttemptOutcome<'_>) {
        for policy in self.admitted.iter().rev() {
            policy.observe(event, outcome, &mut self.ctx);
        }
    }
}

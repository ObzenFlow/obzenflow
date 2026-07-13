// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::super::retry::{await_before_execution, AttemptDisposition, RetryWaitError};
use super::super::boundary::PerEffectPolicyBoundary;
use super::super::disposition::effect_attempt_disposition;
use super::attempt::{ActiveExecutionExit, AdmittedEffectAttempt};
use super::invocation::RetryInvocationState;
use super::termination::{
    finish_active_deadline, finish_active_drain, finish_admission_exit,
    force_aborted_during_execution, unrepresentable_budget,
};
use obzenflow_core::event::payloads::observability_payload::RetryExhaustionCause;
use obzenflow_core::ChainEvent;
use obzenflow_runtime::effects::{
    EffectBoundary, EffectBoundaryOutcome, EffectBoundaryReport, EffectExecutor, EffectIdentity,
};
use obzenflow_runtime::stages::common::{BoundaryStopIntent, BoundaryStopReceiver};
use std::num::NonZeroU32;

/// Coordinate one logical invocation. Policy-onion mechanics for each
/// physical attempt live in `attempt`, while invocation-wide retry evidence
/// and termination construction live in their dedicated modules.
pub(crate) async fn run(
    boundary: &PerEffectPolicyBoundary,
    identity: &EffectIdentity,
    event: &ChainEvent,
    execute: &mut dyn EffectExecutor,
    mut stop: BoundaryStopReceiver,
) -> EffectBoundaryReport {
    let Some(owner) = boundary.retry_owner(identity.effect_type) else {
        return boundary
            .around_effect(identity, event, execute.attempt(NonZeroU32::MIN))
            .await;
    };
    let Some(mut invocation) = RetryInvocationState::new(owner.clone(), identity, event.id) else {
        return unrepresentable_budget(&owner);
    };
    let chain = boundary
        .chains
        .get(identity.effect_type)
        .expect("retry owner implies an effect policy chain");

    loop {
        let admitted = match AdmittedEffectAttempt::admit(
            chain,
            event,
            invocation.budget.deadline(),
            &mut stop,
        )
        .await
        {
            Ok(admitted) => admitted,
            Err(exit) => return finish_admission_exit(invocation, exit),
        };

        if let Some(interruption) = pre_execution_interruption(&invocation, &stop) {
            return finish_admission_exit(
                invocation,
                admitted.reject_before_execution(event, interruption),
            );
        }

        let mut settled = match admitted
            .execute(
                event,
                execute,
                invocation.attempt,
                invocation.budget.deadline(),
                &mut stop,
            )
            .await
        {
            ActiveExecutionExit::Settled(settled) => settled,
            ActiveExecutionExit::Drain {
                cause,
                control_events,
            } => return finish_active_drain(invocation, *cause, control_events),
            ActiveExecutionExit::Abort => {
                return invocation.silent_report(force_aborted_during_execution())
            }
        };
        invocation.extend_events(std::mem::take(&mut settled.control_events));
        if settled.active_deadline {
            return finish_active_deadline(invocation);
        }

        match effect_attempt_disposition(&settled.result) {
            AttemptDisposition::Completed => {
                invocation.record_success_if_recovering();
                return invocation.report(EffectBoundaryOutcome::Executed(settled.result));
            }
            AttemptDisposition::TerminalFailure { kind } => {
                if invocation.recovery_started {
                    invocation.exhaust_current(RetryExhaustionCause::TerminalFailure, Some(kind));
                }
                return invocation.report(EffectBoundaryOutcome::Executed(settled.result));
            }
            AttemptDisposition::RetryableFailure { kind, retry_after } => {
                if let Some(cause) = retry_barrier(&invocation, &settled) {
                    invocation.exhaust_current(cause, Some(kind));
                    return invocation.report(EffectBoundaryOutcome::Executed(settled.result));
                }
                let delay = match invocation
                    .budget
                    .delay_after(invocation.attempt, retry_after)
                {
                    Ok(delay) => delay,
                    Err(cause) => {
                        invocation.exhaust_current(cause, Some(kind));
                        return invocation.report(EffectBoundaryOutcome::Executed(settled.result));
                    }
                };
                invocation.record_attempt_failed(kind.clone(), delay);
                match await_before_execution(
                    tokio::time::sleep(delay),
                    invocation.budget.deadline(),
                    &mut stop,
                )
                .await
                {
                    Ok(()) => invocation.advance(),
                    Err(RetryWaitError::Deadline) => {
                        invocation.exhaust_current(RetryExhaustionCause::TotalWallTime, Some(kind));
                        return invocation.report(EffectBoundaryOutcome::Executed(settled.result));
                    }
                    Err(RetryWaitError::Drain) => {
                        invocation
                            .exhaust_current(RetryExhaustionCause::DrainRequested, Some(kind));
                        return invocation.report(EffectBoundaryOutcome::Executed(settled.result));
                    }
                    Err(RetryWaitError::Abort) => {
                        return invocation
                            .silent_report(EffectBoundaryOutcome::Executed(settled.result));
                    }
                }
            }
            AttemptDisposition::NotExecuted => unreachable!(),
        }
    }
}

fn pre_execution_interruption(
    invocation: &RetryInvocationState,
    stop: &BoundaryStopReceiver,
) -> Option<RetryWaitError> {
    if !invocation.budget.can_start_execution() {
        return Some(RetryWaitError::Deadline);
    }
    match stop.intent() {
        BoundaryStopIntent::Running => None,
        BoundaryStopIntent::Drain => Some(RetryWaitError::Drain),
        BoundaryStopIntent::Abort => Some(RetryWaitError::Abort),
    }
}

fn retry_barrier(
    invocation: &RetryInvocationState,
    settled: &super::attempt::SettledEffectAttempt,
) -> Option<RetryExhaustionCause> {
    if settled.drain_requested {
        Some(RetryExhaustionCause::DrainRequested)
    } else if !settled.recovery_allowed {
        Some(RetryExhaustionCause::PolicyRejected)
    } else if invocation.budget.next_attempt(invocation.attempt).is_none() {
        Some(RetryExhaustionCause::MaxAttempts)
    } else {
        None
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::retry::{
    await_active_execution, await_before_execution, RetryBudget, RetryWaitError,
};
use super::{
    handler_retry_after, SinkAdmission, SinkAdmissionGuard, SinkDeliveryPolicyOutcome, SinkPolicy,
    SinkPolicyCtx,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::ChainEvent;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::{BoundaryStopIntent, BoundaryStopReceiver};
use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryAttemptOutcome, SinkDeliveryBoundaryOutcome, SinkDeliveryBoundaryReport,
    SinkDeliveryExecutor, SinkDeliveryRejection,
};
use std::sync::Arc;

type SinkAdmitGuard = Option<Box<dyn SinkAdmissionGuard>>;

/// One physical sink-delivery policy onion. Its guards and context cannot
/// escape an attempt, which keeps settlement and guard release ordered.
struct SinkAttemptOnion<'a> {
    ctx: SinkPolicyCtx,
    admitted: Vec<(&'a Arc<dyn SinkPolicy>, SinkAdmitGuard)>,
}

impl<'a> SinkAttemptOnion<'a> {
    fn new() -> Self {
        Self {
            ctx: SinkPolicyCtx::new(),
            admitted: Vec::new(),
        }
    }

    fn admit(&mut self, policy: &'a Arc<dyn SinkPolicy>, guard: SinkAdmitGuard) {
        self.admitted.push((policy, guard));
    }

    fn reject(
        mut self,
        policy: &'static str,
        reason: String,
        wait_error: Option<RetryWaitError>,
    ) -> RejectedSinkAttempt {
        let outcome = SinkDeliveryPolicyOutcome::RejectedBy {
            policy,
            reason: &reason,
        };
        for (prior, _) in self.admitted.iter().rev() {
            prior.observe(&outcome, &mut self.ctx);
        }

        RejectedSinkAttempt {
            rejection: SinkDeliveryRejection {
                policy: policy.to_string(),
                reason,
            },
            control_events: self.ctx.take_control_events(),
            wait_error,
            // Rejected attempts retain prior reservations until the coordinator
            // has constructed the terminal report and any lifecycle row.
            _held_guards: self
                .admitted
                .into_iter()
                .filter_map(|(_, guard)| guard)
                .collect(),
        }
    }

    fn commit_execution(&mut self) {
        for (policy, _) in &self.admitted {
            policy.commit_execution(&mut self.ctx);
        }
    }

    fn settle(
        mut self,
        attempt_outcome: SinkDeliveryAttemptOutcome,
        drain_requested: bool,
        deadline_outcome_unknown: bool,
    ) -> SettledSinkAttempt {
        let policy_outcome = policy_outcome(&attempt_outcome, &mut self.ctx);
        for (policy, _) in self.admitted.iter().rev() {
            policy.observe(&policy_outcome, &mut self.ctx);
        }
        let recovery_allowed = self
            .admitted
            .iter()
            .all(|(policy, _)| policy.recovery_allowed_after_settlement(&self.ctx));

        SettledSinkAttempt {
            outcome: attempt_outcome,
            control_events: self.ctx.take_control_events(),
            recovery_allowed,
            drain_requested,
            deadline_outcome_unknown,
        }
    }
}

pub(super) struct RejectedSinkAttempt {
    pub(super) rejection: SinkDeliveryRejection,
    pub(super) control_events: Vec<ChainEvent>,
    pub(super) wait_error: Option<RetryWaitError>,
    _held_guards: Vec<Box<dyn SinkAdmissionGuard>>,
}

pub(super) struct SettledSinkAttempt {
    pub(super) outcome: SinkDeliveryAttemptOutcome,
    pub(super) control_events: Vec<ChainEvent>,
    pub(super) recovery_allowed: bool,
    pub(super) drain_requested: bool,
    pub(super) deadline_outcome_unknown: bool,
}

pub(super) enum RetryableSinkAttempt {
    Settled(SettledSinkAttempt),
    Rejected(RejectedSinkAttempt),
    Aborted(Vec<Box<dyn SinkAdmissionGuard>>),
}

pub(super) async fn run_once(
    policies: &[Arc<dyn SinkPolicy>],
    execute: &mut dyn SinkDeliveryExecutor,
) -> SinkDeliveryBoundaryReport {
    if policies.is_empty() {
        return SinkDeliveryBoundaryReport {
            outcome: SinkDeliveryBoundaryOutcome::Attempted(execute.attempt().await),
            control_events: Vec::new(),
        };
    }

    let mut onion = SinkAttemptOnion::new();
    for policy in policies {
        match policy.admit(&mut onion.ctx).await {
            SinkAdmission::Admit(guard) => onion.admit(policy, guard),
            SinkAdmission::Reject { reason } => {
                let rejected = onion.reject(policy.label(), reason, None);
                return SinkDeliveryBoundaryReport {
                    outcome: SinkDeliveryBoundaryOutcome::Rejected(rejected.rejection),
                    control_events: rejected.control_events,
                };
            }
        }
    }

    onion.commit_execution();
    let settled = onion.settle(execute.attempt().await, false, false);
    SinkDeliveryBoundaryReport {
        outcome: SinkDeliveryBoundaryOutcome::Attempted(settled.outcome),
        control_events: settled.control_events,
    }
}

pub(super) async fn run_retryable_attempt(
    policies: &[Arc<dyn SinkPolicy>],
    execute: &mut dyn SinkDeliveryExecutor,
    budget: &RetryBudget,
    stop: &mut BoundaryStopReceiver,
) -> RetryableSinkAttempt {
    let mut onion = SinkAttemptOnion::new();
    for policy in policies {
        let admission =
            await_before_execution(policy.admit(&mut onion.ctx), budget.deadline(), stop).await;
        match admission {
            Ok(SinkAdmission::Admit(guard)) => onion.admit(policy, guard),
            Ok(SinkAdmission::Reject { reason }) => {
                return RetryableSinkAttempt::Rejected(onion.reject(policy.label(), reason, None));
            }
            Err(wait_error) => {
                return RetryableSinkAttempt::Rejected(onion.reject(
                    "retry_coordinator",
                    admission_interruption_reason(wait_error).to_string(),
                    Some(wait_error),
                ));
            }
        }
    }

    if let Some(barrier) = pre_execution_barrier(budget, stop) {
        return RetryableSinkAttempt::Rejected(onion.reject(
            "retry_coordinator",
            pre_execution_interruption_reason(barrier).to_string(),
            Some(barrier),
        ));
    }

    let active = await_active_execution(
        || {
            onion.commit_execution();
            execute.attempt()
        },
        budget.deadline(),
        stop,
    )
    .await;
    match active {
        Ok((outcome, drain_requested)) => {
            RetryableSinkAttempt::Settled(onion.settle(outcome, drain_requested, false))
        }
        Err(RetryWaitError::Deadline) => {
            let outcome = SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Timeout(
                "deadline_outcome_unknown".to_string(),
            )));
            RetryableSinkAttempt::Settled(onion.settle(outcome, false, true))
        }
        Err(RetryWaitError::Drain) => RetryableSinkAttempt::Rejected(onion.reject(
            "retry_coordinator",
            "graceful drain requested before sink executor start".to_string(),
            Some(RetryWaitError::Drain),
        )),
        Err(RetryWaitError::Abort) => RetryableSinkAttempt::Aborted(
            onion
                .admitted
                .into_iter()
                .filter_map(|(_, guard)| guard)
                .collect(),
        ),
    }
}

fn policy_outcome<'a>(
    outcome: &'a SinkDeliveryAttemptOutcome,
    ctx: &mut SinkPolicyCtx,
) -> SinkDeliveryPolicyOutcome<'a> {
    match outcome {
        SinkDeliveryAttemptOutcome::Delivered(Ok(report)) => {
            SinkDeliveryPolicyOutcome::Delivered { report }
        }
        SinkDeliveryAttemptOutcome::Delivered(Err(error)) => {
            ctx.set_attempt_failure(error.kind(), handler_retry_after(error));
            SinkDeliveryPolicyOutcome::Failed
        }
        SinkDeliveryAttemptOutcome::Panicked { .. } => {
            ctx.set_attempt_failure(ErrorKind::Unknown, None);
            SinkDeliveryPolicyOutcome::Failed
        }
    }
}

fn pre_execution_barrier(
    budget: &RetryBudget,
    stop: &BoundaryStopReceiver,
) -> Option<RetryWaitError> {
    if !budget.can_start_execution() {
        return Some(RetryWaitError::Deadline);
    }
    match stop.intent() {
        BoundaryStopIntent::Running => None,
        BoundaryStopIntent::Drain => Some(RetryWaitError::Drain),
        BoundaryStopIntent::Abort => Some(RetryWaitError::Abort),
    }
}

fn admission_interruption_reason(wait_error: RetryWaitError) -> &'static str {
    match wait_error {
        RetryWaitError::Deadline => {
            "retry total-wall deadline expired during sink policy admission"
        }
        RetryWaitError::Drain => "graceful drain requested during sink policy admission",
        RetryWaitError::Abort => "force abort requested during sink policy admission",
    }
}

fn pre_execution_interruption_reason(wait_error: RetryWaitError) -> &'static str {
    match wait_error {
        RetryWaitError::Deadline => "retry total-wall deadline expired before sink executor start",
        RetryWaitError::Drain => "graceful drain requested before sink executor start",
        RetryWaitError::Abort => "force abort requested before sink executor start",
    }
}

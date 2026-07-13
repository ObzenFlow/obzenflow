// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One physical source-poll policy onion.
//!
//! Admission creates the onion in declaration order. The retained guards live
//! until reverse settlement and recovery eligibility have both been observed.

use super::context::SourcePolicyCtx;
use super::contract::{
    SourceAdmission, SourceAdmissionGuard, SourceAfterPoll, SourceBatchFacts, SourcePolicy,
    SourcePollOutcome,
};
use super::disposition::source_poll_outcome;
use crate::middleware::control::policy::retry::{
    await_before_execution, await_settlement, RetryBudget, RetryWaitError,
};
use obzenflow_core::{ChainEvent, WriterId};
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use obzenflow_runtime::stages::source::{SourcePollCompletion, SourcePollReport};
use std::sync::Arc;

type SourceAdmitGuard = Option<Box<dyn SourceAdmissionGuard>>;

pub(super) enum AdmissionFailure {
    Rejected {
        reason: String,
        control_events: Vec<ChainEvent>,
    },
    Interrupted {
        wait_error: RetryWaitError,
        reason: &'static str,
        control_events: Vec<ChainEvent>,
    },
}

pub(super) struct Settlement {
    pub deadline_expired: bool,
    pub drain_requested: bool,
}

/// Retains the context, admitted policies, and their guards for one physical
/// source call. Dropping this value releases the onion only after settlement.
pub(super) struct SourceAttemptOnion<'a> {
    ctx: SourcePolicyCtx,
    admitted: Vec<(&'a Arc<dyn SourcePolicy>, SourceAdmitGuard)>,
}

impl<'a> SourceAttemptOnion<'a> {
    pub(super) async fn admit(
        policies: &'a [Arc<dyn SourcePolicy>],
        writer_id: WriterId,
        budget: &RetryBudget,
        stop: &mut BoundaryStopReceiver,
    ) -> Result<Self, AdmissionFailure> {
        let mut ctx = SourcePolicyCtx::new_retry_physical_call(writer_id);
        let mut admitted = Vec::new();

        for policy in policies {
            match await_before_execution(policy.admit(&mut ctx), budget.deadline(), stop).await {
                Ok(SourceAdmission::Admit(guard)) => admitted.push((policy, guard)),
                Ok(SourceAdmission::Reject { reason }) => {
                    let outcome = SourcePollOutcome::RejectedBy {
                        policy: policy.label(),
                        reason: &reason,
                    };
                    for (prior, _) in admitted.iter().rev() {
                        prior.observe(&outcome, &mut ctx);
                    }
                    return Err(AdmissionFailure::Rejected {
                        reason,
                        control_events: ctx.take_control_events(),
                    });
                }
                Err(wait_error) => {
                    let reason = admission_interruption_reason(wait_error);
                    let outcome = SourcePollOutcome::NotExecuted { reason };
                    for (prior, _) in admitted.iter().rev() {
                        prior.observe(&outcome, &mut ctx);
                    }
                    return Err(AdmissionFailure::Interrupted {
                        wait_error,
                        reason,
                        control_events: ctx.take_control_events(),
                    });
                }
            }
        }

        Ok(Self { ctx, admitted })
    }

    /// Execution-based reservations are charged at the physical-call commit
    /// point, immediately before the executor future is constructed.
    pub(super) fn commit_execution(&mut self) {
        for (policy, _) in &self.admitted {
            policy.commit_execution(&mut self.ctx);
        }
    }

    /// Settle the successfully started attempt in reverse policy order.
    pub(super) async fn settle(
        &mut self,
        poll: &SourcePollReport,
        budget: &RetryBudget,
        stop: &mut BoundaryStopReceiver,
    ) -> Result<Settlement, RetryWaitError> {
        let mut deadline_expired = false;
        let mut drain_requested = false;
        let batch = match &poll.result {
            Ok(SourcePollCompletion::Batch(batch)) if !batch.is_empty() => {
                Some(SourceBatchFacts::from_events(batch))
            }
            _ => None,
        };
        let outcome = source_poll_outcome(poll);

        for (policy, _) in self.admitted.iter().rev() {
            if let Some(batch) = batch {
                if !deadline_expired {
                    match await_settlement(
                        policy.after_poll(batch, &mut self.ctx),
                        budget.deadline(),
                        stop,
                    )
                    .await
                    {
                        Ok((SourceAfterPoll::Proceed, settlement_drain)) => {
                            drain_requested |= settlement_drain;
                        }
                        Err(RetryWaitError::Deadline) => deadline_expired = true,
                        Err(RetryWaitError::Drain) => {
                            unreachable!("post-call settlement remembers drain without cancelling")
                        }
                        Err(RetryWaitError::Abort) => return Err(RetryWaitError::Abort),
                    }
                }
            }
            policy.observe(&outcome, &mut self.ctx);
        }

        Ok(Settlement {
            deadline_expired,
            drain_requested,
        })
    }

    /// Reverse-observe an attempt whose policies admitted but whose executor
    /// never began.
    pub(super) fn observe_not_executed(&mut self, reason: &'static str) -> Vec<ChainEvent> {
        let outcome = SourcePollOutcome::NotExecuted { reason };
        for (policy, _) in self.admitted.iter().rev() {
            policy.observe(&outcome, &mut self.ctx);
        }
        self.ctx.take_control_events()
    }

    pub(super) fn take_control_events(&mut self) -> Vec<ChainEvent> {
        self.ctx.take_control_events()
    }

    pub(super) fn recovery_allowed(&self) -> bool {
        self.admitted
            .iter()
            .all(|(policy, _)| policy.recovery_allowed_after_settlement(&self.ctx))
    }
}

fn admission_interruption_reason(wait_error: RetryWaitError) -> &'static str {
    match wait_error {
        RetryWaitError::Deadline => {
            "retry total-wall deadline expired during source policy admission"
        }
        RetryWaitError::Drain => "graceful drain requested during source policy admission",
        RetryWaitError::Abort => "force abort requested during source policy admission",
    }
}

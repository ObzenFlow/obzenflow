// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Executes and settles one physical source-poll attempt.

use super::attempt::{AdmissionFailure, SourceAttemptOnion};
use super::contract::SourcePolicy;
use crate::middleware::control::policy::retry::{
    await_active_execution, RetryBudget, RetryWaitError,
};
use obzenflow_core::{ChainEvent, WriterId};
use obzenflow_runtime::prelude::SourceError;
use obzenflow_runtime::stages::common::{BoundaryStopIntent, BoundaryStopReceiver};
use obzenflow_runtime::stages::source::{SourcePollExecutor, SourcePollReport};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

pub(super) struct AttemptBarriers {
    pub active_deadline: bool,
    pub settlement_deadline: bool,
    pub drain_requested: bool,
    pub recovery_allowed: bool,
}

pub(super) struct SettledSourceAttempt {
    pub poll: SourcePollReport,
    pub barriers: AttemptBarriers,
    pub control_events: Vec<ChainEvent>,
}

pub(super) enum RejectionKind {
    Policy,
    Interrupted(RetryWaitError),
    Abort,
}

pub(super) struct RejectedSourceAttempt {
    pub reason: String,
    pub kind: RejectionKind,
    pub control_events: Vec<ChainEvent>,
}

pub(super) enum RetryableSourceAttempt {
    Settled(SettledSourceAttempt),
    Rejected(RejectedSourceAttempt),
}

pub(super) async fn execute_physical_attempt(
    policies: &[Arc<dyn SourcePolicy>],
    writer_id: WriterId,
    execute: &mut dyn SourcePollExecutor,
    attempt_number: NonZeroU32,
    budget: &RetryBudget,
    stop: &mut BoundaryStopReceiver,
) -> RetryableSourceAttempt {
    let mut onion = match SourceAttemptOnion::admit(policies, writer_id, budget, stop).await {
        Ok(onion) => onion,
        Err(failure) => return RetryableSourceAttempt::Rejected(admission_rejection(failure)),
    };

    if let Some(interruption) = pre_execution_barrier(budget, stop) {
        let reason = before_executor_reason(interruption);
        let control_events = onion.observe_not_executed(reason);
        return RetryableSourceAttempt::Rejected(RejectedSourceAttempt {
            reason: reason.to_string(),
            kind: RejectionKind::Interrupted(interruption),
            control_events,
        });
    }

    let active = await_active_execution(
        || {
            onion.commit_execution();
            execute.attempt(attempt_number)
        },
        budget.deadline(),
        stop,
    )
    .await;
    let (poll, mut drain_requested, active_deadline) = match active {
        Ok((poll, drain_requested)) => (poll, drain_requested, false),
        Err(RetryWaitError::Deadline) => (cancelled_poll(), false, true),
        Err(RetryWaitError::Drain) => {
            let reason = "graceful drain requested before source executor start";
            let control_events = onion.observe_not_executed(reason);
            return RetryableSourceAttempt::Rejected(RejectedSourceAttempt {
                reason: reason.to_string(),
                kind: RejectionKind::Interrupted(RetryWaitError::Drain),
                control_events,
            });
        }
        Err(RetryWaitError::Abort) => {
            return RetryableSourceAttempt::Rejected(RejectedSourceAttempt {
                reason: "force abort requested during source poll".to_string(),
                kind: RejectionKind::Abort,
                control_events: Vec::new(),
            });
        }
    };

    let settlement = match onion.settle(&poll, budget, stop).await {
        Ok(settlement) => settlement,
        Err(RetryWaitError::Abort) => {
            return RetryableSourceAttempt::Rejected(RejectedSourceAttempt {
                reason: "force abort requested during source policy settlement".to_string(),
                kind: RejectionKind::Abort,
                control_events: Vec::new(),
            });
        }
        Err(RetryWaitError::Deadline | RetryWaitError::Drain) => {
            unreachable!("source settlement reports deadline and drain in-band")
        }
    };
    drain_requested |= settlement.drain_requested;
    let control_events = onion.take_control_events();
    let recovery_allowed = onion.recovery_allowed();
    drop(onion);

    RetryableSourceAttempt::Settled(SettledSourceAttempt {
        poll,
        barriers: AttemptBarriers {
            active_deadline,
            settlement_deadline: settlement.deadline_expired,
            drain_requested,
            recovery_allowed,
        },
        control_events,
    })
}

fn admission_rejection(failure: AdmissionFailure) -> RejectedSourceAttempt {
    match failure {
        AdmissionFailure::Rejected {
            reason,
            control_events,
        } => RejectedSourceAttempt {
            reason,
            kind: RejectionKind::Policy,
            control_events,
        },
        AdmissionFailure::Interrupted {
            wait_error,
            reason,
            control_events,
        } => RejectedSourceAttempt {
            reason: reason.to_string(),
            kind: RejectionKind::Interrupted(wait_error),
            control_events,
        },
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

fn before_executor_reason(interruption: RetryWaitError) -> &'static str {
    match interruption {
        RetryWaitError::Deadline => {
            "retry total-wall deadline expired before source executor start"
        }
        RetryWaitError::Drain => "graceful drain requested before source executor start",
        RetryWaitError::Abort => "force abort requested before source executor start",
    }
}

fn cancelled_poll() -> SourcePollReport {
    SourcePollReport {
        result: Err(SourceError::Timeout("source_poll_cancelled".to_string())),
        poll_duration: Duration::ZERO,
    }
}

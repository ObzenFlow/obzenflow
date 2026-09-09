// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime stop observations and their absolute completion and escalation bounds.

use obzenflow_runtime::__private::lifecycle::{FlowCancelCause, FlowStopStatus};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) enum StopCommand {
    Graceful,
    Cancel,
    Timeout,
}

#[derive(Clone, Copy, Debug)]
pub(in super::super) enum StopReason {
    Graceful,
    Cancel,
    BeforeRun,
}

/// A boundary classification of the current Runtime observation, not execution truth.
#[derive(Clone, Copy, Debug)]
pub(in super::super) enum FlowActivity {
    BeforeRun,
    Executing,
    Terminal,
}

#[derive(Clone, Debug)]
pub(in super::super) struct StopInput {
    pub activity: FlowActivity,
    pub admitted: FlowStopStatus,
    pub at: Instant,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum CompletionBound {
    AwaitingAdmission(Instant),
    Admitted(Instant),
    BeforeRun(Instant),
}

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Escalation {
    AwaitingAdmission,
    Graceful(Instant),
    Requested(StopCommand),
    Cancelling,
}

#[derive(Clone, Debug, PartialEq)]
pub(in super::super) struct Settlement {
    pub(super) bound: CompletionBound,
    pub(super) escalation: Escalation,
}

impl Settlement {
    pub fn completion_deadline(&self) -> Instant {
        match self.bound {
            CompletionBound::AwaitingAdmission(at)
            | CompletionBound::Admitted(at)
            | CompletionBound::BeforeRun(at) => at,
        }
    }

    pub fn graceful_deadline(&self) -> Option<Instant> {
        match self.escalation {
            Escalation::Graceful(at) => Some(at),
            _ => None,
        }
    }

    pub(super) fn observe(&mut self, status: &FlowStopStatus, grace: Duration) {
        if matches!(self.bound, CompletionBound::BeforeRun(_)) {
            return;
        }
        if !matches!(status, FlowStopStatus::NotRequested) {
            self.bound = CompletionBound::Admitted(completion_deadline(
                status,
                self.completion_deadline(),
                grace,
            ));
        }
        match status {
            FlowStopStatus::Graceful { deadline }
                if !matches!(self.escalation, Escalation::Requested(_)) =>
            {
                self.escalation = Escalation::Graceful(*deadline);
            }
            FlowStopStatus::Cancelling { .. } => self.escalation = Escalation::Cancelling,
            _ => {}
        }
    }
}

/// Derive the outer bound from Runtime admission, never from a repeated request.
pub(in super::super) fn completion_deadline(
    status: &FlowStopStatus,
    not_admitted: Instant,
    grace: Duration,
) -> Instant {
    match status {
        FlowStopStatus::NotRequested => not_admitted,
        FlowStopStatus::Graceful { deadline } => *deadline + grace,
        FlowStopStatus::Cancelling {
            admitted_at,
            cause,
            graceful_deadline,
        } => {
            if *cause == FlowCancelCause::GracefulTimeout {
                graceful_deadline.unwrap_or(*admitted_at) + grace
            } else {
                *admitted_at + grace
            }
        }
    }
}

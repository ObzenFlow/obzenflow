// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime's selected failure and acknowledged terminal outcome. Neither is a
//! stop deadline or an application/host policy. The retained result belongs to
//! one execution and is never restored from an earlier run's journal.

use obzenflow_core::event::types::ViolationCause;
use obzenflow_core::EventId;
use std::sync::{Arc, OnceLock};

#[derive(Clone, Debug)]
pub(crate) struct ExecutionFailure {
    pub reason: String,
    pub cause: Option<ViolationCause>,
}

#[derive(Clone, Debug)]
pub(crate) enum ExecutionOutcome {
    Completed,
    Cancelled {
        reason: String,
    },
    Failed(ExecutionFailure),
    /// The supervisor completed teardown before execution began.
    NotStarted,
}

#[derive(Debug)]
pub(crate) struct PublishedTermination {
    pub outcome: ExecutionOutcome,
    pub event_id: Option<EventId>,
}

pub(crate) type PublishedOutcome = Arc<OnceLock<PublishedTermination>>;

#[derive(Default)]
pub(crate) struct TerminationState {
    pub failure: Option<ExecutionFailure>,
    pub published: PublishedOutcome,
}

impl TerminationState {
    pub fn fail(&mut self, reason: String, cause: Option<ViolationCause>) {
        // The first accepted failure survives subsequent cleanup failures/stops.
        self.failure
            .get_or_insert(ExecutionFailure { reason, cause });
    }
}

pub(crate) fn execution_result(
    published: &PublishedOutcome,
) -> Result<(), crate::errors::FlowError> {
    let terminal = published.get().ok_or_else(|| {
        crate::errors::FlowError::ExecutionFailed(Box::new(std::io::Error::other(
            "Pipeline supervisor finished without an acknowledged terminal outcome",
        )))
    })?;
    tracing::debug!(terminal_event_id = ?terminal.event_id, "Observed acknowledged pipeline termination");
    match &terminal.outcome {
        ExecutionOutcome::Failed(failure) => Err(crate::errors::FlowError::ExecutionFailed(
            Box::new(std::io::Error::other(failure.reason.clone())),
        )),
        ExecutionOutcome::Completed
        | ExecutionOutcome::Cancelled { .. }
        | ExecutionOutcome::NotStarted => Ok(()),
    }
}

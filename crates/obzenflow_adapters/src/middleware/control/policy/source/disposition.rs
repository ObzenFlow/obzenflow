// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::contract::{SourceBatchFacts, SourcePollOutcome};
use crate::middleware::control::policy::retry::AttemptDisposition;
use obzenflow_runtime::prelude::SourceError;
use obzenflow_runtime::stages::source::{SourcePollCompletion, SourcePollReport};

pub(super) fn source_poll_outcome(report: &SourcePollReport) -> SourcePollOutcome<'_> {
    match &report.result {
        Ok(SourcePollCompletion::Batch(batch)) if batch.is_empty() => SourcePollOutcome::Empty {
            poll_duration: report.poll_duration,
        },
        Ok(SourcePollCompletion::Batch(batch)) => SourcePollOutcome::Delivered {
            batch: SourceBatchFacts::from_events(batch),
            poll_duration: report.poll_duration,
        },
        Ok(SourcePollCompletion::Eof) => SourcePollOutcome::Eof {
            poll_duration: report.poll_duration,
        },
        Err(err) => SourcePollOutcome::Failed {
            error: err,
            poll_duration: report.poll_duration,
        },
    }
}

pub(super) fn source_attempt_disposition(report: &SourcePollReport) -> AttemptDisposition {
    use obzenflow_core::event::status::processing_status::ErrorKind;

    match &report.result {
        Ok(_) => AttemptDisposition::Completed,
        Err(SourceError::Timeout(_)) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::Timeout,
            retry_after: None,
        },
        Err(SourceError::Transport(_)) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::Remote,
            retry_after: None,
        },
        Err(SourceError::RateLimited { retry_after, .. }) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::RateLimited,
            retry_after: *retry_after,
        },
        Err(SourceError::Deserialization(_)) => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::Deserialization,
        },
        Err(SourceError::PermanentFailure(_)) => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::PermanentFailure,
        },
        Err(SourceError::Other(_)) => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::Unknown,
        },
    }
}

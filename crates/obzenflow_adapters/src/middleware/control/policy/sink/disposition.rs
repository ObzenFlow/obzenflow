// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::retry::AttemptDisposition;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::sink::journal_sink::SinkDeliveryAttemptOutcome;
use std::time::Duration;

pub(super) fn sink_attempt_disposition(outcome: &SinkDeliveryAttemptOutcome) -> AttemptDisposition {
    match outcome {
        SinkDeliveryAttemptOutcome::Delivered(Ok(_)) => AttemptDisposition::Completed,
        SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Timeout(_))) => {
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::Timeout,
                retry_after: None,
            }
        }
        SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(_))) => {
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::Remote,
                retry_after: None,
            }
        }
        SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::RateLimited {
            retry_after,
            ..
        })) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::RateLimited,
            retry_after: *retry_after,
        },
        SinkDeliveryAttemptOutcome::Delivered(Err(error)) => {
            AttemptDisposition::TerminalFailure { kind: error.kind() }
        }
        SinkDeliveryAttemptOutcome::Panicked { .. } => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::Unknown,
        },
    }
}

pub(super) fn handler_retry_after(error: &HandlerError) -> Option<Duration> {
    match error {
        HandlerError::RateLimited { retry_after, .. } => *retry_after,
        _ => None,
    }
}

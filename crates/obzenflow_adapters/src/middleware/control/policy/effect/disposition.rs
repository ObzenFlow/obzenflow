// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::retry::AttemptDisposition;
use crate::middleware::MiddlewareAbortCause;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::EffectFailureCause;
use obzenflow_core::ChainEvent;
use obzenflow_runtime::effects::{EffectAbortReason, EffectError};

pub(super) fn abort_reason_from_cause(cause: MiddlewareAbortCause) -> EffectAbortReason {
    EffectAbortReason {
        cause: EffectFailureCause {
            source: cause.source,
            code: cause.code,
        },
        message: cause.message,
        retry: cause.retry,
    }
}

pub(super) fn effect_attempt_disposition(
    result: &Result<Vec<ChainEvent>, EffectError>,
) -> AttemptDisposition {
    match result {
        Ok(_) => AttemptDisposition::Completed,
        Err(EffectError::TransientExecution(_)) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::Remote,
            retry_after: None,
        },
        Err(EffectError::RateLimited { retry_after, .. }) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::RateLimited,
            retry_after: *retry_after,
        },
        Err(EffectError::Serialization(_)) => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::Deserialization,
        },
        Err(
            EffectError::Execution(_)
            | EffectError::Journal(_)
            | EffectError::MissingRecordedEffect { .. }
            | EffectError::DuplicateRecordedEffect { .. }
            | EffectError::DescriptorMismatch { .. }
            | EffectError::RecordedFailure { .. }
            | EffectError::BoundaryRejected { .. }
            | EffectError::TypedOutcomeCoordination { .. }
            | EffectError::EffectProvenanceMismatch(_)
            | EffectError::IncompleteOutcomeGroup { .. }
            | EffectError::MissingIdempotencyKey { .. }
            | EffectError::UndeclaredEffect { .. }
            | EffectError::UndeclaredOutput { .. }
            | EffectError::EmitUnsupported { .. }
            | EffectError::MissingEffectPort { .. }
            | EffectError::TransactionalCommitMissing { .. }
            | EffectError::ReplayArchive(_),
        ) => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::Unknown,
        },
    }
}

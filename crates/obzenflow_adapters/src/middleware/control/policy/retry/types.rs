// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::middleware::MiddlewareAttachmentId;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use std::num::NonZeroU32;
use std::time::Duration;

/// Materialisation-validated breaker recovery policy.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct BoundaryRetryPolicy {
    pub max_attempts: NonZeroU32,
    pub backoff: BackoffStrategy,
    pub max_single_delay: Duration,
    pub max_total_wall_time: Duration,
}

impl BoundaryRetryPolicy {
    /// Delay before the next one-based attempt. The completed attempt number is
    /// converted to the existing zero-based backoff input.
    pub fn configured_delay_after(&self, completed_attempt: NonZeroU32) -> Duration {
        self.backoff
            .calculate_delay(completed_attempt.get().saturating_sub(1) as usize)
            .min(self.max_single_delay)
    }
}

/// Immutable recovery-owner facts exposed by one breaker policy to its boundary.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct BoundaryRetryOwner {
    pub attachment_id: MiddlewareAttachmentId,
    pub stage_id: StageId,
    pub writer_id: WriterId,
    pub protected_unit_label: String,
    pub sink_configured_target_id: Option<obzenflow_core::Ulid>,
    pub policy: BoundaryRetryPolicy,
}

/// Surface-neutral dynamic result used to decide whether another physical call
/// is eligible. Breaker health classification remains independent.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptDisposition {
    Completed,
    RetryableFailure {
        kind: ErrorKind,
        retry_after: Option<Duration>,
    },
    TerminalFailure {
        kind: ErrorKind,
    },
    NotExecuted,
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::context::SourcePolicyCtx;
use crate::middleware::BoundaryRetryOwner;
use async_trait::async_trait;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::ChainEvent;
use obzenflow_runtime::prelude::SourceError;
use std::time::Duration;

/// RAII guard returned by source-policy admission for reserved resources.
pub trait SourceAdmissionGuard: Send + Sync {}

impl<T: Send + Sync> SourceAdmissionGuard for T {}

/// Admission decision from one source policy.
pub enum SourceAdmission {
    /// Admit the poll, optionally holding a guard across the protected attempt.
    Admit(Option<Box<dyn SourceAdmissionGuard>>),
    /// Reject before polling. 115a policies do not return this, but the seam is
    /// reserved for future fail-fast policies.
    Reject { reason: String },
}

/// Post-poll source-policy decision. 115a supports proceeding only, while the
/// enum leaves the trait extensible for future post-poll policy decisions.
pub enum SourceAfterPoll {
    Proceed,
}

/// Source-batch facts exposed to control policies after a successful poll.
///
/// Policies need the control-relevant shape of the batch, not raw payload or
/// lineage access. The boundary computes these facts once from the delivered
/// batch before invoking policy hooks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceBatchFacts {
    pub event_count: usize,
    pub has_error_marked: bool,
}

impl SourceBatchFacts {
    pub fn from_events(events: &[ChainEvent]) -> Self {
        Self {
            event_count: events.len(),
            has_error_marked: events.iter().any(|event| {
                matches!(event.processing_info.status, ProcessingStatus::Error { .. })
            }),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.event_count == 0
    }
}

/// Raw source-poll outcome shown independently to each policy.
pub enum SourcePollOutcome<'a> {
    Delivered {
        batch: SourceBatchFacts,
        poll_duration: Duration,
    },
    Empty {
        poll_duration: Duration,
    },
    Eof {
        poll_duration: Duration,
    },
    Failed {
        error: &'a SourceError,
        poll_duration: Duration,
    },
    RejectedBy {
        policy: &'static str,
        reason: &'a str,
    },
    /// Admission was reserved but no physical source poll began.
    NotExecuted {
        reason: &'a str,
    },
}

/// A source resilience policy behind the adapter-owned boundary.
#[async_trait]
pub trait SourcePolicy: Send + Sync {
    fn label(&self) -> &'static str;

    async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission;

    /// Commit any execution-based reservations immediately before the source
    /// poll executor starts. Default policies reserve no such resource.
    fn commit_execution(&self, _ctx: &mut SourcePolicyCtx) {}

    async fn after_poll(
        &self,
        _batch: SourceBatchFacts,
        _ctx: &mut SourcePolicyCtx,
    ) -> SourceAfterPoll {
        SourceAfterPoll::Proceed
    }

    fn observe(&self, outcome: &SourcePollOutcome<'_>, ctx: &mut SourcePolicyCtx);

    #[doc(hidden)]
    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        None
    }

    #[doc(hidden)]
    fn recovery_allowed_after_settlement(&self, _ctx: &SourcePolicyCtx) -> bool {
        true
    }
}

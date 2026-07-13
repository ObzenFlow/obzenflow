// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::context::SourcePolicyCtx;
use super::contract::{
    SourceAdmission, SourceAdmissionGuard, SourceAfterPoll, SourceBatchFacts, SourcePolicy,
    SourcePollOutcome,
};
use super::disposition::source_poll_outcome;
use super::recovery;
use crate::middleware::BoundaryRetryOwner;
use obzenflow_core::WriterId;
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use obzenflow_runtime::stages::source::{
    SourceBoundary, SourceBoundaryFuture, SourceBoundaryOutcome, SourceBoundaryReport,
    SourcePollCompletion, SourcePollExecution, SourcePollExecutor,
};
use std::sync::Arc;

/// Source boundary backed by a declared-order policy chain.
pub struct PerSourcePolicyBoundary {
    policies: Arc<Vec<Arc<dyn SourcePolicy>>>,
    writer_id: WriterId,
}

impl PerSourcePolicyBoundary {
    pub fn new(policies: Vec<Arc<dyn SourcePolicy>>, writer_id: WriterId) -> Self {
        Self {
            policies: Arc::new(policies),
            writer_id,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    pub(super) fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        let mut owners = self
            .policies
            .iter()
            .filter_map(|policy| policy.retry_owner());
        let owner = owners.next()?;
        debug_assert!(
            owners.next().is_none(),
            "binder must reject multiple retry owners"
        );
        Some(owner)
    }

    pub(super) fn policies(&self) -> &[Arc<dyn SourcePolicy>] {
        self.policies.as_slice()
    }

    pub(super) fn writer_id(&self) -> WriterId {
        self.writer_id
    }
}

type SourceAdmitGuard = Option<Box<dyn SourceAdmissionGuard>>;

impl SourceBoundary for PerSourcePolicyBoundary {
    fn graceful_drain_settles_active_attempt(&self) -> bool {
        self.retry_owner().is_some()
    }

    fn around_poll<'a>(&'a self, execute: SourcePollExecution<'a>) -> SourceBoundaryFuture<'a> {
        Box::pin(async move {
            if self.policies.is_empty() {
                return SourceBoundaryReport {
                    outcome: SourceBoundaryOutcome::Polled(execute.await),
                    control_events: Vec::new(),
                };
            }

            let mut ctx = SourcePolicyCtx::new(self.writer_id);
            let mut admitted: Vec<(&Arc<dyn SourcePolicy>, SourceAdmitGuard)> = Vec::new();

            for policy in self.policies.iter() {
                match policy.admit(&mut ctx).await {
                    SourceAdmission::Admit(guard) => admitted.push((policy, guard)),
                    SourceAdmission::Reject { reason } => {
                        let outcome = SourcePollOutcome::RejectedBy {
                            policy: policy.label(),
                            reason: &reason,
                        };
                        for (prior, _) in admitted.iter().rev() {
                            prior.observe(&outcome, &mut ctx);
                        }
                        return SourceBoundaryReport {
                            outcome: SourceBoundaryOutcome::Rejected { reason },
                            control_events: ctx.take_control_events(),
                        };
                    }
                }
            }

            for (policy, _) in &admitted {
                policy.commit_execution(&mut ctx);
            }
            let poll = execute.await;
            let batch = match &poll.result {
                Ok(SourcePollCompletion::Batch(batch)) if !batch.is_empty() => {
                    Some(SourceBatchFacts::from_events(batch))
                }
                _ => None,
            };
            let outcome = source_poll_outcome(&poll);
            for (policy, _) in admitted.iter().rev() {
                if let Some(batch) = batch {
                    match policy.after_poll(batch, &mut ctx).await {
                        SourceAfterPoll::Proceed => {}
                    }
                }
                policy.observe(&outcome, &mut ctx);
            }

            SourceBoundaryReport {
                outcome: SourceBoundaryOutcome::Polled(poll),
                control_events: ctx.take_control_events(),
            }
        })
    }

    fn around_retryable_poll<'a>(
        &'a self,
        execute: &'a mut dyn SourcePollExecutor,
        stop: BoundaryStopReceiver,
    ) -> SourceBoundaryFuture<'a> {
        recovery::around_retryable_poll(self, execute, stop)
    }
}

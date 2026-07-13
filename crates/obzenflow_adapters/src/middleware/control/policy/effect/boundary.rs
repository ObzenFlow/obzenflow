// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::attachment::EffectPolicyAttachment;
use super::contract::{EffectAttemptOutcome, PolicyAdmission};
use super::disposition::abort_reason_from_cause;
use super::recovery;
use crate::middleware::{BoundaryRetryOwner, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use obzenflow_runtime::effects::{
    EffectBoundary, EffectBoundaryOutcome, EffectBoundaryReport, EffectExecution, EffectExecutor,
    EffectIdentity,
};
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use std::collections::HashMap;
use std::sync::Arc;

/// Effect boundary backed by per-effect policy chains, keyed by the declared
/// effect type. Effects with no declared policies execute unguarded.
pub struct PerEffectPolicyBoundary {
    pub(super) chains: HashMap<&'static str, Arc<Vec<EffectPolicyAttachment>>>,
}

impl PerEffectPolicyBoundary {
    pub fn new(chains: HashMap<&'static str, Arc<Vec<EffectPolicyAttachment>>>) -> Self {
        Self { chains }
    }

    pub(super) fn retry_owner(&self, effect_type: &'static str) -> Option<BoundaryRetryOwner> {
        let chain = self.chains.get(effect_type)?;
        let mut owners = chain.iter().filter_map(EffectPolicyAttachment::retry_owner);
        let owner = owners.next()?;
        debug_assert!(
            owners.next().is_none(),
            "binder must reject multiple retry owners"
        );
        Some(owner)
    }
}

#[async_trait]
impl EffectBoundary for PerEffectPolicyBoundary {
    async fn around_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        execute: EffectExecution,
    ) -> EffectBoundaryReport {
        let Some(chain) = self.chains.get(identity.effect_type) else {
            return EffectBoundaryReport {
                outcome: EffectBoundaryOutcome::Executed(execute.await),
                control_events: Vec::new(),
            };
        };

        // The boundary is reached only when the effect executes live, so the
        // scope is structural (FLOWIP-120c): no per-policy replay check.
        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
        let mut admitted: Vec<&EffectPolicyAttachment> = Vec::new();

        for policy in chain.iter() {
            match policy.admit(event, &mut ctx).await {
                PolicyAdmission::Admit => admitted.push(policy),
                PolicyAdmission::Synthesize { results, cause: _ } => {
                    let attempt = EffectAttemptOutcome::SkippedBy(policy.label());
                    for prior in admitted.iter().rev() {
                        prior.observe(event, &attempt, &mut ctx);
                    }
                    let control_events = ctx.take_control_events();
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Skipped {
                            results,
                            source: Some(policy.label().to_string()),
                        },
                        control_events,
                    };
                }
                PolicyAdmission::Reject(cause) => {
                    let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                    for prior in admitted.iter().rev() {
                        prior.observe(event, &attempt, &mut ctx);
                    }
                    let control_events = ctx.take_control_events();
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause)),
                        control_events,
                    };
                }
            }
        }

        let call_started = tokio::time::Instant::now();
        for policy in &admitted {
            policy.commit_execution(&mut ctx);
        }
        let result = execute.await;
        // FLOWIP-115f: record the protected call's wall-clock duration on the
        // boundary context so a slow-call policy (circuit breaker) reads it here,
        // rather than from a per-event `processing_time` field that is now stamped
        // at commit time, after this observe pass.
        ctx.insert::<crate::middleware::context_keys::EffectCallDurationNanos>(
            call_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
        let attempt = EffectAttemptOutcome::Executed(&result);
        for policy in admitted.iter().rev() {
            policy.observe(event, &attempt, &mut ctx);
        }
        let control_events = ctx.take_control_events();
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(result),
            control_events,
        }
    }

    async fn around_retryable_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        execute: &mut dyn EffectExecutor,
        stop: BoundaryStopReceiver,
    ) -> EffectBoundaryReport {
        recovery::run(self, identity, event, execute, stop).await
    }

    fn retry_enabled(&self, identity: &EffectIdentity) -> bool {
        self.retry_owner(identity.effect_type).is_some()
    }
}

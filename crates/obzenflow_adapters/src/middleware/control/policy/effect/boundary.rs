// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::attachment::EffectPolicyAttachment;
use super::contract::{EffectAttemptOutcome, PolicyAdmission};
use crate::middleware::control::circuit_breaker::{CircuitBreakerMiddleware, RecoveryDirective};
use crate::middleware::{
    EventAwareEffectPolicy, Middleware, MiddlewareAbortCause, MiddlewareContext,
};
use async_trait::async_trait;
use obzenflow_core::event::EffectFailureCause;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use obzenflow_runtime::effects::{
    EffectAbortReason, EffectBoundary, EffectBoundaryOutcome, EffectBoundaryReport, EffectIdentity,
    EffectOperation,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant as StdInstant;

/// A per-effect chain, partitioned once at construction so the execution
/// paths are explicit in the type and no per-invocation breaker discovery
/// exists. Plain chains structurally cannot acquire recovery authority.
enum CompiledEffectChain {
    Plain(Arc<Vec<EffectPolicyAttachment>>),
    Recovering {
        outer: Vec<EffectPolicyAttachment>,
        breaker: Arc<CircuitBreakerMiddleware>,
        inner: Vec<EffectPolicyAttachment>,
    },
}

impl CompiledEffectChain {
    fn compile(chain: Arc<Vec<EffectPolicyAttachment>>) -> Self {
        let mut outer = Vec::new();
        let mut breaker: Option<Arc<CircuitBreakerMiddleware>> = None;
        let mut inner = Vec::new();
        for attachment in chain.iter() {
            if breaker.is_none() {
                if let Some(found) = attachment.circuit_breaker_policy() {
                    breaker = Some(found.clone());
                    continue;
                }
                outer.push(attachment.clone());
            } else {
                // The control registry rejects a second breaker per
                // stage-and-effect key at materialisation; a duplicate in a
                // hand-built chain acts as an inner policy of the first.
                inner.push(attachment.clone());
            }
        }
        match breaker {
            None => Self::Plain(chain),
            Some(breaker) => Self::Recovering {
                outer,
                breaker,
                inner,
            },
        }
    }
}

/// Effect boundary backed by per-effect policy chains, keyed by the declared
/// effect type. Effects with no declared policies execute unguarded.
pub struct PerEffectPolicyBoundary {
    chains: HashMap<&'static str, CompiledEffectChain>,
}

impl PerEffectPolicyBoundary {
    pub fn new(chains: HashMap<&'static str, Arc<Vec<EffectPolicyAttachment>>>) -> Self {
        Self {
            chains: chains
                .into_iter()
                .map(|(effect_type, chain)| (effect_type, CompiledEffectChain::compile(chain)))
                .collect(),
        }
    }
}

fn abort_reason_from_cause(cause: MiddlewareAbortCause) -> EffectAbortReason {
    EffectAbortReason {
        cause: EffectFailureCause {
            source: cause.source,
            code: cause.code,
        },
        message: cause.message,
        retry: cause.retry,
    }
}

fn observe_reverse(
    policies: &[&EffectPolicyAttachment],
    event: &ChainEvent,
    attempt: &EffectAttemptOutcome<'_>,
    ctx: &mut MiddlewareContext,
) {
    for policy in policies.iter().rev() {
        policy.observe(event, attempt, ctx);
    }
}

async fn execute_chain_once(
    chain: &[EffectPolicyAttachment],
    event: &ChainEvent,
    operation: &mut EffectOperation,
) -> EffectBoundaryReport {
    let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
    let mut admitted: Vec<&EffectPolicyAttachment> = Vec::new();

    for policy in chain {
        match policy.admit(event, &mut ctx).await {
            PolicyAdmission::Admit => admitted.push(policy),
            PolicyAdmission::Synthesize { results, cause: _ } => {
                let attempt = EffectAttemptOutcome::SkippedBy(policy.label());
                observe_reverse(&admitted, event, &attempt, &mut ctx);
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Skipped {
                        results,
                        source: Some(policy.label().to_string()),
                    },
                    control_events: ctx.take_control_events(),
                };
            }
            PolicyAdmission::Reject(cause) => {
                let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                observe_reverse(&admitted, event, &attempt, &mut ctx);
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause)),
                    control_events: ctx.take_control_events(),
                };
            }
        }
    }

    let call_started = StdInstant::now();
    let result = operation.execute().await;
    ctx.insert::<crate::middleware::context_keys::EffectCallDurationNanos>(
        call_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
    );
    let attempt = EffectAttemptOutcome::Executed(&result);
    observe_reverse(&admitted, event, &attempt, &mut ctx);
    EffectBoundaryReport {
        outcome: EffectBoundaryOutcome::Executed(result),
        control_events: ctx.take_control_events(),
    }
}

#[async_trait]
impl EffectBoundary for PerEffectPolicyBoundary {
    async fn around_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        mut operation: EffectOperation,
    ) -> EffectBoundaryReport {
        let (outer, breaker, inner) = match self.chains.get(identity.effect_type) {
            None => {
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
                    control_events: Vec::new(),
                };
            }
            Some(CompiledEffectChain::Plain(chain)) => {
                return execute_chain_once(chain, event, &mut operation).await;
            }
            Some(CompiledEffectChain::Recovering {
                outer,
                breaker,
                inner,
            }) => (outer, breaker, inner),
        };

        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
        let mut admitted_outer: Vec<&EffectPolicyAttachment> = Vec::new();
        for policy in outer {
            match policy.admit(event, &mut ctx).await {
                PolicyAdmission::Admit => admitted_outer.push(policy),
                PolicyAdmission::Synthesize { results, cause: _ } => {
                    let attempt = EffectAttemptOutcome::SkippedBy(policy.label());
                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Skipped {
                            results,
                            source: Some(policy.label().to_string()),
                        },
                        control_events: ctx.take_control_events(),
                    };
                }
                PolicyAdmission::Reject(cause) => {
                    let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause)),
                        control_events: ctx.take_control_events(),
                    };
                }
            }
        }

        let breaker_label = Middleware::label(breaker.as_ref());
        match EventAwareEffectPolicy::admit(breaker.as_ref(), event, &mut ctx).await {
            PolicyAdmission::Admit => {}
            PolicyAdmission::Synthesize { results, cause: _ } => {
                let attempt = EffectAttemptOutcome::SkippedBy(breaker_label);
                observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Skipped {
                        results,
                        source: Some(breaker_label.to_string()),
                    },
                    control_events: ctx.take_control_events(),
                };
            }
            PolicyAdmission::Reject(cause) => {
                let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause)),
                    control_events: ctx.take_control_events(),
                };
            }
        }

        let mut control_events = ctx.take_control_events();
        let mut session = breaker.begin_effect_recovery(&ctx, identity.cursor.clone(), event.id);

        loop {
            let attempt_report = execute_chain_once(inner, event, &mut operation).await;
            control_events.extend(attempt_report.control_events);
            let result = match attempt_report.outcome {
                EffectBoundaryOutcome::Executed(result) => result,
                EffectBoundaryOutcome::Skipped { results, source } => {
                    session.settle_not_executed(&mut ctx);
                    let label = source.as_deref().unwrap_or("inner_effect_policy");
                    let attempt = EffectAttemptOutcome::SkippedBy(label);
                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                    control_events.extend(ctx.take_control_events());
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Skipped { results, source },
                        control_events,
                    };
                }
                EffectBoundaryOutcome::Aborted(reason) => {
                    session.settle_not_executed(&mut ctx);
                    let cause = MiddlewareAbortCause {
                        source: reason.cause.source.clone(),
                        code: reason.cause.code.clone(),
                        message: reason.message.clone(),
                        retry: reason.retry,
                        event: None,
                    };
                    let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                    control_events.extend(ctx.take_control_events());
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Aborted(reason),
                        control_events,
                    };
                }
            };

            if let RecoveryDirective::RetryAfter(delay) = session.assess(event, &result, &mut ctx) {
                control_events.extend(ctx.take_control_events());
                tokio::time::sleep(delay).await;
                if session.recheck_after_delay(&mut ctx) {
                    continue;
                }
            }

            session.settle(event, &mut ctx);
            let attempt = EffectAttemptOutcome::Executed(&result);
            observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
            control_events.extend(ctx.take_control_events());
            return EffectBoundaryReport {
                outcome: EffectBoundaryOutcome::Executed(result),
                control_events,
            };
        }
    }
}

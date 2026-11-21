// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::attachment::EffectPolicyAttachment;
use super::contract::{EffectAttemptOutcome, PolicyAdmission};
use crate::middleware::control::EffectResilienceMiddleware;
use crate::middleware::{MiddlewareAbortCause, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::event::observability::{NoObservations, ObservationRecorder};
use obzenflow_core::event::EffectFailureCause;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use obzenflow_runtime::effects::{
    AffineEffectBoundaryReport, AffineEffectOperation, EffectAbortReason, EffectBoundary,
    EffectBoundaryOutcome, EffectBoundaryReport, EffectIdentity, RepeatableEffectOperation,
    SingleUseEffectBoundaryReport, SingleUseEffectOperation,
};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

/// A per-effect chain, partitioned once at construction so the execution
/// paths are explicit in the type and no per-invocation breaker discovery
/// exists. Plain chains structurally cannot acquire recovery authority.
enum CompiledEffectChain {
    Plain(Arc<Vec<EffectPolicyAttachment>>),
    Resilient {
        outer: Vec<EffectPolicyAttachment>,
        resilience: Arc<EffectResilienceMiddleware>,
    },
}

impl CompiledEffectChain {
    fn compile(chain: Arc<Vec<EffectPolicyAttachment>>) -> Self {
        if let Some(resilience) = chain
            .iter()
            .find_map(EffectPolicyAttachment::effect_resilience_policy)
            .cloned()
        {
            let outer = chain
                .iter()
                .filter(|attachment| attachment.effect_resilience_policy().is_none())
                .cloned()
                .collect();
            return Self::Resilient { outer, resilience };
        }
        Self::Plain(chain)
    }
}

/// Effect boundary backed by per-effect policy chains, keyed by the declared
/// effect type. Effects with no declared policies execute unguarded.
pub struct PerEffectPolicyBoundary {
    recorder: OnceLock<Arc<dyn ObservationRecorder>>,
    chains: HashMap<&'static str, CompiledEffectChain>,
}

impl PerEffectPolicyBoundary {
    fn observation_recorder(&self) -> Arc<dyn ObservationRecorder> {
        self.recorder
            .get()
            .cloned()
            .unwrap_or_else(|| Arc::new(NoObservations))
    }

    pub fn new(chains: HashMap<&'static str, Arc<Vec<EffectPolicyAttachment>>>) -> Self {
        Self {
            recorder: OnceLock::new(),
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

pub(in crate::middleware::control) async fn execute_chain_once(
    recorder: Arc<dyn ObservationRecorder>,
    effect_type: &str,
    chain: &[EffectPolicyAttachment],
    event: &ChainEvent,
    operation: &mut RepeatableEffectOperation,
) -> EffectBoundaryReport {
    let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary)
        .with_observation_recorder(recorder)
        .with_effect_subject(effect_type);
    let mut admitted: Vec<&EffectPolicyAttachment> = Vec::new();

    for policy in chain {
        match policy.admit(event, &mut ctx).await {
            PolicyAdmission::Admit => admitted.push(policy),
            PolicyAdmission::Reject(cause) => {
                let cause = *cause;
                let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                observe_reverse(&admitted, event, &attempt, &mut ctx);
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause)),
                    control_events: ctx.take_control_events(),
                };
            }
        }
    }

    let result = operation.execute().await;
    let attempt = EffectAttemptOutcome::Executed(&result);
    observe_reverse(&admitted, event, &attempt, &mut ctx);
    EffectBoundaryReport {
        outcome: EffectBoundaryOutcome::Executed(result),
        control_events: ctx.take_control_events(),
    }
}

async fn execute_single_use_chain_once(
    recorder: Arc<dyn ObservationRecorder>,
    effect_type: &str,
    chain: &[EffectPolicyAttachment],
    event: &ChainEvent,
    operation: SingleUseEffectOperation,
) -> SingleUseEffectBoundaryReport {
    let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary)
        .with_observation_recorder(recorder)
        .with_effect_subject(effect_type);
    let mut admitted: Vec<&EffectPolicyAttachment> = Vec::new();

    for policy in chain {
        match policy.admit(event, &mut ctx).await {
            PolicyAdmission::Admit => admitted.push(policy),
            PolicyAdmission::Reject(cause) => {
                let cause = *cause;
                let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                observe_reverse(&admitted, event, &attempt, &mut ctx);
                return operation.abort(abort_reason_from_cause(cause), ctx.take_control_events());
            }
        }
    }

    let execution = operation.execute().await;
    let attempt = EffectAttemptOutcome::Executed(execution.result());
    observe_reverse(&admitted, event, &attempt, &mut ctx);
    execution.into_report(ctx.take_control_events())
}

async fn execute_affine_chain_once(
    recorder: Arc<dyn ObservationRecorder>,
    effect_type: &str,
    chain: &[EffectPolicyAttachment],
    event: &ChainEvent,
    operation: AffineEffectOperation,
) -> AffineEffectBoundaryReport {
    let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary)
        .with_observation_recorder(recorder)
        .with_effect_subject(effect_type);
    let mut admitted: Vec<&EffectPolicyAttachment> = Vec::new();

    for policy in chain {
        match policy.admit(event, &mut ctx).await {
            PolicyAdmission::Admit => admitted.push(policy),
            PolicyAdmission::Reject(cause) => {
                let cause = *cause;
                let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                observe_reverse(&admitted, event, &attempt, &mut ctx);
                return operation.abort(abort_reason_from_cause(cause), ctx.take_control_events());
            }
        }
    }

    let execution = operation.execute().await;
    let attempt = EffectAttemptOutcome::Executed(execution.result());
    observe_reverse(&admitted, event, &attempt, &mut ctx);
    execution.into_report(ctx.take_control_events())
}

#[async_trait]
impl EffectBoundary for PerEffectPolicyBoundary {
    fn supports_sink_groups(&self) -> bool {
        true
    }

    fn sink_admission_wait(&self, effect_type: &'static str) -> std::time::Duration {
        match self.chains.get(effect_type) {
            Some(CompiledEffectChain::Resilient { resilience, .. }) => {
                resilience.sink_admission_wait()
            }
            _ => std::time::Duration::ZERO,
        }
    }

    async fn admit_sink_request(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
    ) -> obzenflow_runtime::effects::SinkAdmission {
        use obzenflow_runtime::effects::{SinkAdmission, SinkPermit};
        let (policies, resilience) = match self.chains.get(identity.effect_type) {
            None => {
                return SinkAdmission::Admitted(SinkPermit {
                    closes_group: false,
                    started: None,
                    finished: None,
                })
            }
            Some(CompiledEffectChain::Plain(chain)) => (chain.as_slice(), None),
            Some(CompiledEffectChain::Resilient { outer, resilience }) => {
                (outer.as_slice(), Some(resilience))
            }
        };
        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary)
            .with_observation_recorder(self.observation_recorder())
            .with_effect_subject(identity.effect_type);
        let mut admitted = Vec::new();
        let mut refusal = None;
        for policy in policies {
            match policy.admit(event, &mut ctx).await {
                PolicyAdmission::Admit => admitted.push(policy.clone()),
                PolicyAdmission::Reject(cause) => {
                    refusal = Some(cause);
                    break;
                }
            }
        }
        let mut reservation = None;
        if let (None, Some(resilience)) = (&refusal, resilience) {
            match resilience.reserve_sink(&mut ctx).await {
                Ok(permit) => reservation = permit,
                Err(cause) => refusal = Some(cause),
            }
        }
        if let Some(cause) = refusal {
            for policy in admitted.iter().rev() {
                policy.observe(event, &EffectAttemptOutcome::RejectedBy(&cause), &mut ctx);
            }
            return SinkAdmission::Refused(
                abort_reason_from_cause(*cause),
                ctx.take_control_events(),
            );
        }
        let parent = event.clone();
        SinkAdmission::Admitted(SinkPermit {
            closes_group: resilience.is_some_and(|r| r.needs_probe()),
            started: reservation
                .map(|permit| Box::new(move || permit.commit()) as Box<dyn FnOnce() + Send + Sync>),
            finished: Some(Box::new(move |outcome| {
                let cause;
                let attempt = match outcome {
                    EffectBoundaryOutcome::Executed(result) => {
                        EffectAttemptOutcome::Executed(result)
                    }
                    EffectBoundaryOutcome::Aborted(reason) => {
                        cause = MiddlewareAbortCause {
                            source: reason.cause.source.clone(),
                            code: reason.cause.code.clone(),
                            message: reason.message.clone(),
                            retry: reason.retry,
                            event: None,
                        };
                        EffectAttemptOutcome::RejectedBy(&cause)
                    }
                };
                for policy in admitted.iter().rev() {
                    policy.observe(&parent, &attempt, &mut ctx);
                }
                ctx.take_control_events()
            })),
        })
    }

    fn install_observation_recorder(&self, recorder: Arc<dyn ObservationRecorder>) {
        self.recorder.get_or_init(|| recorder);
    }

    async fn around_repeatable_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        mut operation: RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        let (outer, resilience) = match self.chains.get(identity.effect_type) {
            None => {
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
                    control_events: Vec::new(),
                };
            }
            Some(CompiledEffectChain::Plain(_)) if identity.additional.is_some() => {
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
                    control_events: Vec::new(),
                }
            }
            Some(CompiledEffectChain::Plain(chain)) => {
                return execute_chain_once(
                    self.observation_recorder(),
                    identity.effect_type,
                    chain,
                    event,
                    &mut operation,
                )
                .await;
            }
            Some(CompiledEffectChain::Resilient { outer, resilience }) => (outer, resilience),
        };

        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary)
            .with_observation_recorder(self.observation_recorder())
            .with_effect_subject(identity.effect_type);
        let mut admitted_outer: Vec<&EffectPolicyAttachment> = Vec::new();
        for policy in if identity.additional.is_some() {
            &[]
        } else {
            outer.as_slice()
        } {
            match policy.admit(event, &mut ctx).await {
                PolicyAdmission::Admit => admitted_outer.push(policy),
                PolicyAdmission::Reject(cause) => {
                    let cause = *cause;
                    let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause)),
                        control_events: ctx.take_control_events(),
                    };
                }
            }
        }

        let mut report = resilience
            .execute_repeatable(identity, event, &mut ctx, &mut operation)
            .await;

        match &report.outcome {
            EffectBoundaryOutcome::Executed(result) => {
                let attempt = EffectAttemptOutcome::Executed(result);
                observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
            }
            EffectBoundaryOutcome::Aborted(reason) => {
                let cause = MiddlewareAbortCause {
                    source: reason.cause.source.clone(),
                    code: reason.cause.code.clone(),
                    message: reason.message.clone(),
                    retry: reason.retry,
                    event: None,
                };
                let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
            }
        }
        report.control_events.extend(ctx.take_control_events());
        report
    }

    async fn around_single_use_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        let (outer, resilience) = match self.chains.get(identity.effect_type) {
            None => return operation.execute().await.into_report(Vec::new()),
            Some(CompiledEffectChain::Plain(chain)) => {
                return execute_single_use_chain_once(
                    self.observation_recorder(),
                    identity.effect_type,
                    chain,
                    event,
                    operation,
                )
                .await;
            }
            Some(CompiledEffectChain::Resilient { outer, resilience }) => (outer, resilience),
        };

        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary)
            .with_observation_recorder(self.observation_recorder())
            .with_effect_subject(identity.effect_type);
        let mut admitted_outer: Vec<&EffectPolicyAttachment> = Vec::new();
        for policy in outer {
            match policy.admit(event, &mut ctx).await {
                PolicyAdmission::Admit => admitted_outer.push(policy),
                PolicyAdmission::Reject(cause) => {
                    let cause = *cause;
                    let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                    return operation
                        .abort(abort_reason_from_cause(cause), ctx.take_control_events());
                }
            }
        }

        let mut report = resilience
            .execute_single_use(identity, event, &mut ctx, operation)
            .await;
        if let Some(result) = report.execution_result() {
            let attempt = EffectAttemptOutcome::Executed(result);
            observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
        } else if let Some(reason) = report.abort_reason() {
            let cause = MiddlewareAbortCause {
                source: reason.cause.source.clone(),
                code: reason.cause.code.clone(),
                message: reason.message.clone(),
                retry: reason.retry,
                event: None,
            };
            let attempt = EffectAttemptOutcome::RejectedBy(&cause);
            observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
        }
        report.extend_control_events(ctx.take_control_events());
        report
    }

    async fn around_affine_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        operation: AffineEffectOperation,
    ) -> AffineEffectBoundaryReport {
        let (outer, resilience) = match self.chains.get(identity.effect_type) {
            None => return operation.execute().await.into_report(Vec::new()),
            Some(CompiledEffectChain::Plain(_)) if identity.additional.is_some() => {
                return operation.execute().await.into_report(Vec::new())
            }
            Some(CompiledEffectChain::Plain(chain)) => {
                return execute_affine_chain_once(
                    self.observation_recorder(),
                    identity.effect_type,
                    chain,
                    event,
                    operation,
                )
                .await;
            }
            Some(CompiledEffectChain::Resilient { outer, resilience }) => (outer, resilience),
        };

        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary)
            .with_observation_recorder(self.observation_recorder())
            .with_effect_subject(identity.effect_type);
        let mut admitted_outer: Vec<&EffectPolicyAttachment> = Vec::new();
        for policy in if identity.additional.is_some() {
            &[]
        } else {
            outer.as_slice()
        } {
            match policy.admit(event, &mut ctx).await {
                PolicyAdmission::Admit => admitted_outer.push(policy),
                PolicyAdmission::Reject(cause) => {
                    let cause = *cause;
                    let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                    return operation
                        .abort(abort_reason_from_cause(cause), ctx.take_control_events());
                }
            }
        }

        let mut report = resilience
            .execute_affine(identity, event, &mut ctx, operation)
            .await;
        if let Some(result) = report.execution_result() {
            let attempt = EffectAttemptOutcome::Executed(result);
            observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
        } else if let Some(reason) = report.abort_reason() {
            let cause = MiddlewareAbortCause {
                source: reason.cause.source.clone(),
                code: reason.cause.code.clone(),
                message: reason.message.clone(),
                retry: reason.retry,
                event: None,
            };
            let attempt = EffectAttemptOutcome::RejectedBy(&cause);
            observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
        }
        report.extend_control_events(ctx.take_control_events());
        report
    }
}

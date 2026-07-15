// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::contract::{
    EffectAttemptOutcome, EffectPolicy, EventAwareEffectPolicy, PolicyAdmission,
};
use crate::middleware::control::circuit_breaker::{effect_error_event, CircuitBreakerMiddleware};
use crate::middleware::{Middleware, MiddlewareAbortCause, MiddlewareAction, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::ChainEvent;
use std::sync::Arc;

/// The opaque per-effect attachment plan. Ordinary policy constructors are
/// public; the breaker's privileged constructor is crate-private, and no
/// label, downcast, hint, registry, or context key grants another policy a
/// repeatable continuation (FLOWIP-115h AR4).
#[derive(Clone)]
pub struct EffectPolicyAttachment {
    kind: EffectPolicyAttachmentKind,
}

#[derive(Clone)]
enum EffectPolicyAttachmentKind {
    Neutral(Arc<dyn EffectPolicy>),
    EventAware(Arc<dyn EventAwareEffectPolicy>),
    CircuitBreaker(Arc<CircuitBreakerMiddleware>),
}

impl EffectPolicyAttachment {
    pub fn neutral(policy: Arc<dyn EffectPolicy>) -> Self {
        Self {
            kind: EffectPolicyAttachmentKind::Neutral(policy),
        }
    }

    pub fn event_aware(policy: Arc<dyn EventAwareEffectPolicy>) -> Self {
        Self {
            kind: EffectPolicyAttachmentKind::EventAware(policy),
        }
    }

    pub(crate) fn circuit_breaker(policy: Arc<CircuitBreakerMiddleware>) -> Self {
        Self {
            kind: EffectPolicyAttachmentKind::CircuitBreaker(policy),
        }
    }

    pub(super) fn circuit_breaker_policy(&self) -> Option<&Arc<CircuitBreakerMiddleware>> {
        match &self.kind {
            EffectPolicyAttachmentKind::CircuitBreaker(policy) => Some(policy),
            EffectPolicyAttachmentKind::Neutral(_) | EffectPolicyAttachmentKind::EventAware(_) => {
                None
            }
        }
    }

    pub(super) fn label(&self) -> &'static str {
        match &self.kind {
            EffectPolicyAttachmentKind::Neutral(policy) => policy.label(),
            EffectPolicyAttachmentKind::EventAware(policy) => policy.label(),
            EffectPolicyAttachmentKind::CircuitBreaker(policy) => {
                Middleware::label(policy.as_ref())
            }
        }
    }

    pub(super) async fn admit(
        &self,
        event: &ChainEvent,
        ctx: &mut MiddlewareContext,
    ) -> PolicyAdmission {
        match &self.kind {
            EffectPolicyAttachmentKind::Neutral(policy) => policy.admit(ctx).await,
            EffectPolicyAttachmentKind::EventAware(policy) => policy.admit(event, ctx).await,
            EffectPolicyAttachmentKind::CircuitBreaker(policy) => policy.admit(event, ctx).await,
        }
    }

    pub(super) fn observe(
        &self,
        event: &ChainEvent,
        attempt: &EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    ) {
        match &self.kind {
            EffectPolicyAttachmentKind::Neutral(policy) => policy.observe(attempt, ctx),
            EffectPolicyAttachmentKind::EventAware(policy) => policy.observe(event, attempt, ctx),
            EffectPolicyAttachmentKind::CircuitBreaker(policy) => {
                policy.observe(event, attempt, ctx)
            }
        }
    }
}

/// Adapt a chain middleware instance into a per-effect policy.
///
/// The bridge is event-aware because the generic `Middleware` trait is
/// event-shaped.
pub fn effect_policy_from_middleware(instance: Arc<dyn Middleware>) -> EffectPolicyAttachment {
    EffectPolicyAttachment::event_aware(Arc::new(ChainSurfacePolicy { inner: instance }))
}

/// Generic per-effect adapter over the chain middleware surface.
struct ChainSurfacePolicy {
    inner: Arc<dyn Middleware>,
}

#[async_trait]
impl EventAwareEffectPolicy for ChainSurfacePolicy {
    fn label(&self) -> &'static str {
        self.inner.label()
    }

    async fn admit(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> PolicyAdmission {
        match self.inner.pre_handle(event, ctx) {
            MiddlewareAction::Continue => PolicyAdmission::Admit,
            MiddlewareAction::Skip { results, cause } => {
                PolicyAdmission::Synthesize { results, cause }
            }
            MiddlewareAction::Abort { cause } => {
                PolicyAdmission::Reject(cause.unwrap_or_else(|| MiddlewareAbortCause {
                    source: obzenflow_core::event::EffectFailureSource::new(self.inner.label()),
                    code: obzenflow_core::event::EffectFailureCode::new("aborted"),
                    message: format!(
                        "middleware '{}' aborted effect execution",
                        self.inner.label()
                    ),
                    retry: obzenflow_core::event::RetryDisposition::NotRetryable,
                    event: None,
                }))
            }
        }
    }

    fn observe(
        &self,
        event: &ChainEvent,
        attempt: &EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    ) {
        match attempt {
            EffectAttemptOutcome::Executed(Ok(outputs)) => {
                self.inner.post_handle(event, outputs, ctx);
            }
            EffectAttemptOutcome::Executed(Err(err)) => {
                let error_event = effect_error_event(event, err);
                self.inner
                    .post_handle(event, std::slice::from_ref(&error_event), ctx);
            }
            EffectAttemptOutcome::SkippedBy(_) | EffectAttemptOutcome::RejectedBy(_) => {
                // The protected call never went out; nothing to observe.
            }
        }
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::contract::{
    EffectAttemptOutcome, EffectPolicy, EventAwareEffectPolicy, PolicyAdmission,
};
use crate::middleware::{
    BoundaryRetryOwner, Middleware, MiddlewareAbortCause, MiddlewareAction, MiddlewareContext,
};
use async_trait::async_trait;
use obzenflow_core::event::{EffectFailureCode, EffectFailureSource, RetryDisposition};
use obzenflow_core::ChainEvent;
use std::sync::Arc;

#[derive(Clone)]
pub enum EffectPolicyAttachment {
    Neutral(Arc<dyn EffectPolicy>),
    EventAware(Arc<dyn EventAwareEffectPolicy>),
}

impl EffectPolicyAttachment {
    pub fn neutral(policy: Arc<dyn EffectPolicy>) -> Self {
        Self::Neutral(policy)
    }

    pub fn event_aware(policy: Arc<dyn EventAwareEffectPolicy>) -> Self {
        Self::EventAware(policy)
    }

    pub(super) fn label(&self) -> &'static str {
        match self {
            Self::Neutral(policy) => policy.label(),
            Self::EventAware(policy) => policy.label(),
        }
    }

    pub(super) async fn admit(
        &self,
        event: &ChainEvent,
        ctx: &mut MiddlewareContext,
    ) -> PolicyAdmission {
        match self {
            Self::Neutral(policy) => policy.admit(ctx).await,
            Self::EventAware(policy) => policy.admit(event, ctx).await,
        }
    }

    pub(super) fn commit_execution(&self, ctx: &mut MiddlewareContext) {
        match self {
            Self::Neutral(policy) => policy.commit_execution(ctx),
            Self::EventAware(policy) => policy.commit_execution(ctx),
        }
    }

    pub(super) fn observe(
        &self,
        event: &ChainEvent,
        attempt: &EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    ) {
        match self {
            Self::Neutral(policy) => policy.observe(attempt, ctx),
            Self::EventAware(policy) => policy.observe(event, attempt, ctx),
        }
    }

    pub(super) fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        match self {
            Self::Neutral(policy) => policy.retry_owner(),
            Self::EventAware(policy) => policy.retry_owner(),
        }
    }

    pub(super) fn recovery_allowed_after_settlement(&self, ctx: &MiddlewareContext) -> bool {
        match self {
            Self::Neutral(policy) => policy.recovery_allowed_after_settlement(ctx),
            Self::EventAware(policy) => policy.recovery_allowed_after_settlement(ctx),
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
                    source: EffectFailureSource::new(self.inner.label()),
                    code: EffectFailureCode::new("aborted"),
                    message: format!(
                        "middleware '{}' aborted effect execution",
                        self.inner.label()
                    ),
                    retry: RetryDisposition::NotRetryable,
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
                let error_event = event.clone().mark_as_error(
                    err.to_string(),
                    obzenflow_core::event::status::processing_status::ErrorKind::Remote,
                );
                self.inner
                    .post_handle(event, std::slice::from_ref(&error_event), ctx);
            }
            EffectAttemptOutcome::SkippedBy(_) | EffectAttemptOutcome::RejectedBy(_) => {
                // The protected call never went out; nothing to observe.
            }
        }
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::contract::{
    EffectAttemptOutcome, EffectPolicy, EventAwareEffectPolicy, PolicyAdmission,
};
use crate::middleware::control::EffectResilienceMiddleware;
use crate::middleware::MiddlewareContext;
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
    EffectResilience(Arc<EffectResilienceMiddleware>),
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

    pub(in crate::middleware::control) fn effect_resilience(
        policy: Arc<EffectResilienceMiddleware>,
    ) -> Self {
        Self {
            kind: EffectPolicyAttachmentKind::EffectResilience(policy),
        }
    }

    pub(super) fn effect_resilience_policy(&self) -> Option<&Arc<EffectResilienceMiddleware>> {
        match &self.kind {
            EffectPolicyAttachmentKind::EffectResilience(policy) => Some(policy),
            _ => None,
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
            EffectPolicyAttachmentKind::EffectResilience(_) => PolicyAdmission::Admit,
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
            EffectPolicyAttachmentKind::EffectResilience(_) => {}
        }
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    DomainFacts, Effect, EffectBindingUse, EffectContext, EffectError, EffectPortSlot,
    EffectPortSlotSet, EffectSafety, IdempotencyKey, Named, NamedEffect,
};
use async_trait::async_trait;
use obzenflow_core::event::schema::EffectOutcomeFacts;
use obzenflow_core::SystemBindingEvidence;
use serde::Serialize;

/// A request a system answers, performed by a handler with `fx.call(request)`.
#[async_trait]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a request",
    label = "`fx.call` performs a request that a system answers",
    note = "perform a portless or hand-written effect with `fx.perform`"
)]
pub trait CallRequest: Clone + std::fmt::Debug + Serialize + Send + Sync + 'static {
    const EFFECT_TYPE: &'static str;
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety;

    /// The port a system implements to answer this request.
    type Port: ?Sized + Send + Sync + 'static;

    /// The facts a successful call records.
    type Outcome: EffectOutcomeFacts + Clone + Send + Sync + 'static;

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        None
    }

    async fn call(
        &self,
        port: &Self::Port,
        ctx: &mut EffectContext,
    ) -> Result<Self::Outcome, EffectError>;
}

/// The named effect that performs one request through the system the stage names.
pub struct Call<R: CallRequest> {
    request: R,
    projection: EffectBindingUse<Call<R>>,
}

impl<R: CallRequest> Call<R> {
    pub(super) fn new(request: R, projection: EffectBindingUse<Call<R>>) -> Self {
        Self {
            request,
            projection,
        }
    }
}

pub(super) const fn call_port_slot<R: CallRequest>() -> EffectPortSlot<R::Port> {
    EffectPortSlot::new("port")
}

impl<R: CallRequest> Clone for Call<R> {
    fn clone(&self) -> Self {
        Self {
            request: self.request.clone(),
            projection: self.projection.clone(),
        }
    }
}

impl<R: CallRequest> std::fmt::Debug for Call<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Call")
            .field("effect_type", &R::EFFECT_TYPE)
            .field("request", &self.request)
            .finish()
    }
}

#[async_trait]
impl<R: CallRequest> Effect for Call<R> {
    const EFFECT_TYPE: &'static str = R::EFFECT_TYPE;
    const SCHEMA_VERSION: u32 = R::SCHEMA_VERSION;
    const SAFETY: EffectSafety = R::SAFETY;
    type BindingMode = Named<SystemBindingEvidence>;
    type Outcome = R::Outcome;
    type OutcomeSemantics = DomainFacts;

    fn label(&self) -> &str {
        R::EFFECT_TYPE
    }

    fn canonical_input(&self) -> serde_json::Value {
        serde_json::json!(self.request)
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        let port = ctx.port(call_port_slot::<R>())?;
        self.request.call(&port, ctx).await
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        self.request.idempotency_key()
    }
}

impl<R: CallRequest> NamedEffect for Call<R> {
    type BindingEvidence = SystemBindingEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.projection
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(call_port_slot::<R>())
    }
}

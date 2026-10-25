// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Compiler-checked effectful stage authoring facade (FLOWIP-120z).

use super::{
    assert_distinct_effect_set, Call, CallRequest, ConfiguredEffect, Effect, EffectBindingUse,
    EffectError, EffectInvocationContext, EffectSet, EffectsCore, NamedEffect, StageCompletion,
};
use obzenflow_core::{
    assert_distinct_stage_fact_set, ChainEvent, Member, StageFactSet, SubsetOf, TypedPayload,
};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;

/// Diagnostic facade proving that one emitted fact belongs to the handler's
/// declared output set.
///
/// The blanket implementation deliberately delegates to the neutral
/// [`Member`] proof. User code should name only `Output`; this trait exists so
/// a failed `emit` reports the authoring relationship rather than the
/// type-level list machinery.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "fact `{Self}` is not declared in this handler's `Output` set",
    label = "`{Self}` cannot be emitted by this handler",
    note = "add `{Self}` to the canonical stage arrow, then mirror the arrow's flat fact set in \
            the handler's `Output`, or remove this `emit` call (FLOWIP-120z)"
)]
pub trait OutputAllowsFact<Output, At> {}

#[diagnostic::do_not_recommend]
impl<T, Output, At> OutputAllowsFact<Output, At> for T
where
    T: TypedPayload,
    Output: StageFactSet,
    Output::Members: Member<T, At>,
{
}

/// Diagnostic facade proving that one performed effect belongs to the
/// handler's declared capability set.
///
/// Its blanket implementation preserves the existing [`Member`] proof while
/// keeping effect permission errors in the vocabulary of `AllowedEffects`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "effect `{Self}` is not declared in this handler's `AllowedEffects` set",
    label = "`{Self}` cannot be performed by this handler",
    note = "add `{Self}` to the handler's `AllowedEffects`, or remove this call"
)]
pub trait AllowedEffectsAllowEffect<AllowedEffects, At> {}

#[diagnostic::do_not_recommend]
impl<E, AllowedEffects, At> AllowedEffectsAllowEffect<AllowedEffects, At> for E
where
    E: Effect,
    AllowedEffects: EffectSet,
    AllowedEffects::Members: Member<E, At>,
{
}

/// Diagnostic facade proving that every fact in an effect outcome belongs to
/// the handler's declared output set.
///
/// The underlying bidirectional stage-contract checks remain elsewhere; this
/// facade names the one-way containment rule enforced at `perform`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "effect `{Self}` has outcome facts outside this handler's `Output` set",
    label = "this effect's outcome does not fit the handler's declared output contract",
    note = "add the intended outcome facts to the canonical stage arrow, then mirror the \
            arrow's flat fact set in the handler's `Output`, or perform an effect whose outcome \
            already fits (FLOWIP-120z)"
)]
pub trait EffectOutcomeFitsOutput<Output, Proof> {}

#[diagnostic::do_not_recommend]
impl<E, Output, Proof> EffectOutcomeFitsOutput<Output, Proof> for E
where
    E: Effect,
    Output: StageFactSet,
    <<E::OutcomeSemantics as super::EffectOutcomeSemantics<E::Outcome>>::PublicFacts as StageFactSet>::Members:
        SubsetOf<Output::Members, Proof>,
{
}

/// Replay-safe effect context restricted to one handler's declared output
/// facts and effect capabilities.
pub struct Effects<Output: StageFactSet, AllowedEffects: EffectSet> {
    core: EffectsCore,
    _capabilities: PhantomData<fn() -> (Output, AllowedEffects)>,
}

impl<Output, AllowedEffects> Effects<Output, AllowedEffects>
where
    Output: StageFactSet,
    AllowedEffects: EffectSet,
{
    pub(crate) fn new(ctx: EffectInvocationContext) -> Self {
        assert_distinct_stage_fact_set::<Output>();
        assert_distinct_effect_set::<AllowedEffects>();
        Self {
            core: EffectsCore::new(ctx),
            _capabilities: PhantomData,
        }
    }

    /// Whether this invocation is reconstructing recorded effect outcomes.
    #[must_use]
    pub fn is_replaying(&self) -> bool {
        self.core.is_replaying()
    }

    /// Runtime-owned integration SPI for an exact declaration-scoped projection.
    #[doc(hidden)]
    pub fn project_named_effect<E, At>(&mut self) -> Result<EffectBindingUse<E>, EffectError>
    where
        E: NamedEffect + AllowedEffectsAllowEffect<AllowedEffects, At>,
    {
        self.core.project_named_effect::<E>()
    }

    #[doc(hidden)]
    pub fn configured_effect_for_next_cursor<E, At, R>(
        &mut self,
        build: impl FnMut(EffectBindingUse<E>) -> Result<E, R>,
    ) -> Result<Result<E, R>, EffectError>
    where
        E: ConfiguredEffect + AllowedEffectsAllowEffect<AllowedEffects, At>,
    {
        self.core.configured_effect_for_next_cursor::<E, R>(build)
    }

    /// Author one flat output fact declared by `Output`.
    pub async fn emit<T, At>(&mut self, fact: T) -> Result<(), EffectError>
    where
        T: TypedPayload + OutputAllowsFact<Output, At>,
    {
        self.core.emit(fact).await
    }

    pub(crate) async fn emit_carrier<C>(&mut self, carrier: C) -> Result<(), EffectError>
    where
        C: obzenflow_core::event::schema::TypedFactSet,
    {
        let facts = carrier
            .into_facts()
            .map_err(|error| EffectError::Serialization(error.to_string()))?;
        self.core.emit_typed_facts(facts).await
    }

    /// Runtime-owned integration SPI that performs one declared first-party
    /// effect whose complete outcome fact set is a subset of this handler's
    /// output set.
    #[doc(hidden)]
    pub async fn perform<E, EffectAt, OutcomeProof>(
        &mut self,
        effect: E,
    ) -> Result<E::Outcome, EffectError>
    where
        E: Effect
            + AllowedEffectsAllowEffect<AllowedEffects, EffectAt>
            + EffectOutcomeFitsOutput<Output, OutcomeProof>,
    {
        // `EffectsCore::perform` contains the replay, repeatable,
        // transactional, and affine state machines. Keep that combined
        // monomorphised future off the handler's task stack: generated affine
        // support must not enlarge an ordinary transactional handler future
        // enough to overflow the executor's default worker stack.
        Box::pin(self.core.perform(effect)).await
    }

    /// Perform one request through the system the handler holds for its port.
    ///
    /// ```
    /// # use async_trait::async_trait;
    /// # use obzenflow_core::TypedPayload;
    /// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
    /// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
    /// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
    /// # struct Order;
    /// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "docs.call.order"; }
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
    /// # struct Charged;
    /// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "docs.call.charged"; }
    /// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
    /// #[derive(Clone, Debug, Serialize)]
    /// struct ChargeCardRequest;
    ///
    /// #[async_trait]
    /// impl CallRequest for ChargeCardRequest {
    ///     const EFFECT_TYPE: &'static str = "docs.call.charge_card";
    ///     type Safety = Idempotent;
    ///     type Port = dyn CardPort;
    ///     type Outcome = Charged;
    ///
    ///     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> {
    ///         card.charge()
    ///     }
    /// }
    ///
    /// type ChargeCard = Call<ChargeCardRequest>;
    ///
    /// #[derive(Clone, Debug, EffectHandler)]
    /// # #[effect_handler(effects = obzenflow_runtime::effects)]
    /// struct Checkout {
    ///     #[system]
    ///     card: System<dyn CardPort>,
    /// }
    ///
    /// #[async_trait]
    /// impl EffectfulTransformHandler for Checkout {
    ///     type Input = Order;
    ///     type Output = obzenflow_core::stage_fact_set![Charged];
    ///     type AllowedEffects = obzenflow_runtime::effect_set![ChargeCard];
    ///
    ///     async fn process(
    ///         &self,
    ///         _: Order,
    ///         fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ///     ) -> Result<StageCompletion<Self::Output>, HandlerError> {
    ///         fx.call(ChargeCardRequest).await?;
    ///         Ok(fx.complete()?)
    ///     }
    /// }
    /// ```
    ///
    /// A request the handler's effect set does not declare does not compile:
    ///
    /// ```compile_fail,E0277
    /// # use async_trait::async_trait;
    /// # use obzenflow_core::TypedPayload;
    /// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
    /// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
    /// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
    /// # struct Order;
    /// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "docs.call.order"; }
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
    /// # struct Charged;
    /// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "docs.call.charged"; }
    /// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
    /// # #[derive(Clone, Debug, Serialize)]
    /// # struct ChargeCardRequest;
    /// # #[async_trait]
    /// # impl CallRequest for ChargeCardRequest {
    /// #     const EFFECT_TYPE: &'static str = "docs.call.charge_card";
    /// #     type Safety = Idempotent;
    /// #     type Port = dyn CardPort;
    /// #     type Outcome = Charged;
    /// #     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> { card.charge() }
    /// # }
    /// # #[derive(Clone, Debug, EffectHandler)]
    /// # #[effect_handler(effects = obzenflow_runtime::effects)]
    /// # struct Checkout { card: System<dyn CardPort> }
    /// #[async_trait]
    /// impl EffectfulTransformHandler for Checkout {
    ///     type Input = Order;
    ///     type Output = obzenflow_core::stage_fact_set![Charged];
    ///     type AllowedEffects = obzenflow_runtime::effect_set![];
    ///
    ///     async fn process(
    ///         &self,
    ///         _: Order,
    ///         fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ///     ) -> Result<StageCompletion<Self::Output>, HandlerError> {
    ///         fx.call(ChargeCardRequest).await?;
    ///         Ok(fx.complete()?)
    ///     }
    /// }
    /// ```
    ///
    /// Neither does a request whose outcome is outside the handler's output set:
    ///
    /// ```compile_fail,E0277
    /// # use async_trait::async_trait;
    /// # use obzenflow_core::TypedPayload;
    /// # use obzenflow_runtime::effects::{Call, CallRequest, EffectContext, EffectError, EffectHandler, Effects, Idempotent, StageCompletion, System};
    /// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
    /// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
    /// # struct Order;
    /// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "docs.call.order"; }
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
    /// # struct Charged;
    /// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "docs.call.charged"; }
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
    /// # struct Shipped;
    /// # impl TypedPayload for Shipped { const EVENT_TYPE: &'static str = "docs.call.shipped"; }
    /// # trait CardPort: Send + Sync { fn charge(&self) -> Result<Charged, EffectError>; }
    /// # #[derive(Clone, Debug, Serialize)]
    /// # struct ChargeCardRequest;
    /// # #[async_trait]
    /// # impl CallRequest for ChargeCardRequest {
    /// #     const EFFECT_TYPE: &'static str = "docs.call.charge_card";
    /// #     type Safety = Idempotent;
    /// #     type Port = dyn CardPort;
    /// #     type Outcome = Charged;
    /// #     async fn call(&self, card: &dyn CardPort, _: &mut EffectContext) -> Result<Charged, EffectError> { card.charge() }
    /// # }
    /// # type ChargeCard = Call<ChargeCardRequest>;
    /// # #[derive(Clone, Debug, EffectHandler)]
    /// # #[effect_handler(effects = obzenflow_runtime::effects)]
    /// # struct Checkout { #[system] card: System<dyn CardPort> }
    /// #[async_trait]
    /// impl EffectfulTransformHandler for Checkout {
    ///     type Input = Order;
    ///     type Output = obzenflow_core::stage_fact_set![Shipped];
    ///     type AllowedEffects = obzenflow_runtime::effect_set![ChargeCard];
    ///
    ///     async fn process(
    ///         &self,
    ///         _: Order,
    ///         fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ///     ) -> Result<StageCompletion<Self::Output>, HandlerError> {
    ///         fx.call(ChargeCardRequest).await?;
    ///         Ok(fx.complete()?)
    ///     }
    /// }
    /// ```
    ///
    /// A value that is not a request does not compile:
    ///
    /// ```compile_fail,E0277
    /// # use async_trait::async_trait;
    /// # use obzenflow_core::TypedPayload;
    /// # use obzenflow_runtime::effects::{EffectHandler, Effects, StageCompletion};
    /// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
    /// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
    /// # use serde::{Deserialize, Serialize};
    /// # #[derive(Clone, Debug, Serialize, Deserialize)]
    /// # struct Order;
    /// # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "docs.call.order"; }
    /// # #[derive(Clone, Debug, EffectHandler)]
    /// # #[effect_handler(effects = obzenflow_runtime::effects)]
    /// # struct Stamper;
    /// #[async_trait]
    /// impl EffectfulTransformHandler for Stamper {
    ///     type Input = Order;
    ///     type Output = Order;
    ///     type AllowedEffects = obzenflow_runtime::effect_set![];
    ///
    ///     async fn process(
    ///         &self,
    ///         order: Order,
    ///         fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ///     ) -> Result<StageCompletion<Self::Output>, HandlerError> {
    ///         fx.call(order).await?;
    ///         Ok(fx.complete()?)
    ///     }
    /// }
    /// ```
    pub async fn call<R, At, Proof>(&mut self, request: R) -> Result<R::Outcome, EffectError>
    where
        R: CallRequest,
        Call<R>:
            AllowedEffectsAllowEffect<AllowedEffects, At> + EffectOutcomeFitsOutput<Output, Proof>,
    {
        let projection = self.core.project_named_effect::<Call<R>>()?;
        Box::pin(self.core.perform(Call::new(request, projection))).await
    }

    /// Capture deterministic ambient data. Capture records are framework
    /// evidence, not stage output facts, and therefore need no output member.
    pub async fn capture<T>(&mut self, label: &'static str, value: T) -> Result<T, EffectError>
    where
        T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
    {
        self.core.capture(label, value).await
    }

    /// Complete an invocation that authored at least one durable output fact.
    pub fn complete(&self) -> Result<StageCompletion<Output>, EffectError> {
        self.core.ensure_authoring_open()?;
        let (committed, event_types) = self.core.committed_fact_evidence();
        if committed == 0 {
            return Err(EffectError::CompletedWithoutOutput {
                stage_key: self.core.stage_key().to_string(),
            });
        }
        Ok(StageCompletion::new(committed, event_types))
    }

    /// Complete a deliberately output-free invocation.
    pub fn complete_empty(&self) -> Result<StageCompletion<Output>, EffectError> {
        self.core.ensure_authoring_open()?;
        let (committed, event_types) = self.core.committed_fact_evidence();
        if committed != 0 {
            return Err(EffectError::CompletedEmptyWithOutput {
                stage_key: self.core.stage_key().to_string(),
                committed,
            });
        }
        Ok(StageCompletion::new(committed, event_types))
    }

    /// Complete with one deterministic fact before the first effect.
    ///
    /// This operation keeps the history preflight, resume-to-live admission,
    /// emission, and completion ordering indivisible at the authoring
    /// surface. It is useful when request preparation fails before an effect
    /// exists, and prevents such a fact from replacing archived or in-doubt
    /// effect history.
    #[doc(hidden)]
    pub async fn complete_with_pre_effect_fact<T, At>(
        &mut self,
        fact: T,
    ) -> Result<StageCompletion<Output>, EffectError>
    where
        T: TypedPayload + OutputAllowsFact<Output, At>,
    {
        self.core.preflight_next_effect_cursor_is_empty().await?;
        self.core.emit_pre_effect(fact).await?;
        self.complete()
    }

    pub(crate) fn drain_committed_facts(&mut self) -> Vec<ChainEvent> {
        self.core.drain_committed_facts()
    }

    pub(crate) fn authoring_halt_fatal(
        &self,
    ) -> Option<crate::stages::common::handler_error::StageFatal> {
        self.core.authoring_halt_fatal()
    }

    pub(crate) async fn end_of_invocation_fatal(
        &mut self,
    ) -> Result<Option<crate::stages::common::handler_error::StageFatal>, EffectError> {
        if let Err(error) = self.core.settle_without_action().await {
            return match self.core.authoring_halt_fatal() {
                Some(fatal) => Ok(Some(fatal)),
                None => Err(error),
            };
        }
        Ok(self.core.settlement_divergence().await)
    }
}

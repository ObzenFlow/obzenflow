// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Compiler-checked effectful stage authoring facade (FLOWIP-120z).

use super::{
    assert_distinct_effect_set, Effect, EffectError, EffectInvocationContext, EffectSet,
    EffectsCore, StageCompletion,
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
/// keeping effect permission errors in the vocabulary of `effects:` and
/// `AllowedEffects`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "effect `{Self}` is not declared in this handler's `AllowedEffects` set",
    label = "`{Self}` cannot be performed by this handler",
    note = "add `{Self}` to the canonical stage `effects:` clause (using `transactional(...)` or \
            `with [...]` when required), then mirror its effect type in the handler's \
            `AllowedEffects`, or remove this `perform` call (FLOWIP-120z)"
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
    <<E as Effect>::Outcome as StageFactSet>::Members: SubsetOf<Output::Members, Proof>,
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

    /// Author one flat output fact declared by `Output`.
    pub async fn emit<T, At>(&mut self, fact: T) -> Result<(), EffectError>
    where
        T: TypedPayload + OutputAllowsFact<Output, At>,
    {
        self.core.emit(fact).await
    }

    /// Perform one declared effect whose complete outcome fact set is a
    /// subset of this handler's output set.
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
        let (committed, event_types) = self.core.committed_fact_evidence();
        if committed != 0 {
            return Err(EffectError::CompletedEmptyWithOutput {
                stage_key: self.core.stage_key().to_string(),
                committed,
            });
        }
        Ok(StageCompletion::new(committed, event_types))
    }

    pub(crate) fn drain_committed_facts(&mut self) -> Vec<ChainEvent> {
        self.core.drain_committed_facts()
    }

    /// Sealed generated-adapter access to the parent composite identity.
    #[doc(hidden)]
    pub fn __generated_parent_composite_activations(
        &self,
    ) -> Vec<obzenflow_core::event::context::CompositeActivationContext> {
        self.core.parent_composite_activations()
    }

    /// Read-only preflight used before a generated pre-effect domain terminal.
    #[doc(hidden)]
    pub async fn __generated_preflight_first_effect_is_empty(&self) -> Result<(), EffectError> {
        self.core.preflight_next_effect_cursor_is_empty().await
    }

    /// Yield at the generated resume-to-live admission barrier.
    #[doc(hidden)]
    pub async fn __generated_request_live_admission(&self) -> Result<(), EffectError> {
        self.core.request_generated_live_admission().await
    }
}

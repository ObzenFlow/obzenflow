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
        T: TypedPayload,
        Output::Members: Member<T, At>,
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
        E: Effect,
        AllowedEffects::Members: Member<E, EffectAt>,
        <E::Outcome as StageFactSet>::Members: SubsetOf<Output::Members, OutcomeProof>,
    {
        self.core.perform(effect).await
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
}

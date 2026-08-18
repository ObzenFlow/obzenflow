// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{
    Effect, EffectBinding, EffectBindingEvidence, EffectBindingUse, EffectContext, EffectError,
    EffectPortSlotSet, EffectRegistrationBuilder, EffectSafety, Effects,
    LogicalEffectBindingName, Named, NamedEffect, StageCompletion,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulStatefulHandler, EffectfulTransformHandler,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Input;
impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.effectful.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct First;
impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.effectful.first";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Second;
impl TypedPayload for Second {
    const EVENT_TYPE: &'static str = "compile_fail.effectful.second";
}

#[derive(Clone, Debug)]
pub struct FirstEffect;

#[async_trait]
impl Effect for FirstEffect {
    const EFFECT_TYPE: &'static str = "compile_fail.effectful.first_effect";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = obzenflow_runtime::effects::Portless;
    type Outcome = First;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

    fn label(&self) -> &str {
        "first"
    }

    fn canonical_input(&self) -> serde_json::Value {
        serde_json::Value::Null
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<First, EffectError> {
        Ok(First)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZeroSlotEvidence(pub &'static str);

impl EffectBindingEvidence for ZeroSlotEvidence {
    const SCHEMA_VERSION: u32 = 1;

    fn canonical_bytes(&self) -> obzenflow_core::BoundedBindingEvidence {
        obzenflow_core::BoundedBindingEvidence::try_new(self.0.as_bytes().to_vec()).unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct ZeroSlotNamedEffect {
    binding: EffectBindingUse<Self>,
}

#[async_trait]
impl Effect for ZeroSlotNamedEffect {
    const EFFECT_TYPE: &'static str = "compile_fail.effectful.zero_slot_named";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = Named<ZeroSlotEvidence>;
    type Outcome = First;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

    fn label(&self) -> &str {
        "zero_slot"
    }

    fn canonical_input(&self) -> serde_json::Value {
        serde_json::Value::Null
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<First, EffectError> {
        Ok(First)
    }
}

impl NamedEffect for ZeroSlotNamedEffect {
    type BindingEvidence = ZeroSlotEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::new()
    }
}

#[derive(Clone, Debug)]
pub struct OtherZeroSlotNamedEffect {
    binding: EffectBindingUse<Self>,
}

#[async_trait]
impl Effect for OtherZeroSlotNamedEffect {
    const EFFECT_TYPE: &'static str = "compile_fail.effectful.other_zero_slot_named";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::Idempotent;
    type BindingMode = Named<ZeroSlotEvidence>;
    type Outcome = First;
    type OutcomeSemantics = obzenflow_runtime::effects::DomainFacts;

    fn label(&self) -> &str {
        "other_zero_slot"
    }

    fn canonical_input(&self) -> serde_json::Value {
        serde_json::Value::Null
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<First, EffectError> {
        Ok(First)
    }
}

impl NamedEffect for OtherZeroSlotNamedEffect {
    type BindingEvidence = ZeroSlotEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::new()
    }
}

pub fn zero_slot_binding() -> EffectBinding<ZeroSlotNamedEffect> {
    EffectRegistrationBuilder::<ZeroSlotNamedEffect>::new(
        LogicalEffectBindingName::new("zero_slot").unwrap(),
        ZeroSlotEvidence("zero"),
    )
    .finish()
    .unwrap()
}

pub fn other_zero_slot_binding() -> EffectBinding<OtherZeroSlotNamedEffect> {
    EffectRegistrationBuilder::<OtherZeroSlotNamedEffect>::new(
        LogicalEffectBindingName::new("other_zero_slot").unwrap(),
        ZeroSlotEvidence("other"),
    )
    .finish()
    .unwrap()
}

#[derive(Clone, Debug)]
pub struct FirstOnly;

#[async_trait]
impl EffectfulTransformHandler for FirstOnly {
    type Input = Input;
    type Output = First;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    async fn process(
        &self,
        _input: Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.emit(First).await?;
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug)]
pub struct FirstAndSecond;

#[async_trait]
impl EffectfulTransformHandler for FirstAndSecond {
    type Input = Input;
    type Output = obzenflow_core::stage_fact_set![First, Second];
    type AllowedEffects = obzenflow_runtime::effect_set![];

    async fn process(
        &self,
        _input: Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.emit(First).await?;
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug)]
pub struct AllowsFirstEffect;

#[async_trait]
impl EffectfulTransformHandler for AllowsFirstEffect {
    type Input = Input;
    type Output = First;
    type AllowedEffects = obzenflow_runtime::effect_set![FirstEffect];

    async fn process(
        &self,
        _input: Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.emit(First).await?;
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug)]
pub struct AllowsZeroSlotNamedEffect;

#[async_trait]
impl EffectfulTransformHandler for AllowsZeroSlotNamedEffect {
    type Input = Input;
    type Output = First;
    type AllowedEffects = obzenflow_runtime::effect_set![ZeroSlotNamedEffect];

    async fn process(
        &self,
        _input: Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.emit(First).await?;
        Ok(fx.complete()?)
    }
}

#[derive(Clone, Debug)]
pub struct StatefulFirstOnly;

#[async_trait]
impl EffectfulStatefulHandler for StatefulFirstOnly {
    type State = ();
    type Input = Input;
    type Output = First;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    fn initial_state(&self) -> Self::State {}

    async fn decide(
        &mut self,
        _state: &Self::State,
        _input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete_empty()?)
    }

    fn apply(
        &mut self,
        _state: &mut Self::State,
        _fact: Self::Output,
    ) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug, obzenflow_core::StageOutputFacts)]
pub enum StatefulFirstAndSecondOutput {
    First(First),
    Second(Second),
}

#[derive(Clone, Debug)]
pub struct StatefulFirstAndSecond;

#[async_trait]
impl EffectfulStatefulHandler for StatefulFirstAndSecond {
    type State = ();
    type Input = Input;
    type Output = StatefulFirstAndSecondOutput;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    fn initial_state(&self) -> Self::State {}

    async fn decide(
        &mut self,
        _state: &Self::State,
        _input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete_empty()?)
    }

    fn apply(
        &mut self,
        _state: &mut Self::State,
        _fact: Self::Output,
    ) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct StatefulAllowsFirstEffect;

#[async_trait]
impl EffectfulStatefulHandler for StatefulAllowsFirstEffect {
    type State = ();
    type Input = Input;
    type Output = First;
    type AllowedEffects = obzenflow_runtime::effect_set![FirstEffect];

    fn initial_state(&self) -> Self::State {}

    async fn decide(
        &mut self,
        _state: &Self::State,
        _input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete_empty()?)
    }

    fn apply(
        &mut self,
        _state: &mut Self::State,
        _fact: Self::Output,
    ) -> Result<(), HandlerError> {
        Ok(())
    }
}

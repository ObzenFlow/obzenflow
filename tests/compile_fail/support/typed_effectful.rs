use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, StageCompletion,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
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
    type Outcome = First;

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

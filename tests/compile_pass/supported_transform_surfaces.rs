// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, TypedTransformHandler,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input;

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_pass.transform.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Output;

impl TypedPayload for Output {
    const EVENT_TYPE: &'static str = "compile_pass.transform.output";
}

#[derive(Clone, Debug)]
struct PureTransform;

impl TypedTransformHandler for PureTransform {
    type Input = Input;
    type Output = Output;

    fn process(&self, _input: Input) -> Result<Output, HandlerError> {
        Ok(Output)
    }
}

#[derive(Clone, Debug)]
struct ExternalWorkTransform;

#[async_trait]
impl EffectfulTransformHandler for ExternalWorkTransform {
    type Input = Input;
    type Output = Output;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    async fn process(
        &self,
        _input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.emit(Output)
            .await
            .map_err(|error| HandlerError::Other(error.to_string()))?;
        Ok(fx.complete()?)
    }
}

fn main() {
    let _ = obzenflow_dsl::transform!(Input -> Output => PureTransform);
    let _ = obzenflow_dsl::effectful_transform!(
        Input -> Output => ExternalWorkTransform,
        effects: [],
        middleware: []
    );
}

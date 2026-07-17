// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::{StageOutputFacts, TypedPayload};
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulStatefulHandler;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input;
impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.stateful.product.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct First;
impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.stateful.product.first";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Second;
impl TypedPayload for Second {
    const EVENT_TYPE: &'static str = "compile_fail.stateful.product.second";
}

#[derive(Clone, Debug, StageOutputFacts)]
struct Product {
    first: First,
    second: Second,
}

#[derive(Clone, Debug)]
struct Handler;

#[async_trait]
impl EffectfulStatefulHandler for Handler {
    type State = ();
    type Input = Input;
    type Output = Product;
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

fn main() {}

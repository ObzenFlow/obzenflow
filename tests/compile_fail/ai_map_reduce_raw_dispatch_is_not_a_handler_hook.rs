// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::ChainEvent;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;

#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, Input};

#[derive(Clone, Debug)]
struct CounterfeitRawDispatch;

#[async_trait]
impl EffectfulTransformHandler for CounterfeitRawDispatch {
    type Input = Input;
    type Output = First;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    async fn process(
        &self,
        _input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        Ok(fx.complete_empty()?)
    }

    async fn __generated_raw_dispatch(
        &self,
        _event: ChainEvent,
        _fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Option<Result<Vec<ChainEvent>, HandlerError>> {
        Some(Ok(Vec::new()))
    }
}

fn main() {}

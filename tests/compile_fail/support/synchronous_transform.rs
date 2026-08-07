// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::{ChainEvent, StageOutputs, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    TransformHandler, TypedTransformHandler,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Input;

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.sync.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtherInput;

impl TypedPayload for OtherInput {
    const EVENT_TYPE: &'static str = "compile_fail.sync.other_input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct First;

impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.sync.first";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Second;

impl TypedPayload for Second {
    const EVENT_TYPE: &'static str = "compile_fail.sync.second";
}

#[derive(Clone, Debug)]
pub struct RawHandler;

#[async_trait]
impl TransformHandler for RawHandler {
    fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct WrongInputHandler;

impl TypedTransformHandler for WrongInputHandler {
    type Input = OtherInput;
    type Output = First;

    fn process(&self, _input: OtherInput) -> Result<First, HandlerError> {
        Ok(First)
    }
}

#[derive(Clone, Debug)]
pub struct EmitsUndeclaredDynamicMember;

impl TypedTransformHandler for EmitsUndeclaredDynamicMember {
    type Input = Input;
    type Output = StageOutputs<Second>;

    fn process(&self, _input: Input) -> Result<Self::Output, HandlerError> {
        Ok(StageOutputs::one(Second))
    }
}

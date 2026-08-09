// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::stateful::traits::StatefulHandler;
use obzenflow_runtime::stages::{StatefulEmission, TypedStatefulHandler};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Input;

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.stateful.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OtherInput;

impl TypedPayload for OtherInput {
    const EVENT_TYPE: &'static str = "compile_fail.stateful.other_input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct First;

impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.stateful.first";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Second;

impl TypedPayload for Second {
    const EVENT_TYPE: &'static str = "compile_fail.stateful.second";
}

#[derive(Clone, Debug)]
pub struct RawHandler;

#[async_trait]
impl StatefulHandler for RawHandler {
    type State = ();

    fn accumulate(&mut self, _state: &mut Self::State, _event: ChainEvent) {}

    fn initial_state(&self) -> Self::State {}

    fn create_events(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug)]
pub struct WrongInputHandler;

impl TypedStatefulHandler for WrongInputHandler {
    type State = ();
    type Input = OtherInput;
    type Output = First;

    fn initial_state(&self) -> Self::State {}

    fn accumulate(&self, _state: &mut Self::State, _input: Self::Input) {}

    fn emit(
        &self,
        _state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: (),
            outputs: vec![First],
        })
    }
}

#[derive(Clone, Debug)]
pub struct FirstOnly;

impl TypedStatefulHandler for FirstOnly {
    type State = ();
    type Input = Input;
    type Output = First;

    fn initial_state(&self) -> Self::State {}

    fn accumulate(&self, _state: &mut Self::State, _input: Self::Input) {}

    fn emit(
        &self,
        _state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: (),
            outputs: vec![First],
        })
    }
}

#[derive(Clone, Debug, obzenflow_core::StageOutputFacts)]
pub enum FirstOrSecond {
    First(First),
    Second(Second),
}

#[derive(Clone, Debug)]
pub struct FirstAndSecond;

impl TypedStatefulHandler for FirstAndSecond {
    type State = ();
    type Input = Input;
    type Output = FirstOrSecond;

    fn initial_state(&self) -> Self::State {}

    fn accumulate(&self, _state: &mut Self::State, _input: Self::Input) {}

    fn emit(
        &self,
        _state: &Self::State,
    ) -> Result<StatefulEmission<Self::State, Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            next_state: (),
            outputs: vec![FirstOrSecond::First(First)],
        })
    }
}

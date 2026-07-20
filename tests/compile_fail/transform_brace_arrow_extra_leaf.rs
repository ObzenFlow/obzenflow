// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::{StageOutputFacts, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::TypedTransformHandler;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input;
impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.transform.extra.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct First;
impl TypedPayload for First {
    const EVENT_TYPE: &'static str = "compile_fail.transform.extra.first";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Extra;
impl TypedPayload for Extra {
    const EVENT_TYPE: &'static str = "compile_fail.transform.extra.second";
}

#[derive(Clone, Debug, StageOutputFacts)]
enum Output {
    First(First),
    Extra(Extra),
}

#[derive(Clone, Debug)]
struct Handler;
impl TypedTransformHandler for Handler {
    type Input = Input;
    type Output = Output;

    fn process(&self, _input: Input) -> Result<Self::Output, HandlerError> {
        Ok(Output::First(First))
    }
}

fn main() {
    let _ = obzenflow_dsl::transform!(Input -> { First } => Handler);
}

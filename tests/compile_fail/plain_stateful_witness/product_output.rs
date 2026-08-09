// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/plain_stateful.rs"]
mod support;
use obzenflow_runtime::stages::{StatefulEmission, TypedStatefulHandler};
use support::{First, Input, Second};

#[derive(Clone, Debug, obzenflow_core::StageOutputFacts)]
struct Product {
    first: First,
    second: Second,
}

#[derive(Clone, Debug)]
struct ProductHandler;

impl TypedStatefulHandler for ProductHandler {
    type State = ();
    type Input = Input;
    type Output = Product;

    fn initial_state(&self) -> Self::State {}

    fn accumulate(&self, _state: &mut Self::State, _input: Self::Input) {}

    fn emit(
        &self,
        _state: &Self::State,
    ) -> Result<
        StatefulEmission<Self::State, Self::Output>,
        obzenflow_runtime::stages::common::handler_error::HandlerError,
    > {
        unreachable!()
    }
}

fn main() {
    let _ = obzenflow_dsl::stateful!(Input -> { First, Second } => ProductHandler);
}

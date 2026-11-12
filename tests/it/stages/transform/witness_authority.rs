// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::TypedPayload;
use obzenflow_dsl::dsl::typing::{wrap_typed_descriptor, StageTypingMetadata, TypeHint};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::TypedTransformHandler;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input;

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "flowip_134b.authority.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Output;

impl TypedPayload for Output {
    const EVENT_TYPE: &'static str = "flowip_134b.authority.output";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CounterfeitInput;

impl TypedPayload for CounterfeitInput {
    const EVENT_TYPE: &'static str = "flowip_134b.authority.counterfeit_input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CounterfeitOutput;

impl TypedPayload for CounterfeitOutput {
    const EVENT_TYPE: &'static str = "flowip_134b.authority.counterfeit_output";
}

#[derive(Clone, Debug)]
struct ExactTransform;

impl TypedTransformHandler for ExactTransform {
    type Input = Input;
    type Output = Output;

    fn process(&self, _input: Input) -> Result<Output, HandlerError> {
        Ok(Output)
    }
}

#[test]
fn canonical_transform_metadata_cannot_be_replaced_by_downstream_rewrapping() {
    let descriptor = obzenflow_dsl::transform!(Input -> Output => ExactTransform);
    let canonical = descriptor
        .typing_metadata()
        .expect("typed transform must expose canonical metadata")
        .clone();
    let counterfeit = StageTypingMetadata::transform(
        TypeHint::exact_payload::<CounterfeitInput>(),
        TypeHint::exact_payload::<CounterfeitOutput>(),
        false,
        None,
    );

    let rewrapped = wrap_typed_descriptor(descriptor, counterfeit);

    assert_eq!(rewrapped.typing_metadata(), Some(&canonical));
}

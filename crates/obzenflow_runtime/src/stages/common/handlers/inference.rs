// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Scalar AI inference handler contract.

use crate::stages::common::handler_error::HandlerError;
use obzenflow_core::ai::{ChatCompletionReply, ChatRequestSpec};
use obzenflow_core::TypedPayload;

/// User-owned handler for one replay-safe scalar inference stage.
///
/// The runtime-facing adapter invokes `prepare`, performs the stage's declared
/// chat effect, and then invokes `interpret` with the exact retained request and
/// recorded reply. Implementations contain domain logic only; binding choice,
/// effect execution, replay, emission, and settlement remain framework-owned.
#[diagnostic::on_unimplemented(
    message = "`{Self}` does not satisfy `InferenceHandler` for this stage",
    label = "this value is not a scalar inference handler",
    note = "implement `InferenceHandler` with `Input`, `Output`, `prepare`, and `interpret`; the associated types must match the inference! arrow"
)]
pub trait InferenceHandler: Send + Sync {
    type Input: TypedPayload + Send + Sync + 'static;
    type Output: TypedPayload + Send + Sync + 'static;

    /// Build the target-free request for one input.
    fn prepare(&self, input: &Self::Input) -> Result<ChatRequestSpec, HandlerError>;

    /// Interpret the recorded reply using the original input and exact request.
    fn interpret(
        &self,
        input: Self::Input,
        request: ChatRequestSpec,
        reply: ChatCompletionReply,
    ) -> Result<Self::Output, HandlerError>;

    /// Durable logic identity used by replay verification.
    fn stage_logic_version(&self) -> &str {
        "1"
    }
}

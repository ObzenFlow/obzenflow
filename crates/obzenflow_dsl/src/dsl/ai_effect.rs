// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared generated-chat invocation seam for the AI authoring facades.

use obzenflow_adapters::ai::{ChatCompletion, ChatCompletionBuildError};
use obzenflow_core::ai::{ChatBindingContract, ChatCompletionReply, ChatRequestSpec};
use obzenflow_core::StageFactSet;
use obzenflow_runtime::effects::{
    AllowedEffectsAllowEffect, EffectOutcomeFitsOutput, EffectSet, Effects,
};

#[derive(Debug)]
pub(crate) enum GeneratedChatInvocationError {
    Build(ChatCompletionBuildError),
    Effect(obzenflow_runtime::effects::EffectError),
}

/// Macro type-checking seam for the lexical `via` operand.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "AI effect-row `via` binding is not a ChatBindingContract",
    label = "expected a ChatBindingContract here",
    note = "`via` selects the credential-free chat contract value; it is not a runtime port name"
)]
pub trait ChatBindingExpression {
    fn clone_chat_contract(&self) -> ChatBindingContract;
}

impl ChatBindingExpression for ChatBindingContract {
    fn clone_chat_contract(&self) -> ChatBindingContract {
        self.clone()
    }
}

#[doc(hidden)]
pub fn clone_chat_contract<Binding>(binding: &Binding) -> ChatBindingContract
where
    Binding: ChatBindingExpression + ?Sized,
{
    binding.clone_chat_contract()
}

/// Bind the target-free spec, construct the one concrete effect, and cross
/// the existing effect runtime exactly once.
///
/// Scalar inference, map, and finalise deliberately share this function so
/// request identity and live-call authority cannot drift by workload shape.
pub(crate) async fn invoke_generated_chat<Output, AllowedEffects, EffectAt, OutcomeProof>(
    fx: &mut Effects<Output, AllowedEffects>,
    binding: &ChatBindingContract,
    request: &ChatRequestSpec,
    label: &'static str,
) -> Result<ChatCompletionReply, GeneratedChatInvocationError>
where
    Output: StageFactSet,
    AllowedEffects: EffectSet,
    ChatCompletion: AllowedEffectsAllowEffect<AllowedEffects, EffectAt>
        + EffectOutcomeFitsOutput<Output, OutcomeProof>,
{
    let bound_request = request.bind_target(binding.target());
    let effect = ChatCompletion::new(
        label,
        bound_request,
        binding.target().clone(),
        binding.estimator().clone(),
    )
    .map_err(GeneratedChatInvocationError::Build)?;
    fx.perform(effect)
        .await
        .map_err(GeneratedChatInvocationError::Effect)
}

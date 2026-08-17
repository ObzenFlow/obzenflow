// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared generated-chat invocation seam for the AI authoring facades.

use obzenflow_adapters::ai::{ChatCompletion, ChatCompletionBuildError};
use obzenflow_adapters::middleware::MiddlewareFactory;
use obzenflow_core::ai::{ChatCompletionReply, ChatRequestSpec};
use obzenflow_core::StageFactSet;
use obzenflow_runtime::effects::{
    AllowedEffectsAllowEffect, EffectBinding, EffectDeclaration, EffectOutcomeFitsOutput,
    EffectSet, Effects,
};

use super::stage_descriptor::EffectPolicyAttachment;

/// Canonical lowering product for one generated `ChatCompletion` capability declaration.
///
/// Exported only so authoring macros expanded in downstream crates can carry
/// the exact declaration and policy vectors produced by the shared effect-entry
/// lowerer into the generated descriptor constructors.
#[doc(hidden)]
pub struct GeneratedChatEffectRow {
    pub binding: EffectBinding<ChatCompletion>,
    pub declarations: Vec<EffectDeclaration>,
    pub policy_attachments: Vec<EffectPolicyAttachment>,
}

#[derive(Debug)]
pub(crate) enum GeneratedChatInvocationError {
    Build(ChatCompletionBuildError),
    Effect(obzenflow_runtime::effects::EffectError),
}

/// Macro type-checking seam for the lexical `via` operand.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "AI `uses`-clause `via` binding is not an EffectBinding<ChatCompletion>",
    label = "expected an EffectBinding<ChatCompletion> here",
    note = "`via` selects typed credential-free binding evidence; it is not a runtime port name"
)]
pub trait ChatBindingExpression {
    fn clone_chat_binding(&self) -> EffectBinding<ChatCompletion>;
}

impl ChatBindingExpression for EffectBinding<ChatCompletion> {
    fn clone_chat_binding(&self) -> EffectBinding<ChatCompletion> {
        self.clone()
    }
}

#[doc(hidden)]
pub fn clone_chat_binding<Binding>(binding: &Binding) -> EffectBinding<ChatCompletion>
where
    Binding: ChatBindingExpression + ?Sized,
{
    binding.clone_chat_binding()
}

/// Inference-specific type-checking seam whose marker preserves the lexical
/// binding name in the compiler diagnostic.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "inference!: binding `{BindingName}` is not an EffectBinding<ChatCompletion>",
    label = "expected an EffectBinding<ChatCompletion> here",
    note = "`via` selects typed credential-free binding evidence; it is not a runtime port name"
)]
pub trait InferenceChatBindingExpression<BindingName> {
    fn clone_inference_chat_binding(&self) -> EffectBinding<ChatCompletion>;
}

impl<BindingName> InferenceChatBindingExpression<BindingName> for EffectBinding<ChatCompletion> {
    fn clone_inference_chat_binding(&self) -> EffectBinding<ChatCompletion> {
        self.clone()
    }
}

#[doc(hidden)]
pub fn clone_inference_chat_binding<Binding, BindingName>(
    binding: &Binding,
) -> EffectBinding<ChatCompletion>
where
    Binding: InferenceChatBindingExpression<BindingName> + ?Sized,
{
    binding.clone_inference_chat_binding()
}

pub(crate) fn require_generated_chat_resilience<'a>(
    surface: &'static str,
    owner_kind: &'static str,
    owner: &str,
    policies: impl IntoIterator<Item = &'a dyn MiddlewareFactory>,
) -> Result<(), String> {
    let declarations = policies
        .into_iter()
        .map(MiddlewareFactory::declaration)
        .collect::<Vec<_>>();
    let resilience_count = declarations
        .iter()
        .filter(|declaration| declaration.is_effect_resilience())
        .count();
    if declarations.len() == 1 && resilience_count == 1 {
        return Ok(());
    }

    Err(format!(
        "{surface}: generated {owner_kind} '{owner}' requires exactly one EffectResilience \
         policy on 'ChatCompletion'; attach `with ai_resilience()` to its `uses` entry \
         (found {resilience_count} EffectResilience policies across {} attachments)",
        declarations.len()
    ))
}

/// Bind the target-free spec, construct the one concrete effect, and cross
/// the existing effect runtime exactly once.
///
/// Scalar inference, map, and finalise deliberately share this function so
/// request identity and live-call authority cannot drift by workload shape.
pub(crate) async fn invoke_generated_chat<Output, AllowedEffects, EffectAt, OutcomeProof>(
    fx: &mut Effects<Output, AllowedEffects>,
    binding: &EffectBinding<ChatCompletion>,
    request: &ChatRequestSpec,
    label: &'static str,
) -> Result<ChatCompletionReply, GeneratedChatInvocationError>
where
    Output: StageFactSet,
    AllowedEffects: EffectSet,
    ChatCompletion: AllowedEffectsAllowEffect<AllowedEffects, EffectAt>
        + EffectOutcomeFitsOutput<Output, OutcomeProof>,
{
    let bound_request = request.bind_target(binding.evidence().target());
    let effect = ChatCompletion::new(label, bound_request, binding.invocation())
        .map_err(GeneratedChatInvocationError::Build)?;
    fx.perform(effect)
        .await
        .map_err(GeneratedChatInvocationError::Effect)
}

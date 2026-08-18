// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Generated scalar AI inference leaf.

use super::ai_effect::GeneratedChatEffectRow;
use super::stage_descriptor::{EffectfulTransformDescriptor, StageDescriptor};
use super::typing::{wrap_typed_descriptor, StageTypingMetadata, TypeHint};
use async_trait::async_trait;
use obzenflow_adapters::ai::{effect_error_to_handler_error, ChatCompletion, ChatEffects};
use obzenflow_core::ai::AiInferenceRole;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::Arc;

pub(crate) const INFERENCE_CHAT_COMPLETION_LABEL: &str = "inference.chat_completion";

struct GeneratedInferenceHandler<Input, Out, Role> {
    role: Arc<Role>,
    _types: PhantomData<fn() -> (Input, Out)>,
}

impl<Input, Out, Role> Clone for GeneratedInferenceHandler<Input, Out, Role> {
    fn clone(&self) -> Self {
        Self {
            role: self.role.clone(),
            _types: PhantomData,
        }
    }
}

impl<Input, Out, Role> fmt::Debug for GeneratedInferenceHandler<Input, Out, Role> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeneratedInferenceHandler")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<Input, Out, Role> EffectfulTransformHandler for GeneratedInferenceHandler<Input, Out, Role>
where
    Input: TypedPayload + Clone + Send + Sync + 'static,
    Out: TypedPayload + Clone + Send + Sync + 'static,
    Role: AiInferenceRole<Input, Out>,
{
    type Input = Input;
    type Output = Out;
    type AllowedEffects = obzenflow_runtime::effect_set![ChatCompletion];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let request = self
            .role
            .prepare(&input)
            .map_err(|error| HandlerError::Domain(format!("{error:?}")))?;
        let reply = fx
            .chat_completion(INFERENCE_CHAT_COMPLETION_LABEL, request.clone())
            .await
            .map_err(HandlerError::from)?;
        let output = self
            .role
            .interpret(input, request, reply)
            .map_err(|error| HandlerError::Domain(format!("{error:?}")))?;
        fx.emit(output)
            .await
            .map_err(effect_error_to_handler_error)?;
        fx.complete().map_err(effect_error_to_handler_error)
    }

    fn stage_logic_version(&self) -> &str {
        Role::LOGIC_VERSION
    }
}

/// Macro-only constructor for the scalar AI leaf.
#[doc(hidden)]
pub fn generated_inference<Input, Out, Role>(
    name: impl Into<String>,
    role: Role,
    effect_row: GeneratedChatEffectRow,
) -> Box<dyn StageDescriptor>
where
    Input: TypedPayload + Clone + Send + Sync + 'static,
    Out: TypedPayload + Clone + Send + Sync + 'static,
    Role: AiInferenceRole<Input, Out>,
{
    let GeneratedChatEffectRow {
        binding: _,
        declarations,
        policy_attachments,
    } = effect_row;
    let handler = GeneratedInferenceHandler::<Input, Out, Role> {
        role: Arc::new(role),
        _types: PhantomData,
    };
    let direct_bound = NonZeroU64::MIN.saturating_add(2);
    let descriptor = EffectfulTransformDescriptor::generated_for_surface::<Input>(
        "inference!",
        "stage",
        name,
        handler,
        declarations,
        policy_attachments,
        direct_bound,
    );
    wrap_typed_descriptor(
        Box::new(descriptor),
        StageTypingMetadata::transform(
            TypeHint::exact_payload::<Input>(),
            TypeHint::exact_payload::<Out>(),
            false,
            None,
        ),
    )
}

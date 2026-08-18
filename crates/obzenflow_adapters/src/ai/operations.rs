// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Domain operations over declaration-scoped AI effect capabilities.

use super::{
    effect_error_to_handler_error, ChatCompletion, ChatCompletionBuildError, EmbeddingGeneration,
    EmbeddingGenerationBuildError,
};
use obzenflow_core::ai::{
    ChatCompletionReply, ChatRequestSpec, ChatTarget, EmbeddingGenerationReply,
    EmbeddingRequestSpec, EmbeddingTarget, ResolvedTokenEstimator,
};
use obzenflow_core::event::schema::SubsetProofEnd;
use obzenflow_core::StageFactSet;
use obzenflow_runtime::effects::{
    AllowedEffectsAllowEffect, EffectBinding, EffectError, EffectSet, Effects,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;

#[derive(Debug, thiserror::Error)]
pub enum ChatOperationError {
    #[error(transparent)]
    Build(#[from] ChatCompletionBuildError),
    #[error(transparent)]
    Effect(#[from] EffectError),
}

#[derive(Debug, thiserror::Error)]
pub enum EmbeddingOperationError {
    #[error(transparent)]
    Build(#[from] EmbeddingGenerationBuildError),
    #[error(transparent)]
    Effect(#[from] EffectError),
}

impl From<ChatOperationError> for HandlerError {
    fn from(error: ChatOperationError) -> Self {
        match error {
            ChatOperationError::Build(error) => HandlerError::Validation(error.to_string()),
            ChatOperationError::Effect(error) => effect_error_to_handler_error(error),
        }
    }
}

impl From<EmbeddingOperationError> for HandlerError {
    fn from(error: EmbeddingOperationError) -> Self {
        match error {
            EmbeddingOperationError::Build(error) => HandlerError::Validation(error.to_string()),
            EmbeddingOperationError::Effect(error) => effect_error_to_handler_error(error),
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait ChatEffects<AllowedEffects: EffectSet> {
    async fn chat_completion<At>(
        &mut self,
        label: &'static str,
        request: ChatRequestSpec,
    ) -> Result<ChatCompletionReply, ChatOperationError>
    where
        ChatCompletion: AllowedEffectsAllowEffect<AllowedEffects, At>;
}

impl<Output, AllowedEffects> ChatEffects<AllowedEffects> for Effects<Output, AllowedEffects>
where
    Output: StageFactSet,
    AllowedEffects: EffectSet,
{
    async fn chat_completion<At>(
        &mut self,
        label: &'static str,
        request: ChatRequestSpec,
    ) -> Result<ChatCompletionReply, ChatOperationError>
    where
        ChatCompletion: AllowedEffectsAllowEffect<AllowedEffects, At>,
    {
        let binding = self.project_named_effect::<ChatCompletion, At>()?;
        let request = request.bind_target(binding.evidence().target());
        let effect = ChatCompletion::new(label, request, binding)?;
        self.perform::<ChatCompletion, At, SubsetProofEnd>(effect)
            .await
            .map_err(ChatOperationError::Effect)
    }
}

#[allow(async_fn_in_trait)]
pub trait EmbeddingEffects<AllowedEffects: EffectSet> {
    async fn generate_embedding<At>(
        &mut self,
        label: &'static str,
        request: EmbeddingRequestSpec,
    ) -> Result<EmbeddingGenerationReply, EmbeddingOperationError>
    where
        EmbeddingGeneration: AllowedEffectsAllowEffect<AllowedEffects, At>;
}

impl<Output, AllowedEffects> EmbeddingEffects<AllowedEffects> for Effects<Output, AllowedEffects>
where
    Output: StageFactSet,
    AllowedEffects: EffectSet,
{
    async fn generate_embedding<At>(
        &mut self,
        label: &'static str,
        request: EmbeddingRequestSpec,
    ) -> Result<EmbeddingGenerationReply, EmbeddingOperationError>
    where
        EmbeddingGeneration: AllowedEffectsAllowEffect<AllowedEffects, At>,
    {
        let binding = self.project_named_effect::<EmbeddingGeneration, At>()?;
        let request = request.bind_target(binding.evidence().target());
        let effect = EmbeddingGeneration::new(label, request, binding)?;
        self.perform::<EmbeddingGeneration, At, SubsetProofEnd>(effect)
            .await
            .map_err(EmbeddingOperationError::Effect)
    }
}

/// Domain-named immutable metadata for definition-time chat planning.
pub trait ChatBindingMetadata {
    fn target(&self) -> &ChatTarget;
    fn estimator(&self) -> &ResolvedTokenEstimator;
}

impl ChatBindingMetadata for EffectBinding<ChatCompletion> {
    fn target(&self) -> &ChatTarget {
        self.evidence().target()
    }

    fn estimator(&self) -> &ResolvedTokenEstimator {
        self.evidence().estimator()
    }
}

/// Domain-named immutable metadata for definition-time embedding planning.
pub trait EmbeddingBindingMetadata {
    fn target(&self) -> &EmbeddingTarget;
}

impl EmbeddingBindingMetadata for EffectBinding<EmbeddingGeneration> {
    fn target(&self) -> &EmbeddingTarget {
        self.evidence().target()
    }
}

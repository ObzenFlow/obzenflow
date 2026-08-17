// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::{effect_error_to_handler_error, ChatCompletion};
use async_trait::async_trait;
use obzenflow_core::ai::{
    ChatMessage, ChatParams, ChatRequestSpec, ChatResponse, ChatResponseFormat, ToolDefinition,
};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{EffectBinding, Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

pub const STANDALONE_CHAT_COMPLETION_LABEL: &str = "standalone.chat_completion";

type PromptMapper<In> = dyn Fn(&In) -> Result<String, HandlerError> + Send + Sync + 'static;
type ResponseMapper<In, Out> =
    dyn Fn(In, ChatResponse) -> Result<Out, HandlerError> + Send + Sync + 'static;

pub(crate) struct ChatTransformSettings {
    pub(crate) system: Option<String>,
    pub(crate) params: ChatParams,
    pub(crate) tools: Vec<ToolDefinition>,
    pub(crate) response_format: Option<ChatResponseFormat>,
}

/// Typed standalone chat handler whose only live authority is `fx.perform`.
pub struct ChatTransform<In, Out> {
    binding: EffectBinding<ChatCompletion>,
    system: Option<String>,
    params: ChatParams,
    tools: Vec<ToolDefinition>,
    response_format: Option<ChatResponseFormat>,
    input_to_prompt: Arc<PromptMapper<In>>,
    response_to_output: Arc<ResponseMapper<In, Out>>,
    logic_version: String,
    _types: PhantomData<fn() -> (In, Out)>,
}

impl<In, Out> ChatTransform<In, Out> {
    pub(crate) fn from_parts(
        binding: EffectBinding<ChatCompletion>,
        settings: ChatTransformSettings,
        input_to_prompt: Arc<PromptMapper<In>>,
        response_to_output: Arc<ResponseMapper<In, Out>>,
        logic_version: String,
    ) -> Self {
        Self {
            binding,
            system: settings.system,
            params: settings.params,
            tools: settings.tools,
            response_format: settings.response_format,
            input_to_prompt,
            response_to_output,
            logic_version,
            _types: PhantomData,
        }
    }
}

impl<In, Out> Clone for ChatTransform<In, Out> {
    fn clone(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            system: self.system.clone(),
            params: self.params.clone(),
            tools: self.tools.clone(),
            response_format: self.response_format.clone(),
            input_to_prompt: Arc::clone(&self.input_to_prompt),
            response_to_output: Arc::clone(&self.response_to_output),
            logic_version: self.logic_version.clone(),
            _types: PhantomData,
        }
    }
}

impl<In, Out> fmt::Debug for ChatTransform<In, Out> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChatTransform")
            .field("binding", &self.binding)
            .field("logic_version", &self.logic_version)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<In, Out> EffectfulTransformHandler for ChatTransform<In, Out>
where
    In: TypedPayload + Send + Sync + 'static,
    Out: TypedPayload + Send + Sync + 'static,
{
    type Input = In;
    type Output = Out;
    type AllowedEffects = obzenflow_runtime::effect_set![ChatCompletion];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let prompt = (self.input_to_prompt)(&input)?;
        let mut messages = Vec::with_capacity(usize::from(self.system.is_some()) + 1);
        if let Some(system) = &self.system {
            messages.push(ChatMessage::system(system.clone()));
        }
        messages.push(ChatMessage::user(prompt));

        let spec = ChatRequestSpec {
            messages,
            params: self.params.clone(),
            tools: self.tools.clone(),
            response_format: self.response_format.clone(),
        };
        let request = spec.bind_target(self.binding.evidence().target());
        let effect = ChatCompletion::new(
            STANDALONE_CHAT_COMPLETION_LABEL,
            request,
            self.binding.invocation(),
        )
        .map_err(|error| HandlerError::Validation(error.to_string()))?;
        let reply = fx
            .perform(effect)
            .await
            .map_err(effect_error_to_handler_error)?;
        let output = (self.response_to_output)(input, reply.response)?;
        fx.emit(output)
            .await
            .map_err(effect_error_to_handler_error)?;
        fx.complete().map_err(effect_error_to_handler_error)
    }

    fn stage_logic_version(&self) -> &str {
        &self.logic_version
    }
}

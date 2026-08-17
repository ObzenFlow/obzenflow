// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::{effect_error_to_handler_error, EmbeddingGeneration};
use async_trait::async_trait;
use obzenflow_core::ai::{EmbeddingParams, EmbeddingRequestSpec, EmbeddingResponse};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{EffectBinding, Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

pub const STANDALONE_EMBEDDING_GENERATION_LABEL: &str = "standalone.embedding_generation";

type InputsMapper<In> = dyn Fn(&In) -> Result<Vec<String>, HandlerError> + Send + Sync + 'static;
type ResponseMapper<In, Out> =
    dyn Fn(In, EmbeddingResponse) -> Result<Out, HandlerError> + Send + Sync + 'static;

/// Typed standalone embedding handler whose only live authority is `fx.perform`.
pub struct EmbeddingTransform<In, Out> {
    binding: EffectBinding<EmbeddingGeneration>,
    params: EmbeddingParams,
    input_to_inputs: Arc<InputsMapper<In>>,
    response_to_output: Arc<ResponseMapper<In, Out>>,
    logic_version: String,
    _types: PhantomData<fn() -> (In, Out)>,
}

impl<In, Out> EmbeddingTransform<In, Out> {
    pub(crate) fn from_parts(
        binding: EffectBinding<EmbeddingGeneration>,
        params: EmbeddingParams,
        input_to_inputs: Arc<InputsMapper<In>>,
        response_to_output: Arc<ResponseMapper<In, Out>>,
        logic_version: String,
    ) -> Self {
        Self {
            binding,
            params,
            input_to_inputs,
            response_to_output,
            logic_version,
            _types: PhantomData,
        }
    }
}

impl<In, Out> Clone for EmbeddingTransform<In, Out> {
    fn clone(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            params: self.params.clone(),
            input_to_inputs: Arc::clone(&self.input_to_inputs),
            response_to_output: Arc::clone(&self.response_to_output),
            logic_version: self.logic_version.clone(),
            _types: PhantomData,
        }
    }
}

impl<In, Out> fmt::Debug for EmbeddingTransform<In, Out> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EmbeddingTransform")
            .field("binding", &self.binding)
            .field("logic_version", &self.logic_version)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<In, Out> EffectfulTransformHandler for EmbeddingTransform<In, Out>
where
    In: TypedPayload + Send + Sync + 'static,
    Out: TypedPayload + Send + Sync + 'static,
{
    type Input = In;
    type Output = Out;
    type AllowedEffects = obzenflow_runtime::effect_set![EmbeddingGeneration];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let spec = EmbeddingRequestSpec {
            inputs: (self.input_to_inputs)(&input)?,
            params: self.params.clone(),
        };
        let request = spec.bind_target(self.binding.evidence().target());
        let effect = EmbeddingGeneration::new(
            STANDALONE_EMBEDDING_GENERATION_LABEL,
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

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_adapters::ai::{
    ChatBindingEvidence, ChatCompletion, InferenceHandler, CHAT_CLIENT,
};
use obzenflow_core::ai::{
    AiClientError, AiFinaliseRole, AiInferenceRole, AiMapRole, AiRoleLogicFailure, ChatClient,
    ChatCompletionReply, ChatParams, ChatRequest, ChatRequestSpec, ChatResponse, ChatTarget,
    ChunkInfo, HeuristicTokenEstimator, Many, ResolvedTokenEstimator,
    TokenEstimatorFallbackReason, TokenEstimatorResolutionInfo,
};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{
    EffectBinding, EffectRegistrationBuilder, LogicalEffectBindingName, ResolvedEffectPort,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Input;

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "trybuild.ai.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Seed;

impl TypedPayload for Seed {
    const EVENT_TYPE: &'static str = "trybuild.ai.seed";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Item;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Partial;

impl TypedPayload for Partial {
    const EVENT_TYPE: &'static str = "trybuild.ai.partial";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Output;

impl TypedPayload for Output {
    const EVENT_TYPE: &'static str = "trybuild.ai.output";
}

pub struct InferenceRole;

impl AiInferenceRole<Input, Output> for InferenceRole {
    fn prepare(&self, _input: &Input) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Ok(spec())
    }

    fn interpret(
        &self,
        _input: Input,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
    ) -> Result<Output, AiRoleLogicFailure> {
        Ok(Output)
    }
}

pub fn inference_handler() -> InferenceHandler<Input, Output> {
    InferenceHandler::from_role(InferenceRole)
}

pub struct MapRole;

impl AiMapRole<Item, Partial> for MapRole {
    fn prepare(
        &self,
        _items: &[Item],
        _chunk: &ChunkInfo,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Ok(spec())
    }

    fn interpret(
        &self,
        _items: Vec<Item>,
        _chunk: ChunkInfo,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
    ) -> Result<Partial, AiRoleLogicFailure> {
        Ok(Partial)
    }
}

pub struct FinaliseRole;

impl AiFinaliseRole<Seed, Many<Partial>, Output> for FinaliseRole {
    fn prepare(
        &self,
        _seed: &Seed,
        _partials: &Many<Partial>,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Ok(spec())
    }

    fn interpret(
        &self,
        _seed: Seed,
        _partials: Many<Partial>,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
    ) -> Result<Output, AiRoleLogicFailure> {
        Ok(Output)
    }
}

struct FixtureChatClient {
    target: ChatTarget,
}

#[async_trait]
impl ChatClient for FixtureChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        unreachable!("trybuild never executes its application-local chat client")
    }
}

pub fn binding() -> EffectBinding<ChatCompletion> {
    let target = ChatTarget::new("trybuild", "model");
    let evidence = ChatBindingEvidence::new(
        target.clone(),
        ResolvedTokenEstimator::new(
            Arc::new(HeuristicTokenEstimator::default()),
            TokenEstimatorResolutionInfo::heuristic(
                "model",
                TokenEstimatorFallbackReason::ExplicitHeuristic,
                None,
            ),
        ),
    )
    .expect("trybuild chat target and estimator models agree");
    EffectRegistrationBuilder::<ChatCompletion>::new(
        LogicalEffectBindingName::new("trybuild_chat").unwrap(),
        evidence,
    )
    .bind_eager_with_metadata(
        CHAT_CLIENT,
        ResolvedEffectPort::new(
            Arc::new(FixtureChatClient {
                target: target.clone(),
            }) as Arc<dyn ChatClient>,
            Arc::new(target),
        ),
    )
    .unwrap()
    .finish()
    .unwrap()
}

fn spec() -> ChatRequestSpec {
    ChatRequestSpec {
        messages: Vec::new(),
        params: ChatParams::default(),
        tools: Vec::new(),
        response_format: None,
    }
}

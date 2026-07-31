// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::{
    AiFinaliseRole, AiInferenceRole, AiMapRole, AiRoleLogicFailure, ChatBindingContract,
    ChatCompletionReply, ChatParams, ChatRequestSpec, ChatTarget, ChunkInfo,
    HeuristicTokenEstimator, Many, ResolvedTokenEstimator, TokenEstimatorFallbackReason,
    TokenEstimatorResolutionInfo,
};
use obzenflow_core::TypedPayload;
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

pub fn contract() -> ChatBindingContract {
    ChatBindingContract::from_resolved(
        ChatTarget::new("trybuild", "model"),
        ResolvedTokenEstimator::new(
            Arc::new(HeuristicTokenEstimator::default()),
            TokenEstimatorResolutionInfo::heuristic(
                "model",
                TokenEstimatorFallbackReason::ExplicitHeuristic,
                None,
            ),
        ),
    )
    .expect("trybuild chat target and estimator models agree")
}

fn spec() -> ChatRequestSpec {
    ChatRequestSpec {
        messages: Vec::new(),
        params: ChatParams::default(),
        tools: Vec::new(),
        response_format: None,
    }
}

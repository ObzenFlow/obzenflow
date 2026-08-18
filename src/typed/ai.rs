// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed AI helper facades.
//!
//! These helpers construct handlers intended for use with typed stage macros.

pub use obzenflow_core::ai::Many;
use obzenflow_core::ai::{
    AiInferenceRole, AiRoleLogicFailure, ChatCompletionReply, ChatRequestSpec,
};
use obzenflow_runtime::stages::transform::ChunkByBudgetBuilder;

#[derive(Clone, Copy, Debug)]
struct FunctionInferenceRole<Input, Out> {
    prepare: fn(&Input) -> Result<ChatRequestSpec, AiRoleLogicFailure>,
    interpret: fn(Input, ChatRequestSpec, ChatCompletionReply) -> Result<Out, AiRoleLogicFailure>,
}

impl<Input, Out> AiInferenceRole<Input, Out> for FunctionInferenceRole<Input, Out>
where
    Input: 'static,
    Out: 'static,
{
    fn prepare(&self, input: &Input) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        (self.prepare)(input)
    }

    fn interpret(
        &self,
        input: Input,
        request: ChatRequestSpec,
        reply: ChatCompletionReply,
    ) -> Result<Out, AiRoleLogicFailure> {
        (self.interpret)(input, request, reply)
    }
}

/// Assemble a stateless default-version scalar inference role from two functions.
pub fn inference_role<Input, Out>(
    prepare: fn(&Input) -> Result<ChatRequestSpec, AiRoleLogicFailure>,
    interpret: fn(Input, ChatRequestSpec, ChatCompletionReply) -> Result<Out, AiRoleLogicFailure>,
) -> impl AiInferenceRole<Input, Out>
where
    Input: 'static,
    Out: 'static,
{
    FunctionInferenceRole { prepare, interpret }
}

pub fn chunk_by_budget<In, Item>() -> ChunkByBudgetBuilder<In, Item> {
    ChunkByBudgetBuilder::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::{AiProvider, ChatParams, ChatResponse, LlmHashes, LlmObservability};

    #[derive(Debug, PartialEq, Eq)]
    struct Input(&'static str);

    #[derive(Debug, PartialEq, Eq)]
    struct Output {
        input: &'static str,
        max_tokens: Option<u32>,
        reply: String,
    }

    fn prepare(_input: &Input) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Ok(ChatRequestSpec {
            messages: Vec::new(),
            params: ChatParams {
                max_tokens: Some(17),
                ..ChatParams::default()
            },
            tools: Vec::new(),
            response_format: None,
        })
    }

    fn interpret(
        input: Input,
        request: ChatRequestSpec,
        reply: ChatCompletionReply,
    ) -> Result<Output, AiRoleLogicFailure> {
        Ok(Output {
            input: input.0,
            max_tokens: request.params.max_tokens,
            reply: reply.response.text,
        })
    }

    fn prepare_error(_input: &Input) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Err(AiRoleLogicFailure::Prompt {
            message: "prepare failed".to_string(),
        })
    }

    fn interpret_error(
        _input: Input,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
    ) -> Result<Output, AiRoleLogicFailure> {
        Err(AiRoleLogicFailure::Parse {
            message: "interpret failed".to_string(),
        })
    }

    fn reply(text: &str) -> ChatCompletionReply {
        ChatCompletionReply {
            response: ChatResponse {
                text: text.to_string(),
                tool_calls: Vec::new(),
                usage: None,
                raw: None,
            },
            observability: LlmObservability::new(
                AiProvider::new("fixture"),
                "model",
                LlmHashes::new("prompt".to_string(), "params".to_string()),
            ),
        }
    }

    fn logic_version<R: AiInferenceRole<Input, Output>>(_role: &R) -> &'static str {
        R::LOGIC_VERSION
    }

    #[test]
    fn inference_role_uses_the_trait_default_logic_version() {
        let role = inference_role(prepare, interpret);
        assert_eq!(logic_version(&role), "1");
    }

    #[test]
    fn inference_role_delegates_prepare_and_forwards_interpret_inputs() {
        let role = inference_role(prepare, interpret);
        let request = role.prepare(&Input("evidence")).unwrap();
        assert_eq!(request.params.max_tokens, Some(17));

        let output = role
            .interpret(Input("evidence"), request, reply("answer"))
            .unwrap();
        assert_eq!(
            output,
            Output {
                input: "evidence",
                max_tokens: Some(17),
                reply: "answer".to_string(),
            }
        );
    }

    #[test]
    fn inference_role_preserves_prepare_and_interpret_errors() {
        let prepare_failure = inference_role(prepare_error, interpret);
        assert_eq!(
            prepare_failure.prepare(&Input("evidence")),
            Err(AiRoleLogicFailure::Prompt {
                message: "prepare failed".to_string(),
            })
        );

        let interpret_failure = inference_role(prepare, interpret_error);
        let request = interpret_failure.prepare(&Input("evidence")).unwrap();
        assert_eq!(
            interpret_failure.interpret(Input("evidence"), request, reply("answer")),
            Err(AiRoleLogicFailure::Parse {
                message: "interpret failed".to_string(),
            })
        );
    }
}

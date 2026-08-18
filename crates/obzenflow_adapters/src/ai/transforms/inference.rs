// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::{effect_error_to_handler_error, ChatCompletion, ChatEffects};
use async_trait::async_trait;
use obzenflow_core::ai::{
    AiInferenceRole, AiRoleLogicFailure, ChatCompletionReply, ChatRequestSpec,
};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use std::fmt;
use std::sync::Arc;

const INFERENCE_CHAT_COMPLETION_LABEL: &str = "inference.chat_completion";

type Prepare<Input> =
    dyn Fn(&Input) -> Result<ChatRequestSpec, AiRoleLogicFailure> + Send + Sync + 'static;
type Interpret<Input, Out> = dyn Fn(Input, ChatRequestSpec, ChatCompletionReply) -> Result<Out, AiRoleLogicFailure>
    + Send
    + Sync
    + 'static;

/// Adapter-owned scalar AI inference handler.
///
/// The handler retains the exact target-free request across the declared chat
/// effect and supplies it to deterministic interpretation with the recorded
/// reply. The `inference!` DSL facade accepts this handler type on the
/// right-hand side of `=>`.
pub struct InferenceHandler<Input, Out> {
    prepare: Arc<Prepare<Input>>,
    interpret: Arc<Interpret<Input, Out>>,
    logic_version: &'static str,
}

impl<Input, Out> InferenceHandler<Input, Out> {
    /// Wrap a reusable or explicitly versioned scalar inference role as a
    /// concrete handler.
    pub fn from_role<Role>(role: Role) -> Self
    where
        Input: 'static,
        Out: 'static,
        Role: AiInferenceRole<Input, Out>,
    {
        let role = Arc::new(role);
        let prepare_role = Arc::clone(&role);
        let interpret_role = role;
        Self {
            prepare: Arc::new(move |input| prepare_role.prepare(input)),
            interpret: Arc::new(move |input, request, reply| {
                interpret_role.interpret(input, request, reply)
            }),
            logic_version: Role::LOGIC_VERSION,
        }
    }

    /// Assemble the default-version handler from stateless function pointers.
    pub fn from_functions(
        prepare: fn(&Input) -> Result<ChatRequestSpec, AiRoleLogicFailure>,
        interpret: fn(
            Input,
            ChatRequestSpec,
            ChatCompletionReply,
        ) -> Result<Out, AiRoleLogicFailure>,
    ) -> Self
    where
        Input: 'static,
        Out: 'static,
    {
        Self {
            prepare: Arc::new(prepare),
            interpret: Arc::new(interpret),
            logic_version: "1",
        }
    }
}

impl<Input, Out> Clone for InferenceHandler<Input, Out> {
    fn clone(&self) -> Self {
        Self {
            prepare: Arc::clone(&self.prepare),
            interpret: Arc::clone(&self.interpret),
            logic_version: self.logic_version,
        }
    }
}

impl<Input, Out> fmt::Debug for InferenceHandler<Input, Out> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InferenceHandler")
            .field("logic_version", &self.logic_version)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<Input, Out> EffectfulTransformHandler for InferenceHandler<Input, Out>
where
    Input: TypedPayload + Clone + Send + Sync + 'static,
    Out: TypedPayload + Clone + Send + Sync + 'static,
{
    type Input = Input;
    type Output = Out;
    type AllowedEffects = obzenflow_runtime::effect_set![ChatCompletion];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let request =
            (self.prepare)(&input).map_err(|error| HandlerError::Domain(format!("{error:?}")))?;
        let reply = fx
            .chat_completion(INFERENCE_CHAT_COMPLETION_LABEL, request.clone())
            .await
            .map_err(HandlerError::from)?;
        let output = (self.interpret)(input, request, reply)
            .map_err(|error| HandlerError::Domain(format!("{error:?}")))?;
        fx.emit(output)
            .await
            .map_err(effect_error_to_handler_error)?;
        fx.complete().map_err(effect_error_to_handler_error)
    }

    fn stage_logic_version(&self) -> &str {
        self.logic_version
    }
}

/// Construct an adapter-owned scalar inference handler from two stateless
/// functions.
pub fn inference_handler<Input, Out>(
    prepare: fn(&Input) -> Result<ChatRequestSpec, AiRoleLogicFailure>,
    interpret: fn(Input, ChatRequestSpec, ChatCompletionReply) -> Result<Out, AiRoleLogicFailure>,
) -> InferenceHandler<Input, Out>
where
    Input: 'static,
    Out: 'static,
{
    InferenceHandler::from_functions(prepare, interpret)
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

    #[test]
    fn function_constructor_returns_a_default_version_handler() {
        let handler = inference_handler(prepare, interpret);
        assert_eq!(handler.logic_version, "1");
    }

    #[test]
    fn function_handler_delegates_and_forwards_interpret_inputs() {
        let handler = inference_handler(prepare, interpret);
        let request = (handler.prepare)(&Input("evidence")).unwrap();
        assert_eq!(request.params.max_tokens, Some(17));

        let output = (handler.interpret)(Input("evidence"), request, reply("answer")).unwrap();
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
    fn function_handler_preserves_prepare_and_interpret_errors() {
        let prepare_failure = inference_handler(prepare_error, interpret);
        assert_eq!(
            (prepare_failure.prepare)(&Input("evidence")),
            Err(AiRoleLogicFailure::Prompt {
                message: "prepare failed".to_string(),
            })
        );

        let interpret_failure = inference_handler(prepare, interpret_error);
        let request = (interpret_failure.prepare)(&Input("evidence")).unwrap();
        assert_eq!(
            (interpret_failure.interpret)(Input("evidence"), request, reply("answer")),
            Err(AiRoleLogicFailure::Parse {
                message: "interpret failed".to_string(),
            })
        );
    }

    struct VersionedRole;

    impl AiInferenceRole<Input, Output> for VersionedRole {
        const LOGIC_VERSION: &'static str = "brief-v2";

        fn prepare(&self, input: &Input) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
            prepare(input)
        }

        fn interpret(
            &self,
            input: Input,
            request: ChatRequestSpec,
            reply: ChatCompletionReply,
        ) -> Result<Output, AiRoleLogicFailure> {
            interpret(input, request, reply)
        }
    }

    #[test]
    fn role_constructor_preserves_the_role_logic_version() {
        let handler = InferenceHandler::from_role(VersionedRole);
        assert_eq!(handler.logic_version, "brief-v2");
    }
}

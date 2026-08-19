// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::{effect_error_to_handler_error, ChatCompletion, ChatEffects};
use async_trait::async_trait;
use obzenflow_runtime::effects::{Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{EffectfulTransformHandler, InferenceHandler};

const INFERENCE_CHAT_COMPLETION_LABEL: &str = "inference.chat_completion";

/// Hidden bridge from authored inference hooks to the ordinary effectful
/// transform stage machinery.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct InferenceHandlerAdapter<H> {
    handler: H,
}

impl<H> InferenceHandlerAdapter<H> {
    #[doc(hidden)]
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl<H> EffectfulTransformHandler for InferenceHandlerAdapter<H>
where
    H: InferenceHandler,
{
    type Input = H::Input;
    type Output = H::Output;
    type AllowedEffects = obzenflow_runtime::effect_set![ChatCompletion];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let request = self.handler.prepare(&input)?;
        let reply = fx
            .chat_completion(INFERENCE_CHAT_COMPLETION_LABEL, request.clone())
            .await?;
        let output = self.handler.interpret(input, request, reply)?;
        fx.emit(output)
            .await
            .map_err(effect_error_to_handler_error)?;
        fx.complete().map_err(effect_error_to_handler_error)
    }

    fn stage_logic_version(&self) -> &str {
        self.handler.stage_logic_version()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::{
        AiProvider, ChatCompletionReply, ChatParams, ChatRequestSpec, ChatResponse, LlmHashes,
        LlmObservability,
    };
    use obzenflow_core::TypedPayload;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Input(String);

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "adapter.inference.input";
    }

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Output {
        input: String,
        max_tokens: Option<u32>,
        reply: String,
    }

    impl TypedPayload for Output {
        const EVENT_TYPE: &'static str = "adapter.inference.output";
    }

    #[derive(Clone, Debug)]
    struct TestHandler;

    impl InferenceHandler for TestHandler {
        type Input = Input;
        type Output = Output;

        fn prepare(&self, _input: &Input) -> Result<ChatRequestSpec, HandlerError> {
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
            &self,
            input: Input,
            request: ChatRequestSpec,
            reply: ChatCompletionReply,
        ) -> Result<Output, HandlerError> {
            Ok(Output {
                input: input.0,
                max_tokens: request.params.max_tokens,
                reply: reply.response.text,
            })
        }
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
    fn adapter_delegates_the_authored_hooks_and_retains_the_request() {
        let adapter = InferenceHandlerAdapter::new(TestHandler);
        let request = adapter
            .handler
            .prepare(&Input("evidence".to_string()))
            .unwrap();
        assert_eq!(request.params.max_tokens, Some(17));

        let output = adapter
            .handler
            .interpret(Input("evidence".to_string()), request, reply("answer"))
            .unwrap();
        assert_eq!(
            output,
            Output {
                input: "evidence".to_string(),
                max_tokens: Some(17),
                reply: "answer".to_string(),
            }
        );
    }

    #[derive(Clone, Debug)]
    struct FailingHandler;

    impl InferenceHandler for FailingHandler {
        type Input = Input;
        type Output = Output;

        fn prepare(&self, _input: &Input) -> Result<ChatRequestSpec, HandlerError> {
            Err(HandlerError::Validation("prepare failed".to_string()))
        }

        fn interpret(
            &self,
            _input: Input,
            _request: ChatRequestSpec,
            _reply: ChatCompletionReply,
        ) -> Result<Output, HandlerError> {
            Err(HandlerError::Domain("interpret failed".to_string()))
        }
    }

    #[test]
    fn adapter_preserves_handler_error_classification() {
        let adapter = InferenceHandlerAdapter::new(FailingHandler);
        assert!(matches!(
            adapter
                .handler
                .prepare(&Input("evidence".to_string())),
            Err(HandlerError::Validation(message)) if message == "prepare failed"
        ));
        assert!(matches!(
            adapter.handler.interpret(
                Input("evidence".to_string()),
                ChatRequestSpec {
                    messages: Vec::new(),
                    params: ChatParams::default(),
                    tools: Vec::new(),
                    response_format: None,
                },
                reply("answer"),
            ),
            Err(HandlerError::Domain(message)) if message == "interpret failed"
        ));
    }

    #[derive(Clone, Debug)]
    struct VersionedHandler;

    impl InferenceHandler for VersionedHandler {
        type Input = Input;
        type Output = Output;

        fn prepare(&self, input: &Input) -> Result<ChatRequestSpec, HandlerError> {
            TestHandler.prepare(input)
        }

        fn interpret(
            &self,
            input: Input,
            request: ChatRequestSpec,
            reply: ChatCompletionReply,
        ) -> Result<Output, HandlerError> {
            TestHandler.interpret(input, request, reply)
        }

        fn stage_logic_version(&self) -> &str {
            "brief-v2"
        }
    }

    #[test]
    fn adapter_forwards_the_handler_logic_version() {
        let adapter = InferenceHandlerAdapter::new(VersionedHandler);
        assert_eq!(adapter.stage_logic_version(), "brief-v2");
    }
}

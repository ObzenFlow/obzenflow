// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::error_mapping::ai_client_error_to_handler_error_with_context;
use async_trait::async_trait;
use obzenflow_core::ai::{
    attach_llm_observability, params_hash_for_chat, prompt_hash_for_chat,
    schema_hash_for_response_format, ChatClient, ChatRequest, ChatResponse, LlmHashes,
    LlmObservability, TokenEstimator,
};
use obzenflow_core::event::chain_event::ChainEventContent;
use obzenflow_core::ChainEvent;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::AsyncTransformHandler;
use serde_json::json;
use std::fmt;
use std::sync::Arc;

type ChatRequestBuilder =
    dyn Fn(&ChainEvent) -> Result<ChatRequest, HandlerError> + Send + Sync + 'static;
type ChatOutputMapper = dyn Fn(&ChainEvent, ChatResponse) -> Result<Vec<ChainEvent>, HandlerError>
    + Send
    + Sync
    + 'static;

/// Async transform handler that executes a chat request through an injected `ChatClient`.
#[derive(Clone)]
pub struct ChatTransform {
    client: Arc<dyn ChatClient>,
    estimator: Option<Arc<dyn TokenEstimator>>,
    request_builder: Arc<ChatRequestBuilder>,
    output_mapper: Arc<ChatOutputMapper>,
}

impl fmt::Debug for ChatTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChatTransform").finish()
    }
}

impl ChatTransform {
    pub fn new<F>(client: Arc<dyn ChatClient>, request_builder: F) -> Self
    where
        F: Fn(&ChainEvent) -> Result<ChatRequest, HandlerError> + Send + Sync + 'static,
    {
        Self {
            client,
            estimator: None,
            request_builder: Arc::new(request_builder),
            output_mapper: Arc::new(default_chat_output_mapper),
        }
    }

    pub fn with_estimator(mut self, estimator: Arc<dyn TokenEstimator>) -> Self {
        self.estimator = Some(estimator);
        self
    }

    pub fn with_output_mapper<F>(mut self, output_mapper: F) -> Self
    where
        F: Fn(&ChainEvent, ChatResponse) -> Result<Vec<ChainEvent>, HandlerError>
            + Send
            + Sync
            + 'static,
    {
        self.output_mapper = Arc::new(output_mapper);
        self
    }
}

#[async_trait]
impl AsyncTransformHandler for ChatTransform {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let request = (self.request_builder)(&event)?;

        let prompt_hash = prompt_hash_for_chat(&request.messages).map_err(|err| {
            HandlerError::Validation(format!("chat request canonicalization failed: {err}"))
        })?;
        let params_hash = params_hash_for_chat(&request).map_err(|err| {
            HandlerError::Validation(format!("chat params canonicalization failed: {err}"))
        })?;
        let schema_hash = schema_hash_for_response_format(request.response_format.as_ref())
            .map_err(|err| {
                HandlerError::Validation(format!("chat schema canonicalization failed: {err}"))
            })?;

        let estimated_input_tokens = self
            .estimator
            .as_ref()
            .map(|estimator| estimator.estimate_chat_request(&request));

        let response = self
            .client
            .chat(request.clone())
            .await
            .map_err(|err| ai_client_error_to_handler_error_with_context(err, Some("chat")))?;

        let usage = response.usage.clone();
        let mut outputs = (self.output_mapper)(&event, response)?;

        let mut hashes = LlmHashes::new(prompt_hash, params_hash);
        hashes.schema_hash = schema_hash;

        let mut llm =
            LlmObservability::new(request.provider.clone(), request.model.clone(), hashes);
        llm.usage = usage;
        llm.estimated_input_tokens = estimated_input_tokens;

        for output in &mut outputs {
            attach_llm_observability(output, llm.clone()).map_err(|err| {
                HandlerError::Validation(format!("failed to attach llm metadata: {err}"))
            })?;
        }

        Ok(outputs)
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

fn default_chat_output_mapper(
    input: &ChainEvent,
    response: ChatResponse,
) -> Result<Vec<ChainEvent>, HandlerError> {
    let mut out = input.clone();

    match &mut out.content {
        ChainEventContent::Data { payload, .. } => {
            payload["llm"] = json!({
                "response_text": response.text,
                "tool_calls": response.tool_calls,
            });
            Ok(vec![out])
        }
        _ => Err(HandlerError::Validation(
            "chat transform expects data events".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::{
        AiClientError, AiProvider, ChatMessage, ChatParams, ChatResponseFormat, ChatRole,
        EstimateSource, TokenCount, TokenEstimate, TokenEstimator, Usage, UsageSource,
    };
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct QueueChatClient {
        outcomes: Mutex<VecDeque<Result<ChatResponse, AiClientError>>>,
    }

    impl QueueChatClient {
        fn enqueue_ok(&self, response: ChatResponse) {
            self.outcomes
                .lock()
                .expect("poisoned")
                .push_back(Ok(response));
        }

        fn enqueue_err(&self, err: AiClientError) {
            self.outcomes.lock().expect("poisoned").push_back(Err(err));
        }
    }

    #[async_trait]
    impl ChatClient for QueueChatClient {
        async fn chat(&self, _req: ChatRequest) -> Result<ChatResponse, AiClientError> {
            self.outcomes
                .lock()
                .expect("poisoned")
                .pop_front()
                .expect("test should enqueue outcomes")
        }
    }

    fn test_event() -> ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "ticket.created",
            json!({"id": "T-1", "text": "help"}),
        )
    }

    #[derive(Debug)]
    struct FixedTokenEstimator {
        tokens: TokenCount,
        source: EstimateSource,
    }

    impl TokenEstimator for FixedTokenEstimator {
        fn estimate_text(&self, _text: &str) -> TokenEstimate {
            TokenEstimate {
                tokens: self.tokens,
                source: self.source,
            }
        }

        fn estimate_chat_request(&self, _req: &ChatRequest) -> TokenEstimate {
            TokenEstimate {
                tokens: self.tokens,
                source: self.source,
            }
        }

        fn source(&self) -> EstimateSource {
            self.source
        }
    }

    #[tokio::test]
    async fn chat_transform_attaches_llm_metadata() {
        let client = Arc::new(QueueChatClient::default());
        client.enqueue_ok(ChatResponse {
            text: "summary".to_string(),
            tool_calls: vec![],
            usage: Some(Usage {
                source: UsageSource::Provider,
                input_tokens: 12,
                output_tokens: 8,
                total_tokens: 20,
            }),
            raw: None,
        });

        let transform = ChatTransform::new(client, |_event| {
            Ok(ChatRequest {
                provider: AiProvider::new("ollama"),
                model: "llama3.1:8b".to_string(),
                messages: vec![ChatMessage {
                    role: ChatRole::user(),
                    content: "summarize".to_string(),
                }],
                params: ChatParams::default(),
                tools: vec![],
                response_format: Some(ChatResponseFormat::JsonObject),
            })
        });

        let outputs = transform
            .process(test_event())
            .await
            .expect("chat transform should succeed");

        assert_eq!(outputs.len(), 1);
        let llm = obzenflow_core::ai::read_llm_observability(&outputs[0])
            .expect("llm read should parse")
            .expect("llm metadata should exist");
        assert_eq!(llm.provider.as_str(), "ollama");
        assert_eq!(llm.model, "llama3.1:8b");
        assert_eq!(llm.hashes.version, "sha256:v1");
    }

    #[tokio::test]
    async fn chat_transform_attaches_estimated_input_tokens_when_estimator_is_present() {
        let client = Arc::new(QueueChatClient::default());
        client.enqueue_ok(ChatResponse {
            text: "ok".to_string(),
            tool_calls: vec![],
            usage: None,
            raw: None,
        });

        let estimator = Arc::new(FixedTokenEstimator {
            tokens: TokenCount::new(123),
            source: EstimateSource::Heuristic,
        });

        let transform = ChatTransform::new(client, |_event| {
            Ok(ChatRequest {
                provider: AiProvider::new("ollama"),
                model: "llama3.1:8b".to_string(),
                messages: vec![ChatMessage {
                    role: ChatRole::user(),
                    content: "hello".to_string(),
                }],
                params: ChatParams::default(),
                tools: vec![],
                response_format: None,
            })
        })
        .with_estimator(estimator);

        let outputs = transform
            .process(test_event())
            .await
            .expect("chat transform should succeed");

        let llm = obzenflow_core::ai::read_llm_observability(&outputs[0])
            .expect("llm read should parse")
            .expect("llm metadata should exist");

        assert_eq!(
            llm.estimated_input_tokens,
            Some(TokenEstimate {
                tokens: TokenCount::new(123),
                source: EstimateSource::Heuristic
            })
        );
    }

    #[tokio::test]
    async fn chat_transform_maps_auth_to_permanent_failure() {
        let client = Arc::new(QueueChatClient::default());
        client.enqueue_err(AiClientError::Auth {
            message: "bad key".to_string(),
        });

        let transform = ChatTransform::new(client, |_event| {
            Ok(ChatRequest {
                provider: AiProvider::new("openai"),
                model: "gpt-4.1-mini".to_string(),
                messages: vec![ChatMessage {
                    role: ChatRole::user(),
                    content: "hello".to_string(),
                }],
                params: ChatParams::default(),
                tools: vec![],
                response_format: None,
            })
        });

        let err = transform
            .process(test_event())
            .await
            .expect_err("auth errors should fail");

        match err {
            HandlerError::PermanentFailure(message) => assert!(message.contains("auth")),
            other => panic!("expected PermanentFailure error, got {other:?}"),
        }
    }
}

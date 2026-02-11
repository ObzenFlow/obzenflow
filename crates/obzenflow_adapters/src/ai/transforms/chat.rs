// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::error_mapping::ai_client_error_to_handler_error_with_context;
use crate::ai::retry::{execute_with_retry, AiRetryPolicy};
use async_trait::async_trait;
use obzenflow_core::ai::{
    attach_llm_observability, params_hash_for_chat, prompt_hash_for_chat,
    schema_hash_for_response_format, ChatClient, ChatRequest, ChatResponse, LlmHashes,
    LlmObservability,
};
use obzenflow_core::event::chain_event::ChainEventContent;
use obzenflow_core::ChainEvent;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::AsyncTransformHandler;
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
    request_builder: Arc<ChatRequestBuilder>,
    output_mapper: Arc<ChatOutputMapper>,
    retry_policy: AiRetryPolicy,
}

impl fmt::Debug for ChatTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ChatTransform")
            .field("retry_policy", &self.retry_policy)
            .finish()
    }
}

impl ChatTransform {
    pub fn new<F>(client: Arc<dyn ChatClient>, request_builder: F) -> Self
    where
        F: Fn(&ChainEvent) -> Result<ChatRequest, HandlerError> + Send + Sync + 'static,
    {
        Self {
            client,
            request_builder: Arc::new(request_builder),
            output_mapper: Arc::new(default_chat_output_mapper),
            retry_policy: AiRetryPolicy::default(),
        }
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

    pub fn with_retry_policy(mut self, retry_policy: AiRetryPolicy) -> Self {
        self.retry_policy = retry_policy;
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

        let response = execute_with_retry(&self.retry_policy, |_attempt| {
            let client = self.client.clone();
            let req = request.clone();
            async move { client.chat(req).await }
        })
        .await
        .map_err(|err| ai_client_error_to_handler_error_with_context(err, Some("chat")))?;

        let usage = response.usage.clone();
        let mut outputs = (self.output_mapper)(&event, response)?;

        let mut hashes = LlmHashes::new(prompt_hash, params_hash);
        hashes.schema_hash = schema_hash;

        let mut llm =
            LlmObservability::new(request.provider.clone(), request.model.clone(), hashes);
        llm.usage = usage;

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
        AiClientError, AiProvider, ChatMessage, ChatParams, ChatResponseFormat, ChatRole, Usage,
        UsageSource,
    };
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    #[derive(Debug, Default)]
    struct QueueChatClient {
        outcomes: Mutex<VecDeque<Result<ChatResponse, AiClientError>>>,
        calls: AtomicUsize,
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

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ChatClient for QueueChatClient {
        async fn chat(&self, _req: ChatRequest) -> Result<ChatResponse, AiClientError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
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
    async fn chat_transform_retries_transient_errors() {
        let client = Arc::new(QueueChatClient::default());
        client.enqueue_err(AiClientError::Timeout {
            message: "attempt 1".to_string(),
        });
        client.enqueue_ok(ChatResponse {
            text: "ok".to_string(),
            tool_calls: vec![],
            usage: None,
            raw: None,
        });

        let transform = ChatTransform::new(client.clone(), |_event| {
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
        })
        .with_retry_policy(AiRetryPolicy {
            max_attempts: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            rate_limit_max_wait: Duration::from_millis(2),
        });

        let outputs = transform
            .process(test_event())
            .await
            .expect("retry should recover");

        assert_eq!(outputs.len(), 1);
        assert_eq!(client.calls(), 2);
    }

    #[tokio::test]
    async fn chat_transform_maps_auth_to_domain_error() {
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
        })
        .with_retry_policy(AiRetryPolicy::no_retry());

        let err = transform
            .process(test_event())
            .await
            .expect_err("auth errors should not retry and should fail");

        match err {
            HandlerError::Domain(message) => assert!(message.contains("auth")),
            other => panic!("expected Domain error, got {other:?}"),
        }
    }
}

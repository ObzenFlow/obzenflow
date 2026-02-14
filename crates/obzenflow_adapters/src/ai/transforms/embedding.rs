// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::error_mapping::ai_client_error_to_handler_error_with_context;
use async_trait::async_trait;
use obzenflow_core::ai::{
    attach_llm_observability, params_hash_for_embedding, prompt_hash_for_embedding_inputs,
    EmbeddingClient, EmbeddingRequest, EmbeddingResponse, LlmHashes, LlmObservability,
};
use obzenflow_core::event::chain_event::ChainEventContent;
use obzenflow_core::ChainEvent;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::AsyncTransformHandler;
use serde_json::json;
use std::fmt;
use std::sync::Arc;

type EmbeddingRequestBuilder =
    dyn Fn(&ChainEvent) -> Result<EmbeddingRequest, HandlerError> + Send + Sync + 'static;
type EmbeddingOutputMapper = dyn Fn(&ChainEvent, EmbeddingResponse) -> Result<Vec<ChainEvent>, HandlerError>
    + Send
    + Sync
    + 'static;

/// Async transform handler that executes an embedding request through an injected `EmbeddingClient`.
#[derive(Clone)]
pub struct EmbeddingTransform {
    client: Arc<dyn EmbeddingClient>,
    request_builder: Arc<EmbeddingRequestBuilder>,
    output_mapper: Arc<EmbeddingOutputMapper>,
}

impl fmt::Debug for EmbeddingTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmbeddingTransform").finish()
    }
}

impl EmbeddingTransform {
    pub fn new<F>(client: Arc<dyn EmbeddingClient>, request_builder: F) -> Self
    where
        F: Fn(&ChainEvent) -> Result<EmbeddingRequest, HandlerError> + Send + Sync + 'static,
    {
        Self {
            client,
            request_builder: Arc::new(request_builder),
            output_mapper: Arc::new(default_embedding_output_mapper),
        }
    }

    pub fn with_output_mapper<F>(mut self, output_mapper: F) -> Self
    where
        F: Fn(&ChainEvent, EmbeddingResponse) -> Result<Vec<ChainEvent>, HandlerError>
            + Send
            + Sync
            + 'static,
    {
        self.output_mapper = Arc::new(output_mapper);
        self
    }
}

#[async_trait]
impl AsyncTransformHandler for EmbeddingTransform {
    async fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let request = (self.request_builder)(&event)?;

        let prompt_hash = prompt_hash_for_embedding_inputs(&request.inputs).map_err(|err| {
            HandlerError::Validation(format!("embedding request canonicalization failed: {err}"))
        })?;
        let params_hash = params_hash_for_embedding(&request).map_err(|err| {
            HandlerError::Validation(format!("embedding params canonicalization failed: {err}"))
        })?;

        let embed_result = self.client.embed(request.clone()).await;
        let response = embed_result
            .map_err(|err| ai_client_error_to_handler_error_with_context(err, Some("embedding")))?;

        let usage = response.usage.clone();
        let mut outputs = (self.output_mapper)(&event, response)?;

        let hashes = LlmHashes::new(prompt_hash, params_hash);
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

fn default_embedding_output_mapper(
    input: &ChainEvent,
    response: EmbeddingResponse,
) -> Result<Vec<ChainEvent>, HandlerError> {
    let mut out = input.clone();

    match &mut out.content {
        ChainEventContent::Data { payload, .. } => {
            let first = response.vectors.first().cloned().unwrap_or_default();
            payload["embedding"] = json!(first);
            payload["embedding_vector_dim"] = json!(response.vector_dim);
            payload["embedding_count"] = json!(response.vectors.len());
            Ok(vec![out])
        }
        _ => Err(HandlerError::Validation(
            "embedding transform expects data events".to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::{AiClientError, AiProvider, EmbeddingParams, Usage, UsageSource};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct QueueEmbeddingClient {
        outcomes: Mutex<VecDeque<Result<EmbeddingResponse, AiClientError>>>,
    }

    impl QueueEmbeddingClient {
        fn enqueue_ok(&self, response: EmbeddingResponse) {
            self.outcomes
                .lock()
                .expect("poisoned")
                .push_back(Ok(response));
        }
    }

    #[async_trait]
    impl EmbeddingClient for QueueEmbeddingClient {
        async fn embed(&self, _req: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError> {
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
    async fn embedding_transform_attaches_llm_metadata() {
        let client = Arc::new(QueueEmbeddingClient::default());
        client.enqueue_ok(EmbeddingResponse {
            vectors: vec![vec![0.1, 0.2, 0.3]],
            vector_dim: 3,
            usage: Some(Usage {
                source: UsageSource::Provider,
                input_tokens: 8,
                output_tokens: 0,
                total_tokens: 8,
            }),
            raw: None,
        });

        let transform = EmbeddingTransform::new(client, |_event| {
            Ok(EmbeddingRequest {
                provider: AiProvider::new("openai"),
                model: "text-embedding-3-small".to_string(),
                inputs: vec!["help".to_string()],
                params: EmbeddingParams::default(),
            })
        });

        let outputs = transform
            .process(test_event())
            .await
            .expect("embedding transform should succeed");

        assert_eq!(outputs.len(), 1);
        let llm = obzenflow_core::ai::read_llm_observability(&outputs[0])
            .expect("llm read should parse")
            .expect("llm metadata should exist");
        assert_eq!(llm.provider.as_str(), "openai");
        assert_eq!(llm.model, "text-embedding-3-small");
    }
}

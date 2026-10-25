// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::ai::{
    AiClientError, ChatClient, ChatRequest, ChatResponse, ChatTarget, EmbeddingClient,
    EmbeddingRequest, EmbeddingResponse, EmbeddingTarget,
};

fn unreachable(endpoint: &str) -> AiClientError {
    AiClientError::Remote {
        message: format!(
            "endpoint `{endpoint}` recorded this call, and the current configuration does not reach it"
        ),
    }
}

pub(crate) struct UnreachableChatClient {
    target: ChatTarget,
    endpoint: String,
}

impl UnreachableChatClient {
    pub(crate) fn new(target: ChatTarget, endpoint: String) -> Self {
        Self { target, endpoint }
    }
}

#[async_trait]
impl ChatClient for UnreachableChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, _: ChatRequest) -> Result<ChatResponse, AiClientError> {
        Err(unreachable(&self.endpoint))
    }
}

pub(crate) struct UnreachableEmbeddingClient {
    target: EmbeddingTarget,
    endpoint: String,
}

impl UnreachableEmbeddingClient {
    pub(crate) fn new(target: EmbeddingTarget, endpoint: String) -> Self {
        Self { target, endpoint }
    }
}

#[async_trait]
impl EmbeddingClient for UnreachableEmbeddingClient {
    fn target(&self) -> &EmbeddingTarget {
        &self.target
    }

    async fn embed(&self, _: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError> {
        Err(unreachable(&self.endpoint))
    }
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::ai::{
    AiProviderError, ChatModel, ChatRequest, ChatResponse, ChatTarget, EmbeddingModel,
    EmbeddingRequest, EmbeddingResponse, EmbeddingTarget,
};

fn unreachable(endpoint: &str) -> AiProviderError {
    AiProviderError::Remote {
        message: format!(
            "endpoint `{endpoint}` recorded this call, and the current configuration does not reach it"
        ),
    }
}

pub(crate) struct UnreachableChatModel {
    target: ChatTarget,
    endpoint: String,
}

impl UnreachableChatModel {
    pub(crate) fn new(target: ChatTarget, endpoint: String) -> Self {
        Self { target, endpoint }
    }
}

#[async_trait]
impl ChatModel for UnreachableChatModel {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, _: ChatRequest) -> Result<ChatResponse, AiProviderError> {
        Err(unreachable(&self.endpoint))
    }
}

pub(crate) struct UnreachableEmbeddingModel {
    target: EmbeddingTarget,
    endpoint: String,
}

impl UnreachableEmbeddingModel {
    pub(crate) fn new(target: EmbeddingTarget, endpoint: String) -> Self {
        Self { target, endpoint }
    }
}

#[async_trait]
impl EmbeddingModel for UnreachableEmbeddingModel {
    fn target(&self) -> &EmbeddingTarget {
        &self.target
    }

    async fn embed(&self, _: EmbeddingRequest) -> Result<EmbeddingResponse, AiProviderError> {
        Err(unreachable(&self.endpoint))
    }
}

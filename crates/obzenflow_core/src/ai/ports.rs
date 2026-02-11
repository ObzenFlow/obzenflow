// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::{AiClientError, ChatRequest, ChatResponse, EmbeddingRequest, EmbeddingResponse};
use async_trait::async_trait;

#[async_trait]
pub trait ChatClient: Send + Sync + 'static {
    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, AiClientError>;
}

#[async_trait]
pub trait EmbeddingClient: Send + Sync + 'static {
    async fn embed(&self, req: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError>;
}

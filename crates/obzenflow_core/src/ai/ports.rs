// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::{
    AiClientError, ChatRequest, ChatResponse, ChatTarget, EmbeddingRequest, EmbeddingResponse,
};
use async_trait::async_trait;

#[async_trait]
pub trait ChatClient: Send + Sync + 'static {
    /// Immutable logical provider/model target served by this client.
    fn target(&self) -> &ChatTarget;

    /// Execute one caller-visible chat-port invocation.
    ///
    /// Implementations may perform SDK or transport retries internally.
    /// Callers must not infer HTTP-send or provider-execution cardinality from
    /// this invocation.
    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, AiClientError>;
}

#[async_trait]
pub trait EmbeddingClient: Send + Sync + 'static {
    async fn embed(&self, req: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError>;
}

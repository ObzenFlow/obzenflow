// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::{
    AiClientError, AiEndpoint, AiModelProviderError, AiModelSpec, AiProvider, ChatModelDescription,
    ChatRequest, ChatResponse, ChatTarget, EmbeddingModelDescription, EmbeddingRequest,
    EmbeddingResponse, EmbeddingTarget,
};
use async_trait::async_trait;
use std::sync::Arc;

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
    /// Immutable provider, model, and endpoint identity served by this client.
    fn target(&self) -> &EmbeddingTarget;

    async fn embed(&self, req: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError>;
}

/// Describes and connects the chat models an operator configures. A host
/// installs one in the build context; tests install a fake.
pub trait ChatModelProvider: Send + Sync + 'static {
    /// Whether endpoints of `provider` can serve chat through this provider.
    fn serves(&self, provider: &AiProvider) -> bool;

    /// Describe one configured model. Performs no I/O and reads no secret.
    fn describe(
        &self,
        endpoint: &AiEndpoint,
        model: &str,
    ) -> Result<ChatModelDescription, AiModelProviderError>;

    /// Rebuild the description of a recorded model. Performs no I/O.
    fn describe_recorded(
        &self,
        spec: &AiModelSpec,
    ) -> Result<ChatModelDescription, AiModelProviderError>;

    /// Build the client that serves a described model. Called only when a
    /// live call first needs it, never on strict replay.
    fn connect(
        &self,
        endpoint: &AiEndpoint,
        description: &ChatModelDescription,
    ) -> Result<Arc<dyn ChatClient>, AiModelProviderError>;
}

/// Describes and connects the embedding models an operator configures.
pub trait EmbeddingModelProvider: Send + Sync + 'static {
    /// Whether endpoints of `provider` can serve embeddings through this
    /// provider.
    fn serves(&self, provider: &AiProvider) -> bool;

    /// Describe one configured model. Performs no I/O and reads no secret.
    fn describe(
        &self,
        endpoint: &AiEndpoint,
        model: &str,
    ) -> Result<EmbeddingModelDescription, AiModelProviderError>;

    /// Rebuild the description of a recorded model. Performs no I/O.
    fn describe_recorded(
        &self,
        spec: &AiModelSpec,
    ) -> Result<EmbeddingModelDescription, AiModelProviderError>;

    /// Build the client that serves a described model. Called only when a
    /// live call first needs it, never on strict replay.
    fn connect(
        &self,
        endpoint: &AiEndpoint,
        description: &EmbeddingModelDescription,
    ) -> Result<Arc<dyn EmbeddingClient>, AiModelProviderError>;
}

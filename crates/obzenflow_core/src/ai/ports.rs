// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::{
    AiEndpoint, AiModelSpec, AiProvider, AiProviderError, AiResolutionError, ChatModelDescription,
    ChatRequest, ChatResponse, ChatTarget, EmbeddingModelDescription, EmbeddingRequest,
    EmbeddingResponse, EmbeddingTarget,
};
use async_trait::async_trait;
use std::sync::Arc;
use std::time::Duration;

/// What an AI stage calls: one configured chat model.
#[async_trait]
pub trait ChatModel: Send + Sync + 'static {
    /// Immutable logical provider/model target served by this model.
    fn target(&self) -> &ChatTarget;

    /// One call to the provider: one send, never repeated here. Repetition
    /// belongs to the retry middleware.
    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, AiProviderError>;
}

/// What an AI stage calls: one configured embedding model.
#[async_trait]
pub trait EmbeddingModel: Send + Sync + 'static {
    /// Immutable provider, model, and endpoint identity served by this model.
    fn target(&self) -> &EmbeddingTarget;

    /// One call to the provider: one send, never repeated here.
    async fn embed(&self, req: EmbeddingRequest) -> Result<EmbeddingResponse, AiProviderError>;
}

/// Describes and connects the chat models an operator configures. A host
/// installs one in the build context.
pub trait ChatModelResolver: Send + Sync + 'static {
    /// Whether endpoints of `provider` can serve chat through this resolver.
    fn serves(&self, provider: &AiProvider) -> bool;

    /// Describe one configured model. Performs no I/O and reads no secret.
    fn describe(
        &self,
        endpoint: &AiEndpoint,
        model: &str,
    ) -> Result<ChatModelDescription, AiResolutionError>;

    /// Rebuild the description of a recorded model. Performs no I/O.
    fn describe_recorded(
        &self,
        spec: &AiModelSpec,
    ) -> Result<ChatModelDescription, AiResolutionError>;

    /// Build the model that serves a description, writing `request_timeout` as
    /// the deadline of each call. Called only when a live call first needs it,
    /// never on strict replay.
    fn connect(
        &self,
        endpoint: &AiEndpoint,
        description: &ChatModelDescription,
        request_timeout: Duration,
    ) -> Result<Arc<dyn ChatModel>, AiResolutionError>;
}

/// Describes and connects the embedding models an operator configures.
pub trait EmbeddingModelResolver: Send + Sync + 'static {
    /// Whether endpoints of `provider` can serve embeddings through this
    /// resolver.
    fn serves(&self, provider: &AiProvider) -> bool;

    /// Describe one configured model. Performs no I/O and reads no secret.
    fn describe(
        &self,
        endpoint: &AiEndpoint,
        model: &str,
    ) -> Result<EmbeddingModelDescription, AiResolutionError>;

    /// Rebuild the description of a recorded model. Performs no I/O.
    fn describe_recorded(
        &self,
        spec: &AiModelSpec,
    ) -> Result<EmbeddingModelDescription, AiResolutionError>;

    /// Build the model that serves a description, writing `request_timeout` as
    /// the deadline of each call. Called only when a live call first needs it,
    /// never on strict replay.
    fn connect(
        &self,
        endpoint: &AiEndpoint,
        description: &EmbeddingModelDescription,
        request_timeout: Duration,
    ) -> Result<Arc<dyn EmbeddingModel>, AiResolutionError>;
}

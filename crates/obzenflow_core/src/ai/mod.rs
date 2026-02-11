// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Provider-agnostic AI contracts and utilities.
//!
//! This module defines the inner-layer contract surface for LLM integrations:
//! request/response DTOs, client ports, structured output helpers, and stable
//! observability/hash conventions.

mod error;
mod hashing;
mod observability;
mod ports;
mod structured_output;
mod types;

pub use error::{AiClientError, StructuredOutputError};
pub use hashing::{
    params_hash_for_chat, params_hash_for_embedding, prompt_hash_for_chat,
    prompt_hash_for_embedding_inputs, schema_hash_for_response_format, schema_hash_from_json,
    schema_hash_from_text, AiHashError, LLM_HASH_VERSION_SHA256_V1,
};
pub use observability::{
    attach_llm_observability, read_llm_observability, LlmCacheInfo, LlmCacheMode, LlmHashes,
    LlmObservability, LlmObservabilityError, LLM_METADATA_KEY,
};
pub use ports::{ChatClient, EmbeddingClient};
pub use structured_output::{StructuredOutputSchema, StructuredOutputSpec, ValidationHook};
pub use types::{
    AiProvider, ChatMessage, ChatParams, ChatRequest, ChatResponse, ChatResponseFormat, ChatRole,
    EmbeddingParams, EmbeddingRequest, EmbeddingResponse, ToolCall, ToolDefinition, Usage,
    UsageSource,
};

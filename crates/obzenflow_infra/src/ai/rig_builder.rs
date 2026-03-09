// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Rig-backed AI transform builders.
//!
//! This module lives in `obzenflow_infra` because it wires infrastructure-level
//! Rig clients into adapter-layer AI transforms. The top-level `obzenflow::ai`
//! module should remain a facade that only re-exports these types.

use crate::ai::rig::{RigChatClient, RigEmbeddingClient};
use async_trait::async_trait;
use obzenflow_adapters::ai::{
    ai_client_error_to_handler_error_with_context, ChatTransform, EmbeddingTransform,
};
use obzenflow_core::ai::{
    AiClientError, AiProvider, ChatClient, ChatMessage, ChatParams, ChatRequest, ChatResponse,
    ChatResponseFormat, EmbeddingClient, EmbeddingParams, EmbeddingRequest, EmbeddingResponse,
    ToolDefinition,
};
use obzenflow_core::http_client::Url;
use obzenflow_core::ChainEvent;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde_json::Value;
use std::collections::BTreeMap;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, OnceLock};

type ChatOutputMapper = dyn Fn(&ChainEvent, ChatResponse) -> Result<Vec<ChainEvent>, HandlerError>
    + Send
    + Sync
    + 'static;

type EmbeddingOutputMapper = dyn Fn(&ChainEvent, EmbeddingResponse) -> Result<Vec<ChainEvent>, HandlerError>
    + Send
    + Sync
    + 'static;

type UserMessageFn =
    Arc<dyn Fn(&ChainEvent) -> Result<String, HandlerError> + Send + Sync + 'static>;
type MessagesFn =
    Arc<dyn Fn(&ChainEvent) -> Result<Vec<ChatMessage>, HandlerError> + Send + Sync + 'static>;
type TemplateFn =
    Arc<dyn Fn(&ChainEvent) -> Result<ChatRequestTemplate, HandlerError> + Send + Sync + 'static>;
type EmbeddingInputsFn =
    Arc<dyn Fn(&ChainEvent) -> Result<Vec<String>, HandlerError> + Send + Sync + 'static>;

/// Extension trait that provides `ChatTransform::builder()` when Rig-backed AI
/// builder support is enabled.
///
/// The top-level `obzenflow::ai` facade re-exports this trait so callers can
/// keep using `ChatTransform::builder()` without depending on infra details.
pub trait ChatTransformExt {
    fn builder() -> ChatTransformBuilder;
}

impl ChatTransformExt for ChatTransform {
    fn builder() -> ChatTransformBuilder {
        ChatTransformBuilder::new()
    }
}

/// Extension trait that provides `EmbeddingTransform::builder()` when
/// Rig-backed AI builder support is enabled.
///
/// The top-level `obzenflow::ai` facade re-exports this trait so callers can
/// keep using `EmbeddingTransform::builder()` without depending on infra
/// details.
pub trait EmbeddingTransformExt {
    fn builder() -> EmbeddingTransformBuilder;
}

impl EmbeddingTransformExt for EmbeddingTransform {
    fn builder() -> EmbeddingTransformBuilder {
        EmbeddingTransformBuilder::new()
    }
}

/// Everything in a `ChatRequest` except `provider` and `model`.
///
/// The builder fills provider/model from its configured client.
///
/// This type is used by [`ChatTransformBuilder::build_request`] /
/// [`ChatTransformBuilder::build_request_lazy`] as an "escape hatch" for
/// per-event control. The returned template is used as-is for `params`, `tools`,
/// and `response_format` (builder-level settings are ignored for those fields).
///
/// If [`ChatTransformBuilder::system`] was set, it is prepended to
/// `template.messages`.
#[derive(Debug, Clone, Default)]
pub struct ChatRequestTemplate {
    pub messages: Vec<ChatMessage>,
    pub params: ChatParams,
    pub tools: Vec<ToolDefinition>,
    pub response_format: Option<ChatResponseFormat>,
}

#[derive(Debug, Clone)]
enum ChatProviderConfig {
    Ollama {
        model: String,
    },
    OpenAi {
        model: String,
        api_key: String,
    },
    OpenAiCompatible {
        model: String,
        api_key: String,
        base_url: String,
    },
}

#[derive(Debug, Clone)]
enum ResolvedChatProviderConfig {
    Ollama {
        model: String,
        base_url: Option<Url>,
    },
    OpenAi {
        model: String,
        api_key: String,
    },
    OpenAiCompatible {
        model: String,
        api_key: String,
        base_url: Url,
    },
}

impl ResolvedChatProviderConfig {
    fn build_client(&self) -> Result<RigChatClient, AiClientError> {
        match self {
            ResolvedChatProviderConfig::Ollama { model, base_url } => {
                RigChatClient::ollama(model.clone(), base_url.clone())
            }
            ResolvedChatProviderConfig::OpenAi { model, api_key } => {
                RigChatClient::openai(model.clone(), api_key.clone())
            }
            ResolvedChatProviderConfig::OpenAiCompatible {
                model,
                api_key,
                base_url,
            } => RigChatClient::openai_compatible(model.clone(), api_key.clone(), base_url.clone()),
        }
    }
}

#[derive(Clone)]
struct LazyRigChatClient {
    config: ResolvedChatProviderConfig,
    inner: Arc<OnceLock<Result<RigChatClient, AiClientError>>>,
}

impl LazyRigChatClient {
    fn new(config: ResolvedChatProviderConfig) -> Self {
        Self {
            config,
            inner: Arc::new(OnceLock::new()),
        }
    }

    fn get_or_init(&self) -> Result<&RigChatClient, AiClientError> {
        match self.inner.get_or_init(|| {
            match catch_unwind(AssertUnwindSafe(|| self.config.build_client())) {
                Ok(result) => result,
                Err(panic) => {
                    let detail = if let Some(msg) = panic.downcast_ref::<&str>() {
                        msg.to_string()
                    } else if let Some(msg) = panic.downcast_ref::<String>() {
                        msg.clone()
                    } else {
                        "unknown panic".to_string()
                    };

                    Err(AiClientError::Other {
                        message: format!("rig client construction panicked: {detail}"),
                    })
                }
            }
        }) {
            Ok(client) => Ok(client),
            Err(err) => Err(err.clone()),
        }
    }
}

#[async_trait]
impl ChatClient for LazyRigChatClient {
    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, AiClientError> {
        let client = self.get_or_init()?;
        client.chat(req).await
    }
}

#[derive(Clone)]
/// Fluent builder for constructing a [`ChatTransform`] backed by Rig providers.
///
/// This lives in `obzenflow_infra` because provider construction is
/// infrastructure work. The top-level `obzenflow::ai` module re-exports it as
/// part of the public facade.
pub struct ChatTransformBuilder {
    provider: Option<ChatProviderConfig>,
    base_url: Option<String>,
    system: Option<String>,
    params: ChatParams,
    tools: Vec<ToolDefinition>,
    response_format: Option<ChatResponseFormat>,
    output_mapper: Option<Arc<ChatOutputMapper>>,
}

impl Default for ChatTransformBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ChatTransformBuilder {
    /// Create a new builder with no provider selected yet.
    pub fn new() -> Self {
        Self {
            provider: None,
            base_url: None,
            system: None,
            params: ChatParams::default(),
            tools: vec![],
            response_format: None,
            output_mapper: None,
        }
    }

    /// Select an Ollama provider + model (default base URL: `http://localhost:11434`).
    pub fn ollama(mut self, model: impl Into<String>) -> Self {
        self.provider = Some(ChatProviderConfig::Ollama {
            model: model.into(),
        });
        self
    }

    /// Select OpenAI's hosted API with the default base URL.
    pub fn openai(mut self, model: impl Into<String>, api_key: impl Into<String>) -> Self {
        self.provider = Some(ChatProviderConfig::OpenAi {
            model: model.into(),
            api_key: api_key.into(),
        });
        self
    }

    /// Select an OpenAI-compatible endpoint (Groq, Together, vLLM, LM Studio, etc.).
    ///
    /// The `base_url` passed here can be overridden by calling [`Self::base_url`]
    /// later in the chain.
    pub fn openai_compatible(
        mut self,
        model: impl Into<String>,
        api_key: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        self.provider = Some(ChatProviderConfig::OpenAiCompatible {
            model: model.into(),
            api_key: api_key.into(),
            base_url: base_url.into(),
        });
        self
    }

    /// Override the provider base URL.
    ///
    /// - Supported for `.ollama(..)` and `.openai_compatible(..)`
    /// - Rejected for `.openai(..)`
    ///
    /// The URL is parsed during `build()` / `build_lazy()` so callers don't need
    /// to call `Url::parse` manually.
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = Some(base_url.into());
        self
    }

    /// Returns the currently selected provider label (`"ollama"`, `"openai"`, or `"unknown"`).
    pub fn provider_label(&self) -> &str {
        match self.provider.as_ref() {
            Some(ChatProviderConfig::Ollama { .. }) => "ollama",
            Some(
                ChatProviderConfig::OpenAi { .. } | ChatProviderConfig::OpenAiCompatible { .. },
            ) => "openai",
            None => "unknown",
        }
    }

    /// Returns the currently selected model label (or `"unknown"` if unset).
    pub fn model_label(&self) -> &str {
        match self.provider.as_ref() {
            Some(ChatProviderConfig::Ollama { model })
            | Some(ChatProviderConfig::OpenAi { model, .. })
            | Some(ChatProviderConfig::OpenAiCompatible { model, .. }) => model.as_str(),
            None => "unknown",
        }
    }

    /// Set a static system prompt prepended to every request.
    pub fn system(mut self, text: impl Into<String>) -> Self {
        self.system = Some(text.into());
        self
    }

    /// Set `ChatParams.temperature` for `build()` / `build_messages()`.
    pub fn temperature(mut self, temperature: f32) -> Self {
        self.params.temperature = Some(temperature);
        self
    }

    /// Set `ChatParams.max_tokens` for `build()` / `build_messages()`.
    pub fn max_tokens(mut self, max_tokens: u32) -> Self {
        self.params.max_tokens = Some(max_tokens);
        self
    }

    /// Set `ChatParams.top_p` for `build()` / `build_messages()`.
    pub fn top_p(mut self, top_p: f32) -> Self {
        self.params.top_p = Some(top_p);
        self
    }

    /// Set `ChatParams.seed` for `build()` / `build_messages()`.
    pub fn seed(mut self, seed: u64) -> Self {
        self.params.seed = Some(seed);
        self
    }

    /// Set a static response format for `build()` / `build_messages()`.
    pub fn response_format(mut self, response_format: ChatResponseFormat) -> Self {
        self.response_format = Some(response_format);
        self
    }

    /// Set static tool definitions for `build()` / `build_messages()`.
    pub fn tools(mut self, tools: Vec<ToolDefinition>) -> Self {
        self.tools = tools;
        self
    }

    /// Set a provider-specific extra param via `ChatParams.extras`.
    ///
    /// If you also set a typed param (e.g. `.temperature()`, `.top_p()`), the
    /// typed value takes precedence over an extra with the same key when the
    /// request is mapped to the provider (086r behavior).
    pub fn extra_param(mut self, key: impl Into<String>, value: Value) -> Self {
        self.params.extras.insert(key.into(), value);
        self
    }

    /// Override the response-to-event mapping.
    ///
    /// This is forwarded to [`ChatTransform::with_output_mapper`].
    pub fn output_mapper<F>(mut self, mapper: F) -> Self
    where
        F: Fn(&ChainEvent, ChatResponse) -> Result<Vec<ChainEvent>, HandlerError>
            + Send
            + Sync
            + 'static,
    {
        self.output_mapper = Some(Arc::new(mapper));
        self
    }

    /// Build a `ChatTransform` where the closure produces a single user message.
    ///
    /// This performs a provider/model preflight check and fails fast with an
    /// actionable error when the provider is unreachable or the model is missing.
    pub async fn build<F>(self, user_message: F) -> Result<ChatTransform, HandlerError>
    where
        F: Fn(&ChainEvent) -> Result<String, HandlerError> + Send + Sync + 'static,
    {
        let target = self
            .chat_request_target()
            .map_err(|err| err.with_prefix("chat"))?;
        let client = self
            .build_chat_client_checked()
            .await
            .map_err(|err| err.with_prefix("chat"))?;

        self.build_with_client(
            client,
            target.0,
            target.1,
            RequestMode::UserMessage(Arc::new(user_message)),
        )
    }

    /// Build a `ChatTransform` without preflight.
    ///
    /// This still validates builder state (provider selected, `base_url` parses),
    /// but the underlying provider client is constructed lazily on the first
    /// inference call.
    pub fn build_lazy<F>(self, user_message: F) -> Result<ChatTransform, HandlerError>
    where
        F: Fn(&ChainEvent) -> Result<String, HandlerError> + Send + Sync + 'static,
    {
        let target = self
            .chat_request_target()
            .map_err(|err| err.with_prefix("chat"))?;
        let client = self
            .build_chat_client_lazy()
            .map_err(|err| err.with_prefix("chat"))?;

        self.build_with_client(
            client,
            target.0,
            target.1,
            RequestMode::UserMessage(Arc::new(user_message)),
        )
    }

    /// Build a `ChatTransform` where the closure returns the full message list.
    ///
    /// If [`Self::system`] is set, it is prepended to the returned messages.
    pub async fn build_messages<F>(self, messages: F) -> Result<ChatTransform, HandlerError>
    where
        F: Fn(&ChainEvent) -> Result<Vec<ChatMessage>, HandlerError> + Send + Sync + 'static,
    {
        let target = self
            .chat_request_target()
            .map_err(|err| err.with_prefix("chat"))?;
        let client = self
            .build_chat_client_checked()
            .await
            .map_err(|err| err.with_prefix("chat"))?;

        self.build_with_client(
            client,
            target.0,
            target.1,
            RequestMode::Messages(Arc::new(messages)),
        )
    }

    /// Lazy counterpart of [`Self::build_messages`] (no preflight).
    pub fn build_messages_lazy<F>(self, messages: F) -> Result<ChatTransform, HandlerError>
    where
        F: Fn(&ChainEvent) -> Result<Vec<ChatMessage>, HandlerError> + Send + Sync + 'static,
    {
        let target = self
            .chat_request_target()
            .map_err(|err| err.with_prefix("chat"))?;
        let client = self
            .build_chat_client_lazy()
            .map_err(|err| err.with_prefix("chat"))?;

        self.build_with_client(
            client,
            target.0,
            target.1,
            RequestMode::Messages(Arc::new(messages)),
        )
    }

    /// Build a `ChatTransform` with per-event control over params/tools/response format.
    ///
    /// The closure returns a [`ChatRequestTemplate`]. The template's `params`,
    /// `tools`, and `response_format` are used as-is (builder-level settings are
    /// ignored for those fields). If [`Self::system`] is set, it is prepended
    /// to `template.messages`.
    pub async fn build_request<F>(self, template: F) -> Result<ChatTransform, HandlerError>
    where
        F: Fn(&ChainEvent) -> Result<ChatRequestTemplate, HandlerError> + Send + Sync + 'static,
    {
        let target = self
            .chat_request_target()
            .map_err(|err| err.with_prefix("chat"))?;
        let client = self
            .build_chat_client_checked()
            .await
            .map_err(|err| err.with_prefix("chat"))?;

        self.build_with_client(
            client,
            target.0,
            target.1,
            RequestMode::Template(Arc::new(template)),
        )
    }

    /// Lazy counterpart of [`Self::build_request`] (no preflight).
    pub fn build_request_lazy<F>(self, template: F) -> Result<ChatTransform, HandlerError>
    where
        F: Fn(&ChainEvent) -> Result<ChatRequestTemplate, HandlerError> + Send + Sync + 'static,
    {
        let client = self
            .build_chat_client_lazy()
            .map_err(|err| err.with_prefix("chat"))?;

        let target = self
            .chat_request_target()
            .map_err(|err| err.with_prefix("chat"))?;

        self.build_with_client(
            client,
            target.0,
            target.1,
            RequestMode::Template(Arc::new(template)),
        )
    }

    fn chat_request_target(&self) -> Result<(AiProvider, String), HandlerError> {
        let Some(config) = &self.provider else {
            return Err(HandlerError::Validation(
                "AI chat builder requires a provider selection: call `.ollama(..)`, `.openai(..)`, or `.openai_compatible(..)`"
                    .to_string(),
            ));
        };

        match config {
            ChatProviderConfig::Ollama { model } => Ok((AiProvider::new("ollama"), model.clone())),
            ChatProviderConfig::OpenAi { model, .. }
            | ChatProviderConfig::OpenAiCompatible { model, .. } => {
                Ok((AiProvider::new("openai"), model.clone()))
            }
        }
    }

    fn resolve_chat_provider_config(&self) -> Result<ResolvedChatProviderConfig, HandlerError> {
        let Some(config) = self.provider.clone() else {
            return Err(HandlerError::Validation(
                "AI chat builder requires a provider selection: call `.ollama(..)`, `.openai(..)`, or `.openai_compatible(..)`"
                    .to_string(),
            ));
        };

        match config {
            ChatProviderConfig::Ollama { model } => {
                let base_url = self
                    .base_url
                    .as_deref()
                    .map(|s| parse_url(s, "base_url"))
                    .transpose()?;

                Ok(ResolvedChatProviderConfig::Ollama { model, base_url })
            }
            ChatProviderConfig::OpenAi { model, api_key } => {
                if self.base_url.is_some() {
                    return Err(HandlerError::Validation(
                        "`base_url` is only supported for `.ollama(..)` and `.openai_compatible(..)`"
                            .to_string(),
                    ));
                }

                Ok(ResolvedChatProviderConfig::OpenAi { model, api_key })
            }
            ChatProviderConfig::OpenAiCompatible {
                model,
                api_key,
                base_url,
            } => {
                let base_url = self.base_url.as_deref().unwrap_or(base_url.as_str());
                let base_url = parse_url(base_url, "base_url")?;

                Ok(ResolvedChatProviderConfig::OpenAiCompatible {
                    model,
                    api_key,
                    base_url,
                })
            }
        }
    }

    async fn build_chat_client_checked(&self) -> Result<Arc<dyn ChatClient>, HandlerError> {
        let Some(config) = self.provider.clone() else {
            return Err(HandlerError::Validation(
                "AI chat builder requires a provider selection: call `.ollama(..)`, `.openai(..)`, or `.openai_compatible(..)`"
                    .to_string(),
            ));
        };

        let client = match config {
            ChatProviderConfig::Ollama { model } => {
                let base_url = self
                    .base_url
                    .as_deref()
                    .map(|s| parse_url(s, "base_url"))
                    .transpose()?;

                RigChatClient::ollama_checked(model, base_url).await
            }
            ChatProviderConfig::OpenAi { model, api_key } => {
                if self.base_url.is_some() {
                    return Err(HandlerError::Validation(
                        "`base_url` is only supported for `.ollama(..)` and `.openai_compatible(..)`"
                            .to_string(),
                    ));
                }

                RigChatClient::openai_checked(model, api_key).await
            }
            ChatProviderConfig::OpenAiCompatible {
                model,
                api_key,
                base_url,
            } => {
                let base_url = self.base_url.as_deref().unwrap_or(base_url.as_str());
                let base_url = parse_url(base_url, "base_url")?;

                RigChatClient::openai_compatible_checked(model, api_key, base_url).await
            }
        }
        .map_err(|err| ai_client_error_to_handler_error_with_context(err, Some("preflight")))?;

        Ok(Arc::new(client))
    }

    fn build_chat_client_lazy(&self) -> Result<Arc<dyn ChatClient>, HandlerError> {
        let resolved = self.resolve_chat_provider_config()?;
        Ok(Arc::new(LazyRigChatClient::new(resolved)))
    }

    fn build_with_client(
        self,
        client: Arc<dyn ChatClient>,
        provider: AiProvider,
        model: String,
        mode: RequestMode,
    ) -> Result<ChatTransform, HandlerError> {
        let ChatTransformBuilder {
            provider: _,
            base_url: _,
            system,
            params,
            tools,
            response_format,
            output_mapper,
        } = self;

        let request_builder = move |event: &ChainEvent| match &mode {
            RequestMode::UserMessage(user_message) => {
                let user_message = user_message(event)?;
                let mut messages = Vec::with_capacity(2);
                if let Some(system) = &system {
                    messages.push(ChatMessage::system(system.clone()));
                }
                messages.push(ChatMessage::user(user_message));

                Ok(ChatRequest {
                    provider: provider.clone(),
                    model: model.clone(),
                    messages,
                    params: params.clone(),
                    tools: tools.clone(),
                    response_format: response_format.clone(),
                })
            }
            RequestMode::Messages(messages_builder) => {
                let mut messages = messages_builder(event)?;
                if let Some(system) = &system {
                    messages.insert(0, ChatMessage::system(system.clone()));
                }

                Ok(ChatRequest {
                    provider: provider.clone(),
                    model: model.clone(),
                    messages,
                    params: params.clone(),
                    tools: tools.clone(),
                    response_format: response_format.clone(),
                })
            }
            RequestMode::Template(template_builder) => {
                let template = template_builder(event)?;
                let ChatRequestTemplate {
                    mut messages,
                    params: template_params,
                    tools: template_tools,
                    response_format: template_response_format,
                } = template;

                if let Some(system) = &system {
                    messages.insert(0, ChatMessage::system(system.clone()));
                }

                Ok(ChatRequest {
                    provider: provider.clone(),
                    model: model.clone(),
                    messages,
                    params: template_params,
                    tools: template_tools,
                    response_format: template_response_format,
                })
            }
        };

        let mut transform = ChatTransform::new(client, request_builder);

        if let Some(output_mapper) = output_mapper {
            transform = transform
                .with_output_mapper(move |event, response| (output_mapper)(event, response));
        }

        Ok(transform)
    }
}

enum RequestMode {
    UserMessage(UserMessageFn),
    Messages(MessagesFn),
    Template(TemplateFn),
}

fn parse_url(value: &str, context: &str) -> Result<Url, HandlerError> {
    Url::parse(value.trim()).map_err(|err| {
        HandlerError::Validation(format!(
            "invalid {context}: expected a URL, got '{value}': {err}"
        ))
    })
}

trait HandlerErrorExt {
    fn with_prefix(self, prefix: &str) -> HandlerError;
}

impl HandlerErrorExt for HandlerError {
    fn with_prefix(self, prefix: &str) -> HandlerError {
        let prefix = prefix.trim();
        if prefix.is_empty() {
            return self;
        }

        match self {
            HandlerError::Timeout(msg) => HandlerError::Timeout(format!("{prefix}: {msg}")),
            HandlerError::Remote(msg) => HandlerError::Remote(format!("{prefix}: {msg}")),
            HandlerError::RateLimited {
                message,
                retry_after,
            } => HandlerError::RateLimited {
                message: format!("{prefix}: {message}"),
                retry_after,
            },
            HandlerError::PermanentFailure(msg) => {
                HandlerError::PermanentFailure(format!("{prefix}: {msg}"))
            }
            HandlerError::Deserialization(msg) => {
                HandlerError::Deserialization(format!("{prefix}: {msg}"))
            }
            HandlerError::Validation(msg) => HandlerError::Validation(format!("{prefix}: {msg}")),
            HandlerError::Domain(msg) => HandlerError::Domain(format!("{prefix}: {msg}")),
            HandlerError::Other(msg) => HandlerError::Other(format!("{prefix}: {msg}")),
        }
    }
}

#[derive(Debug, Clone)]
enum EmbeddingProviderConfig {
    Ollama {
        model: String,
    },
    OpenAi {
        model: String,
        api_key: String,
    },
    OpenAiCompatible {
        model: String,
        api_key: String,
        base_url: String,
    },
}

#[derive(Debug, Clone)]
enum ResolvedEmbeddingProviderConfig {
    Ollama {
        model: String,
        base_url: Option<Url>,
        dimensions: Option<usize>,
    },
    OpenAi {
        model: String,
        api_key: String,
        dimensions: Option<usize>,
    },
    OpenAiCompatible {
        model: String,
        api_key: String,
        base_url: Url,
        dimensions: Option<usize>,
    },
}

impl ResolvedEmbeddingProviderConfig {
    fn build_client(&self) -> Result<RigEmbeddingClient, AiClientError> {
        match self {
            ResolvedEmbeddingProviderConfig::Ollama {
                model,
                base_url,
                dimensions,
            } => RigEmbeddingClient::ollama(model.clone(), base_url.clone(), *dimensions),
            ResolvedEmbeddingProviderConfig::OpenAi {
                model,
                api_key,
                dimensions,
            } => RigEmbeddingClient::openai(model.clone(), api_key.clone(), *dimensions),
            ResolvedEmbeddingProviderConfig::OpenAiCompatible {
                model,
                api_key,
                base_url,
                dimensions,
            } => RigEmbeddingClient::openai_compatible(
                model.clone(),
                api_key.clone(),
                base_url.clone(),
                *dimensions,
            ),
        }
    }
}

#[derive(Clone)]
struct LazyRigEmbeddingClient {
    config: ResolvedEmbeddingProviderConfig,
    inner: Arc<OnceLock<Result<RigEmbeddingClient, AiClientError>>>,
}

impl LazyRigEmbeddingClient {
    fn new(config: ResolvedEmbeddingProviderConfig) -> Self {
        Self {
            config,
            inner: Arc::new(OnceLock::new()),
        }
    }

    fn get_or_init(&self) -> Result<&RigEmbeddingClient, AiClientError> {
        match self.inner.get_or_init(|| {
            match catch_unwind(AssertUnwindSafe(|| self.config.build_client())) {
                Ok(result) => result,
                Err(panic) => {
                    let detail = if let Some(msg) = panic.downcast_ref::<&str>() {
                        msg.to_string()
                    } else if let Some(msg) = panic.downcast_ref::<String>() {
                        msg.clone()
                    } else {
                        "unknown panic".to_string()
                    };

                    Err(AiClientError::Other {
                        message: format!("rig client construction panicked: {detail}"),
                    })
                }
            }
        }) {
            Ok(client) => Ok(client),
            Err(err) => Err(err.clone()),
        }
    }
}

#[async_trait]
impl EmbeddingClient for LazyRigEmbeddingClient {
    async fn embed(&self, req: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError> {
        let client = self.get_or_init()?;
        client.embed(req).await
    }
}

#[derive(Clone)]
/// Fluent builder for constructing an [`EmbeddingTransform`] backed by Rig providers.
pub struct EmbeddingTransformBuilder {
    provider: Option<EmbeddingProviderConfig>,
    base_url: Option<String>,
    dimensions: Option<usize>,
    output_mapper: Option<Arc<EmbeddingOutputMapper>>,
}

impl Default for EmbeddingTransformBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EmbeddingTransformBuilder {
    /// Create a new builder with no provider selected yet.
    pub fn new() -> Self {
        Self {
            provider: None,
            base_url: None,
            dimensions: None,
            output_mapper: None,
        }
    }

    /// Select an Ollama provider + model (default base URL: `http://localhost:11434`).
    pub fn ollama(mut self, model: impl Into<String>) -> Self {
        self.provider = Some(EmbeddingProviderConfig::Ollama {
            model: model.into(),
        });
        self
    }

    /// Select OpenAI's hosted API with the default base URL.
    pub fn openai(mut self, model: impl Into<String>, api_key: impl Into<String>) -> Self {
        self.provider = Some(EmbeddingProviderConfig::OpenAi {
            model: model.into(),
            api_key: api_key.into(),
        });
        self
    }

    /// Select an OpenAI-compatible endpoint (Groq, Together, vLLM, LM Studio, etc.).
    ///
    /// The `base_url` passed here can be overridden by calling [`Self::base_url`]
    /// later in the chain.
    pub fn openai_compatible(
        mut self,
        model: impl Into<String>,
        api_key: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        self.provider = Some(EmbeddingProviderConfig::OpenAiCompatible {
            model: model.into(),
            api_key: api_key.into(),
            base_url: base_url.into(),
        });
        self
    }

    /// Override the provider base URL.
    ///
    /// - Supported for `.ollama(..)` and `.openai_compatible(..)`
    /// - Rejected for `.openai(..)`
    ///
    /// The URL is parsed during `build()` / `build_lazy()`.
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = Some(base_url.into());
        self
    }

    /// Returns the currently selected provider label (`"ollama"`, `"openai"`, or `"unknown"`).
    pub fn provider_label(&self) -> &str {
        match self.provider.as_ref() {
            Some(EmbeddingProviderConfig::Ollama { .. }) => "ollama",
            Some(
                EmbeddingProviderConfig::OpenAi { .. }
                | EmbeddingProviderConfig::OpenAiCompatible { .. },
            ) => "openai",
            None => "unknown",
        }
    }

    /// Returns the currently selected model label (or `"unknown"` if unset).
    pub fn model_label(&self) -> &str {
        match self.provider.as_ref() {
            Some(EmbeddingProviderConfig::Ollama { model })
            | Some(EmbeddingProviderConfig::OpenAi { model, .. })
            | Some(EmbeddingProviderConfig::OpenAiCompatible { model, .. }) => model.as_str(),
            None => "unknown",
        }
    }

    /// Set the embedding vector dimensionality.
    ///
    /// This value is used both to configure the provider client (where supported)
    /// and to set `EmbeddingRequest.params.dimensions` for hash/observability
    /// consistency.
    pub fn dimensions(mut self, dimensions: usize) -> Self {
        self.dimensions = Some(dimensions);
        self
    }

    /// Override the response-to-event mapping.
    ///
    /// This is forwarded to [`EmbeddingTransform::with_output_mapper`].
    pub fn output_mapper<F>(mut self, mapper: F) -> Self
    where
        F: Fn(&ChainEvent, EmbeddingResponse) -> Result<Vec<ChainEvent>, HandlerError>
            + Send
            + Sync
            + 'static,
    {
        self.output_mapper = Some(Arc::new(mapper));
        self
    }

    /// Build an `EmbeddingTransform` with provider/model preflight.
    pub async fn build<F>(self, inputs: F) -> Result<EmbeddingTransform, HandlerError>
    where
        F: Fn(&ChainEvent) -> Result<Vec<String>, HandlerError> + Send + Sync + 'static,
    {
        let target = self
            .embedding_request_target()
            .map_err(|err| err.with_prefix("embedding"))?;
        let client = self
            .build_embedding_client_checked()
            .await
            .map_err(|err| err.with_prefix("embedding"))?;

        self.build_with_client(client, target.0, target.1, Arc::new(inputs))
    }

    /// Build an `EmbeddingTransform` without preflight.
    ///
    /// This still validates builder state, but the provider client is
    /// constructed lazily on the first inference call.
    pub fn build_lazy<F>(self, inputs: F) -> Result<EmbeddingTransform, HandlerError>
    where
        F: Fn(&ChainEvent) -> Result<Vec<String>, HandlerError> + Send + Sync + 'static,
    {
        let target = self
            .embedding_request_target()
            .map_err(|err| err.with_prefix("embedding"))?;
        let client = self
            .build_embedding_client_lazy()
            .map_err(|err| err.with_prefix("embedding"))?;

        self.build_with_client(client, target.0, target.1, Arc::new(inputs))
    }

    fn embedding_request_target(&self) -> Result<(AiProvider, String), HandlerError> {
        let Some(config) = &self.provider else {
            return Err(HandlerError::Validation(
                "AI embedding builder requires a provider selection: call `.ollama(..)`, `.openai(..)`, or `.openai_compatible(..)`"
                    .to_string(),
            ));
        };

        match config {
            EmbeddingProviderConfig::Ollama { model } => {
                Ok((AiProvider::new("ollama"), model.clone()))
            }
            EmbeddingProviderConfig::OpenAi { model, .. }
            | EmbeddingProviderConfig::OpenAiCompatible { model, .. } => {
                Ok((AiProvider::new("openai"), model.clone()))
            }
        }
    }

    fn resolve_embedding_provider_config(
        &self,
    ) -> Result<ResolvedEmbeddingProviderConfig, HandlerError> {
        let Some(config) = self.provider.clone() else {
            return Err(HandlerError::Validation(
                "AI embedding builder requires a provider selection: call `.ollama(..)`, `.openai(..)`, or `.openai_compatible(..)`"
                    .to_string(),
            ));
        };

        match config {
            EmbeddingProviderConfig::Ollama { model } => {
                let base_url = self
                    .base_url
                    .as_deref()
                    .map(|s| parse_url(s, "base_url"))
                    .transpose()?;

                Ok(ResolvedEmbeddingProviderConfig::Ollama {
                    model,
                    base_url,
                    dimensions: self.dimensions,
                })
            }
            EmbeddingProviderConfig::OpenAi { model, api_key } => {
                if self.base_url.is_some() {
                    return Err(HandlerError::Validation(
                        "`base_url` is only supported for `.ollama(..)` and `.openai_compatible(..)`"
                            .to_string(),
                    ));
                }

                Ok(ResolvedEmbeddingProviderConfig::OpenAi {
                    model,
                    api_key,
                    dimensions: self.dimensions,
                })
            }
            EmbeddingProviderConfig::OpenAiCompatible {
                model,
                api_key,
                base_url,
            } => {
                let base_url = self.base_url.as_deref().unwrap_or(base_url.as_str());
                let base_url = parse_url(base_url, "base_url")?;

                Ok(ResolvedEmbeddingProviderConfig::OpenAiCompatible {
                    model,
                    api_key,
                    base_url,
                    dimensions: self.dimensions,
                })
            }
        }
    }

    async fn build_embedding_client_checked(
        &self,
    ) -> Result<Arc<dyn EmbeddingClient>, HandlerError> {
        let Some(config) = self.provider.clone() else {
            return Err(HandlerError::Validation(
                "AI embedding builder requires a provider selection: call `.ollama(..)`, `.openai(..)`, or `.openai_compatible(..)`"
                    .to_string(),
            ));
        };

        let client = match config {
            EmbeddingProviderConfig::Ollama { model } => {
                let base_url = self
                    .base_url
                    .as_deref()
                    .map(|s| parse_url(s, "base_url"))
                    .transpose()?;

                RigEmbeddingClient::ollama_checked(model, base_url, self.dimensions).await
            }
            EmbeddingProviderConfig::OpenAi { model, api_key } => {
                if self.base_url.is_some() {
                    return Err(HandlerError::Validation(
                        "`base_url` is only supported for `.ollama(..)` and `.openai_compatible(..)`"
                            .to_string(),
                    ));
                }

                RigEmbeddingClient::openai_checked(model, api_key, self.dimensions).await
            }
            EmbeddingProviderConfig::OpenAiCompatible {
                model,
                api_key,
                base_url,
            } => {
                let base_url = self.base_url.as_deref().unwrap_or(base_url.as_str());
                let base_url = parse_url(base_url, "base_url")?;

                RigEmbeddingClient::openai_compatible_checked(
                    model,
                    api_key,
                    base_url,
                    self.dimensions,
                )
                .await
            }
        }
        .map_err(|err| ai_client_error_to_handler_error_with_context(err, Some("preflight")))?;

        Ok(Arc::new(client))
    }

    fn build_embedding_client_lazy(&self) -> Result<Arc<dyn EmbeddingClient>, HandlerError> {
        let resolved = self.resolve_embedding_provider_config()?;
        Ok(Arc::new(LazyRigEmbeddingClient::new(resolved)))
    }

    fn build_with_client(
        self,
        client: Arc<dyn EmbeddingClient>,
        provider: AiProvider,
        model: String,
        inputs: EmbeddingInputsFn,
    ) -> Result<EmbeddingTransform, HandlerError> {
        let dimensions = self.dimensions;

        let EmbeddingTransformBuilder {
            provider: _,
            base_url: _,
            dimensions: _,
            output_mapper,
        } = self;

        let request_builder = move |event: &ChainEvent| {
            let inputs = inputs(event)?;
            let params = EmbeddingParams {
                dimensions,
                extras: BTreeMap::new(),
            };

            Ok(EmbeddingRequest {
                provider: provider.clone(),
                model: model.clone(),
                inputs,
                params,
            })
        };

        let mut transform = EmbeddingTransform::new(client, request_builder);
        if let Some(output_mapper) = output_mapper {
            transform = transform
                .with_output_mapper(move |event, response| (output_mapper)(event, response));
        }

        Ok(transform)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::{AiClientError, ChatResponseFormat, ToolDefinition};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use obzenflow_runtime::stages::common::handlers::AsyncTransformHandler;
    use serde_json::json;
    use std::sync::Mutex;

    #[derive(Debug, Default)]
    struct RecordingChatClient {
        last_request: Mutex<Option<ChatRequest>>,
    }

    impl RecordingChatClient {
        fn take_last_request(&self) -> ChatRequest {
            self.last_request
                .lock()
                .expect("poisoned")
                .take()
                .expect("request should be recorded")
        }
    }

    #[async_trait]
    impl ChatClient for RecordingChatClient {
        async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, AiClientError> {
            *self.last_request.lock().expect("poisoned") = Some(req);
            Ok(ChatResponse {
                text: "ok".to_string(),
                tool_calls: vec![],
                usage: None,
                raw: None,
            })
        }
    }

    #[derive(Debug, Default)]
    struct RecordingEmbeddingClient {
        last_request: Mutex<Option<EmbeddingRequest>>,
    }

    impl RecordingEmbeddingClient {
        fn take_last_request(&self) -> EmbeddingRequest {
            self.last_request
                .lock()
                .expect("poisoned")
                .take()
                .expect("request should be recorded")
        }
    }

    #[async_trait]
    impl EmbeddingClient for RecordingEmbeddingClient {
        async fn embed(&self, req: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError> {
            *self.last_request.lock().expect("poisoned") = Some(req);
            Ok(EmbeddingResponse {
                vectors: vec![vec![0.1, 0.2, 0.3]],
                vector_dim: 3,
                usage: None,
                raw: None,
            })
        }
    }

    fn tool(name: &str) -> ToolDefinition {
        ToolDefinition {
            name: name.to_string(),
            description: None,
            parameters_schema: None,
        }
    }

    fn test_event() -> ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"x": 1}),
        )
    }

    #[test]
    fn chat_builder_build_lazy_constructs_ollama_transform() {
        let _transform = ChatTransform::builder()
            .ollama("llama3.1:8b")
            .system("x")
            .temperature(0.2)
            .response_format(ChatResponseFormat::Text)
            .build_lazy(|_event| Ok("hi".to_string()))
            .expect("builder should succeed");
    }

    #[test]
    fn chat_builder_build_messages_lazy_constructs_transform() {
        let _transform = ChatTransform::builder()
            .ollama("llama3.1:8b")
            .system("x")
            .build_messages_lazy(|_event| Ok(vec![ChatMessage::user("hi")]))
            .expect("builder should succeed");
    }

    #[test]
    fn chat_builder_build_request_lazy_constructs_transform() {
        let _transform = ChatTransform::builder()
            .ollama("llama3.1:8b")
            .system("x")
            .build_request_lazy(|_event| {
                Ok(ChatRequestTemplate {
                    messages: vec![ChatMessage::user("hi")],
                    ..Default::default()
                })
            })
            .expect("builder should succeed");
    }

    #[test]
    fn embedding_builder_build_lazy_constructs_ollama_transform() {
        let _transform = EmbeddingTransform::builder()
            .ollama("nomic-embed-text")
            .dimensions(768)
            .build_lazy(|_event| Ok(vec!["hello".to_string()]))
            .expect("builder should succeed");
    }

    #[tokio::test]
    async fn chat_builder_system_is_prepended_for_messages_mode() {
        let client = Arc::new(RecordingChatClient::default());

        let transform = ChatTransformBuilder::new()
            .system("SYS")
            .build_with_client(
                client.clone(),
                AiProvider::new("ollama"),
                "llama3.1:8b".to_string(),
                RequestMode::Messages(Arc::new(|_event| Ok(vec![ChatMessage::user("USER")]))),
            )
            .expect("builder should succeed");

        transform
            .process(test_event())
            .await
            .expect("transform should succeed");

        let req = client.take_last_request();
        assert_eq!(
            req.messages,
            vec![ChatMessage::system("SYS"), ChatMessage::user("USER")]
        );
    }

    #[tokio::test]
    async fn chat_builder_template_wins_over_builder_params_tools_and_response_format() {
        let client = Arc::new(RecordingChatClient::default());

        let transform = ChatTransformBuilder::new()
            .system("SYS")
            .temperature(0.2)
            .max_tokens(800)
            .tools(vec![tool("a")])
            .response_format(ChatResponseFormat::JsonObject)
            .build_with_client(
                client.clone(),
                AiProvider::new("ollama"),
                "llama3.1:8b".to_string(),
                RequestMode::Template(Arc::new(|_event| {
                    let params = ChatParams {
                        temperature: Some(0.9),
                        top_p: Some(0.5),
                        ..Default::default()
                    };

                    Ok(ChatRequestTemplate {
                        messages: vec![ChatMessage::user("USER")],
                        params,
                        tools: vec![tool("b")],
                        response_format: None,
                    })
                })),
            )
            .expect("builder should succeed");

        transform
            .process(test_event())
            .await
            .expect("transform should succeed");

        let req = client.take_last_request();
        assert_eq!(
            req.messages,
            vec![ChatMessage::system("SYS"), ChatMessage::user("USER")]
        );
        assert_eq!(req.params.temperature, Some(0.9));
        assert_eq!(req.params.max_tokens, None);
        assert_eq!(req.params.top_p, Some(0.5));
        assert_eq!(req.tools.len(), 1);
        assert_eq!(req.tools[0].name, "b");
        assert_eq!(req.response_format, None);
    }

    #[tokio::test]
    async fn embedding_builder_dimensions_flow_to_request_params() {
        let client = Arc::new(RecordingEmbeddingClient::default());

        let transform = EmbeddingTransformBuilder::new()
            .dimensions(768)
            .build_with_client(
                client.clone(),
                AiProvider::new("ollama"),
                "nomic-embed-text".to_string(),
                Arc::new(|_event| Ok(vec!["hello".to_string()])),
            )
            .expect("builder should succeed");

        transform
            .process(test_event())
            .await
            .expect("transform should succeed");

        let req = client.take_last_request();
        assert_eq!(req.params.dimensions, Some(768));
    }

    #[test]
    fn chat_builder_build_lazy_constructs_openai_compatible_transform() {
        let _transform = ChatTransform::builder()
            .openai_compatible("mixtral-8x7b", "sk-test", "http://localhost:9999/v1")
            .build_lazy(|_event| Ok("hi".to_string()))
            .expect("builder should succeed");
    }

    #[test]
    fn chat_builder_rejects_base_url_for_openai_default() {
        let err = ChatTransform::builder()
            .openai("gpt-4.1-mini", "sk-test")
            .base_url("http://localhost:9999")
            .build_lazy(|_event| Ok("hi".to_string()))
            .expect_err("builder should reject base_url for openai default");

        assert!(matches!(err, HandlerError::Validation(_)));
    }
}

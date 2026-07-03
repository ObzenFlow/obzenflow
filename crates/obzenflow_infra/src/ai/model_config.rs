// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! `ModelConfig` is a small facade for AI example wiring.
//!
//! It centralises provider/model/env resolution and estimator attachment so
//! examples do not repeat infrastructure boilerplate.

use super::{resolve_chat_model_profile, ChatTransformBuilder};
use anyhow::anyhow;
use obzenflow_adapters::ai::ChatTransform;
use obzenflow_core::ai::{
    ChatModelProfile, ChatResponse, ChatResponseFormat, ChunkInfo, ResolvedTokenEstimator,
    TokenCount, TokenEstimator, ToolDefinition, UserPrompt,
};
use obzenflow_core::http_client::Url;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::Value;
use std::sync::Arc;

const ENV_OPENAI_API_KEY: &str = "OPENAI_API_KEY";
const ENV_OPENAI_BASE_URL: &str = "OPENAI_BASE_URL";
const ENV_OLLAMA_BASE_URL: &str = "OLLAMA_BASE_URL";

const DEFAULT_PROVIDER: &str = "ollama";
const DEFAULT_MODEL_OLLAMA: &str = "llama3.1:8b";
const DEFAULT_MODEL_OPENAI: &str = "gpt-4.1-mini";
const DEFAULT_MODEL_OPENAI_COMPATIBLE: &str = "llama3.1:8b";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderKind {
    Ollama,
    OpenAi,
    OpenAiCompatible,
}

impl ProviderKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ollama => "ollama",
            Self::OpenAi => "openai",
            Self::OpenAiCompatible => "openai_compatible",
        }
    }

    fn default_model(self) -> &'static str {
        match self {
            Self::Ollama => DEFAULT_MODEL_OLLAMA,
            Self::OpenAi => DEFAULT_MODEL_OPENAI,
            Self::OpenAiCompatible => DEFAULT_MODEL_OPENAI_COMPATIBLE,
        }
    }
}

#[derive(Debug, Clone)]
enum ProviderConfig {
    Ollama { base_url: Option<String> },
    OpenAi { api_key: String },
    OpenAiCompatible { api_key: String, base_url: String },
}

#[derive(Debug, Clone)]
pub struct ModelConfig {
    profile: ChatModelProfile,
    provider: ProviderConfig,
}

impl ModelConfig {
    pub fn ollama(model: impl Into<String>) -> Self {
        let model = model.into();
        let profile = resolve_chat_model_profile(model.as_str());
        Self {
            profile,
            provider: ProviderConfig::Ollama { base_url: None },
        }
    }

    pub fn openai(model: impl Into<String>, api_key: impl Into<String>) -> Self {
        let model = model.into();
        let profile = resolve_chat_model_profile(model.as_str());
        Self {
            profile,
            provider: ProviderConfig::OpenAi {
                api_key: api_key.into(),
            },
        }
    }

    pub fn openai_compatible(
        model: impl Into<String>,
        api_key: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        let model = model.into();
        let profile = resolve_chat_model_profile(model.as_str());
        Self {
            profile,
            provider: ProviderConfig::OpenAiCompatible {
                api_key: api_key.into(),
                base_url: base_url.into(),
            },
        }
    }

    /// Build from the resolved `ai.models.*` section (FLOWIP-010). The
    /// provider token is registry-validated; the API key resolves through
    /// the visible `SecretRef` name at this point of use only.
    pub fn from_config(
        config: &obzenflow_runtime::runtime_config::AiModelsConfig,
    ) -> anyhow::Result<Self> {
        let provider_raw = config.provider.value.as_str();
        let provider_kind = parse_provider(provider_raw).ok_or_else(|| {
            anyhow!("unsupported ai.models.provider='{provider_raw}' (expected 'ollama', 'openai', or 'openai_compatible')")
        })?;

        let model = config
            .model
            .as_ref()
            .map(|resolved| resolved.value.clone())
            .unwrap_or_else(|| provider_kind.default_model().to_string());
        let profile = resolve_chat_model_profile(model.as_str());

        let base_url = config
            .base_url
            .as_ref()
            .map(|resolved| resolved.value.clone());
        let resolve_api_key = || -> anyhow::Result<String> {
            let secret = config.api_key_env.value.resolve().map_err(|err| {
                anyhow!(
                    "ai.models.api_key_env: {err} (required when ai.models.provider={})",
                    provider_kind.as_str()
                )
            })?;
            Ok(secret.expose().to_string())
        };

        let provider = match provider_kind {
            ProviderKind::Ollama => {
                if let Some(url) = &base_url {
                    Url::parse(url.as_str())
                        .map_err(|err| anyhow!("invalid ai.models.base_url: {err}"))?;
                }
                ProviderConfig::Ollama { base_url }
            }
            ProviderKind::OpenAi => ProviderConfig::OpenAi {
                api_key: resolve_api_key()?,
            },
            ProviderKind::OpenAiCompatible => {
                let base_url = base_url.ok_or_else(|| {
                    anyhow!(
                        "ai.models.base_url is required when ai.models.provider={}",
                        provider_kind.as_str()
                    )
                })?;
                Url::parse(base_url.as_str())
                    .map_err(|err| anyhow!("invalid ai.models.base_url: {err}"))?;

                ProviderConfig::OpenAiCompatible {
                    api_key: resolve_api_key()?,
                    base_url,
                }
            }
        };

        Ok(Self { profile, provider })
    }

    /// Build from example-local environment names (`<PREFIX>PROVIDER`,
    /// `<PREFIX>MODEL`, plus the standard `OPENAI_API_KEY` /
    /// `OPENAI_BASE_URL` / `OLLAMA_BASE_URL` value variables). Framework
    /// configuration goes through `from_config`; the killed
    /// `OBZENFLOW_AI_*` fallbacks are gone.
    pub fn from_env_with_prefix(prefix: &str) -> anyhow::Result<Self> {
        let provider_var = prefixed_env_name(Some(prefix), "PROVIDER");
        let model_var = prefixed_env_name(Some(prefix), "MODEL");

        let (provider_var_name, provider_raw) =
            resolve_named_value(provider_var.as_deref(), DEFAULT_PROVIDER);

        let provider_kind = parse_provider(provider_raw.as_str()).ok_or_else(|| {
            anyhow!("unsupported {provider_var_name}='{provider_raw}' (expected 'ollama', 'openai', or 'openai_compatible')")
        })?;

        let (_model_var_name, model) =
            resolve_named_value(model_var.as_deref(), provider_kind.default_model());

        let profile = resolve_chat_model_profile(model.as_str());

        let provider = match provider_kind {
            ProviderKind::Ollama => {
                let base_url = env_value(ENV_OLLAMA_BASE_URL).map(|value| {
                    Url::parse(value.as_str())
                        .map(|_| value)
                        .map_err(|err| anyhow!("invalid {ENV_OLLAMA_BASE_URL}: {err}"))
                });
                let base_url = base_url.transpose()?;

                ProviderConfig::Ollama { base_url }
            }
            ProviderKind::OpenAi => {
                let api_key = env_value(ENV_OPENAI_API_KEY).ok_or_else(|| {
                    anyhow!(
                        "{ENV_OPENAI_API_KEY} is required when {provider_var_name}={}",
                        provider_kind.as_str()
                    )
                })?;

                ProviderConfig::OpenAi { api_key }
            }
            ProviderKind::OpenAiCompatible => {
                let api_key = env_value(ENV_OPENAI_API_KEY).ok_or_else(|| {
                    anyhow!(
                        "{ENV_OPENAI_API_KEY} is required when {provider_var_name}={}",
                        provider_kind.as_str()
                    )
                })?;

                let base_url = env_value(ENV_OPENAI_BASE_URL).ok_or_else(|| {
                    anyhow!(
                        "{ENV_OPENAI_BASE_URL} is required when {provider_var_name}={}",
                        provider_kind.as_str()
                    )
                })?;
                Url::parse(base_url.as_str())
                    .map_err(|err| anyhow!("invalid {ENV_OPENAI_BASE_URL}: {err}"))?;

                ProviderConfig::OpenAiCompatible { api_key, base_url }
            }
        };

        Ok(Self { profile, provider })
    }

    pub fn provider_label(&self) -> &str {
        match self.provider {
            ProviderConfig::Ollama { .. } => ProviderKind::Ollama.as_str(),
            ProviderConfig::OpenAi { .. } => ProviderKind::OpenAi.as_str(),
            ProviderConfig::OpenAiCompatible { .. } => ProviderKind::OpenAiCompatible.as_str(),
        }
    }

    pub fn model_label(&self) -> &str {
        self.profile.model.as_str()
    }

    pub fn context_window(&self) -> Option<TokenCount> {
        self.profile.context_window
    }

    pub fn estimator(&self) -> Arc<dyn TokenEstimator> {
        self.profile.estimator.estimator()
    }

    pub fn resolved_estimator(&self) -> &ResolvedTokenEstimator {
        &self.profile.estimator
    }

    /// Return a pre-configured chat builder for advanced cases where
    /// `ModelChatBuilder` does not expose enough control (custom response
    /// formats, tool definitions, multi-message conversations).
    ///
    /// Advanced callers must attach the estimator themselves:
    ///
    /// ```ignore
    /// let handler = ai
    ///     .chat_builder()
    ///     .system(prompt)
    ///     .build_typed_lazy::<In, Out>(prompt_fn, parse_fn)?
    ///     .with_resolved_estimator(ai.resolved_estimator().clone());
    /// ```
    pub fn chat_builder(&self) -> ChatTransformBuilder {
        match &self.provider {
            ProviderConfig::Ollama { base_url } => {
                let mut builder =
                    ChatTransformBuilder::new().ollama(self.model_label().to_string());
                if let Some(base_url) = base_url.as_ref() {
                    builder = builder.base_url(base_url.clone());
                }
                builder
            }
            ProviderConfig::OpenAi { api_key } => {
                ChatTransformBuilder::new().openai(self.model_label().to_string(), api_key.clone())
            }
            ProviderConfig::OpenAiCompatible { api_key, base_url } => ChatTransformBuilder::new()
                .openai_compatible(
                    self.model_label().to_string(),
                    api_key.clone(),
                    base_url.clone(),
                ),
        }
    }

    /// Return a builder for constructing an LLM chat handler.
    ///
    /// The returned `ModelChatBuilder` is pre-configured with provider, model,
    /// credentials, and base URL from this `ModelConfig`. Chain per-handler
    /// settings and finish with a terminal method such as `build_map_items`:
    ///
    /// ```ignore
    /// let handler = ai.chat()
    ///     .system(system_prompt)
    ///     .temperature(0.2)
    ///     .max_tokens(800)
    ///     .context(MyCtx { ... })
    ///     .build_map_items(my_prompt, my_parse)?;
    /// ```
    pub fn chat(&self) -> ModelChatBuilder {
        ModelChatBuilder {
            inner: self.chat_builder(),
            resolved_estimator: self.resolved_estimator().clone(),
        }
    }

    fn base_url_for_display(&self) -> Option<&str> {
        match &self.provider {
            ProviderConfig::Ollama { base_url } => base_url.as_deref(),
            ProviderConfig::OpenAi { .. } => None,
            ProviderConfig::OpenAiCompatible { base_url, .. } => Some(base_url.as_str()),
        }
    }
}

/// Pre-configured chat builder that carries estimator metadata from a
/// `ModelConfig`.
///
/// Finish with one of the terminal methods (for example `build_map_items` or
/// `build_reduce_seeded`) to produce a ready `ChatTransform` handler with
/// automatic estimator attachment.
pub struct ModelChatBuilder {
    inner: ChatTransformBuilder,
    resolved_estimator: ResolvedTokenEstimator,
}

/// A [`ModelChatBuilder`] with a bound shared context value.
///
/// Returned by [`ModelChatBuilder::context`]. The context is stored as an
/// `Arc<Ctx>` and passed to prompt/parse functions by reference.
pub struct ModelChatBuilderWithContext<Ctx> {
    inner: ModelChatBuilder,
    ctx: Arc<Ctx>,
}

impl ModelChatBuilder {
    pub fn system(mut self, text: impl Into<String>) -> Self {
        self.inner = self.inner.system(text);
        self
    }

    pub fn temperature(mut self, temperature: f32) -> Self {
        self.inner = self.inner.temperature(temperature);
        self
    }

    pub fn max_tokens(mut self, max_tokens: u32) -> Self {
        self.inner = self.inner.max_tokens(max_tokens);
        self
    }

    pub fn top_p(mut self, top_p: f32) -> Self {
        self.inner = self.inner.top_p(top_p);
        self
    }

    pub fn seed(mut self, seed: u64) -> Self {
        self.inner = self.inner.seed(seed);
        self
    }

    pub fn response_format(mut self, response_format: ChatResponseFormat) -> Self {
        self.inner = self.inner.response_format(response_format);
        self
    }

    pub fn tools(mut self, tools: Vec<ToolDefinition>) -> Self {
        self.inner = self.inner.tools(tools);
        self
    }

    pub fn extra_param(mut self, key: impl Into<String>, value: Value) -> Self {
        self.inner = self.inner.extra_param(key, value);
        self
    }

    /// Bind a shared context value that will be passed to prompt and parse functions.
    pub fn context<Ctx>(self, ctx: Ctx) -> ModelChatBuilderWithContext<Ctx>
    where
        Ctx: Send + Sync + 'static,
    {
        ModelChatBuilderWithContext {
            inner: self,
            ctx: Arc::new(ctx),
        }
    }

    /// Build a map-role `ChatTransform` over chunk items.
    ///
    /// Runs provider preflight (verifies the AI provider is reachable and the model exists)
    /// before constructing the transform. Errors surface at build time, not on first inference.
    ///
    /// The input payload is deserialised as `Vec<Item>`, but the prompt closure receives `&[Item]`.
    ///
    /// Type inference: using named functions for `prompt` and `parse` is usually enough for the
    /// compiler to infer `Item` and `Out` at the call site.
    pub async fn build_map_items<Item, Out>(
        self,
        prompt: impl Fn(&[Item]) -> Result<UserPrompt, HandlerError> + Send + Sync + 'static,
        parse: impl Fn(ChatResponse) -> Result<Out, HandlerError> + Send + Sync + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Item: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilder {
            inner,
            resolved_estimator,
        } = self;
        let transform = inner.build_map_items(prompt, parse).await?;
        Ok(transform.with_resolved_estimator(resolved_estimator))
    }

    /// Build a map-role `ChatTransform` over chunk items, passing chunk planning metadata
    /// through to the prompt function.
    ///
    /// Runs provider preflight before constructing the transform.
    pub async fn build_map_items_with_chunk_info<Item, Out>(
        self,
        prompt: impl Fn(&[Item], &ChunkInfo) -> Result<UserPrompt, HandlerError> + Send + Sync + 'static,
        parse: impl Fn(ChatResponse) -> Result<Out, HandlerError> + Send + Sync + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Item: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilder {
            inner,
            resolved_estimator,
        } = self;
        let transform = inner.build_map_items_with_chunk_info(prompt, parse).await?;
        Ok(transform.with_resolved_estimator(resolved_estimator))
    }

    /// Build a map-role `ChatTransform` over chunk items, passing the prompt to `parse`.
    ///
    /// Runs provider preflight before constructing the transform.
    ///
    /// Determinism: this method calls `prompt` twice (once to build the request, once to
    /// provide the prompt to `parse`). Prompt functions must therefore be deterministic and
    /// side-effect free.
    pub async fn build_map_items_with_prompt<Item, Out>(
        self,
        prompt: impl Fn(&[Item]) -> Result<UserPrompt, HandlerError> + Send + Sync + 'static,
        parse: impl Fn(UserPrompt, ChatResponse) -> Result<Out, HandlerError> + Send + Sync + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Item: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilder {
            inner,
            resolved_estimator,
        } = self;
        let transform = inner.build_map_items_with_prompt(prompt, parse).await?;
        Ok(transform.with_resolved_estimator(resolved_estimator))
    }

    /// Build a map-role `ChatTransform` where parsing needs access to the input items.
    ///
    /// Runs provider preflight before constructing the transform.
    pub async fn build_map_items_with_input<Item, Out>(
        self,
        prompt: impl Fn(&[Item]) -> Result<UserPrompt, HandlerError> + Send + Sync + 'static,
        parse: impl Fn(Vec<Item>, ChatResponse) -> Result<Out, HandlerError> + Send + Sync + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Item: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilder {
            inner,
            resolved_estimator,
        } = self;
        let transform = inner.build_map_items_with_input(prompt, parse).await?;
        Ok(transform.with_resolved_estimator(resolved_estimator))
    }

    /// Build a seeded reduce-role `ChatTransform`.
    ///
    /// Runs provider preflight before constructing the transform.
    ///
    /// The input payload is deserialised as `(Seed, Vec<Partial>)`, but the prompt closure receives
    /// `(&Seed, &[Partial])`.
    ///
    /// Type inference: using named functions for `prompt` and `parse` is usually enough for the
    /// compiler to infer `Seed`, `Partial`, and `Out` at the call site.
    pub async fn build_reduce_seeded<Seed, Partial, Out>(
        self,
        prompt: impl Fn(&Seed, &[Partial]) -> Result<UserPrompt, HandlerError> + Send + Sync + 'static,
        parse: impl Fn(Seed, Vec<Partial>, ChatResponse) -> Result<Out, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Seed: DeserializeOwned + Send + Sync + 'static,
        Partial: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilder {
            inner,
            resolved_estimator,
        } = self;
        let transform = inner.build_reduce_seeded(prompt, parse).await?;
        Ok(transform.with_resolved_estimator(resolved_estimator))
    }

    /// Build a seeded reduce-role `ChatTransform`, passing the prompt to `parse`.
    ///
    /// Runs provider preflight before constructing the transform.
    ///
    /// Determinism: this method calls `prompt` twice (once to build the request, once to
    /// provide the prompt to `parse`). Prompt functions must therefore be deterministic and
    /// side-effect free.
    pub async fn build_reduce_seeded_with_prompt<Seed, Partial, Out>(
        self,
        prompt: impl Fn(&Seed, &[Partial]) -> Result<UserPrompt, HandlerError> + Send + Sync + 'static,
        parse: impl Fn(Seed, Vec<Partial>, UserPrompt, ChatResponse) -> Result<Out, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Seed: DeserializeOwned + Send + Sync + 'static,
        Partial: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilder {
            inner,
            resolved_estimator,
        } = self;
        let transform = inner.build_reduce_seeded_with_prompt(prompt, parse).await?;
        Ok(transform.with_resolved_estimator(resolved_estimator))
    }
}

impl<Ctx> ModelChatBuilderWithContext<Ctx>
where
    Ctx: Send + Sync + 'static,
{
    pub async fn build_map_items<Item, Out>(
        self,
        prompt: impl Fn(&Ctx, &[Item]) -> Result<UserPrompt, HandlerError> + Send + Sync + 'static,
        parse: impl Fn(&Ctx, ChatResponse) -> Result<Out, HandlerError> + Send + Sync + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Item: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilderWithContext { inner, ctx } = self;
        let ctx_prompt = ctx.clone();
        let ctx_parse = ctx.clone();
        inner
            .build_map_items(
                move |items| prompt(ctx_prompt.as_ref(), items),
                move |response| parse(ctx_parse.as_ref(), response),
            )
            .await
    }

    pub async fn build_map_items_with_chunk_info<Item, Out>(
        self,
        prompt: impl Fn(&Ctx, &[Item], &ChunkInfo) -> Result<UserPrompt, HandlerError>
            + Send
            + Sync
            + 'static,
        parse: impl Fn(&Ctx, ChatResponse) -> Result<Out, HandlerError> + Send + Sync + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Item: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilderWithContext { inner, ctx } = self;
        let ctx_prompt = ctx.clone();
        let ctx_parse = ctx.clone();
        inner
            .build_map_items_with_chunk_info(
                move |items, info| prompt(ctx_prompt.as_ref(), items, info),
                move |response| parse(ctx_parse.as_ref(), response),
            )
            .await
    }

    pub async fn build_map_items_with_prompt<Item, Out>(
        self,
        prompt: impl Fn(&Ctx, &[Item]) -> Result<UserPrompt, HandlerError> + Send + Sync + 'static,
        parse: impl Fn(&Ctx, UserPrompt, ChatResponse) -> Result<Out, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Item: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilderWithContext { inner, ctx } = self;
        let ctx_prompt = ctx.clone();
        let ctx_parse = ctx.clone();
        inner
            .build_map_items_with_prompt(
                move |items| prompt(ctx_prompt.as_ref(), items),
                move |user_prompt, response| parse(ctx_parse.as_ref(), user_prompt, response),
            )
            .await
    }

    pub async fn build_map_items_with_input<Item, Out>(
        self,
        prompt: impl Fn(&Ctx, &[Item]) -> Result<UserPrompt, HandlerError> + Send + Sync + 'static,
        parse: impl Fn(&Ctx, Vec<Item>, ChatResponse) -> Result<Out, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Item: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilderWithContext { inner, ctx } = self;
        let ctx_prompt = ctx.clone();
        let ctx_parse = ctx.clone();
        inner
            .build_map_items_with_input(
                move |items| prompt(ctx_prompt.as_ref(), items),
                move |items, response| parse(ctx_parse.as_ref(), items, response),
            )
            .await
    }

    pub async fn build_reduce_seeded<Seed, Partial, Out>(
        self,
        prompt: impl Fn(&Ctx, &Seed, &[Partial]) -> Result<UserPrompt, HandlerError>
            + Send
            + Sync
            + 'static,
        parse: impl Fn(&Ctx, Seed, Vec<Partial>, ChatResponse) -> Result<Out, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Seed: DeserializeOwned + Send + Sync + 'static,
        Partial: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilderWithContext { inner, ctx } = self;
        let ctx_prompt = ctx.clone();
        let ctx_parse = ctx.clone();
        inner
            .build_reduce_seeded(
                move |seed, partials| prompt(ctx_prompt.as_ref(), seed, partials),
                move |seed, partials, response| parse(ctx_parse.as_ref(), seed, partials, response),
            )
            .await
    }

    pub async fn build_reduce_seeded_with_prompt<Seed, Partial, Out>(
        self,
        prompt: impl Fn(&Ctx, &Seed, &[Partial]) -> Result<UserPrompt, HandlerError>
            + Send
            + Sync
            + 'static,
        parse: impl Fn(&Ctx, Seed, Vec<Partial>, UserPrompt, ChatResponse) -> Result<Out, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Result<ChatTransform, HandlerError>
    where
        Seed: DeserializeOwned + Send + Sync + 'static,
        Partial: DeserializeOwned + Send + Sync + 'static,
        Out: Serialize + TypedPayload + Send + Sync + 'static,
    {
        let ModelChatBuilderWithContext { inner, ctx } = self;
        let ctx_prompt = ctx.clone();
        let ctx_parse = ctx.clone();
        inner
            .build_reduce_seeded_with_prompt(
                move |seed, partials| prompt(ctx_prompt.as_ref(), seed, partials),
                move |seed, partials, user_prompt, response| {
                    parse(ctx_parse.as_ref(), seed, partials, user_prompt, response)
                },
            )
            .await
    }
}

impl std::fmt::Display for ModelConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut lines = Vec::new();

        lines.push(format!("provider: {}", self.provider_label()));
        lines.push(format!("model: {}", self.model_label()));
        if let Some(base_url) = self.base_url_for_display() {
            lines.push(format!("base_url: {base_url}"));
        }

        let info = self.resolved_estimator().info();
        lines.push(format!(
            "token_estimator: {:?}",
            self.resolved_estimator().source()
        ));
        if let Some(tokenizer_backend) = info.tokenizer_backend.as_deref() {
            lines.push(format!("token_estimator_backend: {tokenizer_backend}"));
        }
        if let Some(reason) = info.fallback_reason.as_ref() {
            lines.push(format!("token_estimator_fallback_reason: {reason}"));
        }
        if let Some(detail) = info.fallback_detail.as_deref() {
            lines.push(format!("token_estimator_fallback_detail: {detail}"));
        }

        match self.context_window() {
            Some(context_window) => lines.push(format!("context_window: {context_window}")),
            None => lines.push("context_window: unknown".to_string()),
        }

        f.write_str(&lines.join("\n"))
    }
}

fn parse_provider(value: &str) -> Option<ProviderKind> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("ollama") {
        return Some(ProviderKind::Ollama);
    }
    if value.eq_ignore_ascii_case("openai") {
        return Some(ProviderKind::OpenAi);
    }
    if value.eq_ignore_ascii_case("openai_compatible") {
        return Some(ProviderKind::OpenAiCompatible);
    }
    None
}

fn prefixed_env_name(prefix: Option<&str>, suffix: &str) -> Option<String> {
    let prefix = prefix?;
    if prefix.trim().is_empty() {
        return None;
    }
    Some(format!("{prefix}{suffix}"))
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn resolve_named_value(name: Option<&str>, default: &str) -> (String, String) {
    if let Some(name) = name {
        if let Some(value) = env_value(name) {
            return (name.to_string(), value);
        }
        return (name.to_string(), default.to_string());
    }
    ("<unset>".to_string(), default.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{env_lock, EnvGuard};
    use obzenflow_core::config::{ConfigScope, ConfigSource, ConfigValueMeta, SecretRef};
    use obzenflow_runtime::runtime_config::{AiModelsConfig, Resolved};

    fn resolved<T>(value: T) -> Resolved<T> {
        Resolved {
            value,
            meta: ConfigValueMeta {
                key_path: "ai.models.test".to_string(),
                source: ConfigSource::Default,
                scope: ConfigScope::Global,
            },
        }
    }

    fn ai_config(
        provider: &str,
        model: Option<&str>,
        base_url: Option<&str>,
        api_key_env: &str,
    ) -> AiModelsConfig {
        AiModelsConfig {
            provider: resolved(provider.to_string()),
            model: model.map(|m| resolved(m.to_string())),
            base_url: base_url.map(|b| resolved(b.to_string())),
            api_key_env: resolved(SecretRef::new(api_key_env)),
        }
    }

    #[test]
    fn from_config_defaults_to_provider_default_model() {
        let ai = ModelConfig::from_config(&ai_config("ollama", None, None, "OPENAI_API_KEY"))
            .expect("should construct defaults");
        assert_eq!(ai.provider_label(), "ollama");
        assert_eq!(ai.model_label(), DEFAULT_MODEL_OLLAMA);
    }

    #[test]
    fn from_config_openai_requires_resolvable_api_key() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["OBZENFLOW_TEST_MISSING_KEY"]);
        guard.remove("OBZENFLOW_TEST_MISSING_KEY");

        let err = ModelConfig::from_config(&ai_config(
            "openai",
            None,
            None,
            "OBZENFLOW_TEST_MISSING_KEY",
        ))
        .expect_err("should reject missing api key");
        let message = err.to_string();
        assert!(message.contains("ai.models.api_key_env"));
        assert!(message.contains("OBZENFLOW_TEST_MISSING_KEY"));
    }

    #[test]
    fn from_config_openai_compatible_requires_base_url() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["OBZENFLOW_TEST_PRESENT_KEY"]);
        guard.set("OBZENFLOW_TEST_PRESENT_KEY", "sk-test");

        let err = ModelConfig::from_config(&ai_config(
            "openai_compatible",
            None,
            None,
            "OBZENFLOW_TEST_PRESENT_KEY",
        ))
        .expect_err("should require base url");
        assert!(err
            .to_string()
            .contains("ai.models.base_url is required when ai.models.provider=openai_compatible"));
    }

    #[test]
    fn from_config_resolves_secret_at_point_of_use() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&["OBZENFLOW_TEST_COMPAT_KEY"]);
        guard.set("OBZENFLOW_TEST_COMPAT_KEY", "sk-compat");

        let ai = ModelConfig::from_config(&ai_config(
            "openai_compatible",
            Some("llama3.1:8b"),
            Some("http://localhost:11434/v1"),
            "OBZENFLOW_TEST_COMPAT_KEY",
        ))
        .expect("should construct");
        assert_eq!(ai.provider_label(), "openai_compatible");
        assert_eq!(ai.model_label(), "llama3.1:8b");
    }

    #[test]
    fn from_env_with_prefix_reads_prefixed_provider_and_model() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&[
            "TEST_AI_PROVIDER",
            "TEST_AI_MODEL",
            ENV_OPENAI_API_KEY,
            ENV_OPENAI_BASE_URL,
            ENV_OLLAMA_BASE_URL,
        ]);
        guard.set("TEST_AI_PROVIDER", "ollama");
        guard.set("TEST_AI_MODEL", "llama3.1:8b");
        guard.remove(ENV_OPENAI_API_KEY);
        guard.remove(ENV_OPENAI_BASE_URL);
        guard.remove(ENV_OLLAMA_BASE_URL);

        let ai = ModelConfig::from_env_with_prefix("TEST_AI_")
            .expect("should construct from prefixed vars");
        assert_eq!(ai.provider_label(), "ollama");
        assert_eq!(ai.model_label(), "llama3.1:8b");
    }

    #[test]
    fn from_env_with_prefix_does_not_use_prefixed_credentials() {
        let _lock = env_lock();
        let guard = EnvGuard::new(&[
            "TEST_AI_PROVIDER",
            "TEST_AI_OPENAI_API_KEY",
            ENV_OPENAI_API_KEY,
            ENV_OPENAI_BASE_URL,
            ENV_OLLAMA_BASE_URL,
        ]);
        guard.set("TEST_AI_PROVIDER", "openai");
        guard.set("TEST_AI_OPENAI_API_KEY", "sk-prefixed");
        guard.remove(ENV_OPENAI_API_KEY);
        guard.remove(ENV_OPENAI_BASE_URL);
        guard.remove(ENV_OLLAMA_BASE_URL);

        let err = ModelConfig::from_env_with_prefix("TEST_AI_")
            .expect_err("should still require OPENAI_API_KEY");
        let message = err.to_string();
        assert!(message.contains("OPENAI_API_KEY is required when TEST_AI_PROVIDER=openai"));
    }
}

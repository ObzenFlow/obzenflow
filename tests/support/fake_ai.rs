// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A deterministic chat model provider and configuration for `ai!` flow tests.

use async_trait::async_trait;
use obzenflow_core::ai::{
    chat_binding_fingerprint, embedding_binding_fingerprint, AiClientError, AiEndpoint,
    AiModelProviderError, AiModelSpec, ChatBindingFingerprint, ChatClient, ChatModelDescription,
    ChatModelProvider, ChatRequest, ChatResponse, ChatTarget, EmbeddingBindingFingerprint,
    EmbeddingClient, EmbeddingDimensions, EmbeddingModelDescription, EmbeddingModelProvider,
    EmbeddingRequest, EmbeddingResponse, EmbeddingTarget, HeuristicTokenEstimator,
    ResolvedTokenEstimator, TokenEstimatorFallbackReason, TokenEstimatorResolutionInfo,
};
use obzenflow_core::config::{ConfigScope, ConfigSource};
use obzenflow_runtime::ai::AiModelProviders;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::{
    AiEndpointTables, CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub type Reply = Arc<dyn Fn(&ChatRequest) -> Result<ChatResponse, AiClientError> + Send + Sync>;

pub type HangFilter = Arc<dyn Fn(&ChatRequest) -> bool + Send + Sync>;

/// What the fake recorded for one call: the endpoint it was connected to,
/// the credential name that endpoint named, and the request.
#[derive(Debug, Clone)]
pub struct RecordedCall {
    pub endpoint: String,
    pub credential: Option<String>,
    pub request: ChatRequest,
}

#[derive(Clone)]
pub struct FakeChatModels {
    pub calls: Arc<AtomicUsize>,
    pub connects: Arc<AtomicUsize>,
    pub recorded: Arc<Mutex<Vec<RecordedCall>>>,
    reply: Reply,
    forbidden: bool,
    hang: Option<(HangFilter, Arc<tokio::sync::Notify>)>,
    gate: Option<Arc<tokio::sync::Semaphore>>,
    client_target: Option<ChatTarget>,
}

impl FakeChatModels {
    pub fn replying(text: impl Fn(&ChatRequest) -> String + Send + Sync + 'static) -> Self {
        Self::with_reply(move |request| Ok(response(text(request))))
    }

    pub fn with_reply(
        reply: impl Fn(&ChatRequest) -> Result<ChatResponse, AiClientError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            connects: Arc::new(AtomicUsize::new(0)),
            recorded: Arc::new(Mutex::new(Vec::new())),
            reply: Arc::new(reply),
            forbidden: false,
            hang: None,
            gate: None,
            client_target: None,
        }
    }

    /// A provider whose calls matching `filter` signal `entered` and never
    /// return; other calls reply with `text`.
    pub fn hanging_when(
        entered: Arc<tokio::sync::Notify>,
        filter: impl Fn(&ChatRequest) -> bool + Send + Sync + 'static,
        text: &'static str,
    ) -> Self {
        Self {
            hang: Some((Arc::new(filter), entered)),
            ..Self::replying(move |_| text.to_string())
        }
    }

    /// A provider that connects clients serving `target` instead of the
    /// described one.
    pub fn with_client_target(target: ChatTarget) -> Self {
        Self {
            client_target: Some(target),
            ..Self::replying(|_| String::new())
        }
    }

    /// A provider whose calls each wait for one permit from `release`.
    pub fn gated(release: Arc<tokio::sync::Semaphore>, text: &'static str) -> Self {
        Self {
            gate: Some(release),
            ..Self::replying(move |_| text.to_string())
        }
    }

    /// A provider whose clients refuse every call, for strict replay.
    pub fn forbidden() -> Self {
        Self {
            forbidden: true,
            ..Self::replying(|_| String::new())
        }
    }

    /// A provider whose calls signal `entered` and never return.
    pub fn hanging(entered: Arc<tokio::sync::Notify>) -> Self {
        Self::hanging_when(entered, |_| true, "")
    }

    pub fn providers(&self) -> AiModelProviders {
        AiModelProviders::default().chat(self.clone())
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn connect_count(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }
}

pub fn response(text: impl Into<String>) -> ChatResponse {
    ChatResponse {
        text: text.into(),
        tool_calls: Vec::new(),
        usage: None,
        raw: None,
    }
}

pub fn last_user_message(request: &ChatRequest) -> String {
    request
        .messages
        .iter()
        .rev()
        .find(|message| message.role.as_str() == "user")
        .map(|message| message.content.clone())
        .unwrap_or_default()
}

fn heuristic(model: &str) -> TokenEstimatorResolutionInfo {
    TokenEstimatorResolutionInfo::heuristic(
        model,
        TokenEstimatorFallbackReason::ExplicitHeuristic,
        None,
    )
}

fn endpoint_identity(endpoint: &AiEndpoint) -> String {
    endpoint
        .base_url
        .as_ref()
        .map_or_else(|| format!("builtin:{}", endpoint.name), |url| url.to_string())
}

impl ChatModelProvider for FakeChatModels {
    fn serves(&self, _provider: &obzenflow_core::ai::AiProvider) -> bool {
        true
    }

    fn describe(
        &self,
        endpoint: &AiEndpoint,
        model: &str,
    ) -> Result<ChatModelDescription, AiModelProviderError> {
        Ok(ChatModelDescription {
            target: ChatTarget::with_binding_fingerprint(
                endpoint.provider.clone(),
                model,
                chat_binding_fingerprint(&endpoint.provider, model, &endpoint_identity(endpoint)),
            ),
            estimator: ResolvedTokenEstimator::new(
                Arc::new(HeuristicTokenEstimator::default()),
                heuristic(model),
            ),
        })
    }

    fn describe_recorded(
        &self,
        spec: &AiModelSpec,
    ) -> Result<ChatModelDescription, AiModelProviderError> {
        Ok(ChatModelDescription {
            target: ChatTarget::with_binding_fingerprint(
                spec.provider.clone(),
                spec.model.clone(),
                ChatBindingFingerprint::new(spec.binding_fingerprint.clone()),
            ),
            estimator: ResolvedTokenEstimator::new(
                Arc::new(HeuristicTokenEstimator::default()),
                spec.estimator.clone().unwrap_or_else(|| heuristic(&spec.model)),
            ),
        })
    }

    fn connect(
        &self,
        endpoint: &AiEndpoint,
        description: &ChatModelDescription,
    ) -> Result<Arc<dyn ChatClient>, AiModelProviderError> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(FakeChatClient {
            target: self
                .client_target
                .clone()
                .unwrap_or_else(|| description.target.clone()),
            endpoint: endpoint.name.clone(),
            credential: endpoint
                .api_key_env
                .as_ref()
                .map(|env| env.env_name().to_string()),
            models: self.clone(),
        }))
    }
}

struct FakeChatClient {
    target: ChatTarget,
    endpoint: String,
    credential: Option<String>,
    models: FakeChatModels,
}

#[async_trait]
impl ChatClient for FakeChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        self.models.calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.models.forbidden,
            "a strict replay must not call the model"
        );
        self.models
            .recorded
            .lock()
            .expect("recorded calls")
            .push(RecordedCall {
                endpoint: self.endpoint.clone(),
                credential: self.credential.clone(),
                request: request.clone(),
            });
        if let Some((filter, entered)) = &self.models.hang {
            if filter(&request) {
                entered.notify_one();
                return std::future::pending().await;
            }
        }
        if let Some(release) = &self.models.gate {
            release
                .acquire()
                .await
                .expect("the provider gate remains open")
                .forget();
        }
        (self.models.reply)(&request)
    }
}

pub type EmbeddingReply =
    Arc<dyn Fn(&EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError> + Send + Sync>;

#[derive(Clone)]
pub struct FakeEmbeddingModels {
    pub calls: Arc<AtomicUsize>,
    pub connects: Arc<AtomicUsize>,
    reply: EmbeddingReply,
    forbidden: bool,
    hang: Option<Arc<tokio::sync::Notify>>,
}

impl FakeEmbeddingModels {
    /// A provider that returns `vector` for every input.
    pub fn constant(vector: Vec<f32>) -> Self {
        Self::with_reply(move |request| {
            Ok(EmbeddingResponse {
                vectors: request.inputs.iter().map(|_| vector.clone()).collect(),
                vector_dim: EmbeddingDimensions::try_from(vector.len() as u32)
                    .expect("a fixture vector has a valid width"),
                usage: None,
            })
        })
    }

    pub fn with_reply(
        reply: impl Fn(&EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError>
            + Send
            + Sync
            + 'static,
    ) -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            connects: Arc::new(AtomicUsize::new(0)),
            reply: Arc::new(reply),
            forbidden: false,
            hang: None,
        }
    }

    pub fn forbidden() -> Self {
        Self {
            forbidden: true,
            ..Self::constant(vec![0.0])
        }
    }

    /// A provider whose calls signal `entered` once the operation would have
    /// succeeded remotely, and never return.
    pub fn hanging(entered: Arc<tokio::sync::Notify>) -> Self {
        Self {
            hang: Some(entered),
            ..Self::constant(vec![0.0])
        }
    }

    pub fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    pub fn connect_count(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }
}

impl EmbeddingModelProvider for FakeEmbeddingModels {
    fn serves(&self, _provider: &obzenflow_core::ai::AiProvider) -> bool {
        true
    }

    fn describe(
        &self,
        endpoint: &AiEndpoint,
        model: &str,
    ) -> Result<EmbeddingModelDescription, AiModelProviderError> {
        Ok(EmbeddingModelDescription {
            target: EmbeddingTarget::new(
                endpoint.provider.clone(),
                model,
                embedding_binding_fingerprint(
                    &endpoint.provider,
                    model,
                    &endpoint_identity(endpoint),
                ),
            ),
        })
    }

    fn describe_recorded(
        &self,
        spec: &AiModelSpec,
    ) -> Result<EmbeddingModelDescription, AiModelProviderError> {
        Ok(EmbeddingModelDescription {
            target: EmbeddingTarget::new(
                spec.provider.clone(),
                spec.model.clone(),
                EmbeddingBindingFingerprint::new(spec.binding_fingerprint.clone()),
            ),
        })
    }

    fn connect(
        &self,
        _endpoint: &AiEndpoint,
        description: &EmbeddingModelDescription,
    ) -> Result<Arc<dyn EmbeddingClient>, AiModelProviderError> {
        self.connects.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(FakeEmbeddingClient {
            target: description.target.clone(),
            models: self.clone(),
        }))
    }
}

struct FakeEmbeddingClient {
    target: EmbeddingTarget,
    models: FakeEmbeddingModels,
}

#[async_trait]
impl EmbeddingClient for FakeEmbeddingClient {
    fn target(&self) -> &EmbeddingTarget {
        &self.target
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError> {
        self.models.calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.models.forbidden,
            "a strict replay must not call the model"
        );
        if let Some(entered) = &self.models.hang {
            entered.notify_one();
            return std::future::pending().await;
        }
        (self.models.reply)(&request)
    }
}

pub fn providers(chat: &FakeChatModels, embedding: &FakeEmbeddingModels) -> AiModelProviders {
    AiModelProviders::default()
        .chat(chat.clone())
        .embedding(embedding.clone())
}

/// One configuration candidate a test admits.
pub type Candidate = (&'static str, ConfigScope, ConfigValue);

pub fn chat_model(model: &str) -> Candidate {
    (
        "ai.models.model",
        ConfigScope::Global,
        ConfigValue::Text(model.to_string()),
    )
}

pub fn embedding_model(model: &str) -> Candidate {
    (
        "ai.embeddings.model",
        ConfigScope::Global,
        ConfigValue::Text(model.to_string()),
    )
}

pub fn enforced_backpressure(window: u64) -> Vec<Candidate> {
    vec![
        (
            "runtime.backpressure.mode",
            ConfigScope::Flow,
            ConfigValue::Text("enforce".to_owned()),
        ),
        (
            "runtime.backpressure.window",
            ConfigScope::Flow,
            ConfigValue::U64(window),
        ),
        (
            "runtime.backpressure.stall_timeout_ms",
            ConfigScope::Flow,
            ConfigValue::U64(3_000),
        ),
    ]
}

pub fn runtime_config(
    candidates: impl IntoIterator<Item = Candidate>,
    endpoints: AiEndpointTables,
) -> Arc<ResolvedRuntimeConfig> {
    let mut set = CandidateSet::default();
    for (key, scope, value) in candidates {
        set.admit(ScopedCandidate::unqualified(
            key,
            scope,
            ConfigSource::File,
            value,
        ))
        .expect("the test candidate admits");
    }
    Arc::new(ResolvedRuntimeConfig::new(set).with_ai_endpoints(endpoints))
}

/// A build context whose AI stages use `models` with every chat stage on
/// `model` at the built-in `ollama` endpoint.
pub fn build_context(models: &FakeChatModels, model: &str) -> FlowBuildContext {
    build_context_with(models, [chat_model(model)])
}

pub fn build_context_with(
    models: &FakeChatModels,
    candidates: impl IntoIterator<Item = Candidate>,
) -> FlowBuildContext {
    FlowBuildContext::new(runtime_config(candidates, AiEndpointTables::default()))
        .with_ai_models(models.providers())
}

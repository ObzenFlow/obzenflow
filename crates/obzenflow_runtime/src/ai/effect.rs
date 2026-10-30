// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects::{
    BoundEffect, ConfiguredEffect, EffectBinding, EffectBindingBuildError, EffectBindingUse,
    EffectName, RecordedReply,
};
use obzenflow_core::ai::{
    AiEndpoint, AiModelProviderError, AiModelSpec, AiProvider, ChatModelProvider,
    EmbeddingModelProvider,
};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;
use std::sync::Arc;

/// A model provider port of one AI family.
pub trait AiProviderPort: Send + Sync + 'static {
    /// Whether endpoints of `provider` can serve this family.
    fn serves(&self, provider: &AiProvider) -> bool;
}

impl AiProviderPort for dyn ChatModelProvider {
    fn serves(&self, provider: &AiProvider) -> bool {
        ChatModelProvider::serves(self, provider)
    }
}

impl AiProviderPort for dyn EmbeddingModelProvider {
    fn serves(&self, provider: &AiProvider) -> bool {
        EmbeddingModelProvider::serves(self, provider)
    }
}

/// An effect only an `ai!` stage performs, one model call bound from configuration
/// at the stage's point or from the run's archive on reconstruction.
pub trait AiEffect:
    ConfiguredEffect<
        OutcomeSemantics = RecordedReply,
        Outcome: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    > + EffectName<Effect = Self, Name = Self>
{
    /// What a role prepares for one call.
    type Request: Clone + Send + Sync + 'static;
    /// What a role receives when the call replies.
    type Reply: Clone + Send + Sync + 'static;
    /// The model provider port of this effect's family.
    type Provider: ?Sized + AiProviderPort;

    /// The configuration section that selects this family's models.
    const SECTION: &'static str;
    /// The family's name in diagnostics.
    const FAMILY: &'static str;

    /// The effect value for one prepared request on the stage's binding.
    fn effect(
        label: &'static str,
        request: &Self::Request,
        binding: EffectBindingUse<Self>,
    ) -> Result<Self, AiRequestError>;

    /// The role-facing reply for one recorded outcome.
    fn reply(request: Self::Request, outcome: Self::Outcome) -> Self::Reply;

    /// Bind one configured or recorded model.
    fn bind(source: AiModelSource<Self::Provider>) -> Result<AiBinding<Self>, AiBindingError>;
}

/// Where an AI stage's model comes from.
pub enum AiModelSource<P: ?Sized> {
    /// Current configuration at the stage's point.
    Configured {
        stage: String,
        endpoint: AiEndpoint,
        model: String,
        provider: Arc<P>,
    },
    /// The spec a reconstructed run's archive recorded. Strict replay never
    /// calls it, and a call through it answers that the endpoint is unreachable.
    Recorded {
        stage: String,
        spec: AiModelSpec,
        provider: Arc<P>,
    },
    /// A spec an earlier run recorded, kept on resume for cursors with history.
    /// It connects through `endpoint` while that endpoint still serves the
    /// recorded target, and is otherwise unreachable.
    Kept {
        stage: String,
        spec: AiModelSpec,
        endpoint: Option<AiEndpoint>,
        provider: Arc<P>,
    },
}

/// One AI stage's binding for one effect, with the spec the run records.
pub struct AiBinding<E: BoundEffect> {
    pub binding: EffectBinding<E>,
    pub spec: AiModelSpec,
}

impl<E: BoundEffect> Clone for AiBinding<E> {
    fn clone(&self) -> Self {
        Self {
            binding: self.binding.clone(),
            spec: self.spec.clone(),
        }
    }
}

impl<E: BoundEffect> fmt::Debug for AiBinding<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiBinding")
            .field("binding", &self.binding)
            .field("spec", &self.spec)
            .finish()
    }
}

/// Why an AI effect's binding could not be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AiBindingError {
    #[error(transparent)]
    Provider(#[from] AiModelProviderError),
    #[error("binding evidence is invalid: {0}")]
    Evidence(String),
    #[error(transparent)]
    Registration(#[from] EffectBindingBuildError),
}

/// A prepared request that could not become an effect.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct AiRequestError(pub String);

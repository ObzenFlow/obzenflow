// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::{ChatModelProvider, EmbeddingModelProvider};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// The model providers a build can bind AI stages with, one per AI family.
///
/// A host installs its providers with
/// [`FlowBuildContext::with_ai_models`](crate::run_context::FlowBuildContext::with_ai_models),
/// and tests install fakes the same way.
#[derive(Clone, Default)]
pub struct AiModelProviders {
    providers: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl AiModelProviders {
    /// Install the chat family's provider.
    pub fn chat(self, provider: impl ChatModelProvider) -> Self {
        let provider: Arc<dyn ChatModelProvider> = Arc::new(provider);
        self.with(provider)
    }

    /// Install the embedding family's provider.
    pub fn embedding(self, provider: impl EmbeddingModelProvider) -> Self {
        let provider: Arc<dyn EmbeddingModelProvider> = Arc::new(provider);
        self.with(provider)
    }

    /// Install a family's provider by its port type.
    pub fn with<P>(mut self, provider: Arc<P>) -> Self
    where
        P: ?Sized + Send + Sync + 'static,
    {
        self.providers.insert(TypeId::of::<P>(), Arc::new(provider));
        self
    }

    /// The installed provider of the family whose port is `P`.
    pub fn provider<P>(&self) -> Option<Arc<P>>
    where
        P: ?Sized + Send + Sync + 'static,
    {
        self.providers
            .get(&TypeId::of::<P>())
            .and_then(|provider| provider.downcast_ref::<Arc<P>>())
            .cloned()
    }

    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

impl fmt::Debug for AiModelProviders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiModelProviders")
            .field("families", &self.providers.len())
            .finish()
    }
}

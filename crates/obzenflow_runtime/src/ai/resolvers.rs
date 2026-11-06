// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::{ChatModelResolver, EmbeddingModelResolver};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

/// The model resolvers a build can bind AI stages with, one per AI family.
///
/// A host installs its resolvers with
/// [`FlowBuildContext::with_ai_models`](crate::run_context::FlowBuildContext::with_ai_models).
#[derive(Clone, Default)]
pub struct AiModelResolvers {
    resolvers: HashMap<TypeId, Arc<dyn Any + Send + Sync>>,
}

impl AiModelResolvers {
    /// Install the chat family's resolver.
    pub fn chat(self, resolver: impl ChatModelResolver) -> Self {
        let resolver: Arc<dyn ChatModelResolver> = Arc::new(resolver);
        self.with(resolver)
    }

    /// Install the embedding family's resolver.
    pub fn embedding(self, resolver: impl EmbeddingModelResolver) -> Self {
        let resolver: Arc<dyn EmbeddingModelResolver> = Arc::new(resolver);
        self.with(resolver)
    }

    /// Install a family's resolver by its port type.
    pub fn with<R>(mut self, resolver: Arc<R>) -> Self
    where
        R: ?Sized + Send + Sync + 'static,
    {
        self.resolvers.insert(TypeId::of::<R>(), Arc::new(resolver));
        self
    }

    /// The installed resolver of the family whose port is `R`.
    pub fn resolver<R>(&self) -> Option<Arc<R>>
    where
        R: ?Sized + Send + Sync + 'static,
    {
        self.resolvers
            .get(&TypeId::of::<R>())
            .and_then(|resolver| resolver.downcast_ref::<Arc<R>>())
            .cloned()
    }

    pub fn is_empty(&self) -> bool {
        self.resolvers.is_empty()
    }
}

impl fmt::Debug for AiModelResolvers {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiModelResolvers")
            .field("families", &self.resolvers.len())
            .finish()
    }
}

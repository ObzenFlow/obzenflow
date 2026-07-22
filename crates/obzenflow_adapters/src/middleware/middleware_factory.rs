// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Middleware factory trait and related types
//!
//! This module defines the factory pattern for creating middleware instances
//! with stage context, solving the problem of injecting stage-specific
//! configuration into middleware at construction time.

use super::{MiddlewareHints, MiddlewareSafety};
use obzenflow_core::event::context::StageType;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::runtime_config::DslConfigDefault;
use std::any::TypeId;
use std::error::Error as StdError;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use thiserror::Error;

use super::carrier::{
    validate_attachment_request, validate_materialized_attachment,
    CheckedMiddlewareSurfaceAttachment, MiddlewareAttachmentRequest,
    MiddlewareAttachmentValidationError, MiddlewareAuthorityError, MiddlewareDeclaration,
    MiddlewareMaterializationContext, MiddlewareSurfaceAttachment,
};
use super::control::ControlMiddlewareAggregator;
pub type MiddlewareFactorySource = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, Error)]
pub enum MiddlewareFactoryError {
    #[error(
        "Invalid configuration for middleware '{middleware}' on stage '{stage_name}': {source}"
    )]
    InvalidConfiguration {
        middleware: String,
        stage_name: String,
        #[source]
        source: MiddlewareFactorySource,
    },

    #[error("Middleware '{middleware}' does not support stage '{stage_name}' ({stage_type})")]
    UnsupportedStageType {
        middleware: String,
        stage_type: StageType,
        stage_name: String,
    },

    #[error("Failed to materialize middleware '{middleware}' for stage '{stage_name}': {source}")]
    MaterializationFailed {
        middleware: String,
        stage_name: String,
        #[source]
        source: MiddlewareFactorySource,
    },
}

pub type MiddlewareFactoryResult<T> = Result<T, MiddlewareFactoryError>;

#[derive(Debug, Error)]
pub enum MiddlewareBindingError {
    #[error(transparent)]
    Attachment(#[from] MiddlewareAttachmentValidationError),
    #[error(transparent)]
    Factory(#[from] MiddlewareFactoryError),
    #[error(transparent)]
    Authority(#[from] MiddlewareAuthorityError),
    #[error("failed to commit middleware authority: {0}")]
    AuthorityCommit(String),
}

/// Complete-mediation gateway for one factory invocation. The context is
/// bound to the declaration's sealed semantic claim, registrations remain
/// invocation-local until the returned attachment is fully validated, and the
/// shared aggregator changes only through one atomic batch commit.
#[doc(hidden)]
pub fn materialize_factory_checked(
    factory: &dyn MiddlewareFactory,
    request: MiddlewareAttachmentRequest<'_>,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
) -> Result<CheckedMiddlewareSurfaceAttachment, MiddlewareBindingError> {
    let declaration = factory.declaration();
    materialize_factory_checked_with_declaration(
        factory,
        &declaration,
        request,
        config,
        stage_type,
        control_middleware,
    )
}

/// Checked materialisation using a declaration already captured by the DSL's
/// complete-set structural validation. This keeps the exact sealed claim that
/// passed coherence validation bound to the later factory invocation.
#[doc(hidden)]
pub fn materialize_factory_checked_with_declaration(
    factory: &dyn MiddlewareFactory,
    declaration: &MiddlewareDeclaration,
    request: MiddlewareAttachmentRequest<'_>,
    config: &StageConfig,
    stage_type: StageType,
    control_middleware: &Arc<ControlMiddlewareAggregator>,
) -> Result<CheckedMiddlewareSurfaceAttachment, MiddlewareBindingError> {
    validate_attachment_request(declaration, &request)?;
    let context = MiddlewareMaterializationContext::new(config, stage_type, declaration, &request);
    let attachment = factory.materialize(request, &context)?;
    validate_materialized_attachment(declaration, &request, &attachment)?;
    context.validate_returned_attachment(&attachment)?;
    control_middleware
        .commit_batch(context.take_pending())
        .map_err(MiddlewareBindingError::AuthorityCommit)?;
    Ok(CheckedMiddlewareSurfaceAttachment::from_validated(
        attachment,
    ))
}

impl MiddlewareFactoryError {
    pub fn invalid_configuration(
        middleware: impl Into<String>,
        stage_name: impl Into<String>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::InvalidConfiguration {
            middleware: middleware.into(),
            stage_name: stage_name.into(),
            source: Box::new(source),
        }
    }

    pub fn unsupported_stage_type(
        middleware: impl Into<String>,
        stage_type: StageType,
        stage_name: impl Into<String>,
    ) -> Self {
        Self::UnsupportedStageType {
            middleware: middleware.into(),
            stage_type,
            stage_name: stage_name.into(),
        }
    }

    pub fn materialization_failed(
        middleware: impl Into<String>,
        stage_name: impl Into<String>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::MaterializationFailed {
            middleware: middleware.into(),
            stage_name: stage_name.into(),
            source: Box::new(source),
        }
    }
}

/// Factory that creates middleware with stage context.
///
/// This trait solves the stage context injection problem by deferring middleware
/// creation until the supervisor is built with full context available.
///
/// Every public factory is hook-bound. [`declaration`](Self::declaration) names
/// its typed observer or control surfaces and [`materialize`](Self::materialize)
/// returns exactly one typed attachment for the requested surface. There is no
/// generic handler-shell fallback: custom observers implement observer ports,
/// and custom controls implement control ports.
pub trait MiddlewareFactory: Send + Sync {
    /// Display label for logs, topology output, metrics labels, and diagnostics.
    fn label(&self) -> &'static str;

    /// Typed override family key used by the resolver.
    fn override_key(&self) -> MiddlewareOverrideKey;

    /// Static, pre-erasure hook declaration. A declaration names at least one
    /// typed surface and has no legacy default.
    fn declaration(&self) -> MiddlewareDeclaration;

    /// Materialize one typed surface attachment for one concrete protected
    /// unit. The adapter-owned checked gateway validates the declaration before
    /// invoking this method and validates the returned claim before committing
    /// staged shared authority.
    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment>;

    /// Scope-free DSL defaults. The DSL adds the surviving declaration's
    /// topology scope and, for inline effect policies, its exact effect subject.
    fn dsl_config_defaults(&self) -> Vec<DslConfigDefault> {
        Vec::new()
    }

    /// Configuration keys this factory can consume at a surviving attachment.
    ///
    /// The common case is exactly the emitted DSL defaults. Factories with
    /// optional values or mode-dependent branches override this independently,
    /// so file configuration can refine an existing attachment without a fake
    /// default and can never create an attachment that the DSL omitted.
    fn consumed_config_keys(&self) -> Vec<&'static str> {
        self.dsl_config_defaults()
            .into_iter()
            .map(|default| default.key_path)
            .collect()
    }

    /// Typed topology slot for config snapshot placement.
    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        None
    }

    /// Which stage types this middleware supports
    ///
    /// Default implementation supports all stage types. Override this
    /// to restrict middleware to specific stage types.
    fn supported_stage_types(&self) -> &[StageType] {
        &[
            StageType::FiniteSource,
            StageType::InfiniteSource,
            StageType::Transform,
            StageType::Sink,
            StageType::Stateful,
            StageType::Join,
        ]
    }

    /// Safety level of this middleware
    ///
    /// Default is Safe. Override for middleware that can cause
    /// data loss or pipeline hangs if misused.
    fn safety_level(&self) -> MiddlewareSafety {
        MiddlewareSafety::Safe
    }

    /// Static hints about this middleware's behavior
    ///
    /// Default implementation returns no hints. Override to provide
    /// information about retry behavior, control event handling, etc.
    fn hints(&self) -> MiddlewareHints {
        MiddlewareHints::default()
    }

    /// Return static configuration for topology observability (FLOWIP-059).
    ///
    /// Middleware factories override this to expose their structural configuration
    /// (thresholds, policies, capacities, etc.) for the `/api/topology` endpoint.
    /// This is purely structural data - no runtime state.
    ///
    /// Default returns None for middleware that doesn't need config exposure.
    fn config_snapshot(&self) -> Option<serde_json::Value> {
        None
    }
}

// Implementation for Box<dyn MiddlewareFactory> to allow boxed factories
impl<F: MiddlewareFactory + ?Sized> MiddlewareFactory for Box<F> {
    fn label(&self) -> &'static str {
        (**self).label()
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        (**self).override_key()
    }

    fn dsl_config_defaults(&self) -> Vec<DslConfigDefault> {
        (**self).dsl_config_defaults()
    }

    fn consumed_config_keys(&self) -> Vec<&'static str> {
        (**self).consumed_config_keys()
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        (**self).topology_config_slot()
    }

    fn supported_stage_types(&self) -> &[StageType] {
        (**self).supported_stage_types()
    }

    fn safety_level(&self) -> MiddlewareSafety {
        (**self).safety_level()
    }

    fn hints(&self) -> MiddlewareHints {
        (**self).hints()
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        (**self).config_snapshot()
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        (**self).declaration()
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        (**self).materialize(request, context)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MiddlewareOverrideKey {
    type_id: TypeId,
    family_label: &'static str,
}

impl MiddlewareOverrideKey {
    pub fn of<T: 'static>(family_label: &'static str) -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            family_label,
        }
    }

    pub fn family_label(&self) -> &'static str {
        self.family_label
    }
}

impl PartialEq for MiddlewareOverrideKey {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id
    }
}

impl Eq for MiddlewareOverrideKey {}

impl Hash for MiddlewareOverrideKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.type_id.hash(state);
    }
}

/// Runtime shell middleware kind.
///
/// Factory placement no longer reads this enum. Typed factory declarations are
/// the sole build-time capability authority. The enum remains only on the two
/// FLOWIP-128g runtime shells and the middleware-chain contract that contains
/// them until that migration completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareKind {
    /// Reacts to an unreliable dependency by delaying, rejecting, or
    /// tripping (circuit breaker, rate limiter).
    Policy,
    /// Deterministic, value-preserving observation (indicator/logging evidence
    /// or enrichment). May not short-circuit processing.
    Observation,
    /// Framework machinery: build-time plan contributors and transitional
    /// structural middleware (backpressure, AI map-reduce, type shaping).
    Structural,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyMiddlewareConfigSlot {
    CircuitBreaker,
    RateLimiter,
    Retry,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    struct TestFamily;

    fn hash_key(key: MiddlewareOverrideKey) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn middleware_override_key_identity_ignores_family_label() {
        let flow_key = MiddlewareOverrideKey::of::<TestFamily>("flow.label");
        let stage_key = MiddlewareOverrideKey::of::<TestFamily>("stage.label");

        assert_eq!(flow_key, stage_key);
        assert_eq!(hash_key(flow_key), hash_key(stage_key));
        assert_eq!(flow_key.family_label(), "flow.label");
        assert_eq!(stage_key.family_label(), "stage.label");
    }
}

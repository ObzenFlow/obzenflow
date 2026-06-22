// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! # Middleware System for ObzenFlow
//!
//! This module provides a composable middleware system for adding cross-cutting
//! concerns like monitoring, logging, rate limiting, and retries to your ObzenFlow
//! pipeline stages without modifying their core logic.
//!
//! ## Middleware Overview
//!
//! Middleware is composable - you can stack multiple middleware on a handler.
//! When used in flows (via the DSL layer), middleware is specified as an array
//! of middleware instances for each stage.
//!
//! The middleware execution order is:
//! 1. First middleware's `pre_handle`
//! 2. Second middleware's `pre_handle`
//! 3. Handler processes the event
//! 4. Second middleware's `post_handle`
//! 5. First middleware's `post_handle`
//!
//! ## Monitoring
//!
//! Monitoring is no longer implemented as middleware.
//! Instead, the runtime exports metrics through the snapshot/exporter path:
//!
//! - application metrics are derived from wide events and journals
//! - infrastructure metrics are observed directly
//! - both are rendered together by the metrics exporter
//!
//! Monitoring views such as Grafana dashboards belong in static monitoring/docs
//! assets rather than as Rust helpers in this crate.
//!
//! ```text
//! // OLD: Monitoring middleware (no longer available)
//! // let monitoring_middleware = old_monitoring_factory();
//!
//! // NEW: Use the runtime metrics surface and external dashboard/query assets
//! ```
//!
//! ## Applying Middleware to Handlers
//!
//! Use the handler extension traits to apply middleware (see builder APIs for current syntax).
//!
//! ## Common Middleware Utilities
//!
//! The `common` module provides pre-built middleware for rate limiting, circuit breaking,
//! and logging; refer to the current control/observability modules for up-to-date builders.
//!
//! ## Custom Middleware
//!
//! You can create custom observation or structural middleware by implementing
//! the `Middleware` trait:
//!
//! ```rust
//! use obzenflow_adapters::middleware::{
//!     Middleware, MiddlewareAction, MiddlewareContext, SourceMiddlewarePhase,
//! };
//! use obzenflow_core::event::chain_event::ChainEvent;
//!
//! struct MyCustomMiddleware;
//!
//! impl Middleware for MyCustomMiddleware {
//!     fn label(&self) -> &'static str {
//!         "my_custom_middleware"
//!     }
//!
//!     fn source_phase(&self) -> SourceMiddlewarePhase {
//!         SourceMiddlewarePhase::Ordinary
//!     }
//!
//!     fn pre_handle(&self, event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
//!         println!("Processing event: {:?}", event.id);
//!         MiddlewareAction::Continue
//!     }
//!     
//!     fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], _ctx: &mut MiddlewareContext) {
//!         println!("Produced {} results", results.len());
//!     }
//! }
//! ```

// Core types
pub mod handler;
mod middleware_factory;
mod middleware_safety;

/// FLOWIP-120i: whether this process is performing a strict replay, read from
/// the installed bootstrap, the same source the journal factory uses to open
/// the replay archive. Middleware setup logs use this to say that configured
/// policies are inert for data-path accounting, so a replay transcript never
/// reads like live policy activity.
pub(crate) fn strict_replay_active() -> bool {
    obzenflow_runtime::bootstrap::replay_bootstrap().is_some()
}

// Handler-specific middleware adapters and legacy middleware wrappers
mod backpressure;

// Common middleware utilities
mod carrier;
mod context;
mod context_keys;
mod function;
mod hints;
pub mod type_shaping;

// Middleware categories
pub mod ai;
pub mod control;
pub mod observability;
mod validation;
// Dangerous middleware examples moved to examples/dangerous_examples.rs
// Factory tests moved to tests/factory_tests.rs
// Note: Monitoring is no longer implemented as middleware.
// Application metrics are journal-derived, infrastructure metrics are observed
// directly, and dashboards/query assets live outside the middleware API.

// Core trait exports
pub(crate) use handler::observation_short_circuit;
pub use handler::{
    ErrorAction, Middleware, MiddlewareAbortCause, MiddlewareAction, SourceMiddlewarePhase,
};
pub use middleware_factory::{
    ControlMiddlewareRole, MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareKind, MiddlewareOverrideKey, MiddlewarePlanContribution,
    TopologyMiddlewareConfigSlot,
};
pub use middleware_safety::MiddlewareSafety;

// Handler-specific exports
pub use handler::{
    AsyncFiniteSourceHandlerExt, AsyncFiniteSourceMiddlewareBuilder, AsyncInfiniteSourceHandlerExt,
    AsyncInfiniteSourceMiddlewareBuilder, AsyncMiddlewareTransform, AsyncTransformHandlerExt,
    AsyncTransformMiddlewareBuilder, FiniteSourceHandlerExt, FiniteSourceMiddlewareBuilder,
    InfiniteSourceHandlerExt, InfiniteSourceMiddlewareBuilder, JoinHandlerMiddlewareExt,
    JoinMiddlewareBuilder, MiddlewareAsyncFiniteSource, MiddlewareAsyncInfiniteSource,
    MiddlewareFiniteSource, MiddlewareInfiniteSource, MiddlewareJoin, MiddlewareSink,
    MiddlewareStateful, MiddlewareTransform, SinkHandlerExt, SinkMiddlewareBuilder,
    StatefulHandlerMiddlewareExt, StatefulMiddlewareBuilder, TransformHandlerExt,
    TransformMiddlewareBuilder, UnifiedMiddlewareTransform,
};

// Common utilities
pub use carrier::{
    validate_attachment_request, EffectSurface, EffectTypeKey, EffectUnitId,
    HostedIngressTargetKey, IngressEndpointKind, IngressRouteScope, IngressSurface, IngressUnitId,
    MiddlewareAttachmentId, MiddlewareAttachmentRequest, MiddlewareAttachmentValidationError,
    MiddlewareCapability, MiddlewareDeclaration, MiddlewareDeclarationIndex,
    MiddlewareDeclarationScope, MiddlewareMaterializationContext, MiddlewareOrigin,
    MiddlewareSurface, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, ProtectedUnit,
    ProtectedUnitId, SinkConfiguredTargetKey, SinkDeliverySurface, SinkDeliveryTarget,
    SinkDeliveryUnitId, SourcePollAttachment, SourcePollSurface, SourcePollUnitId,
    SourceStageIngressOwner,
};
pub use context::MiddlewareContext;
pub use control::policy::{
    effect_policy_from_middleware, EffectAttemptOutcome, EffectPolicy, EffectPolicyAttachment,
    EventAwareEffectPolicy, PerEffectPolicyBoundary, PerSinkDeliveryPolicyBoundary,
    PerSourcePolicyBoundary, PolicyAdmission, SinkAdmission, SinkAdmissionGuard,
    SinkDeliveryPolicyOutcome, SinkPolicy, SinkPolicyCtx, SourceAdmission, SourceAdmissionGuard,
    SourceAfterPoll, SourceBatchFacts, SourcePolicy, SourcePolicyCtx, SourcePollOutcome,
};
pub use function::{middleware_fn, FnMiddleware};
pub use hints::{Attempts, BackoffKind, BatchingHint, MiddlewareHints, RetryHint};
pub use observability::timing::TimingMiddleware;
pub use type_shaping::{IntoEffectPolicyParts, OutcomeShapingMiddleware, TypeShapingMiddleware};

// Control middleware
pub use control::{
    circuit_breaker, rate_limit, rate_limit_with_burst, CircuitBreakerBuilder,
    CircuitBreakerMiddleware, RateLimiterBuilder, RateLimiterFactory, RateLimiterMiddleware,
};

// Backpressure (config + topology observability; FLOWIP-086k)
pub use backpressure::{backpressure, BackpressureMiddlewareFactory};

pub use observability::LoggingMiddleware;

// Middleware validation helpers
pub use validation::{validate_middleware_safety, ValidationResult};

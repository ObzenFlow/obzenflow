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
//! You can also create custom middleware by implementing the `Middleware` trait:
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
mod middleware_factory;
mod middleware_safety;
mod middleware_trait;

/// FLOWIP-120i: whether this process is performing a strict replay, read from
/// the installed bootstrap, the same source the journal factory uses to open
/// the replay archive. Middleware setup logs use this to say that configured
/// policies are inert for data-path accounting, so a replay transcript never
/// reads like live policy activity.
pub(crate) fn strict_replay_active() -> bool {
    obzenflow_runtime::bootstrap::replay_bootstrap().is_some()
}

// Handler-specific middleware adapters
mod backpressure;
mod join_middleware;
mod sink_middleware;
mod sink_policy;
mod source_middleware;
mod source_policy;
mod stateful_middleware;
mod transform_middleware;

// Common middleware utilities
mod carrier;
mod context;
mod context_keys;
mod effect_policy;
mod function;
mod hints;
pub mod type_shaping;

// Middleware categories
pub mod ai;
pub mod control;
pub mod observability;
mod system;
// Dangerous middleware examples moved to examples/dangerous_examples.rs
// Factory tests moved to tests/factory_tests.rs
// Note: Monitoring is no longer implemented as middleware.
// Application metrics are journal-derived, infrastructure metrics are observed
// directly, and dashboards/query assets live outside the middleware API.

// Core trait exports
pub use middleware_factory::{
    ControlMiddlewareRole, MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareKind, MiddlewareOverrideKey, MiddlewarePlanContribution,
    TopologyMiddlewareConfigSlot,
};
pub use middleware_safety::MiddlewareSafety;
pub(crate) use middleware_trait::observation_short_circuit;
pub use middleware_trait::{
    ErrorAction, Middleware, MiddlewareAbortCause, MiddlewareAction, SourceMiddlewarePhase,
    SourcePacer,
};

// Handler-specific exports
pub use join_middleware::{JoinHandlerMiddlewareExt, JoinMiddlewareBuilder, MiddlewareJoin};
pub use sink_middleware::{MiddlewareSink, SinkHandlerExt, SinkMiddlewareBuilder};
pub use sink_policy::{
    PerSinkDeliveryPolicyBoundary, SinkAdmission, SinkAdmissionGuard, SinkDeliveryPolicyOutcome,
    SinkPolicy, SinkPolicyCtx,
};
pub use source_middleware::{
    AsyncFiniteSourceHandlerExt, AsyncFiniteSourceMiddlewareBuilder, AsyncInfiniteSourceHandlerExt,
    AsyncInfiniteSourceMiddlewareBuilder, FiniteSourceHandlerExt, FiniteSourceMiddlewareBuilder,
    InfiniteSourceHandlerExt, InfiniteSourceMiddlewareBuilder, MiddlewareAsyncFiniteSource,
    MiddlewareAsyncInfiniteSource, MiddlewareFiniteSource, MiddlewareInfiniteSource,
};
pub use source_policy::{
    batch_has_error_marked, PerSourcePolicyBoundary, SourceAdmission, SourceAdmissionGuard,
    SourceAfterPoll, SourcePolicy, SourcePolicyCtx, SourcePollOutcome,
};
pub use stateful_middleware::{
    MiddlewareStateful, StatefulHandlerMiddlewareExt, StatefulMiddlewareBuilder,
};
pub use transform_middleware::{
    AsyncMiddlewareTransform, AsyncTransformHandlerExt, AsyncTransformMiddlewareBuilder,
    MiddlewareTransform, TransformHandlerExt, TransformMiddlewareBuilder,
    UnifiedMiddlewareTransform,
};

// Common utilities
pub use carrier::{
    validate_attachment_request, EffectSurface, EffectTypeKey, EffectUnitId,
    MiddlewareAttachmentId, MiddlewareAttachmentRequest, MiddlewareAttachmentValidationError,
    MiddlewareCapability, MiddlewareDeclaration, MiddlewareMaterializationContext,
    MiddlewareOrigin, MiddlewareSurface, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
    ProtectedUnit, ProtectedUnitId, SinkConfiguredTargetKey, SinkDeliverySurface,
    SinkDeliveryTarget, SinkDeliveryUnitId, SourcePollAttachment, SourcePollSurface,
    SourcePollUnitId,
};
pub use context::MiddlewareContext;
pub use effect_policy::{
    effect_policy_from_middleware, EffectAttemptOutcome, EffectPolicy, PerEffectPolicyBoundary,
    PolicyAdmission,
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

// Re-export observability middleware for backward compatibility
pub use observability::{
    BoundaryConfig, BoundaryTrackingMiddleware, FlowBoundaryTracker, FlowMetrics,
    LoggingMiddleware, SystemEnrichmentMiddleware,
};

// System middleware exports
pub use system::{
    outcome_enrichment, validate_middleware_safety, OutcomeEnrichmentMiddleware, ValidationResult,
};

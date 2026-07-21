// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! # Middleware System for ObzenFlow
//!
//! This module provides a composable middleware system for adding cross-cutting
//! concerns like monitoring, logging, rate limiting, and retries to your ObzenFlow
//! pipeline stages without modifying their core logic.
//!
//! ## Middleware model
//!
//! Middleware weaves a cross-cutting concern at a named stage join point without
//! touching handler code, and splits by capability:
//!
//! - **Observers** publish journalled evidence (logging and service-level
//!   indicator samples) and structurally cannot steer control
//!   flow: their return type only carries evidence. This is the tool for custom
//!   observability and auditability aspects.
//! - **Control** middleware (circuit breaker, rate limiter) admits, paces,
//!   rejects, or synthesizes at a live-I/O boundary.
//!
//! Built-in observers are constructed with `indicator()` / `latency()` and
//! `log()`; built-in control middleware with `circuit_breaker()` and
//! `rate_limit()`. In the DSL, middleware is attached per stage as an array.
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
//! ## Custom observers
//!
//! Author a custom observability or auditability aspect by implementing the
//! observer hook for the surface you care about and exposing it through a small
//! factory. The observer hook (`HandlerObserver`, `StatefulObserver`,
//! `JoinObserver`, `SourcePollObserver`, `SinkDeliveryObserver`,
//! `OutputCommitObserver`, ...) returns an `ObserverReport` carrying journalled
//! evidence and nothing else, so it cannot pause, reject, retry, or otherwise
//! steer control. It needs no downcasts and no execution-scope handling: the
//! runtime suppresses a `LiveOnly` observer under strict replay by placement.
//!
//! ```ignore
//! use obzenflow_adapters::middleware::observer::{
//!     HandlerObserver, HandlerObserverContext, ObserverReport,
//! };
//!
//! struct CountInputs;
//!
//! impl HandlerObserver for CountInputs {
//!     fn label(&self) -> &'static str { "count_inputs" }
//!
//!     fn after_handle(
//!         &self,
//!         _ctx: &HandlerObserverContext<'_>,
//!         _outputs: &mut [obzenflow_core::ChainEvent],
//!     ) -> ObserverReport {
//!         // Build and attach an evidence row, or return `ObserverReport::empty()`.
//!         ObserverReport::empty()
//!     }
//! }
//!
//! // Expose it through a `MiddlewareFactory` whose `declaration()` names the
//! // observer surface(s) and whose `materialize()` returns the observer
//! // attachment, then attach `count_inputs()` to a stage in the DSL.
//! ```
//!
//! The built-in `observability::indicator` and `observability::logging` modules
//! are complete worked examples of this factory + observer-hook shape, and
//! FLOWIP-115f documents the authoring contract. To affect control flow instead,
//! implement a control hook rather than an observer.

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
pub mod observer;
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
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult, MiddlewareKind,
    MiddlewareOverrideKey, TopologyMiddlewareConfigSlot,
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
    Flowip128gLegacyShellAttachment, HostedIngressTargetKey, IngressEndpointKind,
    IngressRouteScope, IngressSurface, IngressUnitId, MiddlewareAttachmentId,
    MiddlewareAttachmentRequest, MiddlewareAttachmentValidationError, MiddlewareCapability,
    MiddlewareDeclaration, MiddlewareDeclarationIndex, MiddlewareDeclarationScope,
    MiddlewareMaterializationContext, MiddlewareOrigin, MiddlewareSurface,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId,
    SinkConfiguredTargetKey, SinkDeliverySurface, SinkDeliveryTarget, SinkDeliveryUnitId,
    SourcePollAttachment, SourcePollSurface, SourcePollUnitId, SourceStageIngressOwner,
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
pub use observability::indicator::{indicator, latency, IndicatorKind, IndicatorMiddlewareFactory};
pub use observer::StageObserverSet;
pub use type_shaping::{IntoEffectPolicyParts, OutcomeShapingMiddleware, TypeShapingMiddleware};

// Control middleware
pub use control::{
    circuit_breaker, failure_rate, rate_limit, rate_limit_with_burst, CircuitBreaker,
    CircuitBreakerBuilder, CircuitBreakerMiddleware, EffectResilience, EffectResilienceConfigError,
    RateLimiter, RateLimiterBuilder, RateLimiterFactory, RateLimiterMiddleware, Retry,
};

pub use observability::{log, LoggingMiddleware, LoggingMiddlewareFactory};

// Middleware validation helpers
pub use validation::{validate_middleware_safety, ValidationResult};

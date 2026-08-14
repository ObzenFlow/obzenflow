// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! # Middleware System for ObzenFlow
//!
//! This module provides typed observer and live-I/O policy attachments for
//! cross-cutting concerns without wrapping stage handlers.
//!
//! ## Middleware model
//!
//! Attachments bind at named stage join points and split by capability:
//!
//! - **Observers** are synchronous, live-only interception points over
//!   immutable runtime views. They return nothing and receive no framework
//!   authority with which to steer or publish execution.
//! - **Control** middleware (circuit breaker, rate limiter, and effect
//!   resilience) admits, paces, or rejects at a live-I/O boundary. Retry exists
//!   only inside effect resilience; it is not a standalone attachment.
//!
//! Custom observers use a surface-specific `*_observer("label", value)`
//! helper. Built-in control middleware uses the checked
//! `CircuitBreaker::builder()` and `rate_limit()`.
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
//! ## Common Middleware Utilities
//!
//! The control modules provide pre-built middleware for rate limiting, circuit
//! breaking, and effect resilience. Ordinary observers are application-owned
//! implementations attached through the surface-specific helpers below.
//!
//! ## Custom observers
//!
//! Author a custom diagnostic aspect by implementing the observer hook for the
//! surface you care about. Ordinary hooks return no value, receive immutable
//! views, and are suppressed while recorded history is reconstructed.
//!
//! ```ignore
//! use obzenflow_adapters::middleware::handler_observer;
//! use obzenflow_runtime::stages::observer::{HandlerObserver, HandlerObserverContext};
//!
//! struct CountInputs;
//!
//! impl HandlerObserver for CountInputs {
//!     fn after_handle(
//!         &self,
//!         ctx: &HandlerObserverContext<'_>,
//!         outputs: &[obzenflow_core::ChainEvent],
//!     ) {
//!         tracing::debug!(stage = ctx.stage_name(), outputs = outputs.len());
//!     }
//! }
//!
//! let observer = handler_observer("count-inputs", CountInputs);
//! ```
//!
//! Application diagnostics use the standard Rust `tracing` ecosystem. This
//! observer layer defines no logging, measurement, journal, or exporter API.

// Core types
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
mod hints;

// Middleware categories
pub mod control;
pub mod observer;
mod validation;
// Dangerous middleware examples moved to examples/dangerous_examples.rs
// Factory tests moved to tests/factory_tests.rs
// Note: Monitoring is no longer implemented as middleware.
// Application metrics are journal-derived, infrastructure metrics are observed
// directly, and dashboards/query assets live outside the middleware API.

pub use middleware_factory::{
    materialize_factory_checked, materialize_factory_checked_with_declaration,
    MiddlewareBindingError, MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareOverrideKey, TopologyMiddlewareConfigSlot,
};
pub use middleware_safety::MiddlewareSafety;

// Common utilities
pub use carrier::{
    validate_attachment_request, validate_effect_control_composition,
    CheckedMiddlewareSurfaceAttachment, EffectControlCompositionError, EffectSurface,
    EffectTypeKey, EffectUnitId, HostedIngressTargetKey, IngressEndpointKind, IngressRouteScope,
    IngressSurface, IngressUnitId, MiddlewareAttachmentId, MiddlewareAttachmentRequest,
    MiddlewareAttachmentValidationError, MiddlewareAuthorityError, MiddlewareCapability,
    MiddlewareDeclaration, MiddlewareDeclarationIndex, MiddlewareDeclarationPosition,
    MiddlewareMaterializationContext, MiddlewareSurface, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId, SinkConfiguredTargetKey,
    SinkDeliverySurface, SinkDeliveryTarget, SinkDeliveryUnitId, SourcePollAttachment,
    SourcePollSurface, SourcePollUnitId, SourceStageIngressOwner,
};
pub(crate) use carrier::{MaterializationClaim, MiddlewareSurfaceAttachmentKind};
pub use context::MiddlewareContext;
pub use control::policy::{
    EffectAttemptOutcome, EffectPolicy, EffectPolicyAttachment, EventAwareEffectPolicy,
    MiddlewareAbortCause, PerEffectPolicyBoundary, PerSinkDeliveryPolicyBoundary,
    PerSourcePolicyBoundary, PolicyAdmission, SinkAdmission, SinkAdmissionGuard,
    SinkDeliveryPolicyOutcome, SinkPolicy, SinkPolicyCtx, SourceAdmission, SourceAdmissionGuard,
    SourceAfterPoll, SourceBatchFacts, SourcePolicy, SourcePolicyCtx, SourcePollOutcome,
};
pub use hints::{BatchingHint, MiddlewareHints};
pub use observer::{
    effect_observer, handler_observer, join_observer, sink_delivery_observer, source_poll_observer,
    stage_lifecycle_observer, stateful_observer, EffectObserverFactory, HandlerObserverFactory,
    JoinObserverFactory, SinkDeliveryObserverFactory, SourcePollObserverFactory,
    StageLifecycleObserverFactory, StageObserverSet, StatefulObserverFactory,
};

// Control middleware
pub use control::{
    rate_limit, rate_limit_with_burst, CircuitBreaker, CircuitBreakerConfigError, EffectResilience,
    EffectResilienceConfigError, FailureHealth, RateLimiter, RateLimiterBuilder,
    RateLimiterFactory, RateLimiterMiddleware, Retry,
};

// Middleware validation helpers
pub use validation::{validate_middleware_safety, ValidationResult};

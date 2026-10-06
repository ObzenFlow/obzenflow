// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Live-I/O policies and passive observers, separate from stage handlers.
//!
//! Constructors and setters return inert values of the same concrete type.
//! The flow validates the complete attachment plan before materialisation.
//!
//! ```
//! use obzenflow::middleware::{circuit_breaker, rate_limit, retry, CircuitBreaker, RateLimiter, Retry};
//! let _: RateLimiter = rate_limit(10.0).burst_capacity(20.0).cost(1.0);
//! let _: CircuitBreaker = circuit_breaker().consecutive_failures(3);
//! let _: Retry = retry().max_attempts(3);
//! ```
//!
//! The former builder, factory, and aggregate authoring routes are unavailable.
//!
//! ```compile_fail
//! use obzenflow::middleware::CircuitBreaker;
//! let _ = CircuitBreaker::builder();
//! ```
//! ```compile_fail
//! use obzenflow::middleware::RateLimiterBuilder;
//! ```
//! ```compile_fail
//! use obzenflow::middleware::RateLimiterFactory;
//! ```
//! ```compile_fail
//! use obzenflow::middleware::CheckedCircuitBreakerBuilder;
//! ```
//! ```compile_fail
//! use obzenflow::middleware::EffectResilience;
//! ```
//! ```compile_fail
//! use obzenflow::middleware::EffectResilienceBuilder;
//! ```
//! ```compile_fail
//! use obzenflow::middleware::rate_limit_with_burst;
//! ```

pub use obzenflow_core::event::context::StageType;
pub use obzenflow_core::event::vector_clock::VectorClock;

pub use obzenflow_adapters::middleware::control::rate_limiter::RateLimiterConfigError;
pub use obzenflow_adapters::middleware::control::{ai_resilience, ControlConfigurationError};
pub use obzenflow_adapters::middleware::{
    circuit_breaker, effect_observer, handler_observer, join_observer, rate_limit, retry,
    sink_delivery_observer, source_poll_observer, stage_lifecycle_observer, stateful_observer,
    CircuitBreaker, CircuitBreakerConfigError, EffectObserverFactory, FailureHealth,
    HandlerObserverFactory, JoinObserverFactory, MiddlewareFactory, MiddlewareFactoryError,
    MiddlewareFactoryResult, RateLimiter, Retry, SinkDeliveryObserver, SinkDeliveryObserverFactory,
    SourcePollObserverFactory, StageLifecycleObserverFactory, StatefulObserverFactory,
};
// Application-owned control factories use the same checked attachment contract
// as built-in policies, without importing implementation crates.
pub use obzenflow_adapters::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, SinkAdmission, SinkDeliveryPolicyOutcome, SinkPolicy, SinkPolicyCtx,
};
pub use obzenflow_runtime::stages::observer::{
    EffectObserver, EffectObserverContext, EffectObserverOutcome, HandlerObserver,
    HandlerObserverContext, JoinDeliverySnapshot, JoinObserver, JoinObserverContext,
    JoinObserverOccurrence, JoinSide, JoinSignalKind, JoinSignalSnapshot, ObserverError,
    ObserverResult, SinkDeliveryAttemptResult, SinkDeliveryObserverContext,
    SinkDeliveryObserverOutcome, SourcePollObserver, SourcePollObserverContext,
    SourcePollObserverOutcome, StageInputPosition, StageLifecycleObserver,
    StageLifecycleObserverContext, StageLifecyclePhase, StatefulObserver, StatefulObserverContext,
};

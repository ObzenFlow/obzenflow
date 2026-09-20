// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Live-I/O policies and passive observers, separate from stage handlers.

pub use obzenflow_core::event::context::StageType;
pub use obzenflow_core::event::vector_clock::VectorClock;

pub use obzenflow_adapters::middleware::control::circuit_breaker::CheckedCircuitBreakerBuilder;
pub use obzenflow_adapters::middleware::control::rate_limiter::RateLimiterConfigError;
pub use obzenflow_adapters::middleware::control::{ai_resilience, EffectResilienceBuilder};
pub use obzenflow_adapters::middleware::{
    effect_observer, handler_observer, join_observer, rate_limit, rate_limit_with_burst,
    sink_delivery_observer, source_poll_observer, stage_lifecycle_observer, stateful_observer,
    CircuitBreaker, CircuitBreakerConfigError, EffectObserverFactory, EffectResilience,
    EffectResilienceConfigError, FailureHealth, HandlerObserverFactory, JoinObserverFactory,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult, RateLimiter,
    RateLimiterBuilder, RateLimiterFactory, Retry, SinkDeliveryObserverFactory,
    SourcePollObserverFactory, StageLifecycleObserverFactory, StatefulObserverFactory,
};
pub use obzenflow_runtime::stages::observer::{
    EffectObserver, EffectObserverContext, EffectObserverOutcome, HandlerObserver,
    HandlerObserverContext, JoinDeliverySnapshot, JoinObserver, JoinObserverContext,
    JoinObserverOccurrence, JoinSide, JoinSignalKind, JoinSignalSnapshot,
    SinkDeliveryAttemptResult, SinkDeliveryObserver, SinkDeliveryObserverContext,
    SinkDeliveryObserverOutcome, SourcePollObserver, SourcePollObserverContext,
    SourcePollObserverOutcome, StageInputPosition, StageLifecycleObserver,
    StageLifecycleObserverContext, StageLifecyclePhase, StatefulObserver, StatefulObserverContext,
};

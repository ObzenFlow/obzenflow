// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

pub mod circuit_breaker;
pub mod composition;
pub mod policy;
pub mod provider;
pub mod rate_limiter;
mod resilience;

// Re-export key types for convenience
pub use circuit_breaker::{
    circuit_breaker, retry, CircuitBreaker, CircuitBreakerConfigError, FailureHealth, Retry,
};
#[cfg(feature = "test-support")]
pub use composition::ai_recovery_rejecting_resilience_for_test;
pub use composition::{
    ai_resilience, compose_effect_controls, AiResilience, BuiltinControlContribution,
    BuiltinControlFamily, ControlCompositionError,
};
pub use provider::ControlMiddlewareAggregator;
pub use rate_limiter::{rate_limit, RateLimiter, RateLimiterMiddleware};
pub use resilience::ControlConfigurationError;
pub(in crate::middleware::control) use resilience::EffectResilienceMiddleware;

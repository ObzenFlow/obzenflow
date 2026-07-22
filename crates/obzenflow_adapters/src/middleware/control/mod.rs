// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

pub mod circuit_breaker;
pub mod policy;
pub mod provider;
pub mod rate_limiter;
mod resilience;

// Re-export key types for convenience
pub use circuit_breaker::{
    ai_circuit_breaker, CircuitBreaker, CircuitBreakerConfigError, FailureHealth, Retry,
};
pub use provider::ControlMiddlewareAggregator;
pub use rate_limiter::{
    rate_limit, rate_limit_with_burst, RateLimiter, RateLimiterBuilder, RateLimiterFactory,
    RateLimiterMiddleware,
};
pub(in crate::middleware::control) use resilience::EffectResilienceMiddleware;
pub use resilience::{EffectResilience, EffectResilienceConfigError};

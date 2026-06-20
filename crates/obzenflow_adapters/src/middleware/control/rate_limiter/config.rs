// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Rate-limiter configuration validation (FLOWIP-115d).
//!
//! Surface-neutral configuration types extracted from the monolithic
//! `rate_limiter` module. This file has no `ChainEvent`, `MiddlewareContext`,
//! or policy-trait dependencies; it only validates numeric configuration and
//! derives the effective bucket capacity and limit rate.

use thiserror::Error;

pub(crate) const DEFAULT_COST_PER_EVENT: f64 = 1.0;

pub(crate) fn effective_capacity(
    events_per_second: f64,
    burst_capacity: Option<f64>,
    cost_per_event: f64,
) -> f64 {
    burst_capacity.unwrap_or_else(|| events_per_second.max(cost_per_event).max(1.0))
}

pub(crate) fn effective_limit_rate(events_per_second: f64, cost_per_event: f64) -> f64 {
    events_per_second / cost_per_event
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ValidatedRateLimiterConfig {
    pub(crate) events_per_second: f64,
    pub(crate) configured_burst_capacity: Option<f64>,
    pub(crate) burst_capacity: f64,
    pub(crate) cost_per_event: f64,
}

impl ValidatedRateLimiterConfig {
    pub(crate) fn limit_rate(self) -> f64 {
        effective_limit_rate(self.events_per_second, self.cost_per_event)
    }
}

#[derive(Debug, Error, Clone, Copy, PartialEq)]
pub(crate) enum RateLimiterConfigError {
    #[error("rate_limiter events_per_second must be finite and > 0, got {events_per_second}")]
    InvalidEventsPerSecond { events_per_second: f64 },

    #[error("rate_limiter cost_per_event must be finite and > 0, got {cost_per_event}")]
    InvalidCostPerEvent { cost_per_event: f64 },

    #[error("rate_limiter burst_capacity must be finite and > 0, got {burst_capacity}")]
    InvalidBurstCapacity { burst_capacity: f64 },

    #[error(
        "rate_limiter burst_capacity ({burst_capacity}) must be >= cost_per_event ({cost_per_event})"
    )]
    BurstCapacityBelowCost {
        burst_capacity: f64,
        cost_per_event: f64,
    },
}

pub(crate) fn validated_rate_limiter_config(
    events_per_second: f64,
    burst_capacity: Option<f64>,
    cost_per_event: f64,
) -> Result<ValidatedRateLimiterConfig, RateLimiterConfigError> {
    if !events_per_second.is_finite() || events_per_second <= 0.0 {
        return Err(RateLimiterConfigError::InvalidEventsPerSecond { events_per_second });
    }

    if !cost_per_event.is_finite() || cost_per_event <= 0.0 {
        return Err(RateLimiterConfigError::InvalidCostPerEvent { cost_per_event });
    }

    if let Some(explicit_burst) = burst_capacity {
        if !explicit_burst.is_finite() || explicit_burst <= 0.0 {
            return Err(RateLimiterConfigError::InvalidBurstCapacity {
                burst_capacity: explicit_burst,
            });
        }

        if explicit_burst < cost_per_event {
            return Err(RateLimiterConfigError::BurstCapacityBelowCost {
                burst_capacity: explicit_burst,
                cost_per_event,
            });
        }
    }

    Ok(ValidatedRateLimiterConfig {
        events_per_second,
        configured_burst_capacity: burst_capacity,
        burst_capacity: effective_capacity(events_per_second, burst_capacity, cost_per_event),
        cost_per_event,
    })
}

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Concrete implementation of `ControlMiddlewareProvider`.
//!
//! This flow-scoped aggregator collects registrations from circuit breaker and
//! rate limiter middleware instances and exposes them through the core trait so
//! runtime_services can consume control middleware state without global state.

use obzenflow_core::control_middleware::{
    CircuitBreakerSnapshotter, ControlMiddlewareProvider, RateLimiterSnapshotter,
};
use obzenflow_core::id::StageId;
use std::collections::HashMap;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, RwLock};

struct CircuitBreakerRegistration {
    metrics_fn: Arc<CircuitBreakerSnapshotter>,
    state: Arc<AtomicU8>,
}

struct RateLimiterRegistration {
    metrics_fn: Arc<RateLimiterSnapshotter>,
}

/// Aggregates control middleware from multiple stages.
///
/// Created once per flow and shared with all middleware and consumers.
#[derive(Default)]
pub struct ControlMiddlewareAggregator {
    circuit_breakers: RwLock<HashMap<StageId, CircuitBreakerRegistration>>,
    rate_limiters: RwLock<HashMap<StageId, RateLimiterRegistration>>,
}

impl ControlMiddlewareAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_circuit_breaker(
        &self,
        stage_id: StageId,
        metrics_fn: Arc<CircuitBreakerSnapshotter>,
        state: Arc<AtomicU8>,
    ) {
        let registration = CircuitBreakerRegistration { metrics_fn, state };

        self.circuit_breakers
            .write()
            .expect("ControlMiddlewareAggregator: circuit_breakers poisoned write lock")
            .insert(stage_id, registration);
    }

    pub fn register_rate_limiter(
        &self,
        stage_id: StageId,
        metrics_fn: Arc<RateLimiterSnapshotter>,
    ) {
        let registration = RateLimiterRegistration { metrics_fn };
        self.rate_limiters
            .write()
            .expect("ControlMiddlewareAggregator: rate_limiters poisoned write lock")
            .insert(stage_id, registration);
    }
}

impl ControlMiddlewareProvider for ControlMiddlewareAggregator {
    fn circuit_breaker_snapshotter(
        &self,
        stage_id: &StageId,
    ) -> Option<Arc<CircuitBreakerSnapshotter>> {
        self.circuit_breakers
            .read()
            .expect("ControlMiddlewareAggregator: circuit_breakers poisoned read lock")
            .get(stage_id)
            .map(|reg| reg.metrics_fn.clone())
    }

    fn rate_limiter_snapshotter(&self, stage_id: &StageId) -> Option<Arc<RateLimiterSnapshotter>> {
        self.rate_limiters
            .read()
            .expect("ControlMiddlewareAggregator: rate_limiters poisoned read lock")
            .get(stage_id)
            .map(|reg| reg.metrics_fn.clone())
    }

    fn circuit_breaker_state(&self, stage_id: &StageId) -> Option<Arc<AtomicU8>> {
        self.circuit_breakers
            .read()
            .expect("ControlMiddlewareAggregator: circuit_breakers poisoned read lock")
            .get(stage_id)
            .map(|reg| reg.state.clone())
    }
}

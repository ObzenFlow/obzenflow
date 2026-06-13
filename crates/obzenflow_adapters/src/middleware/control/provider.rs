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

/// Registry key: a stage-level instance registers under `None`, a per-effect
/// instance under `Some(effect_type)` (FLOWIP-120c gap G3). One policy
/// instance guards one protected dependency, so per-effect cardinality is
/// bounded by the stage's declared `effects:` set.
type ControlKey = (StageId, Option<String>);

/// Aggregates control middleware from multiple stages.
///
/// Created once per flow and shared with all middleware and consumers.
#[derive(Default)]
pub struct ControlMiddlewareAggregator {
    circuit_breakers: RwLock<HashMap<ControlKey, CircuitBreakerRegistration>>,
    rate_limiters: RwLock<HashMap<ControlKey, RateLimiterRegistration>>,
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
        self.register_circuit_breaker_keyed(stage_id, None, metrics_fn, state);
    }

    /// Register a per-effect circuit breaker instance (FLOWIP-120c).
    pub fn register_circuit_breaker_for_effect(
        &self,
        stage_id: StageId,
        effect_type: String,
        metrics_fn: Arc<CircuitBreakerSnapshotter>,
        state: Arc<AtomicU8>,
    ) {
        self.register_circuit_breaker_keyed(stage_id, Some(effect_type), metrics_fn, state);
    }

    fn register_circuit_breaker_keyed(
        &self,
        stage_id: StageId,
        effect_type: Option<String>,
        metrics_fn: Arc<CircuitBreakerSnapshotter>,
        state: Arc<AtomicU8>,
    ) {
        let registration = CircuitBreakerRegistration { metrics_fn, state };

        self.circuit_breakers
            .write()
            .expect("ControlMiddlewareAggregator: circuit_breakers poisoned write lock")
            .insert((stage_id, effect_type), registration);
    }

    pub fn register_rate_limiter(
        &self,
        stage_id: StageId,
        metrics_fn: Arc<RateLimiterSnapshotter>,
    ) {
        self.register_rate_limiter_keyed(stage_id, None, metrics_fn);
    }

    /// Register a per-effect rate limiter instance (FLOWIP-120c).
    pub fn register_rate_limiter_for_effect(
        &self,
        stage_id: StageId,
        effect_type: String,
        metrics_fn: Arc<RateLimiterSnapshotter>,
    ) {
        self.register_rate_limiter_keyed(stage_id, Some(effect_type), metrics_fn);
    }

    fn register_rate_limiter_keyed(
        &self,
        stage_id: StageId,
        effect_type: Option<String>,
        metrics_fn: Arc<RateLimiterSnapshotter>,
    ) {
        let registration = RateLimiterRegistration { metrics_fn };
        self.rate_limiters
            .write()
            .expect("ControlMiddlewareAggregator: rate_limiters poisoned write lock")
            .insert((stage_id, effect_type), registration);
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
            .get(&(*stage_id, None))
            .map(|reg| reg.metrics_fn.clone())
    }

    fn rate_limiter_snapshotter(&self, stage_id: &StageId) -> Option<Arc<RateLimiterSnapshotter>> {
        self.rate_limiters
            .read()
            .expect("ControlMiddlewareAggregator: rate_limiters poisoned read lock")
            .get(&(*stage_id, None))
            .map(|reg| reg.metrics_fn.clone())
    }

    fn circuit_breaker_state(&self, stage_id: &StageId) -> Option<Arc<AtomicU8>> {
        self.circuit_breakers
            .read()
            .expect("ControlMiddlewareAggregator: circuit_breakers poisoned read lock")
            .get(&(*stage_id, None))
            .map(|reg| reg.state.clone())
    }

    fn effect_circuit_breaker_snapshotters(
        &self,
        stage_id: &StageId,
    ) -> Vec<(String, Arc<CircuitBreakerSnapshotter>)> {
        let mut entries: Vec<(String, Arc<CircuitBreakerSnapshotter>)> = self
            .circuit_breakers
            .read()
            .expect("ControlMiddlewareAggregator: circuit_breakers poisoned read lock")
            .iter()
            .filter_map(|((sid, effect), reg)| {
                (sid == stage_id)
                    .then(|| effect.clone().map(|e| (e, reg.metrics_fn.clone())))
                    .flatten()
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    fn effect_rate_limiter_snapshotters(
        &self,
        stage_id: &StageId,
    ) -> Vec<(String, Arc<RateLimiterSnapshotter>)> {
        let mut entries: Vec<(String, Arc<RateLimiterSnapshotter>)> = self
            .rate_limiters
            .read()
            .expect("ControlMiddlewareAggregator: rate_limiters poisoned read lock")
            .iter()
            .filter_map(|((sid, effect), reg)| {
                (sid == stage_id)
                    .then(|| effect.clone().map(|e| (e, reg.metrics_fn.clone())))
                    .flatten()
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }
}

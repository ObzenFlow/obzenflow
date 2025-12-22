//! Concrete implementation of `ControlMiddlewareProvider`.
//!
//! This flow-scoped aggregator collects registrations from circuit breaker and
//! rate limiter middleware instances and exposes them through the core trait so
//! runtime_services can consume control middleware state without global state.

use obzenflow_core::control_middleware::{
    CircuitBreakerContractInfo, CircuitBreakerContractMode, CircuitBreakerSnapshotter,
    ControlMiddlewareProvider, RateLimiterSnapshotter,
};
use obzenflow_core::id::StageId;
use std::collections::HashMap;
use std::sync::atomic::AtomicU8;
use std::sync::{Arc, RwLock};

struct CircuitBreakerRegistration {
    metrics_fn: Arc<CircuitBreakerSnapshotter>,
    state: Arc<AtomicU8>,
    contract_info: CircuitBreakerContractInfo,
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
        mode: CircuitBreakerContractMode,
        has_fallback: bool,
    ) {
        let registration = CircuitBreakerRegistration {
            metrics_fn,
            state,
            contract_info: CircuitBreakerContractInfo {
                mode,
                has_opened_since_registration: false,
                has_fallback_configured: has_fallback,
            },
        };

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

    fn rate_limiter_snapshotter(
        &self,
        stage_id: &StageId,
    ) -> Option<Arc<RateLimiterSnapshotter>> {
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

    fn circuit_breaker_contract_info(
        &self,
        stage_id: &StageId,
    ) -> Option<CircuitBreakerContractInfo> {
        self.circuit_breakers
            .read()
            .expect("ControlMiddlewareAggregator: circuit_breakers poisoned read lock")
            .get(stage_id)
            .map(|reg| reg.contract_info)
    }

    fn mark_circuit_breaker_opened(&self, stage_id: &StageId) {
        if let Some(reg) = self
            .circuit_breakers
            .write()
            .expect("ControlMiddlewareAggregator: circuit_breakers poisoned write lock")
            .get_mut(stage_id)
        {
            reg.contract_info.has_opened_since_registration = true;
        }
    }
}


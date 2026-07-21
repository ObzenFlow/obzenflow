// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Concrete implementation of `ControlPlaneProvider`.
//!
//! This flow-scoped aggregator collects registrations from circuit breaker and
//! rate limiter middleware instances and exposes them through the runtime
//! control-plane port so runtime services can consume read-only control state
//! without depending on adapter middleware traits.

use crate::middleware::{EffectTypeKey, SourcePolicy};
use obzenflow_core::id::StageId;
use obzenflow_runtime::control_plane::{
    CircuitBreakerSnapshotter, CircuitBreakerStateView, ControlPlaneProvider,
    RateLimiterSnapshotter,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

struct CircuitBreakerRegistration {
    metrics_fn: Arc<CircuitBreakerSnapshotter>,
    /// FLOWIP-115b: typed read-only state view published by the breaker, the
    /// single breaker state authority.
    state_view: Arc<dyn CircuitBreakerStateView>,
}

struct RateLimiterRegistration {
    metrics_fn: Arc<RateLimiterSnapshotter>,
}

/// Registry key: a stage-level instance registers under `None`, a per-effect
/// instance under `Some(effect_type)` (FLOWIP-120c gap G3). One policy
/// instance guards one protected dependency, so per-effect cardinality is
/// bounded by the stage's declared `effects:` set.
type ControlKey = (StageId, Option<EffectTypeKey>);

/// Aggregates control middleware from multiple stages.
///
/// Created once per flow and shared with all middleware and consumers.
#[derive(Default)]
pub struct ControlMiddlewareAggregator {
    circuit_breakers: RwLock<HashMap<ControlKey, CircuitBreakerRegistration>>,
    rate_limiters: RwLock<HashMap<ControlKey, RateLimiterRegistration>>,
    // FLOWIP-115a: adapter-owned source policy chains. The descriptor turns
    // these into one runtime-neutral source boundary.
    source_policies: RwLock<HashMap<ControlKey, Vec<Arc<dyn SourcePolicy>>>>,
}

impl ControlMiddlewareAggregator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_circuit_breaker(
        &self,
        stage_id: StageId,
        metrics_fn: Arc<CircuitBreakerSnapshotter>,
        state_view: Arc<dyn CircuitBreakerStateView>,
    ) -> Result<(), String> {
        self.register_circuit_breaker_keyed(stage_id, None, metrics_fn, state_view)
    }

    /// Register a per-effect circuit breaker instance (FLOWIP-120c).
    pub fn register_circuit_breaker_for_effect(
        &self,
        stage_id: StageId,
        effect_type: EffectTypeKey,
        metrics_fn: Arc<CircuitBreakerSnapshotter>,
        state_view: Arc<dyn CircuitBreakerStateView>,
    ) -> Result<(), String> {
        self.register_circuit_breaker_keyed(stage_id, Some(effect_type), metrics_fn, state_view)
    }

    fn register_circuit_breaker_keyed(
        &self,
        stage_id: StageId,
        effect_type: Option<EffectTypeKey>,
        metrics_fn: Arc<CircuitBreakerSnapshotter>,
        state_view: Arc<dyn CircuitBreakerStateView>,
    ) -> Result<(), String> {
        let registration = CircuitBreakerRegistration {
            metrics_fn,
            state_view,
        };

        let mut registrations = self
            .circuit_breakers
            .write()
            .expect("ControlMiddlewareAggregator: circuit_breakers poisoned write lock");
        let key = (stage_id, effect_type);
        if registrations.contains_key(&key) {
            return Err(format!(
                "a circuit breaker is already registered for stage {stage_id} and effect {:?}",
                key.1.as_ref().map(EffectTypeKey::as_str)
            ));
        }
        registrations.insert(key, registration);
        Ok(())
    }

    pub fn register_rate_limiter(
        &self,
        stage_id: StageId,
        metrics_fn: Arc<RateLimiterSnapshotter>,
    ) -> Result<(), String> {
        self.register_rate_limiter_keyed(stage_id, None, metrics_fn)
    }

    /// Register a per-effect rate limiter instance (FLOWIP-120c).
    pub fn register_rate_limiter_for_effect(
        &self,
        stage_id: StageId,
        effect_type: EffectTypeKey,
        metrics_fn: Arc<RateLimiterSnapshotter>,
    ) -> Result<(), String> {
        self.register_rate_limiter_keyed(stage_id, Some(effect_type), metrics_fn)
    }

    fn register_rate_limiter_keyed(
        &self,
        stage_id: StageId,
        effect_type: Option<EffectTypeKey>,
        metrics_fn: Arc<RateLimiterSnapshotter>,
    ) -> Result<(), String> {
        let registration = RateLimiterRegistration { metrics_fn };
        let mut registrations = self
            .rate_limiters
            .write()
            .expect("ControlMiddlewareAggregator: rate_limiters poisoned write lock");
        let key = (stage_id, effect_type);
        if registrations.contains_key(&key) {
            return Err(format!(
                "a rate limiter is already registered for stage {stage_id} and effect {:?}",
                key.1.as_ref().map(EffectTypeKey::as_str)
            ));
        }
        registrations.insert(key, registration);
        Ok(())
    }

    /// FLOWIP-115a: append a source policy for a stage, in declared order.
    pub fn register_source_policy(&self, stage_id: StageId, policy: Arc<dyn SourcePolicy>) {
        self.source_policies
            .write()
            .expect("ControlMiddlewareAggregator: source_policies poisoned write lock")
            .entry((stage_id, None))
            .or_default()
            .push(policy);
    }

    /// FLOWIP-115a: the source policies registered for a stage, in declared
    /// order.
    pub fn source_policies(&self, stage_id: &StageId) -> Vec<Arc<dyn SourcePolicy>> {
        self.source_policies
            .read()
            .expect("ControlMiddlewareAggregator: source_policies poisoned read lock")
            .get(&(*stage_id, None))
            .cloned()
            .unwrap_or_default()
    }
}

impl ControlPlaneProvider for ControlMiddlewareAggregator {
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

    fn circuit_breaker_state_view(
        &self,
        stage_id: &StageId,
    ) -> Option<Arc<dyn CircuitBreakerStateView>> {
        self.circuit_breakers
            .read()
            .expect("ControlMiddlewareAggregator: circuit_breakers poisoned read lock")
            .get(&(*stage_id, None))
            .map(|reg| reg.state_view.clone())
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
                    .then(|| {
                        effect
                            .clone()
                            .map(|e| (e.as_str().to_string(), reg.metrics_fn.clone()))
                    })
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
                    .then(|| {
                        effect
                            .clone()
                            .map(|e| (e.as_str().to_string(), reg.metrics_fn.clone()))
                    })
                    .flatten()
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }
}

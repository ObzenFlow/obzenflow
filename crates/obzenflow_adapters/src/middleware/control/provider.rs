// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Concrete implementation of `ControlPlaneProvider`.
//!
//! This flow-scoped aggregator collects registrations from circuit breaker and
//! rate limiter middleware instances and exposes them through the runtime
//! control-plane port so runtime services can consume read-only control state
//! without depending on adapter middleware traits.

use crate::middleware::EffectTypeKey;
use obzenflow_core::id::StageId;
use obzenflow_runtime::control_plane::{
    CircuitBreakerSnapshotter, CircuitBreakerStateView, ControlPlaneProvider,
    RateLimiterSnapshotter,
};
use std::collections::{HashMap, HashSet};
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

pub(crate) enum PendingControlRegistration {
    CircuitBreaker {
        stage_id: StageId,
        effect_type: Option<EffectTypeKey>,
        metrics_fn: Arc<CircuitBreakerSnapshotter>,
        state_view: Arc<dyn CircuitBreakerStateView>,
    },
    RateLimiter {
        stage_id: StageId,
        effect_type: Option<EffectTypeKey>,
        metrics_fn: Arc<RateLimiterSnapshotter>,
    },
}

impl PendingControlRegistration {
    pub(crate) fn target(&self) -> (StageId, Option<&EffectTypeKey>) {
        match self {
            Self::CircuitBreaker {
                stage_id,
                effect_type,
                ..
            }
            | Self::RateLimiter {
                stage_id,
                effect_type,
                ..
            } => (*stage_id, effect_type.as_ref()),
        }
    }
}

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

    /// Atomically commit one checked factory invocation's staged authority.
    /// Both maps are locked in breaker-then-limiter order, every key is
    /// preflighted (including duplicates within the batch), and insertion only
    /// begins after the complete batch is known to be valid.
    pub(crate) fn commit_batch(
        &self,
        pending: Vec<PendingControlRegistration>,
    ) -> Result<(), String> {
        let mut circuit_breakers = self
            .circuit_breakers
            .write()
            .expect("ControlMiddlewareAggregator: circuit_breakers poisoned write lock");
        let mut rate_limiters = self
            .rate_limiters
            .write()
            .expect("ControlMiddlewareAggregator: rate_limiters poisoned write lock");

        let mut pending_breakers = HashSet::new();
        let mut pending_limiters = HashSet::new();
        for registration in &pending {
            let (kind, key, existing, duplicate) = match registration {
                PendingControlRegistration::CircuitBreaker {
                    stage_id,
                    effect_type,
                    ..
                } => {
                    let key = (*stage_id, effect_type.clone());
                    let existing = circuit_breakers.contains_key(&key);
                    let duplicate = !pending_breakers.insert(key.clone());
                    ("circuit breaker", key, existing, duplicate)
                }
                PendingControlRegistration::RateLimiter {
                    stage_id,
                    effect_type,
                    ..
                } => {
                    let key = (*stage_id, effect_type.clone());
                    let existing = rate_limiters.contains_key(&key);
                    let duplicate = !pending_limiters.insert(key.clone());
                    ("rate limiter", key, existing, duplicate)
                }
            };
            if existing || duplicate {
                return Err(format!(
                    "a {kind} is already registered for stage {} and effect {:?}",
                    key.0,
                    key.1.as_ref().map(EffectTypeKey::as_str)
                ));
            }
        }

        for registration in pending {
            match registration {
                PendingControlRegistration::CircuitBreaker {
                    stage_id,
                    effect_type,
                    metrics_fn,
                    state_view,
                } => {
                    circuit_breakers.insert(
                        (stage_id, effect_type),
                        CircuitBreakerRegistration {
                            metrics_fn,
                            state_view,
                        },
                    );
                }
                PendingControlRegistration::RateLimiter {
                    stage_id,
                    effect_type,
                    metrics_fn,
                } => {
                    rate_limiters.insert(
                        (stage_id, effect_type),
                        RateLimiterRegistration { metrics_fn },
                    );
                }
            }
        }
        Ok(())
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

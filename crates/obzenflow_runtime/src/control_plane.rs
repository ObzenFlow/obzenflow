// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime-neutral control-plane ports and DTOs.
//!
//! Outer-layer middleware adapters publish read-only metrics/state projections
//! through these ports. Runtime code consumes the projections without depending
//! on adapter middleware traits, factories, or concrete implementations.

use obzenflow_core::StageId;
use std::sync::Arc;

/// Snapshot of cumulative circuit breaker metrics.
#[derive(Debug, Clone, Default)]
pub struct CircuitBreakerMetrics {
    pub requests_total: u64,
    pub successes_total: u64,
    pub failures_total: u64,
    pub rejections_total: u64,
    pub opened_total: u64,
    pub time_closed_seconds: f64,
    pub time_open_seconds: f64,
    pub time_half_open_seconds: f64,
    /// Current breaker state encoded by the publishing adapter
    /// (`0=closed`, `1=open`, `2=half_open`).
    pub state: u8,
}

/// Snapshot of cumulative rate limiter metrics.
#[derive(Debug, Clone, Default)]
pub struct RateLimiterMetrics {
    pub events_total: u64,
    pub delayed_total: u64,
    pub tokens_consumed_total: f64,
    pub delay_seconds_total: f64,

    /// Current tokens available in the bucket.
    pub bucket_tokens: f64,
    /// Maximum capacity of the bucket.
    pub bucket_capacity: f64,
}

/// Circuit breaker state encoding retained for legacy metric compatibility.
pub mod cb_state {
    pub const CLOSED: u8 = 0;
    pub const OPEN: u8 = 1;
    pub const HALF_OPEN: u8 = 2;
}

/// Typed read-only circuit breaker state (FLOWIP-115b).
///
/// This is the projection seam that replaces direct reads of the breaker's
/// private state cell. Source completion, instrumentation, topology, and
/// exporters consult this view instead of a raw mutable cell, so there is one
/// breaker state authority per protected unit. FLOWIP-115i swaps the private
/// state core behind this same view without changing consumers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreakerState {
    pub fn is_open(self) -> bool {
        matches!(self, Self::Open)
    }

    /// Decode the legacy numeric encoding. Unknown values fall back to
    /// `Closed`, matching the historical breaker behavior.
    pub fn from_u8(value: u8) -> Self {
        match value {
            cb_state::OPEN => Self::Open,
            cb_state::HALF_OPEN => Self::HalfOpen,
            _ => Self::Closed,
        }
    }

    /// The legacy numeric encoding kept for `CircuitBreakerMetrics.state`
    /// compatibility.
    pub fn as_u8(self) -> u8 {
        match self {
            Self::Closed => cb_state::CLOSED,
            Self::Open => cb_state::OPEN,
            Self::HalfOpen => cb_state::HALF_OPEN,
        }
    }

    /// Stable label string used in metrics, topology, and serialized
    /// diagnostics.
    pub fn stable_label(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }

    /// Stable Prometheus gauge value (`0=closed`, `0.5=half_open`, `1=open`).
    pub fn stable_gauge(self) -> f64 {
        match self {
            Self::Closed => 0.0,
            Self::Open => 1.0,
            Self::HalfOpen => 0.5,
        }
    }
}

/// A read-only snapshot of one protected unit's breaker state plus a monotonic
/// generation for staleness reasoning by topology/completion/instrumentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CircuitBreakerStateSnapshot {
    pub state: CircuitBreakerState,
    pub generation: u64,
}

/// Read-only view of a breaker's authoritative state. Published by the
/// per-protected-unit state authority; it is a projection, not a transition
/// surface, so holders cannot mutate breaker state.
pub trait CircuitBreakerStateView: Send + Sync + std::fmt::Debug {
    fn snapshot(&self) -> CircuitBreakerStateSnapshot;

    /// Convenience: is the breaker currently open?
    fn is_open(&self) -> bool {
        self.snapshot().state.is_open()
    }
}

/// Snapshotter closure type for circuit breaker metrics.
pub type CircuitBreakerSnapshotter = dyn Fn() -> CircuitBreakerMetrics + Send + Sync;

/// Snapshotter closure type for rate limiter metrics.
pub type RateLimiterSnapshotter = dyn Fn() -> RateLimiterMetrics + Send + Sync;

/// Runtime-facing provider of control-plane state and metrics.
///
/// Adapter-owned control middleware may implement this trait, but runtime code
/// only sees this read-only port. It does not expose middleware factories,
/// execution hooks, or adapter carrier concepts.
pub trait ControlPlaneProvider: Send + Sync {
    /// Get circuit breaker snapshotter for a stage.
    fn circuit_breaker_snapshotter(
        &self,
        stage_id: &StageId,
    ) -> Option<Arc<CircuitBreakerSnapshotter>>;

    /// Get rate limiter snapshotter for a stage.
    fn rate_limiter_snapshotter(&self, stage_id: &StageId) -> Option<Arc<RateLimiterSnapshotter>>;

    /// Get a typed read-only circuit breaker state view for a stage.
    fn circuit_breaker_state_view(
        &self,
        _stage_id: &StageId,
    ) -> Option<Arc<dyn CircuitBreakerStateView>> {
        None
    }

    /// Enumerate per-effect circuit breaker snapshotters for a stage, keyed by
    /// declared effect type.
    fn effect_circuit_breaker_snapshotters(
        &self,
        _stage_id: &StageId,
    ) -> Vec<(String, Arc<CircuitBreakerSnapshotter>)> {
        Vec::new()
    }

    /// Enumerate per-effect rate limiter snapshotters for a stage, keyed by
    /// declared effect type.
    fn effect_rate_limiter_snapshotters(
        &self,
        _stage_id: &StageId,
    ) -> Vec<(String, Arc<RateLimiterSnapshotter>)> {
        Vec::new()
    }
}

/// Null implementation for flows without control-plane publishers.
#[derive(Debug, Clone, Default)]
pub struct NoControlPlane;

impl ControlPlaneProvider for NoControlPlane {
    fn circuit_breaker_snapshotter(&self, _: &StageId) -> Option<Arc<CircuitBreakerSnapshotter>> {
        None
    }

    fn rate_limiter_snapshotter(&self, _: &StageId) -> Option<Arc<RateLimiterSnapshotter>> {
        None
    }
}

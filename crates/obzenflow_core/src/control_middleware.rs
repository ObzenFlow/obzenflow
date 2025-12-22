//! Traits and DTOs for control middleware (circuit breaker, rate limiter).
//!
//! This module defines the *ports* that inner layers can depend on without
//! pulling in concrete implementations from outer layers. Implementations
//! live in adapters and are injected at construction time.

use crate::id::StageId;
use std::sync::atomic::AtomicU8;
use std::sync::Arc;

// ============================================================================
// Circuit Breaker Metrics (replaces control_metrics_registry CB portion)
// ============================================================================

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
    /// Current breaker state encoded by middleware (0=closed,1=open,2=half_open).
    pub state: u8,
}

// ============================================================================
// Rate Limiter Metrics (replaces control_metrics_registry RL portion)
// ============================================================================

/// Snapshot of cumulative rate limiter metrics.
#[derive(Debug, Clone, Default)]
pub struct RateLimiterMetrics {
    pub events_total: u64,
    pub delayed_total: u64,
    pub tokens_consumed_total: f64,
    pub delay_seconds_total: f64,

    // Bucket state for gauge metrics (FLOWIP-059a-3 Issue 3)
    /// Current tokens available in the bucket.
    pub bucket_tokens: f64,
    /// Maximum capacity of the bucket.
    pub bucket_capacity: f64,
}

// ============================================================================
// Circuit Breaker State (replaces circuit_breaker_registry)
// ============================================================================

/// Circuit breaker state values.
pub mod cb_state {
    pub const CLOSED: u8 = 0;
    pub const OPEN: u8 = 1;
    pub const HALF_OPEN: u8 = 2;
}

// ============================================================================
// Circuit Breaker Contract Info (replaces circuit_breaker_contract_registry)
// ============================================================================

/// How contract policies should interpret circuit breaker activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CircuitBreakerContractMode {
    #[default]
    Strict,
    BreakerAware,
}

/// Contract-related metadata for a breaker-protected stage.
#[derive(Debug, Clone, Copy, Default)]
pub struct CircuitBreakerContractInfo {
    pub mode: CircuitBreakerContractMode,
    pub has_opened_since_registration: bool,
    pub has_fallback_configured: bool,
}

// ============================================================================
// Unified Provider Trait
// ============================================================================

/// Snapshotter closure type for circuit breaker metrics.
pub type CircuitBreakerSnapshotter = dyn Fn() -> CircuitBreakerMetrics + Send + Sync;

/// Snapshotter closure type for rate limiter metrics.
pub type RateLimiterSnapshotter = dyn Fn() -> RateLimiterMetrics + Send + Sync;

/// Provider of control middleware state and metrics for stages.
///
/// Implemented by a flow-scoped aggregator in the adapters crate. Consumed by
/// runtime_services for instrumentation, control strategies, and contract policies.
pub trait ControlMiddlewareProvider: Send + Sync {
    // --- Snapshotters (for caching in instrumentation) ---

    /// Get circuit breaker snapshotter for a stage.
    fn circuit_breaker_snapshotter(
        &self,
        stage_id: &StageId,
    ) -> Option<Arc<CircuitBreakerSnapshotter>>;

    /// Get rate limiter snapshotter for a stage.
    fn rate_limiter_snapshotter(&self, stage_id: &StageId) -> Option<Arc<RateLimiterSnapshotter>>;

    // --- State (for control strategies / retry logic) ---

    /// Get circuit breaker current state for a stage.
    fn circuit_breaker_state(&self, stage_id: &StageId) -> Option<Arc<AtomicU8>>;

    // --- Contract Info (for contract policies) ---

    /// Get circuit breaker contract info for a stage.
    fn circuit_breaker_contract_info(
        &self,
        stage_id: &StageId,
    ) -> Option<CircuitBreakerContractInfo>;

    /// Mark that the breaker for this stage has opened at least once.
    fn mark_circuit_breaker_opened(&self, stage_id: &StageId);
}

/// Null implementation for flows without control middleware.
#[derive(Debug, Clone, Default)]
pub struct NoControlMiddleware;

impl ControlMiddlewareProvider for NoControlMiddleware {
    fn circuit_breaker_snapshotter(
        &self,
        _: &StageId,
    ) -> Option<Arc<CircuitBreakerSnapshotter>> {
        None
    }

    fn rate_limiter_snapshotter(&self, _: &StageId) -> Option<Arc<RateLimiterSnapshotter>> {
        None
    }

    fn circuit_breaker_state(&self, _: &StageId) -> Option<Arc<AtomicU8>> {
        None
    }

    fn circuit_breaker_contract_info(&self, _: &StageId) -> Option<CircuitBreakerContractInfo> {
        None
    }

    fn mark_circuit_breaker_opened(&self, _: &StageId) {}
}


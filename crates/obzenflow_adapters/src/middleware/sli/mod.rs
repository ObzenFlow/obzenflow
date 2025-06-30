/// Service Level Indicator (SLI) Middleware
/// 
/// This module provides middleware implementations that compute business-level
/// indicators from raw operational events. SLIs bridge the gap between low-level
/// operational metrics and high-level business objectives.
/// 
/// ## Architecture
/// 
/// SLI middleware sits in Layer 2 of the middleware stack:
/// ```text
/// Layer 1: Operational → emits raw events (circuit breaker, retry, etc.)
/// Layer 2: SLI        → computes indicators from raw events
/// Layer 3: Monitoring → tracks metrics from raw events
/// Layer 4: SLO        → ensures objectives based on SLI events
/// Layer 5: Debug      → observes all events
/// ```
/// 
/// ## Available Components
/// 
/// - **CircuitBreakerSLI**: Computes availability indicators
/// - **RetrySLI**: Computes success rate and retry efficiency
/// - **LatencySLI**: Computes latency percentiles and compliance
/// - **SLOTracker**: Tracks compliance with Service Level Objectives
/// 
/// ## Usage Example
/// 
/// ```rust,no_run
/// # use obzenflow_adapters::middleware::sli::{CircuitBreakerSLI, RetrySLI, LatencySLI};
/// # use obzenflow_adapters::middleware::{CircuitBreakerMiddleware, RetryMiddleware};
/// # use obzenflow_adapters::monitoring::taxonomies::red::RED;
/// # use std::time::Duration;
/// // Example middleware stack configuration:
/// // Layer 1: Operational
/// let circuit_breaker = CircuitBreakerMiddleware::new(10);
/// let retry = RetryMiddleware::exponential(3);
/// 
/// // Layer 2: SLI computation  
/// let cb_sli = CircuitBreakerSLI::factory();
/// let retry_sli = RetrySLI::factory();
/// let latency_sli = LatencySLI::factory_with_targets(
///     Duration::from_millis(50),   // P50 target
///     Duration::from_millis(100),  // P90 target  
///     Duration::from_millis(200),  // P95 target
///     Duration::from_millis(500),  // P99 target
/// );
/// 
/// // Layer 3: Monitoring
/// let monitoring = RED::monitoring();
/// ```

mod circuit_breaker_sli;
mod retry_sli;
mod latency_sli;
mod slo_tracker;

pub use circuit_breaker_sli::{CircuitBreakerSLI, CircuitBreakerSLIFactory};
pub use retry_sli::{RetrySLI, RetrySLIFactory};
pub use latency_sli::{LatencySLI, LatencySLIFactory};
pub use slo_tracker::{SLOTracker, SLOTrackerFactory, SLODefinition, AlertConfig};

// Re-export convenience factory methods
pub use self::{
    circuit_breaker_sli::CircuitBreakerSLI as CircuitBreakerSLIMiddleware,
    retry_sli::RetrySLI as RetrySLIMiddleware,
    latency_sli::LatencySLI as LatencySLIMiddleware,
    slo_tracker::SLOTracker as SLOTrackerMiddleware,
};
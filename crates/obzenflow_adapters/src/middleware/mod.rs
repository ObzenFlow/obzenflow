//! # Middleware System for FlowState
//!
//! This module provides a composable middleware system for adding cross-cutting
//! concerns like monitoring, logging, rate limiting, and retries to your FlowState
//! pipeline stages without modifying their core logic.
//!
//! ## Middleware Overview
//!
//! Middleware is composable - you can stack multiple middleware on a handler.
//! When used in flows (via the DSL layer), middleware is specified as an array
//! of middleware instances for each stage.
//!
//! The middleware execution order is:
//! 1. First middleware's `pre_handle`
//! 2. Second middleware's `pre_handle`
//! 3. Handler processes the event
//! 4. Second middleware's `post_handle`
//! 5. First middleware's `post_handle`
//!
//! ## Monitoring with FLOWIP-056-666
//!
//! With FLOWIP-056-666 Wide Events, monitoring is no longer implemented as middleware.
//! Instead, metrics are automatically derived from the event journal by MetricsAggregator.
//!
//! The monitoring taxonomies (RED, USE, Golden Signals, SAAFE) are now documentation-only
//! and provide Prometheus queries and Grafana dashboards for viewing metrics.
//!
//! ```rust
//! // OLD: Monitoring middleware (no longer available)
//! // let red_middleware = RED::monitoring();
//!
//! // NEW: Metrics are automatically collected from the journal
//! // See obzenflow_adapters::monitoring::aggregator::MetricsAggregator
//! ```
//!
//! ## Available Monitoring Views
//!
//! FlowState provides several monitoring taxonomies as view definitions:
//!
//! ### RED (Rate, Errors, Duration)
//! Best for request/response systems and sources. Tracks:
//! - **Rate**: Events processed per second
//! - **Errors**: Error count and rate
//! - **Duration**: Processing time distribution
//!
//! ```rust
//! use obzenflow_adapters::monitoring::taxonomies::red::RED;
//!
//! // Get Prometheus queries for RED metrics
//! let queries = RED::prometheus_queries("my_flow", "my_stage");
//! ```
//!
//! ### USE (Utilization, Saturation, Errors)
//! Ideal for resource-focused stages like transforms. Tracks:
//! - **Utilization**: Resource usage percentage
//! - **Saturation**: Queue depth and backpressure
//! - **Errors**: Processing errors
//!
//! ```rust
//! use obzenflow_adapters::monitoring::taxonomies::use_taxonomy::USE;
//!
//! // Get Prometheus queries for USE metrics
//! let queries = USE::prometheus_queries("my_flow", "my_stage");
//! ```
//!
//! ### GoldenSignals (Latency, Traffic, Errors, Saturation)
//! Comprehensive monitoring for critical stages. Tracks:
//! - **Latency**: End-to-end processing time
//! - **Traffic**: Request volume
//! - **Errors**: Error rate and types
//! - **Saturation**: Resource saturation
//!
//! ```rust
//! use obzenflow_adapters::monitoring::taxonomies::golden_signals::GoldenSignals;
//!
//! // Get Prometheus queries for Golden Signals metrics
//! let queries = GoldenSignals::prometheus_queries("my_flow", "my_stage");
//! ```
//!
//! ### SAAFE (Saturation, Anomalies, Amendments, Failures, Errors)
//! Advanced monitoring for sinks and data quality. Tracks:
//! - **Saturation**: Backpressure and queue depth
//! - **Anomalies**: Unusual patterns in data
//! - **Amendments**: Data corrections/updates
//! - **Failures**: Persistent failures
//! - **Errors**: Transient errors
//!
//! ```rust
//! use obzenflow_adapters::monitoring::taxonomies::saafe::SAAFE;
//!
//! // Get Prometheus queries for SAAFE metrics
//! let queries = SAAFE::prometheus_queries("my_flow", "my_stage");
//! ```
//!
//! ## Applying Middleware to Handlers
//!
//! Use the handler extension traits to apply middleware (see builder APIs for current syntax).
//!
//! ## Common Middleware Utilities
//!
//! The `common` module provides pre-built middleware for rate limiting, circuit breaking,
//! and logging; refer to the current control/observability modules for up-to-date builders.
//!
//! ## Custom Middleware
//!
//! You can also create custom middleware by implementing the `Middleware` trait:
//!
//! ```rust
//! use obzenflow_adapters::middleware::{Middleware, MiddlewareAction, MiddlewareContext};
//! use obzenflow_core::event::chain_event::ChainEvent;
//!
//! struct MyCustomMiddleware;
//!
//! impl Middleware for MyCustomMiddleware {
//!     fn pre_handle(&self, event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
//!         println!("Processing event: {:?}", event.id);
//!         MiddlewareAction::Continue
//!     }
//!     
//!     fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], _ctx: &mut MiddlewareContext) {
//!         println!("Produced {} results", results.len());
//!     }
//! }
//! ```

// Core types
mod middleware_factory;
mod middleware_safety;
mod middleware_trait;

// Handler-specific middleware adapters
mod join_middleware;
mod sink_middleware;
mod source_middleware;
mod stateful_middleware;
mod transform_middleware;
mod backpressure;

// Common middleware utilities
mod context;
mod function;
mod hints;

// Middleware categories
pub mod control;
pub mod observability;
pub mod state;
mod system;
// Dangerous middleware examples moved to examples/dangerous_examples.rs
// Factory tests moved to tests/factory_tests.rs
// Note: With FLOWIP-056-666, monitoring is no longer implemented as middleware.
// Metrics are automatically derived from the event journal by MetricsAggregator.
// Taxonomies now provide Prometheus queries and Grafana dashboards:
// - RED::prometheus_queries()
// - USE::prometheus_queries()
// - GoldenSignals::prometheus_queries()
// - SAAFE::prometheus_queries()

// Core trait exports
pub use middleware_factory::MiddlewareFactory;
pub use middleware_safety::MiddlewareSafety;
pub use middleware_trait::{ErrorAction, Middleware, MiddlewareAction};

// Handler-specific exports
pub use join_middleware::{JoinHandlerMiddlewareExt, JoinMiddlewareBuilder, MiddlewareJoin};
pub use sink_middleware::{MiddlewareSink, SinkHandlerExt, SinkMiddlewareBuilder};
pub use source_middleware::{
    AsyncFiniteSourceHandlerExt, AsyncFiniteSourceMiddlewareBuilder, AsyncInfiniteSourceHandlerExt,
    AsyncInfiniteSourceMiddlewareBuilder, FiniteSourceHandlerExt, FiniteSourceMiddlewareBuilder,
    InfiniteSourceHandlerExt, InfiniteSourceMiddlewareBuilder, MiddlewareAsyncFiniteSource,
    MiddlewareAsyncInfiniteSource, MiddlewareFiniteSource, MiddlewareInfiniteSource,
};
pub use stateful_middleware::{
    MiddlewareStateful, StatefulHandlerMiddlewareExt, StatefulMiddlewareBuilder,
};
pub use transform_middleware::{
    AsyncMiddlewareTransform, AsyncTransformHandlerExt, AsyncTransformMiddlewareBuilder,
    MiddlewareTransform, TransformHandlerExt, TransformMiddlewareBuilder,
};

// Common utilities
pub use context::{MiddlewareContext, MiddlewareEvent};
pub use function::{middleware_fn, FnMiddleware};
pub use hints::{Attempts, BackoffKind, BatchingHint, MiddlewareHints, RetryHint};
pub use observability::timing::TimingMiddleware;
pub use state::windowing::{WindowingMiddleware, WindowingMiddlewareFactory};

// Control middleware
pub use control::{
    circuit_breaker, rate_limit, rate_limit_with_burst, CircuitBreakerBuilder,
    CircuitBreakerMiddleware, RateLimiterFactory, RateLimiterMiddleware,
};

// Backpressure (config + topology observability; FLOWIP-086k)
pub use backpressure::{backpressure, BackpressureMiddlewareFactory};

// Re-export observability middleware for backward compatibility
pub use observability::{
    BoundaryConfig, BoundaryTrackingMiddleware, FlowBoundaryTracker, FlowMetrics,
    LoggingMiddleware, SystemEnrichmentMiddleware,
};

// System middleware exports
pub use system::{
    outcome_enrichment, validate_middleware_safety, OutcomeEnrichmentMiddleware, ValidationResult,
};
// Monitoring is provided via taxonomy-specific methods

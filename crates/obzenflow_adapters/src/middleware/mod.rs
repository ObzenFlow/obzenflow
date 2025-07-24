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
//! Use the handler extension traits to apply middleware:
//!
//! ```rust
//! use obzenflow_adapters::middleware::{TransformHandlerExt, LoggingMiddleware};
//! use obzenflow_runtime_services::stages::common::handlers::TransformHandler;
//! use obzenflow_core::event::chain_event::ChainEvent;
//!
//! struct MyTransform;
//!
//! impl TransformHandler for MyTransform {
//!     fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
//!         vec![event]
//!     }
//! }
//!
//! // Apply middleware directly (monitoring middleware is created via factories in descriptors)
//! let handler_with_middleware = MyTransform
//!     .middleware()
//!     .with(LoggingMiddleware::new())
//!     .build();
//! ```
//!
//! ## Common Middleware Utilities
//!
//! The `common` module provides pre-built middleware for common patterns:
//!
//! ```rust
//! use obzenflow_adapters::middleware::control::{rate_limit, RetryBuilder, RetryStrategy};
//! use obzenflow_adapters::middleware::observability::LoggingMiddleware;
//! use std::time::Duration;
//!
//! // Rate limiting - limits events per second
//! let rate_limiter = rate_limit(100.0);
//!
//! // Retry logic - uses exponential backoff
//! let retry_middleware = RetryBuilder::new()
//!     .strategy(RetryStrategy::Exponential { base: 2.0, max_delay: Duration::from_secs(60) })
//!     .max_attempts(3)
//!     .initial_delay(Duration::from_millis(100))
//!     .build();
//!
//! // Logging - logs events and results
//! let logging_middleware = LoggingMiddleware::new();
//! ```
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
mod middleware_trait;
mod middleware_factory;
mod middleware_safety;

// Handler-specific middleware adapters
mod transform_middleware;
mod source_middleware;
mod sink_middleware;

// Common middleware utilities
mod function;
mod hints;
mod context;

// Middleware categories
pub mod control;
pub mod state;
pub mod observability;
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
pub use middleware_trait::{Middleware, MiddlewareAction, ErrorAction};
pub use middleware_factory::MiddlewareFactory;
pub use middleware_safety::MiddlewareSafety;

// Handler-specific exports
pub use transform_middleware::{MiddlewareTransform, TransformHandlerExt, TransformMiddlewareBuilder};
pub use source_middleware::{
    MiddlewareFiniteSource, MiddlewareInfiniteSource,
    FiniteSourceHandlerExt, InfiniteSourceHandlerExt,
    FiniteSourceMiddlewareBuilder, InfiniteSourceMiddlewareBuilder
};
pub use sink_middleware::{MiddlewareSink, SinkHandlerExt, SinkMiddlewareBuilder};

// Common utilities
pub use function::{FnMiddleware, middleware_fn};
pub use context::{MiddlewareContext, MiddlewareEvent};
pub use hints::{MiddlewareHints, RetryHint, Attempts, BackoffKind, BatchingHint};
pub use state::windowing::{WindowingMiddleware, WindowingMiddlewareFactory};
pub use observability::timing::TimingMiddleware;

// Control middleware
pub use control::{
    CircuitBreakerMiddleware, CircuitBreakerBuilder, circuit_breaker,
    RateLimiterMiddleware, RateLimiterFactory, rate_limit, rate_limit_with_burst,
    RetryMiddleware, RetryBuilder, RetryStrategy,
};

// Re-export observability middleware for backward compatibility
pub use observability::{
    FlowBoundaryTracker, BoundaryTrackingMiddleware, BoundaryConfig, FlowMetrics,
    LoggingMiddleware, SystemEnrichmentMiddleware
};

// System middleware exports
pub use system::{
    OutcomeEnrichmentMiddleware, outcome_enrichment,
    validate_middleware_safety, ValidationResult
};
// Monitoring is provided via taxonomy-specific methods

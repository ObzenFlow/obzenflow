//! Observability middleware for monitoring and metrics
//!
//! This module contains middleware implementations that enhance the observability
//! of FlowState pipelines by adding timing, logging, and other instrumentation.

pub mod timing;
pub mod logging_middleware;
pub mod system_enrichment;
pub mod flow_boundary;

pub use timing::TimingMiddleware;
pub use logging_middleware::LoggingMiddleware;
pub use system_enrichment::{SystemEnrichmentMiddleware, SystemEnrichmentMiddlewareFactory, system_enrichment};
pub use flow_boundary::{FlowBoundaryTracker, BoundaryTrackingMiddleware, BoundaryConfig, FlowMetrics};
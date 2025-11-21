//! Observability middleware for monitoring and metrics
//!
//! This module contains middleware implementations that enhance the observability
//! of FlowState pipelines by adding timing, logging, and other instrumentation.

pub mod flow_boundary;
pub mod logging_middleware;
pub mod system_enrichment;
pub mod timing;

pub use flow_boundary::{
    BoundaryConfig, BoundaryTrackingMiddleware, FlowBoundaryTracker, FlowMetrics,
};
pub use logging_middleware::LoggingMiddleware;
pub use system_enrichment::{
    system_enrichment, SystemEnrichmentMiddleware, SystemEnrichmentMiddlewareFactory,
};
pub use timing::TimingMiddleware;

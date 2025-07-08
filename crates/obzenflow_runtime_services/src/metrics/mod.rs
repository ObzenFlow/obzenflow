//! Metrics module for ObzenFlow runtime services
//!
//! This module provides metrics configuration and collection capabilities
//! for monitoring ObzenFlow pipelines and stages.

pub mod default_config;
pub mod infra_observer;
pub mod aggregator;

pub use default_config::DefaultMetricsConfig;
pub use infra_observer::InfraMetricsObserver;
pub use aggregator::{MetricsAggregator, MetricsAggregatorFactory};
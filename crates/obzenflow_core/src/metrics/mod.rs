//! Metrics traits and types for the core domain
//!
//! This module defines abstract interfaces for metrics collection
//! and export, using the wide events approach from FLOWIP-056c.

pub mod observer;
pub mod exporter;
pub mod snapshots;
pub mod primitives;
pub use exporter::{MetricsExporter, NoOpMetricsExporter};
pub use snapshots::{
    AppMetricsSnapshot, InfraMetricsSnapshot, HistogramSnapshot,
    FlowMetricsSnapshot, JournalMetricsSnapshot, StageInfraMetrics
};
pub use primitives::{Counter, Gauge, Histogram};
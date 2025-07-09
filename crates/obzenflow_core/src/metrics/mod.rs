//! Metrics observation traits for the core domain
//!
//! This module defines the abstract interface for observing events
//! for metrics collection, following the Observer pattern.

pub mod observer;
pub mod exporter;
pub mod snapshots;
pub mod primitives;

pub use observer::{MetricsObserver, NoOpMetricsObserver};
pub use exporter::{MetricsExporter, NoOpMetricsExporter};
pub use snapshots::{
    AppMetricsSnapshot, InfraMetricsSnapshot, HistogramSnapshot,
    FlowMetricsSnapshot, JournalMetricsSnapshot, StageInfraMetrics
};
pub use primitives::{Counter, Gauge, Histogram};
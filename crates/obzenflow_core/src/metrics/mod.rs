// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics traits and types for the core domain
//!
//! This module defines abstract interfaces for metrics collection
//! and export, using the wide events approach from FLOWIP-056c.

pub mod exporter;
pub mod observer;
pub mod percentile;
pub mod primitives;
pub mod snapshots;

pub use exporter::{MetricsExporter, NoOpMetricsExporter};
pub use percentile::{Percentile, PercentileExt};
pub use primitives::{Counter, Gauge, Histogram};
pub use snapshots::{
    AppMetricsSnapshot, ContractMetricsSnapshot, FlowLifecycleMetricsSnapshot, FlowMetricsSnapshot,
    HistogramSnapshot, InfraMetricsSnapshot, JournalMetricsSnapshot, StageInfraMetrics,
    StageMetadata, StageMetricsSnapshot,
};

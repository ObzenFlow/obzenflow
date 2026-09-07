// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics traits and types for the core domain
//!
//! This module defines abstract interfaces for metrics collection
//! and export, using the wide events approach.

#[doc(hidden)]
pub mod composite;
pub mod observer;
pub mod percentile;
pub mod primitives;
pub mod snapshot_exporter;
pub mod snapshots;

pub use composite::{
    BoundaryDirection, CompositeContract, CompositeDurationBucket, CompositeDurationHistogram,
    CompositeDurationInvalid, CompositeMemberHealth, CompositePortTraffic,
};
#[doc(hidden)]
pub use composite::{
    BoundaryMetricsView, CompositeBoundary, CompositeBoundaryEdge, CompositeBoundaryPort,
    CompositeDurationAccumulator,
};
pub use percentile::{Percentile, PercentileExt};
pub use primitives::{Counter, Gauge, Histogram};
pub use snapshot_exporter::MetricsSnapshotExporter;
pub use snapshots::{
    AiChunkingMetricsSnapshot, AppMetricsSnapshot, ContractMetricEdgeKey, ContractMetricResultKey,
    ContractMetricViolationKey, ContractMetricsSnapshot, ContractViolationCauseLabel,
    FlowLifecycleMetricsSnapshot, FlowMetricsSnapshot, HistogramSnapshot, InfraMetricsSnapshot,
    JournalMetricsSnapshot, SinkOperationFailureMetric, StageInfraMetrics, StageMetadata,
    StageMetricsSnapshot,
};

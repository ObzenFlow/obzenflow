// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics aggregator implementation

pub mod builder;
pub mod config;
pub mod constants;
pub mod fsm;
pub mod handle;
pub mod inputs;
pub mod instrumentation;
pub mod supervisor;
pub mod tail_read;

// Re-export commonly used types
// Note: MetricsAggregatorSupervisor is intentionally NOT exported - use MetricsAggregatorBuilder
pub use builder::MetricsAggregatorBuilder;
pub use config::DefaultMetricsConfig;
pub use fsm::{
    MetricsAggregatorAction, MetricsAggregatorContext, MetricsAggregatorEvent,
    MetricsAggregatorState, MetricsStore, StageMetrics,
};
pub use handle::{MetricsHandle, MetricsHandleExt};
pub use inputs::MetricsInputs;

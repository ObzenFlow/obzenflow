// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Snapshot read models and deterministic monitoring projections.
//! Runtime aggregates application facts; Infra selects and operates delivery.
pub mod aggregator;
pub mod exporters;
pub mod metrics;
pub mod projections;
pub mod read_model;
pub use read_model::{MetricsReadModel, MetricsReadView, MetricsSubscription};

// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Snapshot read models and deterministic monitoring projections.
//! Runtime aggregates execution facts; Adapters retains and translates observations.
//! Infra assembles the application and hosts the Prometheus endpoint.
pub mod projections;
pub mod read_model;
pub use read_model::{MetricsReadModel, MetricsReadView};

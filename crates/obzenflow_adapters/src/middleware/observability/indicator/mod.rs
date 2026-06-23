// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Service-level indicator observer middleware (FLOWIP-115f).
//!
//! `indicator()` records one per-execution service-level-indicator *sample* as a
//! durable, journalled wide event (one `MiddlewareLifecycle::Indicator` row per
//! operation execution). A sample is the raw, observe-only input an SLI is
//! computed from: it carries the raw measured value and its identity only. The
//! objective (threshold) and good/bad evaluation are read-side, never embedded,
//! and it never steers control flow.
//!
//! Aggregation into ratios, percentiles, windows, and error budgets is the job
//! of FLOWIP-115l, which reads these rows; this module only publishes them.
//! `latency()` is the convenience constructor for the only implemented kind.

mod factory;
mod middleware;
mod observers;

pub use factory::{
    indicator, latency, IndicatorConfigError, IndicatorFamily, IndicatorMiddlewareFactory,
};
pub use middleware::{IndicatorConfig, IndicatorMiddleware};

// Re-export the journalled evidence types for authoring ergonomics, so a user
// can write `indicator().kind(IndicatorKind::Latency)` from the adapters crate.
pub use obzenflow_core::event::payloads::observability_payload::{
    IndicatorKind, IndicatorSample, IndicatorTag,
};

#[cfg(test)]
mod tests;

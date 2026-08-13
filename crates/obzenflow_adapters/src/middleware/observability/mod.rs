// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Observe-only middleware for quantitative execution measurements.
//!
//! This module contains observer implementations that enhance pipeline
//! observability without making control-flow decisions.

pub mod indicator;

pub use indicator::{indicator, latency, IndicatorKind, IndicatorMiddlewareFactory};

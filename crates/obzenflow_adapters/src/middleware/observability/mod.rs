// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Observe-only middleware for timing and logging.
//!
//! This module contains observer implementations that enhance pipeline
//! observability without making control-flow decisions.

pub mod indicator;
pub mod logging;

pub use indicator::{indicator, latency, IndicatorKind, IndicatorMiddlewareFactory};
pub use logging::{log, LoggingMiddleware, LoggingMiddlewareFactory};

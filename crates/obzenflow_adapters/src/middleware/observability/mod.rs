// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Observe-only middleware for timing and logging.
//!
//! This module contains observer implementations that enhance pipeline
//! observability without making control-flow decisions.

pub mod logging;
pub mod timing;

pub use logging::LoggingMiddleware;
pub use timing::{timing, TimingMiddleware, TimingMiddlewareFactory};

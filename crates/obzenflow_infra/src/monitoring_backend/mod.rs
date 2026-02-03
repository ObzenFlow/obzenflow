// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Monitoring backend implementations
//!
//! This module contains concrete monitoring backend implementations
//! like Prometheus, StatsD, etc. These are infrastructure concerns
//! that implement the monitoring traits from the adapters layer.

#[cfg(feature = "prometheus")]
pub mod prometheus;

// Future: statsd, datadog, etc.

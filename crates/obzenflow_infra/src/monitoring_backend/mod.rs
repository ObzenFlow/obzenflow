// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Monitoring backend implementations
//!
//! This module contains concrete monitoring backend implementations
//! such as Prometheus. These are infrastructure concerns
//! that implement the monitoring traits from the adapters layer.

#[cfg(feature = "prometheus")]
pub mod prometheus;

// Future backends should be added only when there is a concrete product need.

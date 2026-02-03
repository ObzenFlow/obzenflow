// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Time-related types for consistent duration handling
//!
//! This module provides types that ensure consistent time unit handling
//! throughout the system, preventing accidental unit conversion errors.

mod metrics_duration;

pub use metrics_duration::MetricsDuration;

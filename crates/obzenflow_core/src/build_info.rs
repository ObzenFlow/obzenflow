// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Build and version information.
//!
//! This module intentionally stays small and dependency-free so archive readers
//! can require exact build identity without pulling in extra crates.

/// The exact current ObzenFlow build version recorded in run manifests.
pub const OBZENFLOW_VERSION: &str = env!("CARGO_PKG_VERSION");

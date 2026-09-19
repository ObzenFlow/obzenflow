// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Build and version information.
//!
//! Package identity is archive provenance. Admission is governed by the
//! Core-owned journal schema version and required interpretation capabilities.

/// The current ObzenFlow package version recorded in run manifests.
pub const OBZENFLOW_VERSION: &str = env!("CARGO_PKG_VERSION");

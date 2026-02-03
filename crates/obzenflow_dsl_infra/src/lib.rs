// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! DSL and infrastructure layer for ObzenFlow
//!
//! This crate provides the high-level DSL (flow! macro) that serves as the primary
//! API for building ObzenFlow pipelines. It orchestrates across all other layers
//! to provide a declarative interface for flow construction.

pub mod dsl;
pub mod middleware_resolution;
pub mod prelude;
pub mod stage_handle_adapter;

// Re-export modules
pub use dsl::*;

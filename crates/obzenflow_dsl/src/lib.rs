// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![doc = include_str!("../README.md")]

pub mod dsl;
pub mod middleware_resolution;
pub mod prelude;
pub mod stage_handle_adapter;

// Re-export modules
pub use dsl::*;

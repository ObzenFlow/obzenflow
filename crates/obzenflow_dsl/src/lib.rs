// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![doc = include_str!("../README.md")]

pub mod dsl;
pub mod prelude;
pub mod stage_handle_adapter;

/// Implementation support resolved through the defining crate by exported macros.
#[doc(hidden)]
pub mod __private;

// Re-export modules
pub use dsl::*;

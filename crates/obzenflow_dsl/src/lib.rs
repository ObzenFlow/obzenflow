// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![doc = include_str!("../README.md")]

pub mod dsl;
pub mod middleware_resolution;
pub mod prelude;
pub mod stage_handle_adapter;

/// The `backpressure:` clause constructors (FLOWIP-115e): `enforced`,
/// `enforced_from_config`, `track_only`, `off`.
pub mod backpressure {
    pub use crate::dsl::backpressure_clause::{
        enforced, enforced_from_config, off, track_only, BackpressureClause,
    };
}

// Re-export modules
pub use dsl::*;

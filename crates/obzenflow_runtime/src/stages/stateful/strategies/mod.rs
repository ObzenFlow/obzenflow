// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful strategies
//!
//! Composable primitives for stateful event processing.
//! These are NOT handlers themselves but building blocks that compose to implement StatefulHandler.

pub mod accumulators;
pub mod emissions;

// Re-export for convenience
pub use accumulators::*;
pub use emissions::*;
